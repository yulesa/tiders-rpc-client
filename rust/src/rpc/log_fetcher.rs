//! Log fetching: filter construction, address batching, and `eth_getLogs` calls.
//!
//! Contains:
//! - `run_log_historical()` — main loop for historical phase, using `JoinSet` to fetch log chunks in parallel and send responses as they complete.
//! - `run_log_live()` — main loop for live phase, polling for new blocks and fetching logs sequentially.
//! - `fetch_logs()` — fetch logs for a block range across multiple `LogRequest`s, merging and sorting results.
//! - `fetch_logs_for_request()` — fetch logs for a single `LogRequest`, slipting large addresses filters if needed.
//! - `build_filter()` — construct an alloy `Filter` from a `LogRequest` and block range.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use alloy::{
    primitives::{Address as AlloyAddress, B256},
    rpc::types::{Filter, Log, ValueOrArray},
};
use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::log_adaptive_concurrency::{retry_logs_with_block_range, LOG_ADAPTIVE_CONCURRENCY, DEFAULT_LOG_CHUNK_SIZE};
use super::shared_helpers::{halved_block_range, is_fatal_error, is_rate_limit_error};
use crate::config::ClientConfig;
use super::arrow_convert::{logs_to_record_batch, select_log_columns};
use crate::query::{LogFields, LogRequest};
use crate::response::ArrowResponse;

use super::provider::RpcProvider;

/// Maximum number of addresses per `eth_getLogs` call.
/// RPC providers typically cap at ~1000.
const MAX_ADDRESSES_PER_REQUEST: usize = 1000;

/// Historical phase for the log pipeline: iterate `[from_block, snapshot_latest_block]`
/// in parallel chunks, bounded by `LOG_ADAPTIVE_CONCURRENCY`.
///
/// Spawns multiple concurrent fetch tasks via `JoinSet`. Each task fetches a
/// chunk of logs, converts to Arrow, and returns the response. Results are sent
/// to the channel as they complete (no ordering guarantee).
pub(super) async fn run_log_historical(
    provider: &RpcProvider,
    log_requests: &[LogRequest],
    start_from: u64,
    snapshot_latest_block: u64,
    log_fields: LogFields,
    config: &ClientConfig,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<u64> {
    let original_max_range = config
        .batch_size
        .map(|b| (b as u64).min(DEFAULT_LOG_CHUNK_SIZE))
        .unwrap_or(DEFAULT_LOG_CHUNK_SIZE);

    // Set the original chunk size from config (persisted globally).
    LOG_ADAPTIVE_CONCURRENCY.set_original_chunk_size(original_max_range);

    let cancel = CancellationToken::new();
    let mut join_set: JoinSet<(u64, u64, Result<ArrowResponse>)> = JoinSet::new();
    let mut retry_queue: VecDeque<(u64, u64)> = VecDeque::new();

    // Next unassigned from_block.
    let mut cursor = start_from;

    loop {
        // Fill join_set up to the current concurrency limit.
        let max_concurrent = LOG_ADAPTIVE_CONCURRENCY.current();
        while join_set.len() < max_concurrent {
            let (chunk_from, chunk_to) = if let Some((rf, rt)) = retry_queue.pop_front() {
                // Priority: retry failed ranges first.
                (rf, rt)
            } else if cursor <= snapshot_latest_block {
                // Claim next chunk from cursor.
                let current_chunk = LOG_ADAPTIVE_CONCURRENCY.chunk_size();
                let from = cursor;
                let to = (from + current_chunk - 1).min(snapshot_latest_block);
                cursor = to + 1;
                (from, to)
            } else {
                // No more work to assign.
                break;
            };

            // Spawn a fetch task.
            let task_provider = provider.clone();
            let task_cancel = cancel.clone();
            let task_log_requests: Vec<LogRequest> = log_requests.to_vec();

            join_set.spawn(async move {
                if task_cancel.is_cancelled() {
                    return (chunk_from, chunk_to, Err(anyhow::anyhow!("cancelled")));
                }

                LOG_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;

                let result = fetch_logs(&task_provider, &task_log_requests, chunk_from, chunk_to)
                    .await
                    .map(|logs| {
                        let logs_batch = select_log_columns(logs_to_record_batch(&logs), &log_fields);
                        ArrowResponse::with_logs(logs_batch)
                    });

                (chunk_from, chunk_to, result)
            });
        }

        // If no tasks in flight and nothing to assign, we're done.
        if join_set.is_empty() {
            break;
        }

        // Await next completion.
        let Some(join_result) = join_set.join_next().await else {
            break;
        };

        // Handle JoinError (task panic / cancellation).
        let (chunk_from, chunk_to, task_result) = match join_result {
            Ok(tuple) => tuple,
            Err(join_err) => {
                error!("Log fetch task panicked: {join_err}");
                cancel.cancel();
                while join_set.join_next().await.is_some() {}
                return Err(anyhow::anyhow!("log fetch task panicked: {join_err}"));
            }
        };

        match task_result {
            Ok(response) => {
                LOG_ADAPTIVE_CONCURRENCY.record_success();

                if tx.send(Ok(response)).await.is_err() {
                    debug!("Stream consumer dropped, stopping historical log fetch");
                    cancel.cancel();
                    while join_set.join_next().await.is_some() {}
                    return Ok(cursor);
                }

                LOG_ADAPTIVE_CONCURRENCY.maybe_reset_chunk_size();
            }
            Err(e) => {
                let err_str = format!("{e:#}");

                // 1. Fatal errors — cancel everything and return.
                if is_fatal_error(&err_str) {
                    cancel.cancel();
                    while join_set.join_next().await.is_some() {}
                    return Err(e);
                }

                // Cancelled tasks — just drop them.
                if cancel.is_cancelled() {
                    continue;
                }

                // 2. Rate-limit errors — reduce concurrency and backoff, re-queue.
                if is_rate_limit_error(&err_str) {
                    LOG_ADAPTIVE_CONCURRENCY.record_rate_limit();
                    LOG_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;
                    retry_queue.push_back((chunk_from, chunk_to));
                    continue;
                }

                // 3. Block-range errors — provider-specific parsing, shrink chunk, re-queue.
                let current_range = Some(LOG_ADAPTIVE_CONCURRENCY.chunk_size());
                if let Some(retry) =
                    retry_logs_with_block_range(&err_str, chunk_from, chunk_to, current_range)
                {
                    warn!(
                        "Log range error, re-queuing {}-{} (was {chunk_from}-{chunk_to})",
                        retry.from, retry.to
                    );
                    if let Some(new_max) = retry.max_block_range {
                        LOG_ADAPTIVE_CONCURRENCY.reduce_chunk_size(new_max);
                    }
                    if retry.backoff {
                        LOG_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;
                    }
                    retry_queue.push_back((retry.from, retry.to));
                    if retry.to < chunk_to {
                        retry_queue.push_back((retry.to + 1, chunk_to));
                    }
                    continue;
                }

                // 4. Unknown error — reduce concurrency, halve range and re-queue.
                LOG_ADAPTIVE_CONCURRENCY.record_error();
                let halved = halved_block_range(chunk_from, chunk_to);
                error!(
                    "Unexpected error fetching logs {chunk_from}-{chunk_to}, \
                     re-queuing {chunk_from}-{halved}: {err_str}"
                );
                let new_range = halved.saturating_sub(chunk_from);
                LOG_ADAPTIVE_CONCURRENCY.reduce_chunk_size(new_range);
                retry_queue.push_back((chunk_from, halved));
                if halved < chunk_to {
                    retry_queue.push_back((halved + 1, chunk_to));
                }
            }
        }
    }

    // Return cursor past the snapshot so the live phase starts correctly.
    Ok(snapshot_latest_block + 1)
}

/// Live phase for the log pipeline: poll for new blocks and fetch logs.
pub(super) async fn run_log_live(
    provider: &RpcProvider,
    log_requests: &[LogRequest],
    start_from: u64,
    log_fields: LogFields,
    config: &ClientConfig,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<()> {
    let poll_interval = Duration::from_millis(config.head_poll_interval_millis);
    let mut from_block = start_from;

    loop {
        tokio::time::sleep(poll_interval).await;

        let head = match provider.get_block_number().await {
            Ok(h) => h,
            Err(e) => {
                error!("Failed to get latest block number: {e:#}, retrying in {}ms", config.retry_backoff_ms);
                tokio::time::sleep(Duration::from_millis(config.retry_backoff_ms)).await;
                continue;
            }
        };

        // Apply reorg-safe distance.
        let safe_head = head.saturating_sub(config.reorg_safe_distance);

        if from_block > safe_head {
            debug!("At head (from_block={from_block}, safe_head={safe_head}), waiting...");
            continue;
        }

        let to_block = safe_head;

        match fetch_logs(provider, log_requests, from_block, to_block).await {
            Ok(logs) => {
                let logs_len = logs.len();

                if logs_len > 0 {
                    info!("Live: fetched {logs_len} logs between blocks {from_block}-{to_block}");
                }

                let logs_batch = select_log_columns(logs_to_record_batch(&logs), &log_fields);
                let response = ArrowResponse::with_logs(logs_batch);

                if tx.send(Ok(response)).await.is_err() {
                    debug!("Stream consumer dropped, stopping live log polling");
                    return Ok(());
                }

                from_block = to_block + 1;
            }
            Err(e) => {
                let err_str = format!("{e:#}");

                if is_fatal_error(&err_str) {
                    return Err(e);
                }

                if is_rate_limit_error(&err_str) {
                    LOG_ADAPTIVE_CONCURRENCY.record_rate_limit();
                    LOG_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;
                    continue;
                }

                if let Some(retry) = retry_logs_with_block_range(&err_str, from_block, to_block, None) {
                    debug!(
                        "Live: block range error, will retry with {}-{}",
                        retry.from, retry.to
                    );
                    // Don't advance — the next iteration will clamp naturally.
                } else {
                    error!(
                        "Live: unexpected error fetching logs {from_block}-{to_block}: {err_str}, retrying in {}ms",
                        config.retry_backoff_ms
                    );
                }
            }
        }
    }
}

/// Fetch logs for all `LogRequest`s in a query for a given block range.
///
/// Multiple `LogRequest`s are OR'd together — each produces a separate
/// `eth_getLogs` call and results are merged, then sorted by
/// `(block_number, log_index)` for deterministic ordering.
pub async fn fetch_logs(
    provider: &RpcProvider,
    requests: &[LogRequest],
    from_block: u64,
    to_block: u64,
) -> Result<Vec<Log>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }

    info!(
        "fetch_logs: requesting logs for blocks ({from_block}..={to_block})",
    );

    let mut all_logs = Vec::new();
    for req in requests {
        let logs = fetch_logs_for_request(provider, req, from_block, to_block).await?;
        all_logs.extend(logs);
    }

    // Sort by (block_number, log_index) for deterministic ordering.
    all_logs.sort_by(|a, b| {
        let block_cmp = a.block_number.cmp(&b.block_number);
        if block_cmp != std::cmp::Ordering::Equal {
            return block_cmp;
        }
        a.log_index.cmp(&b.log_index)
    });

    info!(
        "fetch_logs: received {} logs for range {from_block}..={to_block}",
        all_logs.len()
    );

    Ok(all_logs)
}

/// Fetch logs for a single `LogRequest` in a given block range.
///
/// If the request has many addresses, they are chunked into groups of
/// `MAX_ADDRESSES_PER_REQUEST` and fetched in parallel (adapted from
/// rindexer's `get_logs_for_address_in_batches()`).
pub async fn fetch_logs_for_request(
    provider: &RpcProvider,
    req: &LogRequest,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<Log>> {
    // If few or no addresses, just do a single request.
    if req.address.len() <= MAX_ADDRESSES_PER_REQUEST {
        let filter = build_filter(req, from_block, to_block);
        return provider.get_logs(&filter).await;
    }

    // Build a base filter without addresses — addresses are added per chunk.
    let no_addr_req = LogRequest {
        address: vec![],
        ..req.clone()
    };
    let base_filter = build_filter(&no_addr_req, from_block, to_block);

    // Chunk addresses and spawn parallel fetches bounded by LOG_ADAPTIVE_CONCURRENCY.
    let semaphore = Arc::new(Semaphore::new(LOG_ADAPTIVE_CONCURRENCY.current()));

    let mut handles = Vec::new();
    for chunk in req.address.chunks(MAX_ADDRESSES_PER_REQUEST) {
        let addr_chunk: Vec<AlloyAddress> = chunk.iter().map(|a| AlloyAddress::from(a.0)).collect();
        let filter = base_filter.clone().address(ValueOrArray::Array(addr_chunk));
        let provider_clone = provider.clone();
        let sem = semaphore.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem
                .acquire_owned()
                .await
                .map_err(|e| anyhow::anyhow!("semaphore closed: {e}"))?;

            provider_clone.get_logs(&filter).await
        }));
    }

    let mut all_logs = Vec::new();
    for handle in handles {
        let logs = handle.await.context("address batch task panicked")??;
        all_logs.extend(logs);
    }

    Ok(all_logs)
}

/// Build an alloy `Filter` from a single `LogRequest` and block range.
///
/// Each `LogRequest` can specify multiple addresses (OR'd by the provider)
/// and multiple values per topic slot (also OR'd by the provider).
pub fn build_filter(req: &LogRequest, from_block: u64, to_block: u64) -> Filter {
    let mut filter = Filter::new().from_block(from_block).to_block(to_block);

    if !req.address.is_empty() {
        let addrs: Vec<AlloyAddress> = req
            .address
            .iter()
            .map(|a| AlloyAddress::from(a.0))
            .collect();
        filter = filter.address(ValueOrArray::Array(addrs));
    }

    if !req.topic0.is_empty() {
        let topics: Vec<B256> = req.topic0.iter().map(|t| B256::from(t.0)).collect();
        filter = filter.event_signature(ValueOrArray::Array(topics));
    }

    if !req.topic1.is_empty() {
        let topics: Vec<B256> = req.topic1.iter().map(|t| B256::from(t.0)).collect();
        filter = filter.topic1(ValueOrArray::Array(topics));
    }

    if !req.topic2.is_empty() {
        let topics: Vec<B256> = req.topic2.iter().map(|t| B256::from(t.0)).collect();
        filter = filter.topic2(ValueOrArray::Array(topics));
    }

    if !req.topic3.is_empty() {
        let topics: Vec<B256> = req.topic3.iter().map(|t| B256::from(t.0)).collect();
        filter = filter.topic3(ValueOrArray::Array(topics));
    }

    filter
}

#[cfg(test)]
#[expect(clippy::panic)]
mod tests {
    use super::*;
    use alloy::network::AnyNetwork;
    use alloy::primitives::{address, b256};
    use alloy::providers::{Provider, ProviderBuilder};

    /// Smoke test: call eth_getLogs directly via a plain alloy provider
    /// (no retry layer) to verify the RPC + filter return logs.
    #[tokio::test]
    async fn raw_alloy_get_logs() {
        let url: url::Url = "https://mainnet.gateway.tenderly.co"
            .parse()
            .unwrap_or_else(|e| panic!("bad url: {e}"));
        let provider = ProviderBuilder::new()
            .network::<AnyNetwork>()
            .connect_http(url);

        let addr = address!("ae78736cd615f374d3085123a210448e74fc6393");
        let topic0 = b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

        let filter = Filter::new()
            .from_block(18_600_000u64)
            .to_block(18_601_000u64)
            .address(ValueOrArray::Value(addr))
            .event_signature(ValueOrArray::Value(topic0));

        let logs = provider
            .get_logs(&filter)
            .await
            .unwrap_or_else(|e| panic!("eth_getLogs failed: {e}"));

        assert!(
            !logs.is_empty(),
            "expected rETH Transfer logs in blocks 18_600_000..18_601_000"
        );
    }
}
