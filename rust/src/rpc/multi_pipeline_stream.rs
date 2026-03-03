//! Multi-pipeline stream.
//!
//! When a query requires more than one RPC pipeline (e.g. via `include_*`
//! flags), this module runs all needed pipelines over the **same block range**
//! in each batch, then merges the results into one [`ArrowResponse`] with all
//! four tables populated.
//!
//! Each pipeline has its own internal retry loop that accumulates data across
//! sub-ranges, so the response is only sent after the full batch range is
//! covered. This means `batch_size` directly controls the memory used per
//! response.

use std::collections::VecDeque;
use std::time::Duration;

use alloy::network::AnyRpcBlock;
use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::ClientConfig;

type LogFetchResult = (u64, u64, Result<Vec<alloy::rpc::types::Log>>);
type BlockFetchResult = (u64, u64, Result<Vec<AnyRpcBlock>>);

use super::arrow_convert::{
    blocks_to_record_batch, logs_to_record_batch, merge_tx_receipts_into_batch,
    select_block_columns, select_log_columns, select_trace_columns, select_transaction_columns,
    traces_to_record_batch, transactions_to_record_batch,
};
use super::shared_helpers::clamp_to_block;
use crate::query::{
    get_blocks_needs_full_txs, get_trace_method, needs_tx_receipts, LogRequest, Pipelines, Query,
};
use crate::response::ArrowResponse;

use super::block_adaptive_concurrency::{retry_block_with_block_range, BLOCK_ADAPTIVE_CONCURRENCY};
use super::block_fetcher::fetch_blocks;
use super::log_adaptive_concurrency::{retry_logs_with_block_range, LOG_ADAPTIVE_CONCURRENCY};
use super::log_fetcher::fetch_logs;
use super::provider::RpcProvider;
use super::shared_helpers::{halved_block_range, is_fatal_error, is_rate_limit_error};
use super::trace_fetcher::fetch_traces;
use super::tx_receipt_fetcher::fetch_tx_receipts;

/// Spawn a coordinated producer task that runs and all
/// pipelines requested via `include_*` flags, merging results per
/// batch into a single [`ArrowResponse`].
///
/// Returns the receiving end of the channel.
pub fn start_coordinated_stream(
    provider: RpcProvider,
    query: Query,
    config: ClientConfig,
    pipelines: Pipelines,
) -> mpsc::Receiver<Result<ArrowResponse>> {
    let (tx, rx) = mpsc::channel(config.buffer_size);

    tokio::spawn(async move {
        if let Err(e) = run_coordinated_stream(provider, query, config, pipelines, tx.clone()).await
        {
            error!("Coordinated stream terminated with error: {e:#}");
            let _ = tx.send(Err(e)).await;
        }
    });

    rx
}

async fn run_coordinated_stream(
    provider: RpcProvider,
    query: Query,
    config: ClientConfig,
    pipelines: Pipelines,
    tx: mpsc::Sender<Result<ArrowResponse>>,
) -> Result<()> {
    let from_block = query.from_block;

    let snapshot_latest_block = match query.to_block {
        Some(to) => to,
        None => provider
            .get_block_number()
            .await
            .context("failed to get current block number for snapshot_latest_block")?
            .saturating_sub(config.reorg_safe_distance),
    };

    let next_block = run_coordinated_historical(
        &provider,
        &query,
        pipelines,
        from_block,
        snapshot_latest_block,
        &config,
        &tx,
    )
    .await?;

    info!("Finished historical coordinated indexing up to block {snapshot_latest_block}");

    if !config.stop_on_head {
        run_coordinated_live(&provider, &query, pipelines, next_block, &config, &tx).await?;
    }

    Ok(())
}

/// Historical phase: iterate `[from_block, snapshot_latest_block]` in chunks,
/// fetching all required pipelines per chunk.
///
/// Each pipeline inside `fetch_all` has its own block-range retry loop, so
/// errors that reach this level are fatal and propagated immediately.
async fn run_coordinated_historical(
    provider: &RpcProvider,
    query: &Query,
    pipelines: Pipelines,
    start_from: u64,
    snapshot_latest_block: u64,
    config: &ClientConfig,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<u64> {
    let max_block_range = config.batch_size.map(|b| b as u64);
    let mut from_block = start_from;

    while from_block <= snapshot_latest_block {
        let to_block = clamp_to_block(from_block, snapshot_latest_block, max_block_range);

        debug!("Coordinated fetch for blocks {from_block}..{to_block}");

        let response = fetch_all(provider, query, pipelines, from_block, to_block, config).await?;

        info!(
            "Coordinated batch: {} blocks ({from_block}-{to_block}), \
             {} block rows, {} tx rows, {} log rows, {} trace rows",
            to_block - from_block + 1,
            response.blocks.num_rows(),
            response.transactions.num_rows(),
            response.logs.num_rows(),
            response.traces.num_rows(),
        );

        if tx.send(Ok(response)).await.is_err() {
            debug!("Stream consumer dropped, stopping historical coordinated fetch");
            return Ok(from_block);
        }

        from_block = to_block + 1;
    }

    Ok(from_block)
}

/// Live phase: poll for new blocks and run all pipelines.
async fn run_coordinated_live(
    provider: &RpcProvider,
    query: &Query,
    pipelines: Pipelines,
    start_from: u64,
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
                error!(
                    "Failed to get latest block number: {e:#}, retrying in {}ms",
                    config.retry_backoff_ms
                );
                tokio::time::sleep(Duration::from_millis(config.retry_backoff_ms)).await;
                continue;
            }
        };

        let safe_head = head.saturating_sub(config.reorg_safe_distance);

        if from_block > safe_head {
            debug!("At head (from_block={from_block}, safe_head={safe_head}), waiting...");
            continue;
        }

        let to_block = safe_head;

        let response = fetch_all(provider, query, pipelines, from_block, to_block, config).await?;

        let has_data = response.blocks.num_rows() > 0
            || response.transactions.num_rows() > 0
            || response.logs.num_rows() > 0
            || response.traces.num_rows() > 0;

        if has_data {
            info!(
                "Live coordinated: {} block rows, {} tx rows, {} log rows, \
                 {} trace rows for blocks {from_block}-{to_block}",
                response.blocks.num_rows(),
                response.transactions.num_rows(),
                response.logs.num_rows(),
                response.traces.num_rows(),
            );
        }

        if tx.send(Ok(response)).await.is_err() {
            debug!("Stream consumer dropped, stopping live coordinated polling");
            return Ok(());
        }

        from_block = to_block + 1;
    }
}

/// Run all required pipelines for a single block range and merge results.
///
/// Which pipelines to run is determined entirely by the `Pipelines` struct,
/// which includes both the pipelines requested via `include_*` flags and
/// those implied by the query's own request types.
async fn fetch_all(
    provider: &RpcProvider,
    query: &Query,
    pipelines: Pipelines,
    from_block: u64,
    to_block: u64,
    config: &ClientConfig,
) -> Result<ArrowResponse> {
    // --- Block pipeline (blocks + transactions) ---
    let (blocks_batch, txs_batch) = if pipelines.blocks_transactions {
        let include_txs = get_blocks_needs_full_txs(query);
        let fetch_receipts_flag = needs_tx_receipts(query);
        let block_fields = &query.fields.block;
        let tx_fields = &query.fields.transaction;

        let blocks = fetch_blocks_concurrent(provider, from_block, to_block, include_txs).await?;
        let blocks_batch = select_block_columns(blocks_to_record_batch(&blocks), block_fields);
        let raw_txs_batch = transactions_to_record_batch(&blocks);

        let merged_txs = if fetch_receipts_flag && raw_txs_batch.num_rows() > 0 {
            let receipts = fetch_tx_receipts(provider, from_block, to_block, config).await?;
            merge_tx_receipts_into_batch(raw_txs_batch, &receipts)
        } else {
            raw_txs_batch
        };

        let txs_batch = select_transaction_columns(merged_txs, tx_fields);

        (blocks_batch, txs_batch)
    } else {
        (
            ArrowResponse::empty().blocks,
            ArrowResponse::empty().transactions,
        )
    };

    // --- Log pipeline ---
    let logs_batch = if pipelines.logs {
        let log_requests = vec![LogRequest::default()];
        let logs = fetch_logs_concurrent(provider, &log_requests, from_block, to_block).await?;
        select_log_columns(logs_to_record_batch(&logs), &query.fields.log)
    } else {
        ArrowResponse::empty().logs
    };

    // --- Trace pipeline ---
    let traces_batch = if pipelines.traces {
        let trace_method = get_trace_method(query);
        let traces = fetch_traces(provider, from_block, to_block, trace_method, config).await?;
        select_trace_columns(traces_to_record_batch(&traces), &query.fields.trace)
    } else {
        ArrowResponse::empty().traces
    };

    Ok(ArrowResponse {
        blocks: blocks_batch,
        transactions: txs_batch,
        logs: logs_batch,
        traces: traces_batch,
    })
}

/// Fetch logs for `[from_block, to_block]` using concurrent JoinSet tasks,
/// automatically splitting into sub-chunks on block-range errors and
/// accumulating all results before returning.
///
/// Returns collected logs sorted by `(block_number, log_index)`. Unlike
/// `run_log_historical` in `log_fetcher`, this accumulates raw logs in memory
/// instead of streaming Arrow responses, because the coordinated pipeline needs
/// to merge them with blocks/traces for the same range before sending.
async fn fetch_logs_concurrent(
    provider: &RpcProvider,
    requests: &[LogRequest],
    from_block: u64,
    to_block: u64,
) -> Result<Vec<alloy::rpc::types::Log>> {
    if from_block > to_block {
        return Ok(Vec::new());
    }

    let cancel = CancellationToken::new();
    let mut join_set: JoinSet<LogFetchResult> = JoinSet::new();
    let mut retry_queue: VecDeque<(u64, u64)> = VecDeque::new();
    let mut all_logs: Vec<alloy::rpc::types::Log> = Vec::new();

    let mut cursor = from_block;

    loop {
        let max_concurrent = LOG_ADAPTIVE_CONCURRENCY.current();
        while join_set.len() < max_concurrent {
            let (chunk_from, chunk_to) = if let Some((rf, rt)) = retry_queue.pop_front() {
                (rf, rt)
            } else if cursor <= to_block {
                let current_chunk = LOG_ADAPTIVE_CONCURRENCY.chunk_size();
                let from = cursor;
                let to = (from + current_chunk - 1).min(to_block);
                cursor = to + 1;
                (from, to)
            } else {
                break;
            };

            let task_provider = provider.clone();
            let task_cancel = cancel.clone();
            let task_requests: Vec<LogRequest> = requests.to_vec();

            join_set.spawn(async move {
                if task_cancel.is_cancelled() {
                    return (chunk_from, chunk_to, Err(anyhow::anyhow!("cancelled")));
                }

                LOG_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;

                let result = fetch_logs(&task_provider, &task_requests, chunk_from, chunk_to).await;
                (chunk_from, chunk_to, result)
            });
        }

        if join_set.is_empty() {
            break;
        }

        let Some(join_result) = join_set.join_next().await else {
            break;
        };

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
            Ok(logs) => {
                LOG_ADAPTIVE_CONCURRENCY.record_success();
                all_logs.extend(logs);
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

                // 2. Rate-limit errors — reduce concurrency, backoff, re-queue.
                if is_rate_limit_error(&err_str) {
                    LOG_ADAPTIVE_CONCURRENCY.record_rate_limit();
                    LOG_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;
                    retry_queue.push_back((chunk_from, chunk_to));
                    continue;
                }

                // 3. Block-range errors — shrink chunk, re-queue.
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

                // 4. Unknown error — halve range and re-queue.
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

    // Sort by (block_number, log_index) for deterministic ordering.
    all_logs.sort_by(|a, b| {
        let block_cmp = a.block_number.cmp(&b.block_number);
        if block_cmp != std::cmp::Ordering::Equal {
            return block_cmp;
        }
        a.log_index.cmp(&b.log_index)
    });

    Ok(all_logs)
}

/// Fetch blocks for `[from_block, to_block]` using concurrent tasks bounded by
/// `BLOCK_ADAPTIVE_CONCURRENCY`, with adaptive chunk sizing and retry queues.
///
/// Returns collected blocks sorted by block number. Unlike `run_block_historical`
/// in `block_fetcher`, this accumulates raw blocks in memory instead of streaming
/// Arrow responses, because the coordinated pipeline needs to merge blocks with
/// logs/traces for the same range before sending.
async fn fetch_blocks_concurrent(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    include_txs: bool,
) -> Result<Vec<AnyRpcBlock>> {
    if from_block > to_block {
        return Ok(Vec::new());
    }

    let cancel = CancellationToken::new();
    let mut join_set: JoinSet<BlockFetchResult> = JoinSet::new();
    let mut retry_queue: VecDeque<(u64, u64)> = VecDeque::new();
    let mut all_blocks: Vec<AnyRpcBlock> = Vec::new();

    let mut cursor = from_block;

    loop {
        // Fill join_set up to the current concurrency limit.
        let max_concurrent = BLOCK_ADAPTIVE_CONCURRENCY.current();
        while join_set.len() < max_concurrent {
            let (chunk_from, chunk_to) = if let Some((rf, rt)) = retry_queue.pop_front() {
                (rf, rt)
            } else if cursor <= to_block {
                let current_chunk = BLOCK_ADAPTIVE_CONCURRENCY.chunk_size();
                let from = cursor;
                let to = (from + current_chunk - 1).min(to_block);
                cursor = to + 1;
                (from, to)
            } else {
                break;
            };

            let task_provider = provider.clone();
            let task_cancel = cancel.clone();

            join_set.spawn(async move {
                if task_cancel.is_cancelled() {
                    return (chunk_from, chunk_to, Err(anyhow::anyhow!("cancelled")));
                }

                BLOCK_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;

                let result = fetch_blocks(&task_provider, chunk_from, chunk_to, include_txs).await;
                (chunk_from, chunk_to, result)
            });
        }

        if join_set.is_empty() {
            break;
        }

        let Some(join_result) = join_set.join_next().await else {
            break;
        };

        let (chunk_from, chunk_to, task_result) = match join_result {
            Ok(tuple) => tuple,
            Err(join_err) => {
                error!("Block fetch task panicked: {join_err}");
                cancel.cancel();
                while join_set.join_next().await.is_some() {}
                return Err(anyhow::anyhow!("block fetch task panicked: {join_err}"));
            }
        };

        match task_result {
            Ok(blocks) => {
                BLOCK_ADAPTIVE_CONCURRENCY.record_success();
                all_blocks.extend(blocks);
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
                    BLOCK_ADAPTIVE_CONCURRENCY.record_rate_limit();
                    BLOCK_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;
                    retry_queue.push_back((chunk_from, chunk_to));
                    continue;
                }

                // 3. Retryable block-range errors — shrink chunk only.
                let current_range = Some(BLOCK_ADAPTIVE_CONCURRENCY.chunk_size());
                if let Some(retry) =
                    retry_block_with_block_range(&err_str, chunk_from, chunk_to, current_range)
                {
                    warn!(
                        "Block range error, re-queuing {}-{} (was {chunk_from}-{chunk_to})",
                        retry.from, retry.to
                    );
                    if let Some(new_max) = retry.max_block_range {
                        BLOCK_ADAPTIVE_CONCURRENCY.reduce_chunk_size(new_max);
                    }
                    if retry.backoff {
                        BLOCK_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;
                    }
                    retry_queue.push_back((retry.from, retry.to));
                    if retry.to < chunk_to {
                        retry_queue.push_back((retry.to + 1, chunk_to));
                    }
                    continue;
                }

                // 4. Unknown error — reduce concurrency, halve range and re-queue.
                BLOCK_ADAPTIVE_CONCURRENCY.record_error();
                let halved = halved_block_range(chunk_from, chunk_to);
                error!(
                    "Unexpected error fetching blocks {chunk_from}-{chunk_to}, \
                     re-queuing {chunk_from}-{halved}: {err_str}"
                );
                let new_range = halved.saturating_sub(chunk_from);
                BLOCK_ADAPTIVE_CONCURRENCY.reduce_chunk_size(new_range);
                retry_queue.push_back((chunk_from, halved));
                if halved < chunk_to {
                    retry_queue.push_back((halved + 1, chunk_to));
                }
            }
        }
    }

    // Sort by block number since JoinSet results arrive out of order.
    all_blocks.sort_by_key(|b| b.header.number);

    Ok(all_blocks)
}
