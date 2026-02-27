//! Streaming pipeline: historical block-range iteration → live head polling.
//!
//! Adapted from rindexer's `fetch_logs_stream()` (fetch_logs.rs:30-149) and
//! `live_indexing_stream()` (fetch_logs.rs:391-711).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use rand::Rng;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::ClientConfig;
use crate::convert::{
    logs_to_record_batch, select_log_columns,
    select_trace_columns, traces_to_record_batch,
};
use crate::query::{
    get_blocks_needs_full_txs, get_trace_method, needs_tx_receipts, LogFields,
    LogRequest, Query, TraceFields, TraceMethod,
};
use crate::response::ArrowResponse;

use super::log_adaptive_concurrency::{
    retry_logs_with_block_range, LOG_ADAPTIVE_CONCURRENCY,
};
use super::shared_helpers::{clamp_to_block, halved_block_range, is_fatal_error, is_rate_limit_error};
use super::block_fetcher::{run_block_historical, run_block_live};
use super::log_fetcher::{fetch_logs, DEFAULT_LOG_CHUNK_SIZE};
use super::provider::RpcProvider;
use super::trace_fetcher::fetch_traces;

// ===========================================================================
// Log stream (eth_getLogs pipeline)
// ===========================================================================

/// Spawn a producer task that fetches logs in block-range batches and sends
/// `ArrowResponse` chunks through a bounded channel.
///
/// Returns the receiving end of the channel.
pub fn start_log_stream(
    provider: RpcProvider,
    query: Query,
    config: ClientConfig,
) -> mpsc::Receiver<Result<ArrowResponse>> {
    let (tx, rx) = mpsc::channel(config.buffer_size);

    tokio::spawn(async move {
        if let Err(e) = run_log_stream(provider, query, config, tx.clone()).await {
            error!("Log stream terminated with error: {e:#}");
            // Send the error into the channel so the consumer sees it rather
            // than a silent end-of-stream.
            let _ = tx.send(Err(e)).await;
        }
    });

    rx
}

async fn run_log_stream(
    provider: RpcProvider,
    query: Query,
    config: ClientConfig,
    tx: mpsc::Sender<Result<ArrowResponse>>,
) -> Result<()> {
    let from_block = query.from_block;

    // Determine snapshot_latest_block: either user-specified or current head.
    let snapshot_latest_block = match query.to_block {
        Some(to) => to,
        None => provider
            .get_block_number()
            .await
            .context("failed to get current block number for snapshot_latest_block")?,
    };

    // Historical phase
    let next_block = run_log_historical(
        &provider,
        &query.logs,
        from_block,
        snapshot_latest_block,
        query.fields.log,
        &config,
        &tx,
    )
    .await?;

    info!("Finished historical log indexing up to block {snapshot_latest_block}");

    // Live phase
    if !config.stop_on_head {
        run_log_live(
            &provider,
            &query.logs,
            next_block,
            query.fields.log,
            &config,
            &tx,
        )
        .await?;
    }

    Ok(())
}

/// Historical phase for the log pipeline: iterate `[from_block, snapshot_latest_block]`
/// in parallel chunks, bounded by `LOG_ADAPTIVE_CONCURRENCY`.
///
/// Spawns multiple concurrent fetch tasks via `JoinSet`. Each task fetches a
/// chunk of logs, converts to Arrow, and returns the response. Results are sent
/// to the channel as they complete (no ordering guarantee).
async fn run_log_historical(
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
        .map(|b| (b as u64).min(DEFAULT_LOG_CHUNK_SIZE as u64))
        .unwrap_or(DEFAULT_LOG_CHUNK_SIZE as u64);

    // Shared chunk size — tasks can shrink it via fetch_min on errors.
    let chunk_size = Arc::new(AtomicU64::new(original_max_range));
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
                let current_chunk = chunk_size.load(Ordering::Relaxed);
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

                // 10% random chance to reset chunk_size to original.
                let mut rng = rand::rng();
                if rng.random_bool(0.10) {
                    chunk_size.store(original_max_range, Ordering::Relaxed);
                }
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
                let current_range = Some(chunk_size.load(Ordering::Relaxed));
                if let Some(retry) =
                    retry_logs_with_block_range(&err_str, chunk_from, chunk_to, current_range)
                {
                    warn!(
                        "Log range error, re-queuing {}-{} (was {chunk_from}-{chunk_to})",
                        retry.from, retry.to
                    );
                    if let Some(new_max) = retry.max_block_range {
                        chunk_size.fetch_min(new_max, Ordering::Relaxed);
                    }
                    if retry.backoff {
                        LOG_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;
                    }
                    retry_queue.push_back((retry.from, retry.to));
                    // Split the remainder into chunks using max_block_range
                    // (the most conservative safe range) at 90% so each piece
                    // has a high chance of succeeding on the first try.
                    if retry.to < chunk_to {
                        let max_range = retry.max_block_range
                            .unwrap_or_else(|| retry.to.saturating_sub(retry.from).max(1));
                        let safe_range = (max_range * 9 / 10).max(1);
                        let mut rem_from = retry.to + 1;
                        while rem_from <= chunk_to {
                            let rem_to = (rem_from + safe_range - 1).min(chunk_to);
                            retry_queue.push_back((rem_from, rem_to));
                            rem_from = rem_to + 1;
                        }
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
                chunk_size.fetch_min(new_range, Ordering::Relaxed);
                retry_queue.push_back((chunk_from, halved));
                // If there's a remainder after the halved portion, re-queue that too.
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
async fn run_log_live(
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

                // Advance past the last seen block.
                if let Some(last_log) = logs.last() {
                    from_block = last_log.block_number.map_or(to_block + 1, |n| n + 1);
                } else {
                    from_block = to_block + 1;
                }
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
                    LOG_ADAPTIVE_CONCURRENCY.record_error();
                    error!(
                        "Live: unexpected error fetching logs {from_block}-{to_block}: {err_str}, retrying in {}ms",
                        config.retry_backoff_ms
                    );
                }
                tokio::time::sleep(Duration::from_millis(config.retry_backoff_ms)).await;
            }
        }
    }
}

// ===========================================================================
// Block stream (eth_getBlockByNumber pipeline)
// ===========================================================================

/// Spawn a producer task that fetches blocks via `eth_getBlockByNumber`,
/// converts to Arrow, and sends `ArrowResponse` chunks through a bounded channel.
///
/// Returns the receiving end of the channel.
pub fn start_block_stream(
    provider: RpcProvider,
    query: Query,
    config: ClientConfig,
) -> mpsc::Receiver<Result<ArrowResponse>> {
    let (tx, rx) = mpsc::channel(config.buffer_size);

    tokio::spawn(async move {
        if let Err(e) = run_block_stream(provider, query, config, tx.clone()).await {
            error!("Block stream terminated with error: {e:#}");
            let _ = tx.send(Err(e)).await;
        }
    });

    rx
}

async fn run_block_stream(
    provider: RpcProvider,
    query: Query,
    config: ClientConfig,
    tx: mpsc::Sender<Result<ArrowResponse>>,
) -> Result<()> {
    let from_block = query.from_block;

    let snapshot_latest_block = match query.to_block {
        Some(to) => to,
        None => provider
            .get_block_number()
            .await
            .context("failed to get current block number for snapshot_latest_block")?,
    };

    let include_txs = get_blocks_needs_full_txs(&query);
    let fetch_receipts_flag = needs_tx_receipts(&query);
    let block_fields = query.fields.block;
    let tx_fields = query.fields.transaction;
    let next_block = run_block_historical(
        &provider,
        include_txs,
        fetch_receipts_flag,
        &block_fields,
        &tx_fields,
        from_block,
        snapshot_latest_block,
        &config,
        &tx,
    )
    .await?;

    info!("Finished historical block indexing up to block {snapshot_latest_block}");

    if !config.stop_on_head {
        run_block_live(
            &provider,
            include_txs,
            fetch_receipts_flag,
            &block_fields,
            &tx_fields,
            next_block,
            &config,
            &tx,
        )
        .await?;
    }

    Ok(())
}

// ===========================================================================
// Trace stream (trace_block / debug_traceBlockByNumber pipeline)
// ===========================================================================

/// Spawn a producer task that fetches traces via `trace_block` or
/// `debug_traceBlockByNumber`, converts to Arrow, and sends `ArrowResponse`
/// chunks through a bounded channel.
///
/// Returns the receiving end of the channel.
pub fn start_trace_stream(
    provider: RpcProvider,
    query: Query,
    config: ClientConfig,
) -> mpsc::Receiver<Result<ArrowResponse>> {
    let (tx, rx) = mpsc::channel(config.buffer_size);

    tokio::spawn(async move {
        if let Err(e) = run_trace_stream(provider, query, config, tx.clone()).await {
            error!("Trace stream terminated with error: {e:#}");
            let _ = tx.send(Err(e)).await;
        }
    });

    rx
}

async fn run_trace_stream(
    provider: RpcProvider,
    query: Query,
    config: ClientConfig,
    tx: mpsc::Sender<Result<ArrowResponse>>,
) -> Result<()> {
    let from_block = query.from_block;

    let snapshot_latest_block = match query.to_block {
        Some(to) => to,
        None => provider
            .get_block_number()
            .await
            .context("failed to get current block number for snapshot_latest_block")?,
    };

    let trace_method = config.trace_method.unwrap_or_else(|| get_trace_method(&query));
    let trace_fields = query.fields.trace;

    let next_block = run_trace_historical(
        &provider,
        trace_method,
        &trace_fields,
        from_block,
        snapshot_latest_block,
        &config,
        &tx,
    )
    .await?;

    info!("Finished historical trace indexing up to block {snapshot_latest_block}");

    if !config.stop_on_head {
        run_trace_live(
            &provider,
            trace_method,
            &trace_fields,
            next_block,
            &config,
            &tx,
        )
        .await?;
    }

    Ok(())
}

/// Historical phase for the trace pipeline.
async fn run_trace_historical(
    provider: &RpcProvider,
    trace_method: TraceMethod,
    trace_fields: &TraceFields,
    start_from: u64,
    snapshot_latest_block: u64,
    config: &ClientConfig,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<u64> {
    let original_max_range = config.batch_size.map(|b| b as u64);
    let retry_backoff_ms = config.retry_backoff_ms;
    let mut max_block_range = original_max_range;
    let mut from_block = start_from;

    while from_block <= snapshot_latest_block {
        let to_block = clamp_to_block(from_block, snapshot_latest_block, max_block_range);

        debug!("Fetching traces for blocks {from_block}..{to_block}");

        match fetch_traces(provider, from_block, to_block, trace_method).await {
            Ok(traces) => {
                let num_traces = traces.len();
                info!("Fetched {num_traces} traces between blocks {from_block}-{to_block}");

                let traces_batch =
                    select_trace_columns(traces_to_record_batch(&traces), trace_fields);
                let response = ArrowResponse::with_traces(traces_batch);

                if tx.send(Ok(response)).await.is_err() {
                    debug!("Stream consumer dropped, stopping historical trace fetch");
                    return Ok(from_block);
                }

                from_block = to_block + 1;

                let mut rng = rand::rng();
                if rng.random_bool(0.10) {
                    max_block_range = original_max_range;
                }
            }
            Err(e) => {
                let err_str = format!("{e:#}");

                if let Some(retry) =
                    retry_logs_with_block_range(&err_str, from_block, to_block, max_block_range)
                {
                    if retry.backoff {
                        warn!(
                            "Trace range error, retrying with {}-{} (was {from_block}-{to_block}), backing off {retry_backoff_ms}ms",
                            retry.from, retry.to
                        );
                        tokio::time::sleep(Duration::from_millis(retry_backoff_ms)).await;
                    } else {
                        warn!(
                            "Trace range error, retrying with {}-{} (was {from_block}-{to_block})",
                            retry.from, retry.to
                        );
                    }
                    from_block = retry.from;
                    max_block_range = retry.max_block_range;
                    continue;
                }

                // Fatal errors are not recoverable — propagate immediately.
                if is_fatal_error(&err_str) {
                    return Err(e);
                }

                let halved = halved_block_range(from_block, to_block);
                error!(
                    "Unexpected error fetching traces {from_block}-{to_block}, \
                     retrying {from_block}-{halved}: {err_str}"
                );
                max_block_range = Some(halved.saturating_sub(from_block));
            }
        }
    }

    Ok(from_block)
}

/// Live phase for the trace pipeline: poll for new blocks.
async fn run_trace_live(
    provider: &RpcProvider,
    trace_method: TraceMethod,
    trace_fields: &TraceFields,
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
                error!("Failed to get latest block number: {e:#}, retrying in {}ms", config.retry_backoff_ms);
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

        match fetch_traces(provider, from_block, to_block, trace_method).await {
            Ok(traces) => {
                let num_traces = traces.len();

                if num_traces > 0 {
                    info!("Live: fetched {num_traces} traces between blocks {from_block}-{to_block}");
                }

                let traces_batch =
                    select_trace_columns(traces_to_record_batch(&traces), trace_fields);
                let response = ArrowResponse::with_traces(traces_batch);

                if tx.send(Ok(response)).await.is_err() {
                    debug!("Stream consumer dropped, stopping live trace polling");
                    return Ok(());
                }

                from_block = to_block + 1;
            }
            Err(e) => {
                let err_str = format!("{e:#}");
                if let Some(retry) = retry_logs_with_block_range(&err_str, from_block, to_block, None) {
                    debug!(
                        "Live: trace range error, will retry with {}-{}",
                        retry.from, retry.to
                    );
                } else {
                    error!(
                        "Live: unexpected error fetching traces {from_block}-{to_block}: {err_str}, retrying in {}ms",
                        config.retry_backoff_ms
                    );
                }
                tokio::time::sleep(Duration::from_millis(config.retry_backoff_ms)).await;
            }
        }
    }
}
