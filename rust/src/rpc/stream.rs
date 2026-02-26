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
    blocks_to_record_batch, clamp_to_block,
    logs_to_record_batch, merge_tx_receipts_into_batch,
    retry_logs_with_block_range, select_block_columns, select_log_columns,
    select_trace_columns, select_transaction_columns, traces_to_record_batch,
    transactions_to_record_batch,
};
use crate::query::{
    get_blocks_needs_full_txs, get_trace_method, needs_tx_receipts, BlockFields, LogFields,
    LogRequest, Query, TraceFields, TraceMethod, TransactionFields,
};
use crate::response::ArrowResponse;

use super::block_adaptive_concurrency::{
    halved_block_range, is_fatal_error, is_rate_limit_error, retry_block_with_block_range,
    BLOCK_ADAPTIVE_CONCURRENCY,
};
use super::block_fetcher::{fetch_blocks, DEFAULT_BLOCK_CHUNK_SIZE};
use super::log_fetcher::fetch_logs;
use super::provider::RpcProvider;
use super::trace_fetcher::fetch_traces;
use super::tx_receipt_fetcher::fetch_tx_receipts_with_retry;

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

/// Historical phase for the log pipeline: iterate `[from_block, snapshot_latest_block]` in
/// chunks, shrinking the range on errors.
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
        .map(|b| (b as u64).min(DEFAULT_BLOCK_CHUNK_SIZE as u64))
        .or(Some(DEFAULT_BLOCK_CHUNK_SIZE as u64));
    let retry_backoff_ms = config.retry_backoff_ms;
    let mut max_block_range = original_max_range;
    let mut from_block = start_from;

    while from_block <= snapshot_latest_block {
        let to_block = clamp_to_block(from_block, snapshot_latest_block, max_block_range);

        debug!("Fetching logs for blocks {from_block}..{to_block}");

        match fetch_logs(provider, log_requests, from_block, to_block).await {
            Ok(logs) => {
                let logs_len = logs.len();
                info!("Fetched {logs_len} logs between blocks {from_block}-{to_block}");

                let logs_batch = select_log_columns(logs_to_record_batch(&logs), &log_fields);
                let response = ArrowResponse::with_logs(logs_batch);

                if tx.send(Ok(response)).await.is_err() {
                    debug!("Stream consumer dropped, stopping historical log fetch");
                    return Ok(from_block);
                }

                // Advance: if we got logs, advance past the last log's block.
                // If empty, advance past to_block.
                if let Some(last_log) = logs.last() {
                    from_block = last_log.block_number.map_or(to_block + 1, |n| n + 1);
                } else {
                    from_block = to_block + 1;
                }

                // 10% random chance to reset to original max range,
                // useful for breaking out of temporary limitations.
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
                            "Log range error, retrying with {}-{} (was {from_block}-{to_block}), backing off {retry_backoff_ms}ms",
                            retry.from, retry.to
                        );
                        tokio::time::sleep(Duration::from_millis(retry_backoff_ms)).await;
                    } else {
                        warn!(
                            "Log range error, retrying with {}-{} (was {from_block}-{to_block})",
                            retry.from, retry.to
                        );
                    }
                    from_block = retry.from;
                    max_block_range = retry.max_block_range;
                    // Don't advance — retry with the new range.
                    continue;
                }

                // Fatal errors are not recoverable — propagate immediately.
                if is_fatal_error(&err_str) {
                    return Err(e);
                }

                // Unrecognized error: halve the range and retry.
                let halved = halved_block_range(from_block, to_block);
                error!(
                    "Unexpected error fetching logs {from_block}-{to_block}, \
                     retrying {from_block}-{halved}: {err_str}"
                );
                max_block_range = Some(halved.saturating_sub(from_block));
            }
        }
    }

    Ok(from_block)
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

/// Historical phase for the block pipeline: iterate `[from_block, snapshot]`
/// in chunks, fetch blocks (and optionally tx receipts), and send Arrow responses.
///
/// Spawns multiple concurrent fetch tasks via `JoinSet`, bounded by the block
/// adaptive concurrency controller. Each task fetches a chunk of blocks,
/// optionally fetches tx receipts, converts to Arrow, and returns the response.
/// Results are sent to the channel as they complete (no ordering guarantee).
async fn run_block_historical(
    provider: &RpcProvider,
    include_txs: bool,
    fetch_receipts_flag: bool,
    block_fields: &BlockFields,
    tx_fields: &TransactionFields,
    start_from: u64,
    snapshot_latest_block: u64,
    config: &ClientConfig,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<u64> {
    let original_max_range = config
        .batch_size
        .map(|b| (b as u64).min(DEFAULT_BLOCK_CHUNK_SIZE as u64))
        .unwrap_or(DEFAULT_BLOCK_CHUNK_SIZE as u64);

    // Shared chunk size — tasks can shrink it via fetch_min on errors.
    let chunk_size = Arc::new(AtomicU64::new(original_max_range));
    let cancel = CancellationToken::new();
    let mut join_set: JoinSet<(u64, u64, Result<ArrowResponse>)> = JoinSet::new();
    let mut retry_queue: VecDeque<(u64, u64)> = VecDeque::new();

    // Next unassigned from_block.
    let mut cursor = start_from;

    let block_fields = *block_fields;
    let tx_fields = *tx_fields;

    loop {
        // Fill join_set up to the current concurrency limit.
        let max_concurrent = BLOCK_ADAPTIVE_CONCURRENCY.current();
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
            let task_chunk_size = Arc::clone(&chunk_size);

            join_set.spawn(async move {
                if task_cancel.is_cancelled() {
                    return (chunk_from, chunk_to, Err(anyhow::anyhow!("cancelled")));
                }

                let result = run_block_fetch_task(
                    &task_provider,
                    chunk_from,
                    chunk_to,
                    include_txs,
                    fetch_receipts_flag,
                    &block_fields,
                    &tx_fields,
                    Some(task_chunk_size.load(Ordering::Relaxed)),
                )
                .await;

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
                error!("Block fetch task panicked: {join_err}");
                cancel.cancel();
                // Drain remaining tasks.
                while join_set.join_next().await.is_some() {}
                return Err(anyhow::anyhow!("block fetch task panicked: {join_err}"));
            }
        };

        match task_result {
            Ok(response) => {
                BLOCK_ADAPTIVE_CONCURRENCY.record_success();

                if tx.send(Ok(response)).await.is_err() {
                    debug!("Stream consumer dropped, stopping historical block fetch");
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
                    BLOCK_ADAPTIVE_CONCURRENCY.record_rate_limit();
                    BLOCK_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;
                    retry_queue.push_back((chunk_from, chunk_to));
                    continue;
                }

                // 3. Retryable block-range errors — shrink chunk only, no concurrency change.
                let current_range = Some(chunk_size.load(Ordering::Relaxed));
                if let Some(retry) =
                    retry_block_with_block_range(&err_str, chunk_from, chunk_to, current_range)
                {
                    warn!(
                        "Block range error, re-queuing {}-{} (was {chunk_from}-{chunk_to})",
                        retry.from, retry.to
                    );
                    if let Some(new_max) = retry.max_block_range {
                        chunk_size.fetch_min(new_max, Ordering::Relaxed);
                    }
                    if retry.backoff {
                        BLOCK_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;
                    }
                    retry_queue.push_back((retry.from, retry.to));
                    // If there's a remainder after the retried portion, re-queue that too.
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

/// Single fetch task for blocks, transactions and receipts: fetch blocks,
/// optionally fetch tx receipts, convert to Arrow.
async fn run_block_fetch_task(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    include_txs: bool,
    fetch_receipts_flag: bool,
    block_fields: &BlockFields,
    tx_fields: &TransactionFields,
    max_block_range: Option<u64>,
) -> Result<ArrowResponse> {
    BLOCK_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;

    let blocks = fetch_blocks(provider, from_block, to_block, include_txs).await?;
    let num_blocks = blocks.len();

    let blocks_batch = select_block_columns(blocks_to_record_batch(&blocks), block_fields);
    let raw_txs_batch = transactions_to_record_batch(&blocks);

    let txs_batch = if fetch_receipts_flag && raw_txs_batch.num_rows() > 0 {
        let tx_receipts = fetch_tx_receipts_with_retry(
            provider,
            from_block,
            to_block,
            max_block_range,
        )
        .await?;
        info!(
            "Fetched {} tx receipts for blocks {from_block}-{to_block}",
            tx_receipts.len()
        );
        merge_tx_receipts_into_batch(raw_txs_batch, &tx_receipts)
    } else {
        raw_txs_batch
    };

    let txs_batch = select_transaction_columns(txs_batch, tx_fields);

    info!(
        "Fetched {num_blocks} blocks between {from_block}-{to_block} \
         ({} block rows, {} tx rows)",
        blocks_batch.num_rows(),
        txs_batch.num_rows()
    );

    Ok(ArrowResponse::with_blocks(blocks_batch, txs_batch))
}

/// Live phase for the block pipeline: poll for new blocks.
async fn run_block_live(
    provider: &RpcProvider,
    include_txs: bool,
    fetch_receipts_flag: bool,
    block_fields: &BlockFields,
    tx_fields: &TransactionFields,
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

        match run_block_fetch_task(
            provider,
            from_block,
            to_block,
            include_txs,
            fetch_receipts_flag,
            block_fields,
            tx_fields,
            None,
        )
        .await
        {
            Ok(response) => {
                if tx.send(Ok(response)).await.is_err() {
                    debug!("Stream consumer dropped, stopping live block polling");
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
                    BLOCK_ADAPTIVE_CONCURRENCY.record_rate_limit();
                    BLOCK_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;
                    continue;
                }

                if let Some(retry) =
                    retry_block_with_block_range(&err_str, from_block, to_block, None)
                {
                    debug!(
                        "Live: block range error, will retry with {}-{}",
                        retry.from, retry.to
                    );
                    // Don't advance — the next iteration will retry naturally.
                } else {
                    error!(
                        "Live: unexpected error fetching blocks {from_block}-{to_block}: {err_str}, retrying in {}ms",
                        config.retry_backoff_ms
                    );
                }
            }
        }
    }
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

// ===========================================================================
// Shared helpers
// ===========================================================================
