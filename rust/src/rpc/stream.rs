//! Streaming pipeline: historical block-range iteration → live head polling.
//!
//! Adapted from rindexer's `fetch_logs_stream()` (fetch_logs.rs:30-149) and
//! `live_indexing_stream()` (fetch_logs.rs:391-711).

use std::time::Duration;

use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use rand::Rng;
use tokio::sync::mpsc;

use crate::config::ClientConfig;
use crate::convert::{
    blocks_to_record_batch, clamp_to_block, halved_block_range, logs_to_record_batch,
    merge_tx_receipts_into_batch, retry_with_block_range, select_block_columns, select_log_columns,
    select_trace_columns, select_transaction_columns, traces_to_record_batch,
    transactions_to_record_batch,
};
use crate::query::{
    get_blocks_needs_full_txs, get_trace_method, needs_tx_receipts, BlockFields, LogFields,
    LogRequest, Query, TraceFields, TraceMethod, TransactionFields,
};
use crate::response::ArrowResponse;

use super::block_fetcher::fetch_blocks;
use super::log_fetcher::fetch_logs;
use super::provider::RpcProvider;
use super::trace_fetcher::fetch_traces;
use super::tx_receipt_fetcher::fetch_tx_receipts;

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
        config.max_block_range,
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
    initial_max_block_range: Option<u64>,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<u64> {
    let original_max_range = initial_max_block_range;
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
                    retry_with_block_range(&err_str, from_block, to_block, max_block_range)
                {
                    warn!(
                        "Block range error, retrying with {}-{} (was {from_block}-{to_block})",
                        retry.from, retry.to
                    );
                    if retry.backoff {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    from_block = retry.from;
                    max_block_range = retry.max_block_range;
                    // Don't advance — retry with the new range.
                    continue;
                }

                // Hard connectivity failures are not recoverable — propagate immediately.
                if is_connection_error(&err_str) {
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
                error!("Failed to get latest block number: {e:#}");
                tokio::time::sleep(Duration::from_secs(1)).await;
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
                if let Some(retry) = retry_with_block_range(&err_str, from_block, to_block, None) {
                    debug!(
                        "Live: block range error, will retry with {}-{}",
                        retry.from, retry.to
                    );
                    // Don't advance — the next iteration will clamp naturally.
                } else {
                    error!(
                        "Live: unexpected error fetching logs {from_block}-{to_block}: {err_str}"
                    );
                }
                // Back off a bit before retrying.
                tokio::time::sleep(Duration::from_secs(1)).await;
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
        config.max_block_range,
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
async fn run_block_historical(
    provider: &RpcProvider,
    include_txs: bool,
    fetch_receipts_flag: bool,
    block_fields: &BlockFields,
    tx_fields: &TransactionFields,
    start_from: u64,
    snapshot_latest_block: u64,
    initial_max_block_range: Option<u64>,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<u64> {
    let original_max_range = initial_max_block_range;
    let mut max_block_range = original_max_range;
    let mut from_block = start_from;

    while from_block <= snapshot_latest_block {
        let to_block = clamp_to_block(from_block, snapshot_latest_block, max_block_range);

        debug!("Fetching blocks {from_block}..{to_block}");

        match fetch_blocks(provider, from_block, to_block, include_txs).await {
            Ok(blocks) => {
                let num_blocks = blocks.len();

                let blocks_batch =
                    select_block_columns(blocks_to_record_batch(&blocks), block_fields);
                let raw_txs_batch = transactions_to_record_batch(&blocks);

                // Optionally fetch tx receipts and merge into the transactions batch.
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

                let response = ArrowResponse::with_blocks(blocks_batch, txs_batch);

                if tx.send(Ok(response)).await.is_err() {
                    debug!("Stream consumer dropped, stopping historical block fetch");
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
                    retry_with_block_range(&err_str, from_block, to_block, max_block_range)
                {
                    warn!(
                        "Block range error, retrying with {}-{} (was {from_block}-{to_block})",
                        retry.from, retry.to
                    );
                    if retry.backoff {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    from_block = retry.from;
                    max_block_range = retry.max_block_range;
                    continue;
                }

                // Hard connectivity failures are not recoverable — propagate immediately.
                if is_connection_error(&err_str) {
                    return Err(e);
                }

                let halved = halved_block_range(from_block, to_block);
                error!(
                    "Unexpected error fetching blocks {from_block}-{to_block}, \
                     retrying {from_block}-{halved}: {err_str}"
                );
                max_block_range = Some(halved.saturating_sub(from_block));
            }
        }
    }

    Ok(from_block)
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
                error!("Failed to get latest block number: {e:#}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let safe_head = head.saturating_sub(config.reorg_safe_distance);

        if from_block > safe_head {
            debug!("At head (from_block={from_block}, safe_head={safe_head}), waiting...");
            continue;
        }

        let to_block = safe_head;

        match fetch_blocks(provider, from_block, to_block, include_txs).await {
            Ok(blocks) => {
                let blocks_batch =
                    select_block_columns(blocks_to_record_batch(&blocks), block_fields);
                let raw_txs_batch = transactions_to_record_batch(&blocks);

                let txs_batch = if fetch_receipts_flag && raw_txs_batch.num_rows() > 0 {
                    let tx_receipts =
                        fetch_tx_receipts_with_retry(provider, from_block, to_block, None).await?;
                    merge_tx_receipts_into_batch(raw_txs_batch, &tx_receipts)
                } else {
                    raw_txs_batch
                };

                let txs_batch = select_transaction_columns(txs_batch, tx_fields);

                if blocks_batch.num_rows() > 0 || txs_batch.num_rows() > 0 {
                    info!(
                        "Live: fetched blocks {from_block}-{to_block} \
                         ({} block rows, {} tx rows)",
                        blocks_batch.num_rows(),
                        txs_batch.num_rows()
                    );
                }

                let response = ArrowResponse::with_blocks(blocks_batch, txs_batch);

                if tx.send(Ok(response)).await.is_err() {
                    debug!("Stream consumer dropped, stopping live block polling");
                    return Ok(());
                }

                from_block = to_block + 1;
            }
            Err(e) => {
                let err_str = format!("{e:#}");
                if let Some(retry) = retry_with_block_range(&err_str, from_block, to_block, None) {
                    debug!(
                        "Live: block range error, will retry with {}-{}",
                        retry.from, retry.to
                    );
                } else {
                    error!(
                        "Live: unexpected error fetching blocks {from_block}-{to_block}: {err_str}"
                    );
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Fetch tx receipts for a block range, automatically retrying with a smaller
/// window if the provider rejects the request.
///
/// When a range is too large, the window is split into smaller sub-windows and each is
/// fetched separately, accumulating all tx receipts into a single result. Hard
/// errors (connection failures or unrecognized errors) stop the stream immediately.
async fn fetch_tx_receipts_with_retry(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    initial_max_block_range: Option<u64>,
) -> Result<Vec<alloy::network::AnyTransactionReceipt>> {
    let mut all_tx_receipts = Vec::new();
    let mut sub_from = from_block;
    let mut max_block_range = initial_max_block_range;

    while sub_from <= to_block {
        let sub_to = clamp_to_block(sub_from, to_block, max_block_range);

        match fetch_tx_receipts(provider, sub_from, sub_to).await {
            Ok(tx_receipts) => {
                all_tx_receipts.extend(tx_receipts);
                sub_from = sub_to + 1;
            }
            Err(e) => {
                let err_str = format!("{e:#}");

                if let Some(retry) =
                    retry_with_block_range(&err_str, sub_from, sub_to, max_block_range)
                {
                    warn!(
                        "Tx receipt range error, retrying with {}-{} (was {sub_from}-{sub_to})",
                        retry.from, retry.to
                    );
                    if retry.backoff {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    sub_from = retry.from;
                    max_block_range = retry.max_block_range;
                    continue;
                }

                // Hard connectivity failures are not recoverable — propagate immediately.
                if is_connection_error(&err_str) {
                    return Err(e);
                }

                // Unrecognized error: halve the range and retry, same as the block pipeline.
                let halved = halved_block_range(sub_from, sub_to);
                error!(
                    "Unexpected error fetching tx receipts {sub_from}-{sub_to}, \
                     retrying {sub_from}-{halved}: {err_str}"
                );
                max_block_range = Some(halved.saturating_sub(sub_from));
            }
        }
    }

    Ok(all_tx_receipts)
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

    let trace_method = get_trace_method(&query);
    let trace_fields = query.fields.trace;

    let next_block = run_trace_historical(
        &provider,
        trace_method,
        &trace_fields,
        from_block,
        snapshot_latest_block,
        config.max_block_range,
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
    initial_max_block_range: Option<u64>,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<u64> {
    let original_max_range = initial_max_block_range;
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
                    retry_with_block_range(&err_str, from_block, to_block, max_block_range)
                {
                    warn!(
                        "Trace range error, retrying with {}-{} (was {from_block}-{to_block})",
                        retry.from, retry.to
                    );
                    if retry.backoff {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    from_block = retry.from;
                    max_block_range = retry.max_block_range;
                    continue;
                }

                // Hard connectivity failures are not recoverable — propagate immediately.
                if is_connection_error(&err_str) {
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
                error!("Failed to get latest block number: {e:#}");
                tokio::time::sleep(Duration::from_secs(1)).await;
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
                if let Some(retry) = retry_with_block_range(&err_str, from_block, to_block, None) {
                    debug!(
                        "Live: trace range error, will retry with {}-{}",
                        retry.from, retry.to
                    );
                } else {
                    error!(
                        "Live: unexpected error fetching traces {from_block}-{to_block}: {err_str}"
                    );
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

// ===========================================================================
// Shared helpers
// ===========================================================================

/// Returns `true` when the error string indicates a hard connectivity failure
/// that cannot be recovered by retrying with a smaller block range.
fn is_connection_error(err_str: &str) -> bool {
    err_str.contains("Connection refused")
        || err_str.contains("connection refused")
        || err_str.contains("No such host")
        || err_str.contains("no such host")
        || err_str.contains("failed to lookup")
}
