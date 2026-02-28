//! Streaming pipeline: historical block-range iteration → live head polling.
//!
//! Adapted from rindexer's `fetch_logs_stream()` (fetch_logs.rs:30-149) and
//! `live_indexing_stream()` (fetch_logs.rs:391-711).

use std::time::Duration;

use anyhow::{Context, Result};
use log::{debug, error, info};
use tokio::sync::mpsc;

use crate::config::ClientConfig;
use crate::convert::{select_trace_columns, traces_to_record_batch};
use crate::query::{
    get_blocks_needs_full_txs, get_trace_method, needs_tx_receipts,
    Query, TraceFields, TraceMethod,
};
use crate::response::ArrowResponse;

use super::block_fetcher::{run_block_historical, run_block_live};
use super::log_fetcher::{run_log_historical, run_log_live};
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
    use super::single_block_adaptive_concurrency::DEFAULT_SINGLE_BLOCK_CHUNK_SIZE;

    let chunk_size = config.batch_size.map(|b| b as u64).unwrap_or(DEFAULT_SINGLE_BLOCK_CHUNK_SIZE);
    let mut from_block = start_from;

    while from_block <= snapshot_latest_block {
        let to_block = (from_block + chunk_size - 1).min(snapshot_latest_block);

        debug!("Fetching traces for blocks {from_block}..{to_block}");

        let traces = fetch_traces(provider, from_block, to_block, trace_method, config).await?;

        let num_traces = traces.len();
        info!("Fetched {num_traces} traces between blocks {from_block}-{to_block}");

        let traces_batch = select_trace_columns(traces_to_record_batch(&traces), trace_fields);
        let response = ArrowResponse::with_traces(traces_batch);

        if tx.send(Ok(response)).await.is_err() {
            debug!("Stream consumer dropped, stopping historical trace fetch");
            return Ok(from_block);
        }

        from_block = to_block + 1;
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

        match fetch_traces(provider, from_block, to_block, trace_method, config).await {
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
            Err(e) => return Err(e),
        }
    }
}
