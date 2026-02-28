//! Streaming pipeline: historical block-range iteration → live head polling.
//!
//! Adapted from rindexer's `fetch_logs_stream()` (fetch_logs.rs:30-149) and
//! `live_indexing_stream()` (fetch_logs.rs:391-711).

use anyhow::{Context, Result};
use log::{error, info};
use tokio::sync::mpsc;

use crate::config::ClientConfig;
use crate::query::{get_blocks_needs_full_txs, get_trace_method, needs_tx_receipts, Query};
use crate::response::ArrowResponse;

use super::provider::RpcProvider;
use super::block_fetcher::{run_block_historical, run_block_live};
use super::log_fetcher::{run_log_historical, run_log_live};
use super::trace_fetcher::{run_trace_historical, run_trace_live};

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

