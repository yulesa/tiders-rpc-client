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

use std::time::Duration;

use anyhow::{Context, Result};
use log::{debug, error, info};
use tokio::sync::mpsc;

use crate::config::ClientConfig;
use crate::convert::{
    blocks_to_record_batch, clamp_to_block, logs_to_record_batch, merge_tx_receipts_into_batch,
    select_block_columns, select_log_columns, select_trace_columns, select_transaction_columns,
    traces_to_record_batch, transactions_to_record_batch,
};
use crate::query::{
    get_blocks_needs_full_txs, get_trace_method, needs_tx_receipts, LogRequest, Pipelines, Query,
};
use crate::response::ArrowResponse;

use super::block_fetcher::fetch_blocks_with_retry;
use super::log_fetcher::fetch_logs_with_retry;
use super::provider::RpcProvider;
use super::trace_fetcher::fetch_traces_with_retry;
use super::tx_receipt_fetcher::fetch_tx_receipts_with_retry;

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
        if let Err(e) =
            run_coordinated_stream(provider, query, config, pipelines, tx.clone()).await
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
            .context("failed to get current block number for snapshot_latest_block")?,
    };

    let next_block = run_coordinated_historical(
        &provider,
        &query,
        pipelines,
        from_block,
        snapshot_latest_block,
        config.batch_size.map(|b| b as u64),
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
#[allow(clippy::too_many_arguments)]
async fn run_coordinated_historical(
    provider: &RpcProvider,
    query: &Query,
    pipelines: Pipelines,
    start_from: u64,
    snapshot_latest_block: u64,
    initial_max_block_range: Option<u64>,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<u64> {
    let mut from_block = start_from;

    while from_block <= snapshot_latest_block {
        let to_block = clamp_to_block(from_block, snapshot_latest_block, initial_max_block_range);

        debug!("Coordinated fetch for blocks {from_block}..{to_block}");

        let response = fetch_all(provider, query, pipelines, from_block, to_block, initial_max_block_range).await?;

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

        let response = fetch_all(provider, query, pipelines, from_block, to_block, None).await?;

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
    max_block_range: Option<u64>,
) -> Result<ArrowResponse> {
    // --- Block pipeline (blocks + transactions) ---
    let (blocks_batch, txs_batch) = if pipelines.blocks_transactions {
        let include_txs = get_blocks_needs_full_txs(query);
        let fetch_receipts_flag = needs_tx_receipts(query);
        let block_fields = &query.fields.block;
        let tx_fields = &query.fields.transaction;

        let blocks =
            fetch_blocks_with_retry(provider, from_block, to_block, include_txs, max_block_range)
                .await?;
        let blocks_batch = select_block_columns(blocks_to_record_batch(&blocks), block_fields);
        let raw_txs_batch = transactions_to_record_batch(&blocks);

        let merged_txs = if fetch_receipts_flag && raw_txs_batch.num_rows() > 0 {
            let receipts = fetch_tx_receipts_with_retry(provider, from_block, to_block, None).await?;
            merge_tx_receipts_into_batch(raw_txs_batch, &receipts)
        } else {
            raw_txs_batch
        };

        let txs_batch = select_transaction_columns(merged_txs, tx_fields);

        (blocks_batch, txs_batch)
    } else {
        (ArrowResponse::empty().blocks, ArrowResponse::empty().transactions)
    };

    // --- Log pipeline ---
    let logs_batch = if pipelines.logs {
        let log_requests = vec![LogRequest::default()];
        let logs = fetch_logs_with_retry(provider, &log_requests, from_block, to_block, max_block_range).await?;
        select_log_columns(logs_to_record_batch(&logs), &query.fields.log)
    } else {
        ArrowResponse::empty().logs
    };

    // --- Trace pipeline ---
    let traces_batch = if pipelines.traces {
        let trace_method = get_trace_method(query);
        let traces = fetch_traces_with_retry(provider, from_block, to_block, trace_method, max_block_range).await?;
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

