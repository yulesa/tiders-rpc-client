//! Streaming pipeline: historical block-range iteration → live head polling.
//!
//! Adapted from rindexer's `fetch_logs_stream()` (fetch_logs.rs:30-149) and
//! `live_indexing_stream()` (fetch_logs.rs:391-711).

use std::time::Duration;

use anyhow::{Context, Result};
use rand::Rng;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::config::StreamConfig;
use crate::convert::{
    clamp_to_block, halved_block_range, logs_to_record_batch, retry_with_block_range,
};
use crate::query::emit_unimplemented_warnings;
use crate::query::{LogRequest, Query};
use crate::response::ArrowResponse;

use super::log_fetcher::fetch_logs;
use super::provider::RpcProvider;

/// Spawn a producer task that fetches logs in block-range batches and sends
/// `ArrowResponse` chunks through a bounded channel.
///
/// Returns the receiving end of the channel.
pub fn start_log_stream(
    provider: RpcProvider,
    query: Query,
    stream_config: StreamConfig,
) -> mpsc::Receiver<Result<ArrowResponse>> {
    let (tx, rx) = mpsc::channel(stream_config.buffer_size);

    tokio::spawn(async move {
        if let Err(e) = run_stream(provider, query, stream_config, tx).await {
            error!("Log stream terminated with error: {e:#}");
        }
    });

    rx
}

async fn run_stream(
    provider: RpcProvider,
    query: Query,
    stream_config: StreamConfig,
    tx: mpsc::Sender<Result<ArrowResponse>>,
) -> Result<()> {
    // Warn about unimplemented pipelines.
    emit_unimplemented_warnings(&query);

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
    let next_block = run_historical(
        &provider,
        &query.logs,
        from_block,
        snapshot_latest_block,
        query.fields.log,
        &tx,
    )
    .await?;

    info!("Finished historical log indexing up to block {snapshot_latest_block}");

    // Live phase
    if !stream_config.stop_on_head {
        run_live(
            &provider,
            &query.logs,
            next_block,
            query.fields.log,
            &stream_config,
            &tx,
        )
        .await?;
    }

    Ok(())
}

/// Historical phase: iterate through `[from_block, snapshot_latest_block]` in
/// chunks, shrinking the range on errors.
async fn run_historical(
    provider: &RpcProvider,
    log_requests: &[LogRequest],
    start_from: u64,
    snapshot_latest_block: u64,
    _log_fields: crate::query::LogFields,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<u64> {
    let original_max_range: Option<u64> = None; // Could come from ClientConfig in the future
    let mut max_block_range = original_max_range;
    let mut from_block = start_from;

    while from_block <= snapshot_latest_block {
        let to_block = clamp_to_block(from_block, snapshot_latest_block, max_block_range);

        debug!("Fetching logs for blocks {from_block}..{to_block}");

        match fetch_logs(provider, log_requests, from_block, to_block).await {
            Ok(logs) => {
                let logs_len = logs.len();

                if logs_len > 0 {
                    info!("Fetched {logs_len} logs between blocks {from_block}-{to_block}");
                }

                let logs_batch = logs_to_record_batch(&logs);
                let response = ArrowResponse::with_logs(logs_batch);

                if tx.send(Ok(response)).await.is_err() {
                    debug!("Stream consumer dropped, stopping historical fetch");
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
                    debug!(
                        "Block range error, retrying with {}-{} (was {from_block}-{to_block})",
                        retry.from, retry.to
                    );
                    from_block = retry.from;
                    max_block_range = retry.max_block_range;
                    // Don't advance — retry with the new range.
                    // We set the clamp so the next iteration uses the new to.
                    continue;
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

/// Live phase: poll for new blocks and fetch logs.
async fn run_live(
    provider: &RpcProvider,
    log_requests: &[LogRequest],
    start_from: u64,
    _log_fields: crate::query::LogFields,
    stream_config: &StreamConfig,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<()> {
    let poll_interval = Duration::from_millis(stream_config.head_poll_interval_millis);
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
        let safe_head = head.saturating_sub(stream_config.reorg_safe_distance);

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

                let logs_batch = logs_to_record_batch(&logs);
                let response = ArrowResponse::with_logs(logs_batch);

                if tx.send(Ok(response)).await.is_err() {
                    debug!("Stream consumer dropped, stopping live polling");
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
