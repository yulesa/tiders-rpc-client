//! Trace fetching: `trace_block` and `debug_traceBlockByNumber` calls.
//!
//! Contains:
//! - `run_trace_historical()` — main loop for historical phase, fetching trace chunks
//!   sequentially and sending one `ArrowResponse` per chunk.
//! - `run_trace_live()` — main loop for live phase, polling for new blocks.
//! - `fetch_traces()` — fetch traces for a block range, with per-block parallelism
//!   bounded by `SINGLE_BLOCK_ADAPTIVE_CONCURRENCY` and per-block retry logic.
//!
//! Each block is retried independently up to `config.max_num_retries` times.
//! A rate-limit error triggers adaptive backoff; a fatal error stops the
//! stream immediately; exhausting all retries propagates an error.
//!
//! If the provider does not support block-level tracing the error message
//! explains why a per-transaction fallback is deliberately not implemented and
//! lists alternative providers and clients.

use std::sync::Arc;
use std::time::Duration;

use alloy::rpc::types::trace::parity::LocalizedTransactionTrace;
use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use tokio::sync::{mpsc, Semaphore};

use super::single_block_adaptive_concurrency::{
    report_rpc_outcome, DEFAULT_SINGLE_BLOCK_CHUNK_SIZE, SINGLE_BLOCK_ADAPTIVE_CONCURRENCY,
};
use super::shared_helpers::{is_fatal_error, is_rate_limit_error};
use crate::config::ClientConfig;
use super::arrow_convert::{select_trace_columns, traces_to_record_batch};
use crate::query::{TraceFields, TraceMethod};
use crate::response::ArrowResponse;

use super::provider::RpcProvider;

/// Historical phase for the trace pipeline: iterate `[start_from, snapshot_latest_block]`
/// in chunks of `config.batch_size`, fetching all blocks in each chunk in parallel
/// bounded by `SINGLE_BLOCK_ADAPTIVE_CONCURRENCY`.
///
/// One `ArrowResponse` is emitted per chunk. Any error (fatal or max-retries exhausted)
/// propagates immediately and stops the stream.
pub(super) async fn run_trace_historical(
    provider: &RpcProvider,
    trace_method: TraceMethod,
    trace_fields: &TraceFields,
    start_from: u64,
    snapshot_latest_block: u64,
    config: &ClientConfig,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<u64> {
    let chunk_size = config
        .batch_size
        .map(|b| b as u64)
        .unwrap_or(DEFAULT_SINGLE_BLOCK_CHUNK_SIZE);
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

    Ok(snapshot_latest_block + 1)
}

/// Live phase for the trace pipeline: poll for new blocks and fetch traces.
pub(super) async fn run_trace_live(
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

        match fetch_traces(provider, from_block, to_block, trace_method, config).await {
            Ok(traces) => {
                let num_traces = traces.len();

                if num_traces > 0 {
                    info!(
                        "Live: fetched {num_traces} traces between blocks {from_block}-{to_block}"
                    );
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

/// Fetch all traces for a contiguous range of blocks.
///
/// Returns a flat `Vec` of all traces, in block-number order (within each
/// block the order matches the provider's response).
///
/// Blocks are fetched in parallel, bounded by a semaphore sized to
/// `SINGLE_BLOCK_ADAPTIVE_CONCURRENCY.current()` at call time.
/// Each block is retried independently up to `config.max_num_retries` times.
/// A rate-limit error notifies the controller (triggering backoff);
/// a fatal error propagates immediately; exhausting all retries returns an error.
pub async fn fetch_traces(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    trace_method: TraceMethod,
    config: &ClientConfig,
) -> Result<Vec<LocalizedTransactionTrace>> {
    if from_block > to_block {
        return Ok(Vec::new());
    }

    let block_numbers: Vec<u64> = (from_block..=to_block).collect();
    let count = block_numbers.len();
    info!(
        "fetch_traces: requesting traces for {count} blocks ({from_block}..={to_block}) \
         via {trace_method:?}"
    );

    // Semaphore to limit concurrent block fetches according to the adaptive concurrency controller.
    // However, the changes in concurrency level won't affect already-running chunk, only when a new chunk start
    // the updated concurrency level will be read and applied. This is a trade-off to avoid the complexity.
    let semaphore = Arc::new(Semaphore::new(SINGLE_BLOCK_ADAPTIVE_CONCURRENCY.current()));
    let max_retries = config.max_num_retries;
    let retry_base_ms = config.retry_base_ms;
    let retry_ceiling_ms = config.retry_ceiling_ms;

    let handles: Vec<_> = block_numbers
        .into_iter()
        .map(|block_num| {
            let provider = provider.clone();
            let sem = semaphore.clone();
            tokio::spawn(async move {
                let _permit = sem
                    .acquire_owned()
                    .await
                    .map_err(|e| anyhow::anyhow!("semaphore closed: {e}"))?;

                for attempt in 0..max_retries {
                    SINGLE_BLOCK_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;

                    let result = provider
                        .get_block_traces(block_num, trace_method)
                        .await
                        .with_context(|| format!("trace fetch for block {block_num}"));

                    match result {
                        Ok(traces) => {
                            report_rpc_outcome(&Ok(()));
                            return Ok(traces);
                        }
                        Err(e) => {
                            let err_str = format!("{e:#}");

                            if is_fatal_error(&err_str) {
                                error!(
                                    "Fatal error fetching traces for block {block_num}: {err_str}"
                                );
                                report_rpc_outcome(&Err(&err_str));
                                return Err(e);
                            }

                            if is_rate_limit_error(&err_str) {
                                warn!(
                                    "Rate limit fetching traces for block {block_num} \
                                     (attempt {attempt}/{max_retries}): {err_str}"
                                );
                                report_rpc_outcome(&Err(&err_str));
                                // backoff is managed by the controller; wait_for_backoff()
                                // at the top of the next iteration will apply it.
                            } else {
                                warn!(
                                    "Transient error fetching traces for block {block_num} \
                                     (attempt {attempt}/{max_retries}): {err_str}"
                                );
                                report_rpc_outcome(&Err(&err_str));
                                let backoff_ms = retry_base_ms
                                    .saturating_mul(1u64 << attempt.min(63))
                                    .min(retry_ceiling_ms);
                                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                            }
                        }
                    }
                }

                Err(anyhow::anyhow!(
                    "max retries ({max_retries}) exceeded for block {block_num}"
                ))
            })
        })
        .collect();

    let mut all_traces = Vec::new();
    for handle in handles {
        let traces = handle
            .await
            .map_err(|e| anyhow::anyhow!("trace fetch task panicked: {e}"))??;
        all_traces.extend(traces);
    }

    info!(
        "fetch_traces: received {} traces for range {from_block}..={to_block}",
        all_traces.len()
    );

    Ok(all_traces)
}
