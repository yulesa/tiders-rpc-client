//! Trace fetching: `trace_block` and `debug_traceBlockByNumber` calls.
//!
//! Fetches all traces for a range of blocks. One call per block is issued
//! in parallel, bounded by a semaphore. Each block is retried independently
//! up to `config.max_num_retries` times before propagating an error.
//!
//! If the provider does not support block-level tracing the error message
//! explains why a per-transaction fallback is deliberately not implemented and
//! lists alternative providers and clients.

use std::sync::Arc;
use std::time::Duration;

use alloy::rpc::types::trace::parity::LocalizedTransactionTrace;
use anyhow::{Context, Result};
use log::{error, info, warn};
use tokio::sync::Semaphore;

use super::single_block_adaptive_concurrency::{
    report_rpc_outcome, SINGLE_BLOCK_ADAPTIVE_CONCURRENCY,
};
use super::shared_helpers::{is_fatal_error, is_rate_limit_error};
use crate::config::ClientConfig;
use crate::query::TraceMethod;

use super::provider::RpcProvider;

/// Fetch all traces for a contiguous range of blocks.
///
/// Returns a flat `Vec` of all traces, in block-number order (within each
/// block the order matches the provider's response).
///
/// Each block is retried independently up to `config.max_num_retries` times.
/// A rate-limit error triggers adaptive backoff; a fatal error stops the
/// stream immediately; exhausting all retries propagates an error.
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
                                error!("Fatal error fetching traces for block {block_num}: {err_str}");
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
