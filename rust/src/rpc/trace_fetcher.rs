//! Trace fetching: `trace_block` and `debug_traceBlockByNumber` calls.
//!
//! Fetches all traces for a range of blocks. One call per block is issued
//! in parallel, bounded by a semaphore.
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

use super::log_adaptive_concurrency::retry_logs_with_block_range;
use super::shared_helpers::{clamp_to_block, halved_block_range, is_fatal_error};
use crate::query::TraceMethod;

use super::adaptive_concurrency::{report_rpc_outcome, ADAPTIVE_CONCURRENCY};
use super::provider::RpcProvider;

/// Fetch all traces for a contiguous range of blocks.
///
/// Returns a flat `Vec` of all traces, in block-number order (within each
/// block the order matches the provider's response).
///
/// Returns an error (with user-friendly guidance) if the provider does not
/// support block-level tracing.
pub async fn fetch_traces(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    trace_method: TraceMethod,
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

    let semaphore = Arc::new(Semaphore::new(ADAPTIVE_CONCURRENCY.current()));

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

                ADAPTIVE_CONCURRENCY.wait_for_backoff().await;

                let result = provider
                    .get_block_traces(block_num, trace_method)
                    .await
                    .with_context(|| format!("trace fetch for block {block_num}"));

                match &result {
                    Ok(_) => report_rpc_outcome(&Ok(())),
                    Err(e) => {
                        let err_str = e.to_string();
                        report_rpc_outcome(&Err(&err_str));
                    }
                }

                result
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

/// Fetch traces for `[from_block, to_block]`, automatically splitting into narrower
/// sub-ranges on provider block-range errors and accumulating results.
pub(super) async fn fetch_traces_with_retry(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    trace_method: TraceMethod,
    initial_max_block_range: Option<u64>,
    retry_backoff_ms: u64,
) -> Result<Vec<LocalizedTransactionTrace>> {
    let mut all_traces: Vec<LocalizedTransactionTrace> = Vec::new();
    let mut sub_from = from_block;
    let mut max_block_range = initial_max_block_range;

    while sub_from <= to_block {
        let sub_to = clamp_to_block(sub_from, to_block, max_block_range);

        match fetch_traces(provider, sub_from, sub_to, trace_method).await {
            Ok(traces) => {
                all_traces.extend(traces);
                sub_from = sub_to + 1;
            }
            Err(e) => {
                let err_str = format!("{e:#}");

                if let Some(retry) =
                    retry_logs_with_block_range(&err_str, sub_from, sub_to, max_block_range)
                {
                    warn!(
                        "Trace pipeline range error, retrying {}-{} (was {sub_from}-{sub_to}), backing off {retry_backoff_ms}ms",
                        retry.from, retry.to
                    );
                    if retry.backoff {
                        tokio::time::sleep(Duration::from_millis(retry_backoff_ms)).await;
                    }
                    sub_from = retry.from;
                    max_block_range = retry.max_block_range;
                    continue;
                }

                if is_fatal_error(&err_str) {
                    return Err(e);
                }

                let halved = halved_block_range(sub_from, sub_to);
                error!(
                    "Trace pipeline unexpected error {sub_from}-{sub_to}, \
                     retrying {sub_from}-{halved}: {err_str}"
                );
                max_block_range = Some(halved.saturating_sub(sub_from));
            }
        }
    }

    Ok(all_traces)
}
