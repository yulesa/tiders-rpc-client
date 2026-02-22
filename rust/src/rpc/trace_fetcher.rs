//! Trace fetching: `trace_block` and `debug_traceBlockByNumber` calls.
//!
//! Fetches all traces for a range of blocks. One call per block is issued
//! in parallel, bounded by a semaphore.
//!
//! If the provider does not support block-level tracing the error message
//! explains why a per-transaction fallback is deliberately not implemented and
//! lists alternative providers and clients.

use std::sync::Arc;

use alloy::rpc::types::trace::parity::LocalizedTransactionTrace;
use anyhow::{Context, Result};
use log::info;
use tokio::sync::Semaphore;

use crate::query::TraceMethod;

use super::provider::RpcProvider;

/// Maximum concurrent trace calls per block.
const MAX_CONCURRENT_TRACE_CALLS: usize = 4;

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

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_TRACE_CALLS));

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

                provider
                    .get_block_traces(block_num, trace_method)
                    .await
                    .with_context(|| format!("trace fetch for block {block_num}"))
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
