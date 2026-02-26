//! Block fetching: `eth_getBlockByNumber` batch calls.
//!
//! Fetches blocks for a range of block numbers. Block numbers are chunked
//! into sub-batches of `rpc_batch_size`, and each sub-batch is sent as a
//! single JSON-RPC batch call in parallel, bounded by the adaptive
//! concurrency semaphore.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use alloy::network::AnyRpcBlock;
use anyhow::{Context, Result};
use log::{error, info, warn};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::block_adaptive_concurrency::{
    halved_block_range, is_fatal_error, is_rate_limit_error, retry_block_with_block_range,
    BLOCK_ADAPTIVE_CONCURRENCY,
};
use super::provider::RpcProvider;

/// Default chunk size for `eth_getBlockByNumber` batch calls.
/// Each chunk of block numbers is sent as a single JSON-RPC batch request.
pub(crate) const DEFAULT_BLOCK_CHUNK_SIZE: usize = 200;

/// Fetch blocks for a range of block numbers.
///
/// Sends the entire `[from_block, to_block]` range as a single JSON-RPC
/// batch call.
pub async fn fetch_blocks(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    include_txs: bool,
) -> Result<Vec<AnyRpcBlock>> {
    if from_block > to_block {
        return Ok(Vec::new());
    }

    let block_numbers: Vec<u64> = (from_block..=to_block).collect();
    let count = block_numbers.len();
    info!("fetch_blocks: requesting {count} blocks ({from_block}..={to_block})");

    let blocks = provider
        .get_block_batch(&block_numbers, include_txs)
        .await
        .with_context(|| {
            format!("eth_getBlockByNumber batch for blocks {from_block}-{to_block}")
        })?;

    info!(
        "fetch_blocks: received {} blocks for range {from_block}..={to_block}",
        blocks.len()
    );

    Ok(blocks)
}

/// Fetch blocks for `[from_block, to_block]` using concurrent tasks bounded by
/// `BLOCK_ADAPTIVE_CONCURRENCY`, with adaptive chunk sizing and retry queues.
///
/// This mirrors the JoinSet-based approach from `run_block_historical` in
/// `stream.rs`, but returns collected blocks instead of sending Arrow responses
/// to a channel. Results are sorted by block number before returning.
pub(super) async fn fetch_blocks_concurrent(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    include_txs: bool,
) -> Result<Vec<AnyRpcBlock>> {
    if from_block > to_block {
        return Ok(Vec::new());
    }

    let chunk_size = Arc::new(AtomicU64::new(DEFAULT_BLOCK_CHUNK_SIZE as u64));
    let cancel = CancellationToken::new();
    let mut join_set: JoinSet<(u64, u64, Result<Vec<AnyRpcBlock>>)> = JoinSet::new();
    let mut retry_queue: VecDeque<(u64, u64)> = VecDeque::new();
    let mut all_blocks: Vec<AnyRpcBlock> = Vec::new();

    let mut cursor = from_block;

    loop {
        // Fill join_set up to the current concurrency limit.
        let max_concurrent = BLOCK_ADAPTIVE_CONCURRENCY.current();
        while join_set.len() < max_concurrent {
            let (chunk_from, chunk_to) = if let Some((rf, rt)) = retry_queue.pop_front() {
                (rf, rt)
            } else if cursor <= to_block {
                let current_chunk = chunk_size.load(Ordering::Relaxed);
                let from = cursor;
                let to = (from + current_chunk - 1).min(to_block);
                cursor = to + 1;
                (from, to)
            } else {
                break;
            };

            let task_provider = provider.clone();
            let task_cancel = cancel.clone();

            join_set.spawn(async move {
                if task_cancel.is_cancelled() {
                    return (chunk_from, chunk_to, Err(anyhow::anyhow!("cancelled")));
                }

                BLOCK_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;

                let result = fetch_blocks(&task_provider, chunk_from, chunk_to, include_txs).await;
                (chunk_from, chunk_to, result)
            });
        }

        if join_set.is_empty() {
            break;
        }

        let Some(join_result) = join_set.join_next().await else {
            break;
        };

        let (chunk_from, chunk_to, task_result) = match join_result {
            Ok(tuple) => tuple,
            Err(join_err) => {
                error!("Block fetch task panicked: {join_err}");
                cancel.cancel();
                while join_set.join_next().await.is_some() {}
                return Err(anyhow::anyhow!("block fetch task panicked: {join_err}"));
            }
        };

        match task_result {
            Ok(blocks) => {
                BLOCK_ADAPTIVE_CONCURRENCY.record_success();
                all_blocks.extend(blocks);
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

                // 3. Retryable block-range errors — shrink chunk only.
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
                if halved < chunk_to {
                    retry_queue.push_back((halved + 1, chunk_to));
                }
            }
        }
    }

    // Sort by block number since JoinSet results arrive out of order.
    all_blocks.sort_by_key(|b| b.header.number);

    Ok(all_blocks)
}
