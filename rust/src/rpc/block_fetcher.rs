//! Block fetching: `eth_getBlockByNumber` batch calls.
//!
//! Fetches blocks for a range of block numbers. Block numbers are chunked
//! into sub-batches of `rpc_batch_size`, and each sub-batch is sent as a
//! single JSON-RPC batch call in parallel, bounded by the adaptive
//! concurrency semaphore.

use std::sync::Arc;
use std::time::Duration;

use alloy::network::AnyRpcBlock;
use anyhow::{Context, Result};
use log::{error, info, warn};
use tokio::sync::Semaphore;

use crate::convert::{clamp_to_block, halved_block_range, is_fatal_error, retry_with_block_range};

use super::adaptive_concurrency::{report_rpc_outcome, ADAPTIVE_CONCURRENCY};
use super::provider::RpcProvider;

/// Default JSON-RPC batch size for `eth_getBlockByNumber` calls.
pub(crate) const DEFAULT_RPC_BATCH_SIZE: usize = 50;

/// Fetch blocks for a range of block numbers.
///
/// Creates a contiguous range `[from_block, to_block]`, chunks into
/// sub-batches of `rpc_batch_size`, and fetches them via JSON-RPC batch
/// calls with bounded concurrency.
pub async fn fetch_blocks(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    include_txs: bool,
    rpc_batch_size: usize,
) -> Result<Vec<AnyRpcBlock>> {
    if from_block > to_block {
        return Ok(Vec::new());
    }

    let block_numbers: Vec<u64> = (from_block..=to_block).collect();
    let count = block_numbers.len();
    info!("fetch_blocks: requesting {count} blocks ({from_block}..={to_block})");

    let semaphore = Arc::new(Semaphore::new(ADAPTIVE_CONCURRENCY.current()));

    let handles: Vec<_> = block_numbers
        .chunks(rpc_batch_size)
        .map(|chunk| {
            let provider = provider.clone();
            let owned_chunk = chunk.to_vec();
            let sem = semaphore.clone();

            tokio::spawn(async move {
                let _permit = sem
                    .acquire_owned()
                    .await
                    .map_err(|e| anyhow::anyhow!("semaphore closed: {e}"))?;

                ADAPTIVE_CONCURRENCY.wait_for_backoff().await;

                info!(
                    "fetch_blocks: sending sub-batch of {} blocks ({}-{})",
                    owned_chunk.len(),
                    owned_chunk.first().copied().unwrap_or(0),
                    owned_chunk.last().copied().unwrap_or(0),
                );

                let result = provider
                    .get_block_batch(&owned_chunk, include_txs)
                    .await
                    .with_context(|| {
                        format!(
                            "eth_getBlockByNumber batch for blocks {}-{}",
                            owned_chunk.first().copied().unwrap_or(0),
                            owned_chunk.last().copied().unwrap_or(0),
                        )
                    });

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

    let mut all_blocks = Vec::new();
    for handle in handles {
        let blocks = handle
            .await
            .map_err(|e| anyhow::anyhow!("block batch task panicked: {e}"))??;
        all_blocks.extend(blocks);
    }

    info!(
        "fetch_blocks: received {} blocks for range {from_block}..={to_block}",
        all_blocks.len()
    );

    Ok(all_blocks)
}

/// Fetch blocks for `[from_block, to_block]`, automatically splitting into narrower
/// sub-ranges on provider block-range errors and accumulating results.
pub(super) async fn fetch_blocks_with_retry(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    include_txs: bool,
    rpc_batch_size: usize,
    initial_max_block_range: Option<u64>,
    retry_backoff_ms: u64,
) -> Result<Vec<AnyRpcBlock>> {
    let mut all_blocks: Vec<AnyRpcBlock> = Vec::new();
    let mut sub_from = from_block;
    let mut max_block_range = initial_max_block_range;

    while sub_from <= to_block {
        let sub_to = clamp_to_block(sub_from, to_block, max_block_range);

        match fetch_blocks(provider, sub_from, sub_to, include_txs, rpc_batch_size).await {
            Ok(blocks) => {
                all_blocks.extend(blocks);
                sub_from = sub_to + 1;
            }
            Err(e) => {
                let err_str = format!("{e:#}");

                if let Some(retry) =
                    retry_with_block_range(&err_str, sub_from, sub_to, max_block_range)
                {
                    warn!(
                        "Block pipeline range error, retrying {}-{} (was {sub_from}-{sub_to}), backing off {retry_backoff_ms}ms",
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
                    "Block pipeline unexpected error {sub_from}-{sub_to}, \
                     retrying {sub_from}-{halved}: {err_str}"
                );
                max_block_range = Some(halved.saturating_sub(sub_from));
            }
        }
    }

    Ok(all_blocks)
}
