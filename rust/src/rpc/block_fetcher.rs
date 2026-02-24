//! Block fetching: `eth_getBlockByNumber` batch calls.
//!
//! Adapted from rindexer's `get_block_by_number_batch_with_size()`
//! (provider.rs:429-506).

use std::time::Duration;

use alloy::network::AnyRpcBlock;
use anyhow::{Context, Result};
use log::{error, info, warn};

use crate::convert::{clamp_to_block, halved_block_range, is_fatal_error, retry_with_block_range};

use super::provider::RpcProvider;

/// Fetch blocks for a range of block numbers.
///
/// This creates a contiguous range `[from_block, to_block]` and fetches them
/// via JSON-RPC batch calls. All blocks in the range are returned;
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
        .get_blocks_by_number(&block_numbers, include_txs)
        .await
        .context("eth_getBlockByNumber batch failed")?;

    info!(
        "fetch_blocks: received {} blocks for range {from_block}..={to_block}",
        blocks.len()
    );

    Ok(blocks)
}

/// Fetch blocks for `[from_block, to_block]`, automatically splitting into narrower
/// sub-ranges on provider block-range errors and accumulating results.
pub(super) async fn fetch_blocks_with_retry(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    include_txs: bool,
    initial_max_block_range: Option<u64>,
) -> Result<Vec<AnyRpcBlock>> {
    let mut all_blocks: Vec<AnyRpcBlock> = Vec::new();
    let mut sub_from = from_block;
    let mut max_block_range = initial_max_block_range;

    while sub_from <= to_block {
        let sub_to = clamp_to_block(sub_from, to_block, max_block_range);

        match fetch_blocks(provider, sub_from, sub_to, include_txs).await {
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
                        "Block pipeline range error, retrying {}-{} (was {sub_from}-{sub_to})",
                        retry.from, retry.to
                    );
                    if retry.backoff {
                        tokio::time::sleep(Duration::from_secs(1)).await;
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
