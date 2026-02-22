//! Block fetching: `eth_getBlockByNumber` batch calls.
//!
//! Adapted from rindexer's `get_block_by_number_batch_with_size()`
//! (provider.rs:429-506).

use alloy::network::AnyRpcBlock;
use anyhow::{Context, Result};
use log::info;

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
