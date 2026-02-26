//! Block fetching: `eth_getBlockByNumber` batch calls.
//!
//! Fetches blocks for a range of block numbers. Block numbers are chunked
//! into sub-batches of `rpc_batch_size`, and each sub-batch is sent as a
//! single JSON-RPC batch call in parallel, bounded by the adaptive
//! concurrency semaphore.

use std::time::Duration;

use alloy::network::AnyRpcBlock;
use anyhow::{Context, Result};
use log::{error, info, warn};

use crate::convert::{clamp_to_block, halved_block_range, is_fatal_error, retry_block_with_block_range};

use super::block_adaptive_concurrency::{report_block_rpc_outcome, BLOCK_ADAPTIVE_CONCURRENCY};
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

    BLOCK_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;

    let result = provider
        .get_block_batch(&block_numbers, include_txs)
        .await
        .with_context(|| {
            format!("eth_getBlockByNumber batch for blocks {from_block}-{to_block}")
        });

    match &result {
        Ok(_) => report_block_rpc_outcome(&Ok(())),
        Err(e) => {
            let err_str = e.to_string();
            report_block_rpc_outcome(&Err(&err_str));
        }
    }

    let blocks = result?;

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
    retry_backoff_ms: u64,
) -> Result<Vec<AnyRpcBlock>> {
    let mut all_blocks: Vec<AnyRpcBlock> = Vec::new();
    let mut sub_from = from_block;
    // fetch_blocks sends the entire sub-range as a single RPC batch call,
    // so we always start at DEFAULT_BLOCK_CHUNK_SIZE.
    let mut max_block_range = Some(DEFAULT_BLOCK_CHUNK_SIZE as u64);

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
                    retry_block_with_block_range(&err_str, sub_from, sub_to, max_block_range)
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
