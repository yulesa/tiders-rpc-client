//! Tx receipt fetching: `eth_getBlockReceipts` batch calls.
//!
//! Fetches all transaction receipts for a range of blocks. One
//! `eth_getBlockReceipts` call per block is issued in parallel, bounded by the
//! provider's semaphore.
//!
//! If the provider does not support `eth_getBlockReceipts` the error message
//! explains why a per-transaction fallback is deliberately not implemented and
//! lists alternative providers and clients.

use std::sync::Arc;

use alloy::network::AnyTransactionReceipt;
use anyhow::{Context, Result};
use log::info;
use tokio::sync::Semaphore;

use super::provider::RpcProvider;

/// Maximum concurrent `eth_getBlockReceipts` calls.
const MAX_CONCURRENT_TX_RECEIPT_CALLS: usize = 4;

/// Fetch all tx receipts for a contiguous range of blocks.
///
/// Returns a flat `Vec` of all tx receipts, in block-number order (within each
/// block the order matches the provider's response, i.e. transaction order).
///
/// Every tx receipt is tagged with `block_number` via the receipt's own field, so
/// callers can join tx receipts to transactions by `(block_number, transaction_index)`.
///
/// Returns an error (with user-friendly guidance) if the provider does not
/// support `eth_getBlockReceipts`.
pub async fn fetch_tx_receipts(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<AnyTransactionReceipt>> {
    if from_block > to_block {
        return Ok(Vec::new());
    }

    let block_numbers: Vec<u64> = (from_block..=to_block).collect();
    let count = block_numbers.len();
    info!("fetch_tx_receipts: requesting tx receipts for {count} blocks ({from_block}..={to_block})");

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_TX_RECEIPT_CALLS));

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
                    .get_block_receipts(block_num)
                    .await
                    .with_context(|| format!("eth_getBlockReceipts for block {block_num}"))
            })
        })
        .collect();

    let mut all_tx_receipts = Vec::new();
    for handle in handles {
        let tx_receipts = handle
            .await
            .map_err(|e| anyhow::anyhow!("tx receipt fetch task panicked: {e}"))??;
        all_tx_receipts.extend(tx_receipts);
    }

    info!(
        "fetch_tx_receipts: received {} tx receipts for range {from_block}..={to_block}",
        all_tx_receipts.len()
    );

    Ok(all_tx_receipts)
}
