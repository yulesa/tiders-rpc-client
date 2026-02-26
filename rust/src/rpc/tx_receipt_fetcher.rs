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
use log::{error, info, warn};
use tokio::sync::Semaphore;

use crate::convert::{clamp_to_block, halved_block_range, is_fatal_error, retry_logs_with_block_range};

use super::adaptive_concurrency::{report_rpc_outcome, ADAPTIVE_CONCURRENCY};
use super::provider::RpcProvider;

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
    info!(
        "fetch_tx_receipts: requesting tx receipts for {count} blocks ({from_block}..={to_block})"
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
                    .get_block_receipts(block_num)
                    .await
                    .with_context(|| format!("eth_getBlockReceipts for block {block_num}"));

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

/// Fetch tx receipts for `[from_block, to_block]`, automatically splitting into narrower
/// sub-ranges on provider block-range errors and accumulating results.
pub(super) async fn fetch_tx_receipts_with_retry(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    initial_max_block_range: Option<u64>,
) -> Result<Vec<AnyTransactionReceipt>> {
    let mut all_receipts: Vec<AnyTransactionReceipt> = Vec::new();
    let mut sub_from = from_block;
    let mut max_block_range = initial_max_block_range;

    while sub_from <= to_block {
        let sub_to = clamp_to_block(sub_from, to_block, max_block_range);

        match fetch_tx_receipts(provider, sub_from, sub_to).await {
            Ok(receipts) => {
                all_receipts.extend(receipts);
                sub_from = sub_to + 1;
            }
            Err(e) => {
                let err_str = format!("{e:#}");

                if let Some(retry) =
                    retry_logs_with_block_range(&err_str, sub_from, sub_to, max_block_range)
                {
                    warn!(
                        "Tx receipt range error, retrying {}-{} (was {sub_from}-{sub_to})",
                        retry.from, retry.to
                    );
                    if retry.backoff {
                        ADAPTIVE_CONCURRENCY.wait_for_backoff().await;
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
                    "Tx receipt pipeline unexpected error {sub_from}-{sub_to}, \
                     retrying {sub_from}-{halved}: {err_str}"
                );
                max_block_range = Some(halved.saturating_sub(sub_from));
            }
        }
    }

    Ok(all_receipts)
}
