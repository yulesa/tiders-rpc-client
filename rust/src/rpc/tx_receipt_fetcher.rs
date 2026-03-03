//! Tx receipt fetching: `eth_getBlockReceipts` batch calls.
//!
//! Fetches all transaction receipts for a range of blocks. One
//! `eth_getBlockReceipts` call per block is issued in parallel, bounded by a
//! semaphore sized to `SINGLE_BLOCK_ADAPTIVE_CONCURRENCY.current()`.
//!
//! Each block is retried independently up to `config.max_num_retries` times.
//! A rate-limit error triggers adaptive backoff; a fatal error stops the
//! stream immediately; exhausting all retries propagates an error.
//!
//! If the provider does not support `eth_getBlockReceipts` the error message
//! explains why a per-transaction fallback is deliberately not implemented and
//! lists alternative providers and clients.

use std::sync::Arc;
use std::time::Duration;

use alloy::network::AnyTransactionReceipt;
use anyhow::{Context, Result};
use log::{error, info, warn};
use tokio::sync::Semaphore;

use super::provider::RpcProvider;
use super::shared_helpers::{is_fatal_error, is_rate_limit_error};
use super::single_block_adaptive_concurrency::{
    report_rpc_outcome, SINGLE_BLOCK_ADAPTIVE_CONCURRENCY,
};
use crate::config::ClientConfig;

/// Fetch all tx receipts for a contiguous range of blocks.
///
/// Returns a flat `Vec` of all tx receipts, in block-number order (within each
/// block the order matches the provider's response, i.e. transaction order).
///
/// Every tx receipt is tagged with `block_number` via the receipt's own field, so
/// callers can join tx receipts to transactions by `(block_number, transaction_index)`.
///
/// Blocks are fetched in parallel, bounded by a semaphore sized to
/// `SINGLE_BLOCK_ADAPTIVE_CONCURRENCY.current()` at call time.
/// Each block is retried independently up to `config.max_num_retries` times.
pub async fn fetch_tx_receipts(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    config: &ClientConfig,
) -> Result<Vec<AnyTransactionReceipt>> {
    if from_block > to_block {
        return Ok(Vec::new());
    }

    let block_numbers: Vec<u64> = (from_block..=to_block).collect();
    let count = block_numbers.len();
    info!(
        "fetch_tx_receipts: requesting tx receipts for {count} blocks ({from_block}..={to_block})"
    );

    // Semaphore to limit concurrent block fetches according to the adaptive concurrency controller.
    // Concurrency level changes take effect at the next chunk boundary, not mid-chunk.
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
                        .get_block_receipts(block_num)
                        .await
                        .with_context(|| format!("eth_getBlockReceipts for block {block_num}"));

                    match result {
                        Ok(receipts) => {
                            report_rpc_outcome(&Ok(()));
                            return Ok(receipts);
                        }
                        Err(e) => {
                            let err_str = format!("{e:#}");

                            if is_fatal_error(&err_str) {
                                error!(
                                    "Fatal error fetching tx receipts for block {block_num}: {err_str}"
                                );
                                report_rpc_outcome(&Err(&err_str));
                                return Err(e);
                            }

                            if is_rate_limit_error(&err_str) {
                                warn!(
                                    "Rate limit fetching tx receipts for block {block_num} \
                                     (attempt {attempt}/{max_retries}): {err_str}"
                                );
                                report_rpc_outcome(&Err(&err_str));
                                // backoff is managed by the controller; wait_for_backoff()
                                // at the top of the next iteration will apply it.
                            } else {
                                warn!(
                                    "Transient error fetching tx receipts for block {block_num} \
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
