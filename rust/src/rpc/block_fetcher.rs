//! Block fetching: `eth_getBlockByNumber` batch calls and transaction filtering.
//!
//! Adapted from rindexer's `get_block_by_number_batch_with_size()`
//! (provider.rs:429-506).

use std::collections::HashSet;

use alloy::consensus::Transaction;
use alloy::eips::Typed2718;
use alloy::network::primitives::BlockTransactions;
use alloy::network::{AnyRpcBlock, AnyRpcTransaction, TransactionResponse};
use anyhow::{Context, Result};
use log::info;

use crate::query::TransactionRequest;

use super::provider::RpcProvider;

/// Fetch blocks (with full transaction objects) for a range of block numbers.
///
/// This creates a contiguous range `[from_block, to_block]` and fetches them
/// via JSON-RPC batch calls.
pub async fn fetch_blocks(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<AnyRpcBlock>> {
    if from_block > to_block {
        return Ok(Vec::new());
    }

    let block_numbers: Vec<u64> = (from_block..=to_block).collect();
    let count = block_numbers.len();
    info!("fetch_blocks: requesting {count} blocks ({from_block}..={to_block})");

    let blocks = provider
        .get_blocks_by_number(&block_numbers, true)
        .await
        .context("eth_getBlockByNumber batch failed")?;

    info!(
        "fetch_blocks: received {} blocks for range {from_block}..={to_block}",
        blocks.len()
    );

    Ok(blocks)
}

/// Check if a transaction matches any of the given `TransactionRequest` filters.
///
/// If `tx_requests` is empty, all transactions match (no filtering).
/// Otherwise, a transaction matches if it satisfies at least one request.
/// Within a single request, all non-empty filter fields must match (AND).
pub fn tx_matches_any_request(
    tx: &AnyRpcTransaction,
    tx_requests: &[TransactionRequest],
) -> bool {
    if tx_requests.is_empty() {
        return true;
    }
    tx_requests.iter().any(|req| tx_matches_request(tx, req))
}

/// Check if a single transaction matches a single `TransactionRequest`.
///
/// Every non-empty filter field must match (AND semantics).
/// Within each field, any value matching suffices (OR semantics).
fn tx_matches_request(tx: &AnyRpcTransaction, req: &TransactionRequest) -> bool {
    // from
    if !req.from.is_empty() {
        let sender = tx.from();
        if !req.from.iter().any(|a| a.0 == sender.0 .0) {
            return false;
        }
    }

    // to
    if !req.to.is_empty() {
        match tx.to() {
            Some(to_addr) => {
                if !req.to.iter().any(|a| a.0 == to_addr.0 .0) {
                    return false;
                }
            }
            None => return false, // contract creation doesn't match a `to` filter
        }
    }

    // sighash (first 4 bytes of input)
    if !req.sighash.is_empty() {
        let input = tx.input();
        if input.len() < 4 {
            return false;
        }
        let sig = &input[..4];
        if !req.sighash.iter().any(|s| s.0 == sig) {
            return false;
        }
    }

    // type
    if !req.type_.is_empty() {
        let ty = tx.ty();
        if !req.type_.contains(&ty) {
            return false;
        }
    }

    // hash
    if !req.hash.is_empty() {
        let tx_hash = tx.tx_hash();
        if !req.hash.iter().any(|h| h.0 == tx_hash.0) {
            return false;
        }
    }

    // status and contract_deployment_address require receipt data — skip for now

    true
}

/// Filter blocks and their transactions according to query parameters.
///
/// Returns `(blocks_for_block_rows, blocks_for_tx_rows)`:
/// - `blocks_for_block_rows`: blocks to convert into block Arrow rows.
///   If `include_all_blocks`, all input blocks; otherwise only those with
///   matching transactions.
/// - `blocks_for_tx_rows`: copies of blocks with only matching transactions,
///   used to convert into transaction Arrow rows.
///
/// When `tx_requests` is empty, all transactions are included.
pub fn filter_blocks(
    blocks: Vec<AnyRpcBlock>,
    tx_requests: &[TransactionRequest],
    include_all_blocks: bool,
) -> (Vec<AnyRpcBlock>, Vec<AnyRpcBlock>) {
    if tx_requests.is_empty() {
        let tx_blocks = blocks.clone();
        return (blocks, tx_blocks);
    }

    let mut blocks_with_matches: HashSet<u64> = HashSet::new();
    let mut tx_filtered_blocks: Vec<AnyRpcBlock> = Vec::with_capacity(blocks.len());

    for block in &blocks {
        let block_num = block.inner.header.inner.number;

        match &block.inner.transactions {
            BlockTransactions::Full(txns) => {
                let matching: Vec<AnyRpcTransaction> = txns
                    .iter()
                    .filter(|tx| tx_matches_any_request(tx, tx_requests))
                    .cloned()
                    .collect();

                if !matching.is_empty() {
                    blocks_with_matches.insert(block_num);
                }

                let mut filtered = block.clone();
                filtered.inner.transactions = BlockTransactions::Full(matching);
                tx_filtered_blocks.push(filtered);
            }
            _ => {
                tx_filtered_blocks.push(block.clone());
            }
        }
    }

    let block_rows = if include_all_blocks {
        blocks
    } else {
        blocks
            .into_iter()
            .filter(|b| blocks_with_matches.contains(&b.inner.header.inner.number))
            .collect()
    };

    let tx_rows: Vec<AnyRpcBlock> = tx_filtered_blocks
        .into_iter()
        .filter(|b| {
            include_all_blocks
                || blocks_with_matches.contains(&b.inner.header.inner.number)
        })
        .collect();

    (block_rows, tx_rows)
}
