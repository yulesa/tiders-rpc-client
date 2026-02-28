//! Block fetching and block-pipeline stream logic.
//!
//! Contains:
//! - `fetch_blocks`: low-level batch RPC call for a block range
//! - `run_block_historical`: JoinSet-based concurrent historical block fetching
//! - `run_block_fetch_task`: single fetch task (blocks + receipts → Arrow)
//! - `run_block_live`: live head-polling for blocks

use std::collections::VecDeque;
use std::time::Duration;

use alloy::network::AnyRpcBlock;
use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::ClientConfig;
use crate::convert::{
    blocks_to_record_batch, merge_tx_receipts_into_batch, select_block_columns,
    select_transaction_columns, transactions_to_record_batch,
};
use crate::query::{BlockFields, TransactionFields};
use crate::response::ArrowResponse;

use super::block_adaptive_concurrency::{retry_block_with_block_range, BLOCK_ADAPTIVE_CONCURRENCY, DEFAULT_BLOCK_CHUNK_SIZE};
use super::shared_helpers::{halved_block_range, is_fatal_error, is_rate_limit_error};
use super::provider::RpcProvider;
use super::tx_receipt_fetcher::fetch_tx_receipts;

/// Historical phase for the block pipeline: iterate `[from_block, snapshot_latest_block]`
/// in chunks, fetch blocks (and optionally tx receipts), and send Arrow responses.
///
/// Spawns multiple concurrent fetch tasks via `JoinSet`, bounded by the block
/// adaptive concurrency controller. Each task fetches a chunk of blocks,
/// optionally fetches tx receipts, converts to Arrow, and returns the response.
/// Results are sent to the channel as they complete (no ordering guarantee).
pub(super) async fn run_block_historical(
    provider: &RpcProvider,
    include_txs: bool,
    fetch_receipts_flag: bool,
    block_fields: &BlockFields,
    tx_fields: &TransactionFields,
    start_from: u64,
    snapshot_latest_block: u64,
    config: &ClientConfig,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<u64> {
    let original_max_range = config
        .batch_size
        .map(|b| (b as u64).min(DEFAULT_BLOCK_CHUNK_SIZE))
        .unwrap_or(DEFAULT_BLOCK_CHUNK_SIZE);

    // Set the original chunk size from config (persisted globally).
    BLOCK_ADAPTIVE_CONCURRENCY.set_original_chunk_size(original_max_range);

    let cancel = CancellationToken::new();
    let mut join_set: JoinSet<(u64, u64, Result<ArrowResponse>)> = JoinSet::new();
    let mut retry_queue: VecDeque<(u64, u64)> = VecDeque::new();

    // Next unassigned from_block.
    let mut cursor = start_from;

    let block_fields = *block_fields;
    let tx_fields = *tx_fields;

    loop {
        // Fill join_set up to the current concurrency limit.
        let max_concurrent = BLOCK_ADAPTIVE_CONCURRENCY.current();
        while join_set.len() < max_concurrent {
            let (chunk_from, chunk_to) = if let Some((rf, rt)) = retry_queue.pop_front() {
                // Priority: retry failed ranges first.
                (rf, rt)
            } else if cursor <= snapshot_latest_block {
                // Claim next chunk from cursor.
                let current_chunk = BLOCK_ADAPTIVE_CONCURRENCY.chunk_size();
                let from = cursor;
                let to = (from + current_chunk - 1).min(snapshot_latest_block);
                cursor = to + 1;
                (from, to)
            } else {
                // No more work to assign.
                break;
            };

            // Spawn a fetch task.
            let task_provider = provider.clone();
            let task_cancel = cancel.clone();
            let task_config = config.clone();

            join_set.spawn(async move {
                if task_cancel.is_cancelled() {
                    return (chunk_from, chunk_to, Err(anyhow::anyhow!("cancelled")));
                }

                let result = run_block_fetch_task(
                    &task_provider,
                    chunk_from,
                    chunk_to,
                    include_txs,
                    fetch_receipts_flag,
                    &block_fields,
                    &tx_fields,
                    &task_config,
                )
                .await;

                (chunk_from, chunk_to, result)
            });
        }

        // If no tasks in flight and nothing to assign, we're done.
        if join_set.is_empty() {
            break;
        }

        // Await next completion.
        let Some(join_result) = join_set.join_next().await else {
            break;
        };

        // Handle JoinError (task panic / cancellation).
        let (chunk_from, chunk_to, task_result) = match join_result {
            Ok(tuple) => tuple,
            Err(join_err) => {
                error!("Block fetch task panicked: {join_err}");
                cancel.cancel();
                // Drain remaining tasks.
                while join_set.join_next().await.is_some() {}
                return Err(anyhow::anyhow!("block fetch task panicked: {join_err}"));
            }
        };

        match task_result {
            Ok(response) => {
                BLOCK_ADAPTIVE_CONCURRENCY.record_success();

                if tx.send(Ok(response)).await.is_err() {
                    debug!("Stream consumer dropped, stopping historical block fetch");
                    cancel.cancel();
                    while join_set.join_next().await.is_some() {}
                    return Ok(cursor);
                }

                BLOCK_ADAPTIVE_CONCURRENCY.maybe_reset_chunk_size();
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

                // 3. Retryable block-range errors — shrink chunk only, no concurrency change.
                let current_range = Some(BLOCK_ADAPTIVE_CONCURRENCY.chunk_size());
                if let Some(retry) =
                    retry_block_with_block_range(&err_str, chunk_from, chunk_to, current_range)
                {
                    warn!(
                        "Block range error, re-queuing {}-{} (was {chunk_from}-{chunk_to})",
                        retry.from, retry.to
                    );
                    if let Some(new_max) = retry.max_block_range {
                        BLOCK_ADAPTIVE_CONCURRENCY.reduce_chunk_size(new_max);
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
                BLOCK_ADAPTIVE_CONCURRENCY.reduce_chunk_size(new_range);
                retry_queue.push_back((chunk_from, halved));
                if halved < chunk_to {
                    retry_queue.push_back((halved + 1, chunk_to));
                }
            }
        }
    }

    // Return cursor past the snapshot so the live phase starts correctly.
    Ok(snapshot_latest_block + 1)
}

/// Live phase for the block pipeline: poll for new blocks.
pub(super) async fn run_block_live(
    provider: &RpcProvider,
    include_txs: bool,
    fetch_receipts_flag: bool,
    block_fields: &BlockFields,
    tx_fields: &TransactionFields,
    start_from: u64,
    config: &ClientConfig,
    tx: &mpsc::Sender<Result<ArrowResponse>>,
) -> Result<()> {
    let poll_interval = Duration::from_millis(config.head_poll_interval_millis);
    let mut from_block = start_from;

    loop {
        tokio::time::sleep(poll_interval).await;

        let head = match provider.get_block_number().await {
            Ok(h) => h,
            Err(e) => {
                error!("Failed to get latest block number: {e:#}, retrying in {}ms", config.retry_backoff_ms);
                tokio::time::sleep(Duration::from_millis(config.retry_backoff_ms)).await;
                continue;
            }
        };

        let safe_head = head.saturating_sub(config.reorg_safe_distance);

        if from_block > safe_head {
            debug!("At head (from_block={from_block}, safe_head={safe_head}), waiting...");
            continue;
        }

        let to_block = safe_head;

        match run_block_fetch_task(
            provider,
            from_block,
            to_block,
            include_txs,
            fetch_receipts_flag,
            block_fields,
            tx_fields,
            config,
        )
        .await
        {
            Ok(response) => {
                if tx.send(Ok(response)).await.is_err() {
                    debug!("Stream consumer dropped, stopping live block polling");
                    return Ok(());
                }

                from_block = to_block + 1;
            }
            Err(e) => {
                let err_str = format!("{e:#}");

                if is_fatal_error(&err_str) {
                    return Err(e);
                }

                if is_rate_limit_error(&err_str) {
                    BLOCK_ADAPTIVE_CONCURRENCY.record_rate_limit();
                    BLOCK_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;
                    continue;
                }

                if let Some(retry) =
                    retry_block_with_block_range(&err_str, from_block, to_block, None)
                {
                    debug!(
                        "Live: block range error, will retry with {}-{}",
                        retry.from, retry.to
                    );
                    // Don't advance — the next iteration will retry naturally.
                } else {
                    error!(
                        "Live: unexpected error fetching blocks {from_block}-{to_block}: {err_str}, retrying in {}ms",
                        config.retry_backoff_ms
                    );
                }
            }
        }
    }
}

/// Single fetch task for blocks, transactions and receipts: fetch blocks,
/// optionally fetch tx receipts, convert to Arrow.
async fn run_block_fetch_task(
    provider: &RpcProvider,
    from_block: u64,
    to_block: u64,
    include_txs: bool,
    fetch_receipts_flag: bool,
    block_fields: &BlockFields,
    tx_fields: &TransactionFields,
    config: &ClientConfig,
) -> Result<ArrowResponse> {
    BLOCK_ADAPTIVE_CONCURRENCY.wait_for_backoff().await;

    let blocks = fetch_blocks(provider, from_block, to_block, include_txs).await?;
    let num_blocks = blocks.len();

    let blocks_batch = select_block_columns(blocks_to_record_batch(&blocks), block_fields);
    let raw_txs_batch = transactions_to_record_batch(&blocks);

    let txs_batch = if fetch_receipts_flag && raw_txs_batch.num_rows() > 0 {
        let tx_receipts = fetch_tx_receipts(provider, from_block, to_block, config).await?;
        info!(
            "Fetched {} tx receipts for blocks {from_block}-{to_block}",
            tx_receipts.len()
        );
        merge_tx_receipts_into_batch(raw_txs_batch, &tx_receipts)
    } else {
        raw_txs_batch
    };

    let txs_batch = select_transaction_columns(txs_batch, tx_fields);

    info!(
        "Fetched {num_blocks} blocks between {from_block}-{to_block} \
         ({} block rows, {} tx rows)",
        blocks_batch.num_rows(),
        txs_batch.num_rows()
    );

    Ok(ArrowResponse::with_blocks(blocks_batch, txs_batch))
}

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
