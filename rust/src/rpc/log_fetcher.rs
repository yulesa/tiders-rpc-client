//! Log fetching: filter construction, address batching, and `eth_getLogs` calls.
//!
//! Adapted from rindexer's `get_logs()` and `get_logs_for_address_in_batches()`
//! (provider.rs:566-640).

use std::sync::Arc;
use std::time::Duration;

use alloy::{
    primitives::{Address as AlloyAddress, B256},
    providers::Provider,
    rpc::types::{Filter, Log, ValueOrArray},
};
use anyhow::{Context, Result};
use log::{error, info, warn};
use tokio::sync::Semaphore;

use super::log_adaptive_concurrency::retry_logs_with_block_range;
use super::shared_helpers::{clamp_to_block, halved_block_range, is_fatal_error};
use crate::query::LogRequest;

use super::adaptive_concurrency::{report_rpc_outcome, ADAPTIVE_CONCURRENCY};
use super::provider::RpcProvider;

/// Maximum number of addresses per `eth_getLogs` call.
/// RPC providers typically cap at ~1000.
const MAX_ADDRESSES_PER_REQUEST: usize = 1000;

/// Fetch logs for all `LogRequest`s in a query for a given block range.
///
/// Multiple `LogRequest`s are OR'd together — each produces a separate
/// `eth_getLogs` call and results are merged, then sorted by
/// `(block_number, log_index)` for deterministic ordering.
pub async fn fetch_logs(
    provider: &RpcProvider,
    requests: &[LogRequest],
    from_block: u64,
    to_block: u64,
) -> Result<Vec<Log>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }

    info!(
        "fetch_logs: requesting logs for blocks ({from_block}..={to_block})",
    );

    let mut all_logs = Vec::new();
    for req in requests {
        let logs = fetch_logs_for_request(provider, req, from_block, to_block).await?;
        all_logs.extend(logs);
    }

    // Sort by (block_number, log_index) for deterministic ordering.
    all_logs.sort_by(|a, b| {
        let block_cmp = a.block_number.cmp(&b.block_number);
        if block_cmp != std::cmp::Ordering::Equal {
            return block_cmp;
        }
        a.log_index.cmp(&b.log_index)
    });

    info!(
        "fetch_logs: received {} logs for range {from_block}..={to_block}",
        all_logs.len()
    );

    Ok(all_logs)
}

/// Fetch logs for a single `LogRequest` in a given block range.
///
/// If the request has many addresses, they are chunked into groups of
/// `MAX_ADDRESSES_PER_REQUEST` and fetched in parallel (adapted from
/// rindexer's `get_logs_for_address_in_batches()`).
pub async fn fetch_logs_for_request(
    provider: &RpcProvider,
    req: &LogRequest,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<Log>> {
    // If few or no addresses, just do a single request.
    if req.address.len() <= MAX_ADDRESSES_PER_REQUEST {
        ADAPTIVE_CONCURRENCY.wait_for_backoff().await;

        let filter = build_filter(req, from_block, to_block);
        let result = provider
            .provider
            .get_logs(&filter)
            .await
            .context("eth_getLogs failed");

        match &result {
            Ok(_) => report_rpc_outcome(&Ok(())),
            Err(e) => {
                let err_str = e.to_string();
                report_rpc_outcome(&Err(&err_str));
            }
        }

        return result;
    }

    // Build a base filter without addresses — addresses are added per chunk.
    let no_addr_req = LogRequest {
        address: vec![],
        ..req.clone()
    };
    let base_filter = build_filter(&no_addr_req, from_block, to_block);

    // Chunk addresses and spawn parallel fetches with adaptive concurrency.
    let semaphore = Arc::new(Semaphore::new(ADAPTIVE_CONCURRENCY.current()));

    let mut handles = Vec::new();
    for chunk in req.address.chunks(MAX_ADDRESSES_PER_REQUEST) {
        let addr_chunk: Vec<AlloyAddress> = chunk.iter().map(|a| AlloyAddress::from(a.0)).collect();
        let filter = base_filter.clone().address(ValueOrArray::Array(addr_chunk));
        let provider_clone = provider.clone();
        let sem = semaphore.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem
                .acquire_owned()
                .await
                .map_err(|e| anyhow::anyhow!("semaphore closed: {e}"))?;

            ADAPTIVE_CONCURRENCY.wait_for_backoff().await;

            let result = provider_clone
                .provider
                .get_logs(&filter)
                .await
                .context("eth_getLogs (batched addresses) failed");

            match &result {
                Ok(_) => report_rpc_outcome(&Ok(())),
                Err(e) => {
                    let err_str = e.to_string();
                    report_rpc_outcome(&Err(&err_str));
                }
            }

            result
        }));
    }

    let mut all_logs = Vec::new();
    for handle in handles {
        let logs = handle.await.context("address batch task panicked")??;
        all_logs.extend(logs);
    }

    Ok(all_logs)
}

/// Fetch logs for `[from_block, to_block]`, automatically splitting into narrower
/// sub-ranges on provider block-range errors and accumulating results.
pub(super) async fn fetch_logs_with_retry(
    provider: &RpcProvider,
    requests: &[LogRequest],
    from_block: u64,
    to_block: u64,
    initial_max_block_range: Option<u64>,
    retry_backoff_ms: u64,
) -> Result<Vec<Log>> {
    let mut all_logs: Vec<Log> = Vec::new();
    let mut sub_from = from_block;
    let mut max_block_range = initial_max_block_range;

    while sub_from <= to_block {
        let sub_to = clamp_to_block(sub_from, to_block, max_block_range);

        match fetch_logs(provider, requests, sub_from, sub_to).await {
            Ok(logs) => {
                all_logs.extend(logs);
                sub_from = sub_to + 1;
            }
            Err(e) => {
                let err_str = format!("{e:#}");

                if let Some(retry) =
                    retry_logs_with_block_range(&err_str, sub_from, sub_to, max_block_range)
                {
                    warn!(
                        "Log pipeline range error, retrying {}-{} (was {sub_from}-{sub_to}), backing off {retry_backoff_ms}ms",
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
                    "Log pipeline unexpected error {sub_from}-{sub_to}, \
                     retrying {sub_from}-{halved}: {err_str}"
                );
                max_block_range = Some(halved.saturating_sub(sub_from));
            }
        }
    }

    Ok(all_logs)
}

/// Build an alloy `Filter` from a single `LogRequest` and block range.
///
/// Each `LogRequest` can specify multiple addresses (OR'd by the provider)
/// and multiple values per topic slot (also OR'd by the provider).
pub fn build_filter(req: &LogRequest, from_block: u64, to_block: u64) -> Filter {
    let mut filter = Filter::new().from_block(from_block).to_block(to_block);

    if !req.address.is_empty() {
        let addrs: Vec<AlloyAddress> = req
            .address
            .iter()
            .map(|a| AlloyAddress::from(a.0))
            .collect();
        filter = filter.address(ValueOrArray::Array(addrs));
    }

    if !req.topic0.is_empty() {
        let topics: Vec<B256> = req.topic0.iter().map(|t| B256::from(t.0)).collect();
        filter = filter.event_signature(ValueOrArray::Array(topics));
    }

    if !req.topic1.is_empty() {
        let topics: Vec<B256> = req.topic1.iter().map(|t| B256::from(t.0)).collect();
        filter = filter.topic1(ValueOrArray::Array(topics));
    }

    if !req.topic2.is_empty() {
        let topics: Vec<B256> = req.topic2.iter().map(|t| B256::from(t.0)).collect();
        filter = filter.topic2(ValueOrArray::Array(topics));
    }

    if !req.topic3.is_empty() {
        let topics: Vec<B256> = req.topic3.iter().map(|t| B256::from(t.0)).collect();
        filter = filter.topic3(ValueOrArray::Array(topics));
    }

    filter
}

#[cfg(test)]
#[expect(clippy::panic)]
mod tests {
    use super::*;
    use alloy::network::AnyNetwork;
    use alloy::primitives::{address, b256};
    use alloy::providers::ProviderBuilder;

    /// Smoke test: call eth_getLogs directly via a plain alloy provider
    /// (no retry layer) to verify the RPC + filter return logs.
    #[tokio::test]
    async fn raw_alloy_get_logs() {
        let url: url::Url = "https://mainnet.gateway.tenderly.co"
            .parse()
            .unwrap_or_else(|e| panic!("bad url: {e}"));
        let provider = ProviderBuilder::new()
            .network::<AnyNetwork>()
            .connect_http(url);

        let addr = address!("ae78736cd615f374d3085123a210448e74fc6393");
        let topic0 = b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

        let filter = Filter::new()
            .from_block(18_600_000u64)
            .to_block(18_601_000u64)
            .address(ValueOrArray::Value(addr))
            .event_signature(ValueOrArray::Value(topic0));

        let logs = provider
            .get_logs(&filter)
            .await
            .unwrap_or_else(|e| panic!("eth_getLogs failed: {e}"));

        assert!(
            !logs.is_empty(),
            "expected rETH Transfer logs in blocks 18_600_000..18_601_000"
        );
    }
}
