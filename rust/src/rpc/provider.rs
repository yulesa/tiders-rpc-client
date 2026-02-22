//! RPC provider setup using alloy's `RpcClient` with `RetryBackoffLayer`.
//!
//! Adapted from rindexer's `create_client()` pattern (provider.rs:707-796).

use std::sync::Arc;
use std::time::Duration;

use alloy::{
    network::{AnyNetwork, AnyRpcBlock, AnyTransactionReceipt},
    providers::{
        fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller},
        Identity, Provider, ProviderBuilder, RootProvider,
    },
    rpc::{
        client::RpcClient,
        types::BlockNumberOrTag,
    },
    transports::{
        http::{reqwest, Http},
        layers::RetryBackoffLayer,
    },
};
use anyhow::{Context, Result};
use log::{error, info};
use tokio::sync::Semaphore;
use url::Url;

use crate::config::ClientConfig;

/// The concrete alloy provider type with default fillers.
type AlloyProvider = FillProvider<
    JoinFill<
        Identity,
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
    >,
    RootProvider<AnyNetwork>,
    AnyNetwork,
>;

/// Default JSON-RPC batch size for `eth_getBlockByNumber` calls.
const DEFAULT_RPC_BATCH_SIZE: usize = 50;

/// Maximum concurrent sub-batches within a single batch block request.
const MAX_CONCURRENT_SUB_BATCHES: usize = 2;

/// A thin wrapper around an alloy provider.
#[derive(Debug, Clone)]
pub struct RpcProvider {
    pub provider: AlloyProvider,
    /// The underlying `RpcClient` — needed for JSON-RPC batch calls which
    /// bypass the higher-level `Provider` trait.
    pub rpc_client: RpcClient,
    /// JSON-RPC batch size for block/receipt batch requests.
    pub rpc_batch_size: usize,
}

impl RpcProvider {
    /// Create a new RPC provider from client configuration.
    ///
    /// Sets up:
    /// - HTTP transport with configurable timeout
    /// - `RetryBackoffLayer` for automatic transport-level retries with
    ///   exponential backoff and compute-unit rate limiting
    pub fn new(config: &ClientConfig) -> Result<Self> {
        let rpc_url = Url::parse(&config.url).context("invalid RPC URL")?;

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.req_timeout_millis))
            .build()
            .context("failed to build HTTP client")?;

        let http = Http::with_client(http_client, rpc_url);

        let retry_layer = RetryBackoffLayer::new(
            config.max_num_retries,
            config.retry_backoff_ms,
            config.compute_units_per_second.unwrap_or(660),
        );

        let rpc_client = RpcClient::builder()
            .layer(retry_layer)
            .transport(http, false);

        let provider = ProviderBuilder::new()
            .network::<AnyNetwork>()
            .connect_client(rpc_client.clone());

        let rpc_batch_size = config.rpc_batch_size.unwrap_or(DEFAULT_RPC_BATCH_SIZE);

        Ok(Self {
            provider,
            rpc_client,
            rpc_batch_size,
        })
    }

    /// Fetch the latest block number from the provider.
    pub async fn get_block_number(&self) -> Result<u64> {
        let number = self
            .provider
            .get_block_number()
            .await
            .context("failed to get block number")?;
        Ok(number)
    }

    /// Fetch blocks by number in batched JSON-RPC calls.
    ///
    /// Adapted from rindexer's `get_block_by_number_batch_with_size()`
    /// (provider.rs:440-506). Deduplicates block numbers, chunks into
    /// sub-batches of `rpc_batch_size`, and executes with bounded concurrency.
    pub async fn get_blocks_by_number(
        &self,
        block_numbers: &[u64],
        include_txs: bool,
    ) -> Result<Vec<AnyRpcBlock>> {
        if block_numbers.is_empty() {
            return Ok(Vec::new());
        }

        // Deduplicate while preserving order.
        let mut seen = std::collections::HashSet::new();
        let deduped: Vec<u64> = block_numbers
            .iter()
            .copied()
            .filter(|n| seen.insert(*n))
            .collect();

        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_SUB_BATCHES));

        let futures: Vec<_> = deduped
            .chunks(self.rpc_batch_size)
            .map(|chunk| {
                let client = self.rpc_client.clone();
                let owned_chunk = chunk.to_vec();
                let sem = semaphore.clone();

                tokio::spawn(async move {
                    let _permit = sem
                        .acquire_owned()
                        .await
                        .map_err(|e| anyhow::anyhow!("semaphore closed: {e}"))?;

                    info!(
                        "get_blocks_by_number: sending sub-batch of {} blocks ({}-{})",
                        owned_chunk.len(),
                        owned_chunk.first().copied().unwrap_or(0),
                        owned_chunk.last().copied().unwrap_or(0),
                    );

                    let mut batch = client.new_batch();
                    let mut request_futures = Vec::with_capacity(owned_chunk.len());

                    for block_num in &owned_chunk {
                        let params =
                            (BlockNumberOrTag::Number(*block_num), include_txs);
                        let call = batch
                            .add_call("eth_getBlockByNumber", &params)
                            .map_err(|e| anyhow::anyhow!("failed to add batch call: {e}"))?;
                        request_futures.push(call);
                    }

                    if let Err(e) = batch.send().await {
                        error!(
                            "Failed to send batch eth_getBlockByNumber ({} blocks): {e:?}",
                            request_futures.len()
                        );
                        return Err(anyhow::anyhow!("batch send failed: {e}"));
                    }

                    let mut results: Vec<Option<AnyRpcBlock>> =
                        Vec::with_capacity(request_futures.len());
                    for f in request_futures {
                        let block = f
                            .await
                            .map_err(|e| anyhow::anyhow!("batch response error: {e}"))?;
                        results.push(block);
                    }

                    Ok::<Vec<AnyRpcBlock>, anyhow::Error>(
                        results.into_iter().flatten().collect(),
                    )
                })
            })
            .collect();

        let mut all_blocks = Vec::new();
        for handle in futures {
            let blocks = handle
                .await
                .map_err(|e| anyhow::anyhow!("block batch task panicked: {e}"))??;
            all_blocks.extend(blocks);
        }

        Ok(all_blocks)
    }

    /// Fetch all transaction receipts for a single block via `eth_getBlockReceipts`.
    ///
    /// This is a non-standard but widely-supported RPC method (Alchemy, QuickNode,
    /// Infura, Chainstack, dRPC, …) that returns all receipts in one call instead
    /// of one call per transaction. If the provider does not support the method it
    /// returns an error whose message contains guidance to the user.
    pub async fn get_block_receipts(
        &self,
        block_number: u64,
    ) -> Result<Vec<AnyTransactionReceipt>> {
        let params = (BlockNumberOrTag::Number(block_number),);
        self.rpc_client
            .request::<_, Option<Vec<AnyTransactionReceipt>>>("eth_getBlockReceipts", params)
            .await
            .map_err(|e| {
                let msg = format!("{e}");
                if msg.contains("Method not found")
                    || msg.contains("method not found")
                    || msg.contains("eth_getBlockReceipts")
                    || msg.contains("not supported")
                    || msg.contains("unsupported")
                {
                    anyhow::anyhow!(
                        "eth_getBlockReceipts is not supported by this RPC provider.\n\
                         \n\
                         Fetching receipts one-by-one (eth_getTransactionReceipt) is deliberately \
                         not implemented because it requires one HTTP request per transaction — \
                         for a block with 200 transactions that is 200 extra round-trips per block, \
                         which is extremely slow and expensive.\n\
                         \n\
                         To fetch receipt fields (gas_used, status, effective_gas_price, …) please:\n\
                         • Switch to a provider that supports eth_getBlockReceipts:\n\
                           Alchemy, QuickNode, Infura, Chainstack, dRPC, Moralis, GetBlock\n\
                         • Or use a cherry client that does not rely on RPC for receipts:\n\
                           cherry-sqd-client  — SQD Network (free, archive, fast)\n\
                           cherry-hypersync-client — HyperSync (Envio, very fast)\n\
                         \n\
                         Original provider error: {msg}"
                    )
                } else {
                    anyhow::anyhow!("eth_getBlockReceipts failed for block {block_number}: {msg}")
                }
            })
            .map(|opt| opt.unwrap_or_default())
    }
}
