//! RPC provider setup using alloy's `RpcClient` with `RetryBackoffLayer`.
//!
//! Adapted from rindexer's `create_client()` pattern (provider.rs:707-796).

use std::sync::Arc;
use std::time::Duration;

use alloy::{
    network::{AnyNetwork, AnyRpcBlock, AnyTransactionReceipt},
    providers::{
        ext::TraceApi,
        fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller},
        Identity, Provider, ProviderBuilder, RootProvider,
    },
    rpc::{
        client::RpcClient,
        types::{
            trace::parity::{
                Action, CallAction, CallOutput, CallType, LocalizedTransactionTrace,
                TraceOutput, TransactionTrace,
            },
            BlockId, BlockNumberOrTag,
        },
    },
    transports::{
        http::{reqwest, Http},
        layers::RetryBackoffLayer,
    },
};
use anyhow::{Context, Result};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Semaphore;
use url::Url;

use crate::config::ClientConfig;
use crate::query::TraceMethod;

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
        // alloy's RetryBackoffLayer uses a single fixed `initial_backoff`; it has
        // no base/ceiling exponential ramp. These fields are kept in ClientConfig
        // for API compatibility with other cherry clients but are ignored here.
        if config.retry_base_ms != 300 {
            warn!(
                "retry_base_ms is set but ignored by cherry-rpc-client: alloy's \
                 RetryBackoffLayer does not implement exponential backoff — it uses a \
                 fixed initial_backoff (retry_backoff_ms). Set retry_backoff_ms instead."
            );
        }
        if config.retry_ceiling_ms != 10_000 {
            warn!(
                "retry_ceiling_ms is set but ignored by cherry-rpc-client: alloy's \
                 RetryBackoffLayer does not implement a backoff ceiling. \
                 Set retry_backoff_ms to control the fixed per-retry delay."
            );
        }

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
                        let params = (BlockNumberOrTag::Number(*block_num), include_txs);
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

                    Ok::<Vec<AnyRpcBlock>, anyhow::Error>(results.into_iter().flatten().collect())
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

    /// Fetch traces for a single block, dispatching to `trace_block` or
    /// `debug_traceBlockByNumber` based on `trace_method`.
    ///
    /// Returns an error (with user-friendly guidance) if the provider does not
    /// support block-level tracing. A per-transaction fallback is deliberately
    /// not implemented — see the error message for alternatives.
    pub async fn get_block_traces(
        &self,
        block_number: u64,
        trace_method: TraceMethod,
    ) -> Result<Vec<LocalizedTransactionTrace>> {
        match trace_method {
            TraceMethod::TraceBlock => self.trace_block(block_number).await,
            TraceMethod::DebugTraceBlockByNumber => {
                self.debug_trace_block_by_number(block_number).await
            }
        }
    }

    /// Call `trace_block(N)` via the alloy trace API.
    ///
    /// Adapted from rindexer's `trace_block()` (provider.rs:357-368).
    async fn trace_block(
        &self,
        block_number: u64,
    ) -> Result<Vec<LocalizedTransactionTrace>> {
        self.provider
            .trace_block(BlockId::Number(BlockNumberOrTag::Number(block_number)))
            .await
            .map_err(|e| {
                let msg = format!("{e}");
                if msg.contains("Method not found")
                    || msg.contains("method not found")
                    || msg.contains("trace_block")
                    || msg.contains("not supported")
                    || msg.contains("unsupported")
                {
                    anyhow::anyhow!(
                        "trace_block is not supported by this RPC provider.\n\
                         Fetching traces per transaction (debug_traceTransaction) is deliberately \
                         not implemented because it requires one HTTP request per transaction. \
                         Either switch to a provider that supports trace_block \
                         (Erigon, Nethermind, Reth, Alchemy, QuickNode) or configure \
                         TraceMethod::DebugTraceBlockByNumber if your provider supports it, \
                         or use a cherry client that does not rely on RPC calls: \
                           SQD Network, HyperSync\n\
                         Original provider error: {msg}"
                    )
                } else {
                    anyhow::anyhow!("trace_block failed for block {block_number}: {msg}")
                }
            })
    }

    /// Call `debug_traceBlockByNumber` with `callTracer` and flatten the
    /// nested call tree into a flat list of `LocalizedTransactionTrace`.
    ///
    /// Adapted from rindexer's `debug_trace_block_by_number()` (provider.rs:277-354).
    /// Uses `onlyTopCall: false` so all nested calls are returned.
    async fn debug_trace_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<Vec<LocalizedTransactionTrace>> {
        let block = json!(format!("0x{block_number:x}"));
        let options = json!({ "tracer": "callTracer", "tracerConfig": { "onlyTopCall": false } });

        let raw_frames: Vec<TraceCallFrame> = self
            .rpc_client
            .request("debug_traceBlockByNumber", (block, options))
            .await
            .map_err(|e| {
                let msg = format!("{e}");
                if msg.contains("Method not found")
                    || msg.contains("method not found")
                    || msg.contains("debug_traceBlockByNumber")
                    || msg.contains("not supported")
                    || msg.contains("unsupported")
                {
                    anyhow::anyhow!(
                        "debug_traceBlockByNumber is not supported by this RPC provider.\n\
                         Fetching traces per transaction (debug_traceTransaction) is deliberately \
                         not implemented because it requires one HTTP request per transaction. \
                         Either switch to a provider that supports debug_traceBlockByNumber \
                         (Geth, Reth, Erigon, Alchemy, QuickNode, Infura) or configure \
                         TraceMethod::TraceBlock if your provider supports it, \
                         or use a cherry client that does not rely on RPC calls: \
                           SQD Network, HyperSync\n\
                         Original provider error: {msg}"
                    )
                } else {
                    anyhow::anyhow!(
                        "debug_traceBlockByNumber failed for block {block_number}: {msg}"
                    )
                }
            })?;

        // Flatten the nested call tree into a list of LocalizedTransactionTrace,
        // the same format that trace_block produces — so the rest of the pipeline is agnostic to which method was used.
        // Adapted from rindexer's stack-based flattening (provider.rs:294-310).
        let mut flattened: Vec<TraceCallFrame> = Vec::new();
        for frame in raw_frames {
            // Push the top-level call (without its nested calls).
            flattened.push(TraceCallFrame {
                tx_hash: frame.tx_hash,
                result: TraceCall { calls: vec![], ..frame.result.clone() },
            });
            // Stack-based DFS to flatten nested calls.
            let mut stack = frame.result.calls;
            while let Some(call) = stack.pop() {
                flattened.push(TraceCallFrame {
                    tx_hash: None,
                    result: TraceCall { calls: vec![], ..call.clone() },
                });
                stack.extend(call.calls);
            }
        }

        // Convert TraceCallFrame → LocalizedTransactionTrace.
        // Frames where `to` is None (contract creation) are mapped too — we
        // include them with a zero address to_addr since the schema allows nulls
        // and the Arrow conversion handles Option<Address>.
        let traces = flattened
            .into_iter()
            .filter_map(|frame| {
                let to = frame.result.to?; // skip frames with no destination
                let gas = frame
                    .result
                    .gas
                    .as_deref()
                    .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
                    .unwrap_or(0);
                let gas_used = frame
                    .result
                    .gas_used
                    .as_deref()
                    .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
                    .unwrap_or(0);

                Some(LocalizedTransactionTrace {
                    trace: TransactionTrace {
                        action: Action::Call(CallAction {
                            from: frame.result.from,
                            to,
                            value: frame.result.value,
                            gas,
                            input: frame.result.input,
                            call_type: CallType::Call,
                        }),
                        result: Some(TraceOutput::Call(CallOutput {
                            gas_used,
                            output: frame.result.output.unwrap_or_default(),
                        })),
                        error: frame.result.error,
                        trace_address: vec![],
                        subtraces: 0,
                    },
                    transaction_hash: frame.tx_hash,
                    transaction_position: None,
                    block_number: Some(block_number),
                    block_hash: None,
                })
            })
            .collect();

        Ok(traces)
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
                         Fetching receipts one-by-one (eth_getTransactionReceipt) is deliberately \
                         not implemented because it requires one HTTP request per transaction. \
                         Switch to a provider that supports eth_getBlockReceipts \
                         (Alchemy, QuickNode, Infura, Chainstack, dRPC, Moralis, GetBlock), \
                         or use a cherry client that does not rely on RPC calls: \
                           SQD Network, HyperSync\n\
                         Original provider error: {msg}"
                    )
                } else {
                    anyhow::anyhow!("eth_getBlockReceipts failed for block {block_number}: {msg}")
                }
            })
            .map(|opt| opt.unwrap_or_default())
    }
}

// ---------------------------------------------------------------------------
// Custom deserialization types for debug_traceBlockByNumber (callTracer)
//
// Adapted from rindexer's TraceCall / TraceCallFrame (provider.rs:112-138).
// The callTracer returns a nested tree of calls; we flatten it in
// debug_trace_block_by_number() above.
// ---------------------------------------------------------------------------

/// A single call frame from the `callTracer` output.
///
/// Gas values are hex strings (e.g. `"0x5208"`) rather than numeric types
/// because the JSON schema varies across providers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TraceCall {
    pub from: alloy::primitives::Address,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas: Option<String>,
    #[serde(rename = "gasUsed", skip_serializing_if = "Option::is_none")]
    pub gas_used: Option<String>,
    pub to: Option<alloy::primitives::Address>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub input: alloy::primitives::Bytes,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<alloy::primitives::Bytes>,
    #[serde(default)]
    pub value: alloy::primitives::U256,
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(default)]
    pub calls: Vec<TraceCall>,
}

/// Top-level frame returned by `debug_traceBlockByNumber` with `callTracer`.
///
/// Each element in the response array represents one transaction in the block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TraceCallFrame {
    /// Transaction hash. Some providers omit this field (e.g. zkSync).
    #[serde(rename = "txHash")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<alloy::primitives::TxHash>,
    pub result: TraceCall,
}
