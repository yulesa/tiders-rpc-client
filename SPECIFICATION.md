# cherry-rpc-client Specification

## Overview

`cherry-rpc-client` is an EVM-only Rust client that fetches blockchain data from any standard Ethereum JSON-RPC provider and outputs it as Arrow RecordBatches conforming to the `cherry-evm-schema` format. It serves as another data provider for the `cherry` pipeline framework.
Unlike SQD and HyperSync which are specialized high-throughput data services, `cherry-rpc-client` works with **any** standard EVM RPC endpoint (Alchemy, Infura, QuickNode, local nodes, etc.), making cherry accessible to all EVM chains without requiring a specialized data provider.

## Role in the Cherry Ecosystem

```
cherry-core/ingest/
├── provider/
│   ├── sqd.rs        → uses sqd-portal-client   → SQD Network API
│   ├── hypersync.rs  → uses hypersync-client     → HyperSync API
│   └── rpc.rs        → uses cherry-rpc-client    → Any EVM JSON-RPC   ← NEW
```

`cherry-rpc-client` is an intermediary layer between `cherry-core/ingest` and any JSON-RPC node.

## Output Contract

All outputs MUST be Arrow RecordBatches conforming to the schemas defined in `cherry-evm-schema`. This is the same format produced by all providers, ensuring interchangeability within cherry-core.

### Output Tables

The client produces a response containing four Arrow RecordBatches:

| Table | Schema | Source RPC Methods |
|---|---|---|
| `blocks` | `cherry_evm_schema::blocks_schema()` | `eth_getBlockByNumber` |
| `transactions` | `cherry_evm_schema::transactions_schema()` | `eth_getBlockByNumber` (with full tx objects), `eth_getBlockReceipts` (or `eth_getTransactionReceipt` fallback) |
| `logs` | `cherry_evm_schema::logs_schema()` | `eth_getLogs` |
| `traces` | `cherry_evm_schema::traces_schema()` | `trace_block` or `debug_traceBlockByNumber` |

Each RecordBatch may be empty (zero rows) if the user didn't request that table or if no data matched the filters.

### ArrowResponse

```rust
pub struct ArrowResponse {
    pub blocks: RecordBatch,       // cherry_evm_schema::blocks_schema()
    pub transactions: RecordBatch, // cherry_evm_schema::transactions_schema()
    pub logs: RecordBatch,         // cherry_evm_schema::logs_schema()
    pub traces: RecordBatch,       // cherry_evm_schema::traces_schema()
}

impl ArrowResponse {
    /// Returns the next block number to query (max block number + 1).
    /// Used for streaming pagination.
    pub fn next_block(&self) -> Result<u64>;
}
```

This mirrors the pattern used by `sqd_portal_client::evm::ArrowResponse`.

## Input: Query

The query specifies a block range, which tables to fetch, and optional filters. The query structure is designed so that `cherry-core/ingest/provider/rpc.rs` can translate a `cherry_ingest::evm::Query` into it, just as `sqd.rs` translates to `sqd_portal_client::evm::Query`.

```rust
pub struct Query {
    pub from_block: u64,
    pub to_block: Option<u64>,       // None = open-ended (stream to head)
    pub include_all_blocks: bool,    // if true, return all blocks even without matching logs/txs

    pub logs: Vec<LogRequest>,
    pub transactions: Vec<TransactionRequest>,
    pub traces: Vec<TraceRequest>,

    pub fields: Fields,              // which columns to populate per table
}
```

### LogRequest

Filters for `eth_getLogs`. Multiple `LogRequest`s are OR'd together.

```rust
pub struct LogRequest {
    pub address: Vec<[u8; 20]>,     // filter by contract address
    pub topic0: Vec<[u8; 32]>,      // filter by event signature
    pub topic1: Vec<[u8; 32]>,      // filter by indexed param 1
    pub topic2: Vec<[u8; 32]>,      // filter by indexed param 2
    pub topic3: Vec<[u8; 32]>,      // filter by indexed param 3
}
```

### TransactionRequest

Signals that transaction data should be fetched for the requested block range. All filter fields **must be empty** — `cherry-rpc-client` will return an error if any filter is set.

```rust
pub struct TransactionRequest {
    pub from: Vec<[u8; 20]>,                    // MUST be empty — not supported
    pub to: Vec<[u8; 20]>,                      // MUST be empty — not supported
    pub sighash: Vec<[u8; 4]>,                  // MUST be empty — not supported
    pub type_: Vec<u8>,                         // MUST be empty — not supported
    pub hash: Vec<[u8; 32]>,                    // MUST be empty — not supported
    pub status: Vec<u8>,                        // MUST be empty — not supported
    pub contract_deployment_address: Vec<[u8; 20]>, // MUST be empty — not supported
}
```

**Why filters are not supported:** `eth_getBlockByNumber` returns every transaction in a block with no server-side filtering. Every block in the requested range is fetched regardless of any filter. Applying filters inside the client would silently hide that cost — the user would pay for thousands of RPC calls while receiving only a handful of rows, with no indication of the expense. If you need filtered transaction data, use SQD or HyperSync which support server-side filtering, or ingest all transactions with this client and filter post-indexing in your database.

**RPC mapping:** `eth_getBlockByNumber(blockNum, true)` returns full transaction objects for all transactions in the block. All transactions are passed through to the output — no in-flight filtering is applied.

Transaction receipt data (gasUsed, cumulativeGasUsed, effectiveGasPrice, contractAddress, status, logsBloom, etc.) is fetched via `eth_getBlockReceipts` (preferred, fetches all receipts for a block in one call) or `eth_getTransactionReceipt` per transaction (fallback).

### TraceRequest

Signals that trace data should be fetched. All filter fields **must be empty** — `cherry-rpc-client` will return an error if any filter is set, for the same reason as `TransactionRequest`: the entire block's traces are fetched regardless.

```rust
pub struct TraceRequest {
    pub type_: Vec<String>,         // MUST be empty — not supported
    pub from: Vec<[u8; 20]>,        // MUST be empty — not supported
    pub to: Vec<[u8; 20]>,          // MUST be empty — not supported
    pub sighash: Vec<[u8; 4]>,      // MUST be empty — not supported
}
```

**RPC mapping:** `trace_block(blockNum)` (Parity/OpenEthereum-style). All traces for the block are returned; filtering is the caller's responsibility post-indexing.

### Fields

Boolean flags controlling which columns to populate. Columns not requested are filled with nulls to match the schema. This allows the client to skip fetching data that isn't needed.

```rust
pub struct Fields {
    pub block: BlockFields,
    pub transaction: TransactionFields,
    pub log: LogFields,
    pub trace: TraceFields,
}
```

Each `*Fields` struct has a boolean for every column in the corresponding `cherry-evm-schema`. For example:

```rust
pub struct BlockFields {
    pub number: bool,
    pub hash: bool,
    pub parent_hash: bool,
    pub nonce: bool,
    pub sha3_uncles: bool,
    pub logs_bloom: bool,
    pub transactions_root: bool,
    pub state_root: bool,
    pub receipts_root: bool,
    pub miner: bool,
    pub difficulty: bool,
    pub total_difficulty: bool,
    pub extra_data: bool,
    pub size: bool,
    pub gas_limit: bool,
    pub gas_used: bool,
    pub timestamp: bool,
    pub base_fee_per_gas: bool,
    pub blob_gas_used: bool,
    pub excess_blob_gas: bool,
    pub parent_beacon_block_root: bool,
    pub withdrawals_root: bool,
    pub withdrawals: bool,
    pub l1_block_number: bool,
    // ... etc
}
```

**Optimization:** If no transaction fields requiring receipt data are requested, skip `eth_getTransactionReceipt` calls entirely. If no trace fields are true, skip `trace_block` calls.

## Input: ClientConfig

```rust
pub struct ClientConfig {
    pub rpc_url: String,
    pub max_num_retries: usize,
    pub retry_backoff_ms: u64,
    pub retry_base_ms: u64,
    pub retry_ceiling_ms: u64,
    pub http_req_timeout_millis: u64,
    pub max_concurrent_requests: usize,  // max parallel RPC calls
    pub batch_size: usize,               // max blocks per batch fetch
}
```

> **PROPOSED CHANGE:** Replace the above `ClientConfig` with a richer version informed by rindexer's production experience. Key changes:
>
> - **Remove** `retry_base_ms` — redundant if we use alloy's `RetryBackoffLayer` which takes a single initial backoff.
> - **Add** `compute_units_per_second: u64` (default: 660) — rate limit budget used by alloy's transport retry layer to pace requests. Providers like Alchemy expose CU budgets.
> - **Rename** `max_concurrent_requests` → `initial_concurrent_requests` (default: 20) — the client adaptively scales within [2, initial*10] based on success/failure rates: +20% after 10 consecutive successes, halve on rate limit. Static caps are either too conservative or too aggressive.
> - **Add** `rpc_batch_size: Option<usize>` (default: None = adaptive 50, range 5-100) — max calls per JSON-RPC batch request. Separate from `batch_size`: you might process 10 blocks at a time but send 50 calls per JSON-RPC batch.
> - **Add** `max_block_range: Option<u64>` (default: None) — max block range per `eth_getLogs` call. Some providers enforce limits. None = no upfront limit, rely on adaptive error recovery.
> - **Add** `trace_method: Option<TraceMethod>` (default: None = auto-detect) — which trace API to use. None tries `trace_block` first, falls back to `debug_traceBlockByNumber`.
>
> ```rust
> pub struct ClientConfig {
>     pub rpc_url: String,
>
>     // --- Retry ---
>     pub max_num_retries: usize,              // default: 5000
>     pub retry_backoff_ms: u64,               // initial backoff (default: 1000)
>     pub retry_ceiling_ms: u64,               // max backoff
>     pub compute_units_per_second: u64,       // default: 660
>
>     // --- HTTP ---
>     pub http_req_timeout_millis: u64,        // default: 90000
>
>     // --- Concurrency ---
>     pub initial_concurrent_requests: usize,  // default: 20, adaptive [2, initial*10]
>
>     // --- Batching ---
>     pub batch_size: usize,                   // max blocks per fetch cycle (default: 10)
>     pub rpc_batch_size: Option<usize>,       // max calls per JSON-RPC batch (default: None = adaptive)
>
>     // --- Block range ---
>     pub max_block_range: Option<u64>,        // max block range per eth_getLogs (default: None)
>
>     // --- Trace method ---
>     pub trace_method: Option<TraceMethod>,   // default: None = auto-detect
> }
>
> pub enum TraceMethod {
>     TraceBlock,              // Parity/OpenEthereum/Erigon
>     DebugTraceBlockByNumber, // Geth with callTracer
> }
> ```

### StreamConfig

Controls streaming behavior, same pattern as `sqd-portal-client`.

```rust
pub struct StreamConfig {
    pub stop_on_head: bool,              // stop when reaching chain head
    pub head_poll_interval_millis: u64,  // poll interval when at chain head
    pub buffer_size: usize,              // mpsc channel buffer size
}
```

> **PROPOSED CHANGE:** Add `reorg_safe_distance` to `StreamConfig`. rindexer uses `indexing_distance_from_head` throughout its live streaming to avoid processing blocks that might be reorganized. Without this, the client may return data from blocks that later get reverted.
>
> ```rust
> pub struct StreamConfig {
>     pub stop_on_head: bool,
>     pub head_poll_interval_millis: u64,
>     pub buffer_size: usize,
>     pub reorg_safe_distance: u64,      // don't process blocks within N of chain head
>                                        // 0 = disabled, typical: 12-64 depending on chain
> }
> ```

## Public API

### Client

```rust
pub struct Client { /* ... */ }

impl Client {
    pub fn new(config: ClientConfig) -> Result<Self>;

    /// Fetch a single batch of data for the given block range.
    /// Returns None if from_block is beyond the chain head.
    pub async fn query(&self, query: &Query) -> Result<Option<ArrowResponse>>;

    /// Stream data continuously, yielding ArrowResponse batches.
    /// Automatically paginates through blocks and waits at chain head.
    pub fn stream(
        self: Arc<Self>,
        query: Query,
        config: StreamConfig,
    ) -> mpsc::Receiver<Result<ArrowResponse>>;

    /// Get the current chain head block number.
    pub async fn get_height(&self) -> Result<u64>;
}
```

### Integration with cherry-core

In `cherry-core/ingest/src/provider/rpc.rs`, the integration follows the same pattern as `sqd.rs`:

```rust
pub async fn start_stream(cfg: ProviderConfig, query: crate::Query) -> Result<DataStream> {
    match query {
        Query::Svm(_) => Err(anyhow!("SVM is not supported by RPC provider")),
        Query::Evm(query) => {
            let rpc_query = evm_query_to_rpc(&query)?;
            let client = cherry_rpc_client::Client::new(/* ... */)?;
            let client = Arc::new(client);
            let receiver = client.stream(rpc_query, stream_config);

            let stream = ReceiverStream::new(receiver).map(|v| {
                v.map(|v| {
                    let mut data = BTreeMap::new();
                    data.insert("blocks".to_owned(), v.blocks);
                    data.insert("transactions".to_owned(), v.transactions);
                    data.insert("logs".to_owned(), v.logs);
                    data.insert("traces".to_owned(), v.traces);
                    data
                })
            });

            Ok(Box::pin(stream))
        }
    }
}
```

And `ProviderKind` in `cherry-core/ingest` gains a third variant:

```rust
pub enum ProviderKind {
    Sqd,
    Hypersync,
    Rpc,        // ← NEW
}
```

## Data Flow Inside cherry-rpc-client

```
Query
  │
  ├─ Determine block range (from_block..to_block or from_block..head)
  │
  ├─ For each batch of blocks (batch_size at a time):
  │   │
  │   ├─ [Parallel] eth_getBlockByNumber(N, true)  → block headers + full txs
  │   │   └─ Populate blocks RecordBatch
  │   │   └─ Populate transactions RecordBatch (from tx objects)
  │   │
  │   ├─ [Parallel] eth_getLogs({ fromBlock: N, toBlock: M, ...filters })
  │   │   └─ Populate logs RecordBatch
  │   │
  │   ├─ [Conditional, Parallel] eth_getTransactionReceipt(txHash)
  │   │   └─ Enrich transactions RecordBatch with receipt fields
  │   │   └─ Only if receipt-only fields are requested
  │   │
  │   └─ [Conditional, Parallel] trace_block(N) or debug_traceBlockByNumber(N)
  │       └─ Populate traces RecordBatch
  │       └─ Only if trace fields are requested
  │
  └─ Apply client-side filters (tx from/to/sighash, trace type/from/to)
  │
  └─ Build ArrowResponse with RecordBatches conforming to cherry-evm-schema
```

> **PROPOSED ADDITION: Phased Implementation**
>
> The data flow above shows all four RPC methods running in parallel within a batch. This is the end-state goal, but the cross-method orchestration (coordinating `eth_getBlockByNumber`, `eth_getLogs`, `eth_getBlockReceipts`, and `trace_block` in a single batch, merging tx objects with receipt data by hash, filtering across tables) is complex.
>
> A phased approach is recommended:
>
> **Phase 1 — Single-method pipelines:** Each RPC method operates as its own independent pipeline. The client supports:
> - **Logs pipeline:** `eth_getLogs` → populates `logs` RecordBatch. Closest to existing proven patterns.
> - **Blocks+Transactions pipeline:** `eth_getBlockByNumber(N, true)` → populates `blocks` and `transactions` RecordBatches. Receipts (`eth_getBlockReceipts`) can be paired here since they map 1:1 to block numbers, making the join trivial.
> - **Traces pipeline:** `trace_block` / `debug_traceBlockByNumber` → populates `traces` RecordBatch.
>
> In Phase 1, a query requesting only logs would populate the `logs` RecordBatch and return empty RecordBatches for the other three tables. Similarly for blocks-only or traces-only queries.
>
> **Phase 2 — Cross-method assembly:** Add the orchestration layer that runs pipelines in parallel within a batch and assembles the full `ArrowResponse`. This is where `include_all_blocks` logic, tx-receipt merging by hash, and field-aware dispatch across tables come in.
>
> The public API (`ArrowResponse` with four fields) remains the same in both phases — unrequested tables are simply empty RecordBatches.

## Key Design Decisions

### 1. Batching Strategy

RPC providers have rate limits, so the client must batch intelligently:

- **Block batching:** Process `batch_size` blocks at a time (default: 10). For each batch, fire parallel requests for all blocks in the range.
- **`eth_getLogs` range queries:** Use the block range variant of `eth_getLogs` to fetch logs for multiple blocks in a single call, rather than one call per block.
- **Receipt batching:** Use `eth_getBlockReceipts` as the default method (supported by all major providers: Alchemy, Infura, QuickNode, Geth, Erigon), falling back to individual `eth_getTransactionReceipt` calls if the provider does not support it.
- **Concurrency limit:** `max_concurrent_requests` caps the number of in-flight RPC calls to avoid hitting rate limits.

> **PROPOSED CHANGE:** Replace static concurrency with adaptive concurrency. rindexer's production experience showed that static limits are either too conservative (slow) or too aggressive (rate limited). The client should start at `initial_concurrent_requests` (default: 20) and adaptively scale:
> - **On success:** After 10 consecutive successes, increase concurrency by 20% and batch size by 20%.
> - **On rate limit (429):** Double the backoff delay (start at 500ms, max 30s), halve batch size (min 5) and concurrency (min 2).
> - **On error:** Reduce concurrency by 10%.
> - **Backoff jitter:** Add 0-50% random jitter to backoff delays to prevent thundering herd.
>
> This replaces the static `max_concurrent_requests` with `initial_concurrent_requests` in `ClientConfig`.

### 2. Field-Aware Fetching

The `Fields` booleans allow skipping entire categories of RPC calls:

| Condition | RPC calls skipped |
|---|---|
| No block fields requested | Skip `eth_getBlockByNumber` (but still need it if txs or logs reference block data) |
| No receipt-only fields (gasUsed, effectiveGasPrice, status, contractAddress, etc.) | Skip `eth_getTransactionReceipt` |
| No trace fields requested | Skip `trace_block` / `debug_traceBlockByNumber` |
| No log fields requested | Skip `eth_getLogs` |

### 3. Transaction Data Split

EVM RPC splits transaction data across two calls:
- **`eth_getBlockByNumber(N, true)`** returns transaction objects with: hash, from, to, value, input, gas, gasPrice, nonce, v, r, s, maxFeePerGas, maxPriorityFeePerGas, type, chainId, accessList, etc.
- **`eth_getTransactionReceipt(hash)`** returns receipt data: gasUsed, cumulativeGasUsed, effectiveGasPrice, contractAddress, status, logsBloom, root, etc.

The client merges these into a single row in the transactions RecordBatch matching `cherry_evm_schema::transactions_schema()`.

### 4. Trace Method Detection

Different nodes support different trace APIs:
- **Parity/OpenEthereum/Erigon:** `trace_block` returns structured traces
- **Geth:** `debug_traceBlockByNumber` with `callTracer` returns a different format

The client should attempt `trace_block` first and fall back to `debug_traceBlockByNumber` if it fails, or allow the user to configure which method to use.

### 5. Type Conversions (RPC JSON → Arrow)

| RPC JSON type | Arrow type (cherry-evm-schema) | Conversion |
|---|---|---|
| Hex string `"0x1a2b"` (quantity) | `Decimal256(76, 0)` | Parse hex → U256 → i256 big-endian bytes |
| Hex string `"0x..."` (data/hash) | `Binary` | Decode hex → raw bytes |
| Hex string `"0x..."` (address, 20 bytes) | `Binary` | Decode hex → 20 bytes |
| Boolean `true`/`false` | `Boolean` | Direct |
| Hex string `"0x01"` (status) | `UInt8` | Parse hex → u8 |
| Hex number (block number) | `UInt64` | Parse hex → u64 |

### 6. Null Handling

Fields that don't exist in the RPC response (e.g., `l1_fee` on non-L2 chains, `blobGasUsed` on pre-Dencun blocks) are written as nulls. This matches how SQD and HyperSync providers handle missing data.

> **PROPOSED ADDITION:**
>
> ### 7. Error Recovery & Block Range Adaptation
>
> RPC providers reject `eth_getLogs` calls that span too many blocks, but different providers return different error messages. The client must parse these errors and adapt the block range. This is based on rindexer's production-hardened `retry_with_block_range()` logic.
>
> **Provider-specific error parsing:**
>
> | Provider | Error Pattern | Recovery Action |
> |---|---|---|
> | Alchemy | `this block range should work: [0x..., 0x...]` | Use the suggested range from the error |
> | Infura, Thirdweb, zkSync, Tenderly | `try with this block range [0x..., 0x...]` | Use the suggested range from the error |
> | Ankr | `block range is too wide` | Fall back to 3000 blocks |
> | QuickNode, 1RPC, zkEVM, Blast, BlockPI | `limited to a N` | Parse N and use it as the new limit |
> | Base | `block range too large` | Fall back to 2000 blocks |
> | Generic | `response is too big` / `error decoding response body` | Halve the current block range |
> | Overload | `error sending request` | Sleep 1s, then halve the range |
>
> **Tiered fallback:** When no provider gives a hint in the error message, use a tiered fallback sequence: 5000 → 500 → 75 → 50 → 45 → 40 → 35 → 30 → 25 → 20 → 15 → 10 → 5 → 1 block(s). Each failure steps down to the next tier.
>
> **Range halving:** When halving, always advance at least 2 blocks to prevent stalling on a single problematic block.
>
> **Recovery:** With 10% probability on each successful fetch, reset the block range limit back to the original `max_block_range` config value. This allows the client to escape temporarily-reduced ranges caused by transient errors.
>
> ### 8. Latest Block Caching
>
> The `get_height()` method and internal chain-head polling should cache the latest block result with a time-based TTL to avoid redundant `eth_getBlockByNumber(latest)` calls. Default cache duration: `chain_block_time / 3` with a minimum of 500ms. This is particularly important during live streaming where head checks happen on every iteration.
>
> ### 9. LogsBloom Pre-filtering (Live Streaming)
>
> During live streaming (polling at chain head), the client should check each new block's LogsBloom filter before issuing `eth_getLogs` calls. If the bloom filter proves that no matching events (by address or topic) exist in a block, the `eth_getLogs` call can be skipped entirely. This avoids unnecessary RPC calls for blocks that contain no relevant data, which is the common case during live streaming where most blocks won't match the query filters.

## RPC Methods Used

| Method | When Used | Notes |
|---|---|---|
| `eth_blockNumber` | `get_height()`, stream pagination | Get current chain head |
| `eth_getBlockByNumber` | Always (when blocks or txs requested) | With `true` for full tx objects |
| `eth_getLogs` | When logs are requested | Supports address + topic filters |
| `eth_getTransactionReceipt` | Fallback when `eth_getBlockReceipts` unavailable | Per-transaction; batched via JSON-RPC batch calls |
| `eth_getBlockReceipts` | Default when receipt fields are requested | Supported by all major providers (Alchemy, Infura, QuickNode, Geth, Erigon). More efficient than per-tx receipt calls. |
| `trace_block` | When traces are requested | Parity/Erigon style |
| `debug_traceBlockByNumber` | Fallback when traces requested | Geth style with `callTracer` |

## Differences from SQD/HyperSync Providers

| Aspect | SQD / HyperSync | RPC |
|---|---|---|
| Server-side filtering | Yes (logs, txs, traces) | Only `eth_getLogs` has server-side filters |
| Throughput | Very high (bulk data) | Limited by RPC rate limits |
| Data completeness | Full historical data, pre-indexed | Depends on node (archive vs full) |
| Chain support | Specific supported chains | Any EVM chain with an RPC |
| Traces | Always available | Requires archive node + trace API |
| Cost | Free tier / paid plans | Depends on RPC provider |

## License

MIT OR Apache-2.0 (same as cherry-core and sqd-portal-client).
