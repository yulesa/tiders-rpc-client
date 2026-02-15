# cherry-rpc-client vs rindexer: RPC Interface Comparison

This document maps each area of the `cherry-rpc-client` specification to corresponding rindexer code, identifying what can be reused, adapted, or must be written from scratch.

## 1. Provider Setup & Configuration

### cherry-rpc-client spec

```rust
pub struct ClientConfig {
    pub rpc_url: String,
    pub max_num_retries: usize,
    pub retry_backoff_ms: u64,
    pub retry_base_ms: u64,
    pub retry_ceiling_ms: u64,
    pub http_req_timeout_millis: u64,
    pub max_concurrent_requests: usize,
    pub batch_size: usize,
}
```

### rindexer implementation

**File:** [core/src/provider.rs:707-796](../rindexer/core/src/provider.rs#L707-L796) - `create_client()`

rindexer creates its provider via alloy's `RpcClient::builder()` with two Tower layers:

```rust
// provider.rs:750
let retry_layer = RetryBackoffLayer::new(5000, 1000, compute_units_per_second.unwrap_or(660));
// 5000 max retries, 1000ms initial backoff, 660 compute units/sec
```

- **HTTP timeout:** 90 seconds hardcoded ([provider.rs:744](../rindexer/core/src/provider.rs#L744))
- **Retry:** Delegated to alloy's `RetryBackoffLayer` — exponential backoff with compute-unit rate limiting
- **Transport:** `reqwest::Client` with custom headers → `Http` transport → `RpcClient`

**Cached provider struct:** [provider.rs:74-85](../rindexer/core/src/provider.rs#L74-L85) - `JsonRpcCachedProvider`
```rust
pub struct JsonRpcCachedProvider {
    provider: Arc<RindexerProvider>,
    client: RpcClient,                           // For batch requests
    cache: Mutex<Option<(Instant, Arc<AnyRpcBlock>)>>,  // Latest block cache
    is_zk_chain: bool,
    pub chain: Chain,
    pub max_block_range: Option<U64>,
    // ...
}
```

### Comparison

| Aspect | cherry-rpc-client spec | rindexer |
|---|---|---|
| Retry mechanism | Custom (max_num_retries, backoff params) | alloy `RetryBackoffLayer` (5000 retries, 1000ms base) |
| HTTP timeout | Configurable (`http_req_timeout_millis`) | Hardcoded 90s |
| Concurrency control | `max_concurrent_requests` in config | Global `AdaptiveConcurrency` singleton + semaphores |
| Batch size | `batch_size` in config | Adaptive (`ADAPTIVE_CONCURRENCY.current_batch_size()`, starts at 50) |
| Block caching | Not specified | Time-based latest-block cache (`cache` field) |
| ZK chain detection | Not specified | Probes `zks_L1ChainId`, adjusts trace behavior |

### Reuse potential

- **High:** The alloy `RetryBackoffLayer` + `RpcClient::builder()` pattern is a clean, well-tested approach. cherry-rpc-client could use the same alloy stack rather than implementing custom retry logic.
- **Adapt:** The `JsonRpcCachedProvider` wrapper pattern is useful but needs simplification — cherry-rpc-client doesn't need manifest/network concepts, just a URL + config.
- **Skip:** ZK chain detection, IPC support, chain state notifications, address filtering modes — these are rindexer-specific.

---

## 2. eth_getLogs — Log Fetching

### cherry-rpc-client spec

```
LogRequest { address, topic0, topic1, topic2, topic3 }
```
Maps to `eth_getLogs` with `{ fromBlock, toBlock, address, topics }`. Multiple `LogRequest`s are OR'd together. Uses block range variant for multi-block fetches.

### rindexer implementation

**Filter construction:** [provider.rs:566-619](../rindexer/core/src/provider.rs#L566-L619) - `get_logs()`
```rust
let base_filter = Filter::new()
    .event_signature(event_filter.event_signature())  // topic0
    .topic1(event_filter.topic1())
    .topic2(event_filter.topic2())
    .topic3(event_filter.topic3())
    .from_block(event_filter.from_block())
    .to_block(event_filter.to_block());
```

**Address handling** has three modes ([provider.rs:583-612](../rindexer/core/src/provider.rs#L583-L612)):
1. `InMemory` — fetch all logs, filter addresses client-side
2. `MaxAddressPerGetLogsRequest(n)` — chunk addresses into groups of `n`
3. Default — chunk at 1000 addresses per request (`DEFAULT_RPC_SUPPORTED_ACCOUNT_FILTERS`)

**Address batching:** [provider.rs:622-640](../rindexer/core/src/provider.rs#L622-L640) - `get_logs_for_address_in_batches()`
- Chunks address set, creates parallel `get_logs()` calls per chunk via `try_join_all`

**Filter type:** [event/rindexer_event_filter.rs:127-253](../rindexer/core/src/event/rindexer_event_filter.rs#L127-L253) - `RindexerEventFilter`
- Three variants: `Address`, `Filter`, `Factory`
- Holds topic filters, block range, contract addresses

**Streaming pipeline:** [indexer/fetch_logs.rs:30-149](../rindexer/core/src/indexer/fetch_logs.rs#L30-L149) - `fetch_logs_stream()`
- Returns `impl Stream<Item = Result<FetchLogsResult, ...>>`
- Uses bounded `mpsc::channel` (default buffer: 4) for backpressure
- Spawns producer task that loops through block ranges
- Transitions from historical to live mode automatically

**Historical log fetch:** [indexer/fetch_logs.rs:157-386](../rindexer/core/src/indexer/fetch_logs.rs#L157-L386) - `fetch_historic_logs_stream()`
- Calls `cached_provider.get_logs(&current_filter)`
- On success: advances `from_block` to `last_log.block_number + 1`
- On error: calls `retry_with_block_range()` to parse error and shrink range

**Block range calculation:** [indexer/fetch_logs.rs:1024-1039](../rindexer/core/src/indexer/fetch_logs.rs#L1024-L1039) - `calculate_process_historic_log_to_block()`
```rust
fn calculate_process_historic_log_to_block(
    new_from_block: &U64,
    snapshot_to_block: &U64,
    max_block_range_limitation: &Option<U64>,
) -> U64 {
    if let Some(max) = max_block_range_limitation {
        let to = new_from_block + max;
        if to > *snapshot_to_block { *snapshot_to_block } else { to }
    } else {
        *snapshot_to_block  // No limit: fetch entire range at once
    }
}
```

### Comparison

| Aspect | cherry-rpc-client spec | rindexer |
|---|---|---|
| Filter model | `Vec<LogRequest>` (OR'd together) | Single `RindexerEventFilter` per event type |
| Address batching | Not specified | Chunks at 1000 addresses, parallel fetch |
| Block range queries | Yes (multi-block `eth_getLogs`) | Yes, with adaptive range shrinking |
| Backpressure | Not specified | Bounded mpsc channel (buffer=4) |
| Live polling | `StreamConfig.head_poll_interval_millis` | 200ms loop with LogsBloom pre-filtering |
| Multiple event types | Multiple `LogRequest`s in one query | One stream per event type |

### Reuse potential

- **High:** The `fetch_logs_stream()` / `fetch_historic_logs_stream()` pattern — bounded channel producer/consumer with adaptive block ranges — is directly applicable.
- **High:** Address chunking logic for large address sets.
- **Adapt:** rindexer processes one event type per stream. cherry-rpc-client needs to handle multiple `LogRequest`s per query, which maps better to multiple `eth_getLogs` calls with different filters, merged into a single response.
- **High:** The `retry_with_block_range()` function (see section 6) is extremely valuable.

---

## 3. eth_getBlockByNumber — Block Fetching

### cherry-rpc-client spec

`eth_getBlockByNumber(blockNum, true)` returns block headers + full transaction objects. Used to populate both the `blocks` and `transactions` RecordBatches.

### rindexer implementation

**Single block (cached):** [provider.rs:208-250](../rindexer/core/src/provider.rs#L208-L250) - `get_latest_block()`
- Time-based cache: only fetches if cache has expired
- Cache duration derived from `block_poll_frequency` config

**Batch block fetch:** [provider.rs:429-506](../rindexer/core/src/provider.rs#L429-L506) - `get_block_by_number_batch_with_size()`
- Uses JSON-RPC batch requests via `client.new_batch()`
- Batch size from `ADAPTIVE_CONCURRENCY.current_batch_size()` (default 50)
- Max 2 concurrent sub-batches via `Semaphore::new(2)` ([provider.rs:459](../rindexer/core/src/provider.rs#L459))
- Deduplicates block numbers before fetching
- Filters out `None` results (blocks the node doesn't have)

```rust
// provider.rs:473-476
for block_num in owned_chunk {
    let params = (BlockNumberOrTag::Number(block_num.to()), include_txs);
    let call = batch.add_call("eth_getBlockByNumber", &params)?;
    request_futures.push(call)
}
```

**Block number fetch:** [provider.rs:252-256](../rindexer/core/src/provider.rs#L252-L256) - `get_block_number()`
- Simple `provider.get_block_number().await`

### Comparison

| Aspect | cherry-rpc-client spec | rindexer |
|---|---|---|
| Batch fetching | `batch_size` blocks at a time, parallel | JSON-RPC batch calls, adaptive size (default 50) |
| Full tx objects | `eth_getBlockByNumber(N, true)` | Same, with `include_txs` param |
| Latest block caching | Not specified | Time-based cache with configurable TTL |
| Deduplication | Not specified | Deduplicates block numbers in batch |

### Reuse potential

- **High:** The `get_block_by_number_batch_with_size()` function — JSON-RPC batching with semaphore-controlled concurrency — is directly reusable.
- **Adapt:** cherry-rpc-client always needs `include_txs=true` when transactions are requested, but should support `include_txs=false` when only block headers are needed.
- **High:** Latest block caching pattern for `get_height()`.

---

## 4. eth_getTransactionReceipt — Receipt Fetching

### cherry-rpc-client spec

Receipt data (gasUsed, cumulativeGasUsed, effectiveGasPrice, contractAddress, status, logsBloom) fetched via `eth_getTransactionReceipt` for matched transactions. Optimization: use `eth_getBlockReceipts` where available, falling back to individual calls.

### rindexer implementation

**Batch receipt fetch:** [provider.rs:509-563](../rindexer/core/src/provider.rs#L509-L563) - `get_tx_receipts_batch()`
```rust
// Batch size: min(1000, adaptive_batch_size * 10)
let batch_size = std::cmp::min(RPC_CHUNK_SIZE, ADAPTIVE_CONCURRENCY.current_batch_size() * 10);
```
- Uses JSON-RPC batch with `eth_getTransactionReceipt` per hash
- **Does NOT use `eth_getBlockReceipts`** — always individual receipt calls

```rust
// provider.rs:532-539
for hash in owned_chunk {
    let call = batch.add_call("eth_getTransactionReceipt", &(hash,))?;
    request_futures.push(call)
}
```

### Comparison

| Aspect | cherry-rpc-client spec | rindexer |
|---|---|---|
| `eth_getBlockReceipts` | Preferred, with fallback | NOT used |
| `eth_getTransactionReceipt` | Fallback | Only method used (batched) |
| Conditional fetching | Skip if no receipt fields requested | Always fetches when needed for native transfers |
| Batch approach | Not detailed | JSON-RPC batch, chunks of min(1000, adaptive*10) |

### Reuse potential

- **High:** The `get_tx_receipts_batch()` batching pattern is directly reusable for the fallback path.
- **New:** cherry-rpc-client should add `eth_getBlockReceipts` as the preferred method (not in rindexer). This is more efficient when fetching receipts for many/all transactions in a block.
- **Adapt:** cherry-rpc-client needs conditional receipt fetching based on `Fields` — only fetch if receipt-only fields are requested.

---

## 5. trace_block / debug_traceBlockByNumber — Trace Fetching

### cherry-rpc-client spec

`trace_block(blockNum)` (Parity/Erigon) or `debug_traceBlockByNumber(blockNum, {"tracer": "callTracer"})` (Geth). Client filters results locally by type, from, to, sighash.

### rindexer implementation

**trace_block:** [provider.rs:357-368](../rindexer/core/src/provider.rs#L357-L368)
```rust
pub async fn trace_block(&self, block_number: U64) -> Result<Vec<LocalizedTransactionTrace>, ProviderError> {
    let traces = self.provider
        .trace_block(BlockId::Number(BlockNumberOrTag::Number(block_number.as_limbs()[0])))
        .await?;
    Ok(traces)
}
```

**debug_traceBlockByNumber:** [provider.rs:277-354](../rindexer/core/src/provider.rs#L277-L354)
- Uses `raw_request("debug_traceBlockByNumber", [block, options])`
- ZK chains: `{"tracer": "callTracer"}` ([provider.rs:284](../rindexer/core/src/provider.rs#L284))
- Non-ZK: `{"tracer": "callTracer", "tracerConfig": {"onlyTopCall": true}}` ([provider.rs:286](../rindexer/core/src/provider.rs#L286))
- **Flattens nested call tree** using stack-based iteration ([provider.rs:294-310](../rindexer/core/src/provider.rs#L294-L310)):
```rust
let mut stack = vec![];
stack.extend(trace.result.calls.into_iter());
while let Some(call) = stack.pop() {
    flattened_calls.push(TraceCallFrame { tx_hash: None, result: TraceCall { calls: vec![], ..call } });
    stack.extend(call.calls.into_iter());
}
```
- **Converts `TraceCallFrame` → `LocalizedTransactionTrace`** ([provider.rs:312-351](../rindexer/core/src/provider.rs#L312-L351)) — maps to alloy's parity trace types

**Trace method dispatch:** [indexer/native_transfer.rs:268-280](../rindexer/core/src/indexer/native_transfer.rs#L268-L280) - `provider_trace_call()`
```rust
match config.method {
    TraceProcessingMethod::TraceBlock => provider.trace_block(block).await,
    TraceProcessingMethod::DebugTraceBlockByNumber => provider.debug_trace_block_by_number(block).await,
    _ => unimplemented!("Unsupported trace method"),
}
```

**Block consumer for traces:** [indexer/native_transfer.rs:283-344](../rindexer/core/src/indexer/native_transfer.rs#L283-L344) - `native_transfer_block_consumer()`
- Fetches blocks with `get_block_by_number_batch(block_numbers, true)`
- Extracts native transfers: `input.is_empty() && !value.is_zero() && to.is_some()`

**Custom deserialization types:** [provider.rs:112-138](../rindexer/core/src/provider.rs#L112-L138)
```rust
pub struct TraceCall {
    pub from: Address,
    pub gas: Option<String>,
    pub gas_used: U256,
    pub to: Option<Address>,
    pub input: Bytes,
    pub value: U256,
    pub typ: String,
    pub calls: Vec<TraceCall>,  // nested
}
```

### Comparison

| Aspect | cherry-rpc-client spec | rindexer |
|---|---|---|
| `trace_block` | Primary method | Supported via alloy `TraceApi` |
| `debug_traceBlockByNumber` | Fallback | Supported via `raw_request`, custom deserialization |
| Call tree flattening | Not specified | Stack-based iteration to flatten nested calls |
| Auto-detection | Try `trace_block` first, fall back | User configures method in YAML |
| Client-side filtering | type, from, to, sighash | Only native transfer filtering (value > 0, empty input) |
| Batching | Not specified | Individual calls per block (no batch) |

### Reuse potential

- **High:** `trace_block()` is a direct alloy API call — trivially reusable.
- **High:** `debug_trace_block_by_number()` — the raw request, custom deserialization types (`TraceCall`, `TraceCallFrame`), and call tree flattening logic are directly reusable.
- **Adapt:** rindexer only filters for native transfers. cherry-rpc-client needs general trace filtering by type/from/to/sighash — new code, but the underlying fetch is the same.
- **New:** cherry-rpc-client should consider whether to auto-detect trace method or require configuration.

---

## 6. Retry Logic & Error Handling

### cherry-rpc-client spec

`ClientConfig` has `max_num_retries`, `retry_backoff_ms`, `retry_base_ms`, `retry_ceiling_ms`.

### rindexer implementation

rindexer has **three layers** of retry/error handling:

#### Layer 1: Transport-level retry (alloy built-in)
[provider.rs:722/750](../rindexer/core/src/provider.rs#L750) — `RetryBackoffLayer::new(5000, 1000, 660)`
- Max 5000 retries, 1000ms initial backoff, 660 compute units/second
- Handles transport errors, HTTP errors, rate limits at the RPC client level

#### Layer 2: Adaptive backoff before every request
[layer_extensions.rs:76-83](../rindexer/core/src/layer_extensions.rs#L76-L83) — `RpcLoggingService`
```rust
let backoff_ms = ADAPTIVE_CONCURRENCY.current_backoff_ms();
if backoff_ms > 0 {
    let jitter = (backoff_ms as f64 * rand::random::<f64>() * 0.5) as u64;
    tokio::time::sleep(Duration::from_millis(backoff_ms + jitter)).await;
}
```
On 429/rate-limit: [layer_extensions.rs:113](../rindexer/core/src/layer_extensions.rs#L113) calls `ADAPTIVE_CONCURRENCY.record_rate_limit()`

#### Layer 3: Application-level block range retry
[indexer/fetch_logs.rs:725-931](../rindexer/core/src/indexer/fetch_logs.rs#L725-L931) — `retry_with_block_range()`

**Provider-specific error parsing** (regex-based):

| Provider | Error Pattern | Action | Lines |
|---|---|---|---|
| Alchemy | `this block range should work: [0x..., 0x...]` | Use suggested range | [754-793](../rindexer/core/src/indexer/fetch_logs.rs#L754-L793) |
| Infura, Thirdweb, zkSync, Tenderly | `try with this block range [0x..., 0x...]` | Use suggested range | [797-814](../rindexer/core/src/indexer/fetch_logs.rs#L797-L814) |
| Ankr | `block range is too wide` | Fallback to 3000 | [817-828](../rindexer/core/src/indexer/fetch_logs.rs#L817-L828) |
| QuickNode, 1RPC, zkEVM, Blast, BlockPI | `limited to a ([\d,.]+)` | Use parsed limit | [831-849](../rindexer/core/src/indexer/fetch_logs.rs#L831-L849) |
| Base | `block range too large` | Fallback to 2000 | [852-863](../rindexer/core/src/indexer/fetch_logs.rs#L852-L863) |
| Generic | `response is too big` | Halve range | [866-875](../rindexer/core/src/indexer/fetch_logs.rs#L866-L875) |
| Overload | `error sending request` | Sleep 1s + halve | [878-885](../rindexer/core/src/indexer/fetch_logs.rs#L878-L885) |

**Tiered fallback:** [indexer/fetch_logs.rs:933-1022](../rindexer/core/src/indexer/fetch_logs.rs#L933-L1022) — `FallbackBlockRange`
- When no provider hint: 5000 → 500 → 75 → 50 → 45 → 40 → 35 → 30 → 25 → 20 → 15 → 10 → 5 → 1

**Range halving utility:** [helpers/evm_log.rs:141-144](../rindexer/core/src/helpers/evm_log.rs#L141-L144)
```rust
pub fn halved_block_number(to_block: U64, from_block: U64) -> U64 {
    let halved_range = (to_block - from_block) / U64::from(2);
    (from_block + halved_range).max(from_block + U64::from(2))  // always advance at least 2
}
```

### Comparison

| Aspect | cherry-rpc-client spec | rindexer |
|---|---|---|
| Transport retry | Custom config params | alloy `RetryBackoffLayer` (5000/1000ms/660CU) |
| Rate limit handling | Not detailed | Adaptive backoff with jitter + batch size reduction |
| Block range errors | Not specified | Provider-specific regex parsing (Alchemy, Infura, QuickNode, etc.) |
| Fallback strategy | Not specified | Tiered fallback: 5000→500→75→...→1 |
| Recovery | Not specified | 10% random chance to reset to original range |

### Reuse potential

- **Very High:** `retry_with_block_range()` is the single most valuable piece of code for cherry-rpc-client. It encodes hard-won knowledge about how different providers behave. Should be extracted and reused almost verbatim.
- **Very High:** `FallbackBlockRange` tiered fallback.
- **High:** The `RetryBackoffLayer` + `RpcLoggingLayer` pattern.
- **Adapt:** cherry-rpc-client's `ClientConfig` retry params could configure the `RetryBackoffLayer` instead of reimplementing retry logic.

---

## 7. Concurrency Control

### cherry-rpc-client spec

`max_concurrent_requests` caps in-flight RPC calls.

### rindexer implementation

**Global adaptive controller:** [adaptive_concurrency.rs:12-32](../rindexer/core/src/adaptive_concurrency.rs#L12-L32) — `AdaptiveConcurrency`
- Lock-free (atomic-based): `current`, `batch_size`, `backoff_ms`, `consecutive_successes`
- Global singleton ([adaptive_concurrency.rs:226-232](../rindexer/core/src/adaptive_concurrency.rs#L226-L232)):
```rust
pub static ADAPTIVE_CONCURRENCY: Lazy<AdaptiveConcurrency> = Lazy::new(|| {
    AdaptiveConcurrency::new(20, 2, 200)  // initial=20, min=2, max=200
});
```

**Scaling behavior:**
- `record_success()` ([adaptive_concurrency.rs:82-146](../rindexer/core/src/adaptive_concurrency.rs#L82-L146)):
  - Reduces backoff by 25%
  - After 10 consecutive successes: +20% concurrency, +20% batch size
- `record_rate_limit()` ([adaptive_concurrency.rs:148-200](../rindexer/core/src/adaptive_concurrency.rs#L148-L200)):
  - Doubles backoff (starts 500ms, max 30s)
  - Halves batch size (min 5) and concurrency (min 2)
- `record_error()` ([adaptive_concurrency.rs:203-221](../rindexer/core/src/adaptive_concurrency.rs#L203-L221)):
  - -10% concurrency

**Semaphores:**
- `Semaphore::new(2)` in block batch requests ([provider.rs:459](../rindexer/core/src/provider.rs#L459))
- `Semaphore::new(callback_concurrency)` for event processing ([indexer/process.rs:91](../rindexer/core/src/indexer/process.rs#L91))

**Native transfer concurrency:** [indexer/native_transfer.rs:175-266](../rindexer/core/src/indexer/native_transfer.rs#L175-L266)
- Independent ramp: starts at 5, max 50
- On error: reduce by 20%
- On 429: sleep 2s, retry
- On success: 10% chance to double (capped)

### Comparison

| Aspect | cherry-rpc-client spec | rindexer |
|---|---|---|
| Model | Static `max_concurrent_requests` | Adaptive: scales up/down based on success/failure |
| Scope | Per-client | Global singleton across all operations |
| Batch size | Static `batch_size` | Adaptive: starts 50, scales 5-100 |
| Backoff | Not detailed | Adaptive with jitter, 0-30s range |

### Reuse potential

- **High:** The `AdaptiveConcurrency` module is well-designed and self-contained. cherry-rpc-client could use it as-is (per-client instance rather than global singleton).
- **Adapt:** Make it per-`Client` rather than global, and allow initial values from `ClientConfig`.
- **High:** The native transfer concurrency ramp pattern (start low, 10% random increase, aggressive backoff) is useful for trace fetching.

---

## 8. Streaming & Pagination

### cherry-rpc-client spec

```rust
pub fn stream(self: Arc<Self>, query: Query, config: StreamConfig) -> mpsc::Receiver<Result<ArrowResponse>>;

pub struct StreamConfig {
    pub stop_on_head: bool,
    pub head_poll_interval_millis: u64,
    pub buffer_size: usize,
}
```

### rindexer implementation

**Log stream:** [indexer/fetch_logs.rs:30-149](../rindexer/core/src/indexer/fetch_logs.rs#L30-L149) - `fetch_logs_stream()`
- Bounded `mpsc::channel` with configurable buffer (default 4)
- Producer task: historical fetch loop → live polling loop
- Returns `impl Stream` (wrapped `ReceiverStream`)

**Live polling:** [indexer/fetch_logs.rs:391-711](../rindexer/core/src/indexer/fetch_logs.rs#L391-L711) - `live_indexing_stream()`
- ~200ms iteration target
- LogsBloom optimization: [helpers/evm_log.rs:107-137](../rindexer/core/src/helpers/evm_log.rs#L107-L137) — `is_relevant_block()` checks bloom filter before calling `eth_getLogs`
- Warns if channel is full (backpressure)
- Supports reorg-safe distance

**Block stream (for traces):** [indexer/native_transfer.rs:109-173](../rindexer/core/src/indexer/native_transfer.rs#L109-L173) - `native_transfer_block_fetch()`
- `mpsc::Sender<U64>` with 4096 buffer
- Pushes individual block numbers
- Respects `end_block` for historical, continues for live

### Comparison

| Aspect | cherry-rpc-client spec | rindexer |
|---|---|---|
| Stream type | `mpsc::Receiver<Result<ArrowResponse>>` | `impl Stream<Item = Result<FetchLogsResult>>` |
| Buffer/backpressure | `StreamConfig.buffer_size` | Configurable channel size (default 4) |
| Stop at head | `StreamConfig.stop_on_head` | `force_no_live_indexing` param |
| Poll interval | `StreamConfig.head_poll_interval_millis` | ~200ms hardcoded loop |
| LogsBloom optimization | Not specified | `is_relevant_block()` skips irrelevant blocks |
| Reorg safety | Not specified | `indexing_distance_from_head` |

### Reuse potential

- **High:** The bounded-channel producer/consumer pattern with historical→live transition.
- **High:** LogsBloom pre-filtering for live mode.
- **Adapt:** rindexer streams raw logs. cherry-rpc-client needs to assemble full `ArrowResponse` (blocks + txs + logs + traces) before sending through the channel.
- **Consider:** Reorg safety via `indexing_distance_from_head` is important for production use — should be added to `StreamConfig` or `ClientConfig`.

---

## 9. Type Conversions

### cherry-rpc-client spec

| RPC JSON type | Arrow type | Conversion |
|---|---|---|
| Hex string (quantity) | Decimal256(76, 0) | Parse hex → U256 → i256 bytes |
| Hex string (data/hash) | Binary | Decode hex → raw bytes |
| Boolean | Boolean | Direct |
| Hex string (status) | UInt8 | Parse hex → u8 |
| Hex number (block number) | UInt64 | Parse hex → u64 |

### rindexer implementation

rindexer uses **alloy types** throughout — the RPC JSON → Rust conversion is handled by alloy's deserialization:
- `AnyRpcBlock` — blocks with all fields
- `AnyTransactionReceipt` — receipts
- `Log` — log entries
- `LocalizedTransactionTrace` — traces

**Key alloy types used:** [provider.rs:1-30](../rindexer/core/src/provider.rs#L1-L30)
- `Address` (20 bytes), `Bytes`, `TxHash`/`B256` (32 bytes), `U256`, `U64`
- `Filter`, `ValueOrArray`, `Log`
- `Action`, `CallAction`, `CallType`, `LocalizedTransactionTrace`, `TransactionTrace`

**Custom types for debug_trace:** [provider.rs:112-138](../rindexer/core/src/provider.rs#L112-L138) — `TraceCall`, `TraceCallFrame` with `serde::Deserialize`

rindexer does NOT convert to Arrow — it works with alloy types directly and writes to databases.

### Comparison

| Aspect | cherry-rpc-client | rindexer |
|---|---|---|
| JSON → Rust types | Needs alloy deserialization | alloy handles this |
| Rust → Arrow | Core requirement (RecordBatch output) | Not applicable |
| Hex parsing | Via alloy types | Via alloy types |

### Reuse potential

- **High:** Using alloy for RPC JSON deserialization is the right approach — same as rindexer.
- **New:** The alloy-types → Arrow conversion is entirely new work specific to cherry-rpc-client. This is the main area where rindexer provides no code to reuse.
- **Reference:** `cherry-evm-schema` already has Arrow builders. The conversion logic maps alloy fields to these builders.

---

## 10. Data Flow Comparison

### cherry-rpc-client spec

```
Query → block range → parallel RPC calls → client-side filtering → Arrow RecordBatches → ArrowResponse
```

### rindexer flow

```
Manifest → Event Registration → fetch_logs_stream (producer/consumer)
  → get_logs() per event per block range
  → handle_logs_result() → trigger_event() → callbacks (DB/CSV/streams)
```

**Key architectural difference:** rindexer processes one event type per stream pipeline, with each event getting its own `fetch_logs_stream`. cherry-rpc-client processes entire blocks at once, fetching all data types (blocks, txs, logs, traces) in parallel per batch.

### Reuse potential

- **Adapt:** rindexer's per-event streaming model doesn't match cherry-rpc-client's per-batch model. The individual RPC call functions are reusable, but the orchestration layer needs to be written fresh.
- **Pattern:** The producer/consumer with backpressure is the right pattern, just applied at a different granularity.

### Implication: phased implementation via single-method pipelines

The spec's `ArrowResponse` bundles four tables (blocks, transactions, logs, traces) that come from different RPC methods. The full vision is to fetch all of them in parallel within a batch and assemble them together. However, this multi-method orchestration — coordinating `eth_getBlockByNumber`, `eth_getLogs`, `eth_getBlockReceipts`, and `trace_block` in a single batch, merging tx objects with receipt data by hash, filtering across tables — is the most complex new work with no rindexer counterpart.

rindexer sidesteps this entirely: each pipeline only calls **one RPC method**. The event pipelines only call `eth_getLogs` ([fetch_logs.rs:30-149](../rindexer/core/src/indexer/fetch_logs.rs#L30-L149)). The trace pipeline only calls `eth_getBlockByNumber` ([native_transfer.rs:283-344](../rindexer/core/src/indexer/native_transfer.rs#L283-L344)). They never combine data from multiple RPC methods into a single output.

This suggests a **phased approach** for cherry-rpc-client:

**Phase 1 — Single-method pipelines:** Each RPC method becomes its own independent pipeline, similar to how rindexer does it. The client could initially support:
- Logs-only: `eth_getLogs` pipeline → populates `logs` RecordBatch (closest to rindexer's `fetch_logs_stream`, most code reuse)
- Blocks-only: `eth_getBlockByNumber` pipeline → populates `blocks` and `transactions` RecordBatches (similar to rindexer's `native_transfer_block_consumer` pattern)
- Receipts could be paired with blocks from the start since `eth_getBlockReceipts` maps 1:1 to block numbers, making the join trivial — fetch blocks, then fetch receipts for the same block numbers, merge by block number + tx index

**Phase 2 — Cross-method assembly:** Once the individual pipelines work, add the orchestration layer that runs them in parallel within a batch and assembles the full `ArrowResponse`. This is where the spec's `include_all_blocks` logic, tx-receipt merging by hash, and field-aware dispatch come in.

This phased approach maximizes rindexer code reuse in Phase 1 (each pipeline mirrors a rindexer pattern directly) and isolates the genuinely new orchestration work in Phase 2.

---

## Summary: Reuse Map

| Component | Reuse Level | Source in rindexer | Notes |
|---|---|---|---|
| HTTP client setup | **High** | [provider.rs:707-796](../rindexer/core/src/provider.rs#L707-L796) | alloy `RpcClient` + `RetryBackoffLayer` |
| `eth_getLogs` with filters | **High** | [provider.rs:566-619](../rindexer/core/src/provider.rs#L566-L619) | Filter construction, address batching |
| `eth_getBlockByNumber` batch | **High** | [provider.rs:429-506](../rindexer/core/src/provider.rs#L429-L506) | JSON-RPC batch with semaphore |
| `eth_getTransactionReceipt` batch | **High** | [provider.rs:509-563](../rindexer/core/src/provider.rs#L509-L563) | JSON-RPC batch |
| `trace_block` | **High** | [provider.rs:357-368](../rindexer/core/src/provider.rs#L357-L368) | Direct alloy API |
| `debug_traceBlockByNumber` | **High** | [provider.rs:277-354](../rindexer/core/src/provider.rs#L277-L354) | Custom types + call flattening |
| Block range error parsing | **Very High** | [fetch_logs.rs:725-931](../rindexer/core/src/indexer/fetch_logs.rs#L725-L931) | Provider-specific regex patterns |
| Fallback block ranges | **Very High** | [fetch_logs.rs:933-1022](../rindexer/core/src/indexer/fetch_logs.rs#L933-L1022) | Tiered 5000→1 fallback |
| Adaptive concurrency | **High** | [adaptive_concurrency.rs](../rindexer/core/src/adaptive_concurrency.rs) | Lock-free, atomic-based scaling |
| Streaming with backpressure | **High** | [fetch_logs.rs:30-149](../rindexer/core/src/indexer/fetch_logs.rs#L30-L149) | Bounded mpsc channel pattern |
| LogsBloom pre-filtering | **Medium** | [helpers/evm_log.rs:107-137](../rindexer/core/src/helpers/evm_log.rs#L107-L137) | Useful for live mode optimization |
| Latest block caching | **Medium** | [provider.rs:208-250](../rindexer/core/src/provider.rs#L208-L250) | Time-based cache |
| `eth_getBlockReceipts` | **New** | Not in rindexer | Must be implemented fresh |
| Alloy → Arrow conversion | **New** | Not in rindexer | Core new work for cherry-rpc-client |
| Multi-table query orchestration | **New** | Not in rindexer | Parallel fetch of blocks+txs+logs+traces |
| Client-side tx/trace filtering | **New** | Not in rindexer | rindexer only filters native transfers |
| Field-aware fetch optimization | **New** | Not in rindexer | Skip RPC calls based on `Fields` booleans |
