# Building cherry-rpc-client in Parts

This document describes how to build cherry-rpc-client incrementally, delivering value at each step. Each part builds on the previous one. The public API (`ArrowResponse` with four RecordBatch fields) stays the same throughout — unrequested or unimplemented tables are empty RecordBatches.

## Part 1: Scaffolding — Client, Config, and Arrow output types

The shell of the crate. No RPC calls yet.

**What to build:**
- `Client::new(config)`, `ClientConfig`, `StreamConfig`
- `Query`, `LogRequest`, `TransactionRequest`, `TraceRequest`, `Fields`, `TraceMethod`
- `ArrowResponse` struct with `next_block()` method
- `Client::query()` and `Client::stream()` that return empty RecordBatches built from `cherry-evm-schema` schemas

**cherry-core changes (one-time):**
- Add `ProviderKind::Rpc` variant to the enum in `cherry-core/ingest`
- Create `cherry-core/ingest/src/provider/rpc.rs` with the thin mapping layer:
  - `cherry_ingest::evm::Query` → `cherry_rpc_client::Query`
  - `cherry_ingest::ProviderConfig` → `cherry_rpc_client::ClientConfig` + `StreamConfig`
  - `cherry_rpc_client::ArrowResponse` → `BTreeMap<String, RecordBatch>`
- Add RPC-specific config fields to `ProviderConfig` (or equivalent)

**Why first:** This lets you wire the client into cherry-core immediately and verify the integration end-to-end with empty data. After this, all subsequent work happens entirely inside cherry-rpc-client — cherry-core is not touched again.

**Rindexer reuse:** None directly. This is pure cherry ecosystem scaffolding.

## Part 2: `eth_getLogs` pipeline

The highest-value, most reusable piece. Log indexing is the most common use case.

**What to build:**
- **RPC provider setup:** alloy `RpcClient` with `RetryBackoffLayer` for transport-level retries. Reuse rindexer's `create_client()` pattern ([provider.rs:707-796](../rindexer/core/src/provider.rs#L707-L796)).
- **Filter construction:** Build alloy `Filter` from `LogRequest` (address, topic0-3, fromBlock, toBlock). Reuse rindexer's pattern from [provider.rs:575-581](../rindexer/core/src/provider.rs#L575-L581).
- **Address batching:** Chunk large address sets into groups of 1000 per `eth_getLogs` call, fetch in parallel. Reuse rindexer's `get_logs_for_address_in_batches()` ([provider.rs:622-640](../rindexer/core/src/provider.rs#L622-L640)).
- **Block range management:** Start with `from_block`, clamp `to_block` to `from_block + max_block_range`. Advance on success. Reuse `calculate_process_historic_log_to_block()` pattern ([fetch_logs.rs:1024-1039](../rindexer/core/src/indexer/fetch_logs.rs#L1024-L1039)).
- **Error recovery:** Port rindexer's `retry_with_block_range()` — provider-specific regex parsing (Alchemy, Infura, QuickNode, Ankr, Base), tiered fallback (5000→1), 10% random recovery. This is the most valuable code to bring over ([fetch_logs.rs:725-931](../rindexer/core/src/indexer/fetch_logs.rs#L725-L931)).
- **Streaming:** Bounded `mpsc::channel` producer/consumer with backpressure. Historical loop then live polling. Reuse `fetch_logs_stream()` pattern ([fetch_logs.rs:30-149](../rindexer/core/src/indexer/fetch_logs.rs#L30-L149)).
- **Arrow conversion:** Map alloy `Log` fields to `cherry_evm_schema::logs_schema()` columns using the schema's builders.

**Output:** `ArrowResponse` with populated `logs` RecordBatch. The `blocks`, `transactions`, and `traces` RecordBatches are empty. When the query includes fields for those tables, emit a warning:

```
WARN: Block/transaction fields requested but eth_getBlockByNumber pipeline is not yet
      implemented. Block and transaction columns will be null.
WARN: Trace fields requested but trace pipeline is not yet implemented.
      Trace columns will be null.
```

**Rindexer reuse:** High. Filter construction, address batching, block range management, error recovery, streaming pattern — all directly adapted from rindexer.

## Part 3: `eth_getBlockByNumber` pipeline

Adds block headers and transaction objects.

**What to build:**
- **JSON-RPC batch calls:** `eth_getBlockByNumber(N, true)` for a range of blocks using alloy's `client.new_batch()`. Reuse rindexer's `get_block_by_number_batch_with_size()` with semaphore-controlled sub-batches ([provider.rs:429-506](../rindexer/core/src/provider.rs#L429-L506)).
- **Arrow conversion — blocks:** Map alloy `AnyRpcBlock` header fields to `cherry_evm_schema::blocks_schema()` columns.
- **Arrow conversion — transactions:** Split the response — transaction objects go to `cherry_evm_schema::transactions_schema()` columns. Only the fields available from `eth_getBlockByNumber` are populated (hash, from, to, value, input, gas, gasPrice, nonce, type, maxFeePerGas, maxPriorityFeePerGas, etc.).
- **Client-side filtering:** Apply `TransactionRequest` filters (from, to, sighash) on the transaction objects before building the RecordBatch.
- **`include_all_blocks`:** If false, only include blocks that have at least one matching transaction.
- **Latest block caching:** Cache `get_height()` results with a time-based TTL (default: block_time/3, min 500ms). Reuse rindexer's `get_latest_block()` caching pattern ([provider.rs:208-250](../rindexer/core/src/provider.rs#L208-L250)).

**Output:** `blocks` and `transactions` RecordBatches are now populated. Receipt-only columns in `transactions` (gasUsed, cumulativeGasUsed, effectiveGasPrice, status, contractAddress) remain null, with a warning:

```
WARN: Receipt fields (gasUsed, status, effectiveGasPrice, ...) requested but receipt
      fetching is not yet implemented. These columns will be null.
```

**Rindexer reuse:** High. JSON-RPC batch pattern and latest block caching are directly adapted.

## Part 4: `eth_getBlockReceipts` pipeline

Adds receipt data to the existing blocks+transactions pipeline.

**What to build:**
- **`eth_getBlockReceipts(N)`** for each block in the batch. This is the preferred method — one call per block returns all receipts.
- **Fallback:** If the provider doesn't support `eth_getBlockReceipts`, fall back to batched `eth_getTransactionReceipt(hash)` per matched transaction. Reuse rindexer's `get_tx_receipts_batch()` ([provider.rs:509-563](../rindexer/core/src/provider.rs#L509-L563)).
- **Merge:** Join receipt data to transaction rows by transaction hash (or block number + transaction index). Populate receipt-only columns: gasUsed, cumulativeGasUsed, effectiveGasPrice, contractAddress, status, logsBloom, root.
- **Field-aware skip:** If no receipt-only fields are requested in `Fields`, skip receipt fetching entirely.

**Output:** `blocks` and `transactions` are now fully populated. No more warnings for those tables.

**Rindexer reuse:** The `get_tx_receipts_batch()` fallback pattern is reusable. `eth_getBlockReceipts` itself is new — rindexer doesn't use it.

## Part 5: `trace_block` / `debug_traceBlockByNumber` pipeline

Adds trace data.

**What to build:**
- **`trace_block(N)`** via alloy's `TraceApi`. Reuse rindexer's `trace_block()` ([provider.rs:357-368](../rindexer/core/src/provider.rs#L357-L368)).
- **`debug_traceBlockByNumber`** via `raw_request` with `callTracer`. Reuse rindexer's custom `TraceCall`/`TraceCallFrame` deserialization types and the stack-based call tree flattening ([provider.rs:277-354](../rindexer/core/src/provider.rs#L277-L354)).
- **Method selection:** Use `ClientConfig.trace_method` if set. If None, try `trace_block` first; on failure, fall back to `debug_traceBlockByNumber` and remember the choice for subsequent calls.
- **Client-side filtering:** Apply `TraceRequest` filters (type, from, to, sighash) on the flattened traces.
- **Arrow conversion:** Map to `cherry_evm_schema::traces_schema()`.

**Output:** All four RecordBatches can now be populated independently. No more warnings.

**Rindexer reuse:** High. Both trace methods, custom deserialization types, and call tree flattening are directly reusable.

## Part 6: Cross-method orchestration

Combines all four pipelines into a single coordinated batch.

**What to build:**
- **Batch coordinator:** For each block batch, determine which pipelines to run based on the query and `Fields`, fire them in parallel via `tokio::join!`, collect results.
- **Field-aware dispatch:** Inspect `Fields` booleans to decide which RPC calls to skip entirely.
- **`include_all_blocks` across tables:** A block is included if any table has matching data in that block, not just one table.
- **LogsBloom pre-filtering:** During live streaming, check each block's LogsBloom before calling `eth_getLogs`. Reuse rindexer's `is_relevant_block()` pattern ([helpers/evm_log.rs:107-137](../rindexer/core/src/helpers/evm_log.rs#L107-L137)).

**Output:** Full `ArrowResponse` with all four tables populated from parallel RPC calls within each batch.

**Rindexer reuse:** LogsBloom pre-filtering is reusable. The orchestration itself is new — rindexer never combines data from multiple RPC methods.

## Part 7: Adaptive concurrency

Replaces static concurrency limits with adaptive scaling.

**What to build:**
- Port rindexer's `AdaptiveConcurrency` ([adaptive_concurrency.rs:12-32](../rindexer/core/src/adaptive_concurrency.rs#L12-L32)) as a per-client instance (not a global singleton).
- Wire it into the transport layer: backoff with jitter before every RPC call, scale up/down based on success/failure/rate-limit signals.
- Allow `ClientConfig.initial_concurrent_requests` and `ClientConfig.rpc_batch_size` to seed the initial values.

**Note:** This could be done earlier (between Parts 2 and 3) since it benefits all pipelines. It's listed last because it's an optimization — the client works without it, just less efficiently. However, if targeting free-tier providers early, moving this up would help avoid rate limit issues.

**Rindexer reuse:** High. The `AdaptiveConcurrency` module is self-contained and well-tested.

## Dependency graph

```
Part 1 (scaffolding + cherry-core changes)
  │
  ├─ Part 2 (eth_getLogs)
  │
  ├─ Part 3 (eth_getBlockByNumber)
  │    │
  │    └─ Part 4 (eth_getBlockReceipts)
  │
  ├─ Part 5 (trace_block / debug_trace)
  │
  └─ Part 7 (adaptive concurrency) ← can start after Part 1, benefits all others
       │
       └─ Part 6 (cross-method orchestration) ← requires Parts 2-5
```

Parts 2, 3, and 5 are independent and can be built in any order after Part 1. Part 4 depends on Part 3 (needs block data to merge with). Part 6 requires all individual pipelines (2-5) to be done. Part 7 can slot in at any point after Part 1.
