# Cherry RPC Client Execution Flow Map

## Overview

This document traces the complete execution flow when running the test
`stream_reth_transfer_logs` in `rust/src/client.rs`:

- **Network**: Ethereum mainnet
- **Contract**: Rocket Pool rETH (0xae78736cd615f374d3085123a210448e74fc6393)
- **Event**: ERC-20 Transfer (topic0 `0xddf252ad...`)
- **Block range**: 18_600_000 → 18_601_000
- **Mode**: streaming, `stop_on_head: true` (historical phase only)

---

## Test Setup

[`rust/src/client.rs:337`](rust/src/client.rs#L337) — The test `stream_reth_transfer_logs` initializes tracing, creates a `Client`, builds a `Query` and a `StreamConfig`, and drives the resulting stream to completion:

```rust
#[tokio::test]
async fn stream_reth_transfer_logs() {
    setup_tracing();
    let client = make_client();
    let query = reth_transfer_query(18_600_000, 18_601_000);
    let stream_config = StreamConfig {
        stop_on_head: true,
        buffer_size: 4,
        ..StreamConfig::default()
    };

    let mut stream = client
        .stream(query, stream_config)
        .unwrap_or_else(|e| panic!("stream creation failed: {e}"));

    // ...consume and validate chunks...
}
```

### Client creation

[`rust/src/client.rs:27`](rust/src/client.rs#L27) — `Client::new(ClientConfig::new(RPC_URL))` creates the `RpcProvider`:

```rust
pub fn new(config: ClientConfig) -> Result<Self> {
    let provider = RpcProvider::new(&config).context("failed to create RPC provider")?;
    Ok(Self { config, provider })
}
```

[`rust/src/rpc/provider.rs:47`](rust/src/rpc/provider.rs#L47) — `RpcProvider::new()` assembles the alloy HTTP transport stack:

1. Parses the RPC URL.
2. Builds a `reqwest::Client` with the configured 90-second timeout.
3. Wraps it in `RetryBackoffLayer` (max 5000 retries, exponential backoff, 660 CU/s rate limit by default).
4. Creates an alloy `RpcClient` and a `ProviderBuilder` targeting `AnyNetwork`.

```rust
let retry_layer = RetryBackoffLayer::new(
    config.max_num_retries,    // 5000
    config.retry_backoff_ms,   // 1000 ms
    config.compute_units_per_second.unwrap_or(660),
);
let rpc_client = RpcClient::builder().layer(retry_layer).transport(http, false);
let provider = ProviderBuilder::new().network::<AnyNetwork>().connect_client(rpc_client);
```

### Query construction

[`rust/src/client.rs:179`](rust/src/client.rs#L179) — `reth_transfer_query()` builds a `Query` with one `LogRequest` for the rETH contract:

```rust
Query {
    from_block: 18_600_000,
    to_block: Some(18_601_000),
    logs: vec![LogRequest {
        address: vec![reth_address()],   // 0xae78736c...
        topic0: vec![transfer_topic0()], // 0xddf252ad...
        ..LogRequest::default()
    }],
    // ...all LogFields set to true, one BlockField, TransactionField, TraceField each
}
```

The `BlockFields`, `TransactionFields`, and `TraceFields` with `true` values will
trigger `emit_unimplemented_warnings()` — these pipelines are not yet implemented,
so those columns will be null in the output.

---

## Stream Creation

[`rust/src/client.rs:70`](rust/src/client.rs#L70) — `client.stream()` calls `start_log_stream()` and wraps the receiver in a `ReceiverStream`:

```rust
pub fn stream(&self, query: Query, stream_config: StreamConfig) -> Result<DataStream> {
    let rx = start_log_stream(self.provider.clone(), query, stream_config);
    Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
}
```

[`rust/src/rpc/stream.rs:26`](rust/src/rpc/stream.rs#L26) — `start_log_stream()` creates a bounded mpsc channel (capacity = `buffer_size: 4`) and spawns a producer task:

```rust
pub fn start_log_stream(provider, query, stream_config) -> mpsc::Receiver<Result<ArrowResponse>> {
    let (tx, rx) = mpsc::channel(stream_config.buffer_size);  // capacity 4

    tokio::spawn(async move {
        if let Err(e) = run_stream(provider, query, stream_config, tx).await {
            error!("Log stream terminated with error: {e:#}");
        }
    });

    rx
}
```

The bounded channel provides **backpressure**: if the consumer (`stream.next()`) is
slow and 4 chunks are already queued, the producer blocks until a slot opens.

---

## Producer Task: `run_stream`

[`rust/src/rpc/stream.rs:42`](rust/src/rpc/stream.rs#L42) — `run_stream()` does three things:

1. **Emit warnings** for unimplemented pipelines (blocks, transactions, traces).
2. **Determine `snapshot_to_block`** — since `to_block: Some(18_601_000)` is set, no RPC call is needed here.
3. **Run the historical phase**, then conditionally run the live phase.

```rust
async fn run_stream(provider, query, stream_config, tx) -> Result<()> {
    emit_unimplemented_warnings(&query);  // warns: block.number, transaction.hash, trace.from

    let snapshot_to_block = query.to_block.unwrap_or(
        provider.get_block_number().await?  // only called if to_block is None
    );

    let next_block = run_historical(&provider, &query.logs, from_block,
                                    snapshot_to_block, query.fields.log, &tx).await?;

    info!("Finished historical log indexing up to block {snapshot_to_block}");

    if !stream_config.stop_on_head {  // false — live phase is SKIPPED in this test
        run_live(...).await?;
    }
    Ok(())
}
```

Since `stop_on_head: true`, `run_live()` is never called. The `tx` sender is
dropped at the end of `run_stream`, which closes the channel and terminates the
stream on the consumer side.

---

## Historical Phase: `run_historical`

[`rust/src/rpc/stream.rs:93`](rust/src/rpc/stream.rs#L93) — `run_historical()` iterates block ranges, calling `fetch_logs` on each chunk:

```rust
async fn run_historical(provider, log_requests, start_from, snapshot_to_block, ..., tx) -> Result<u64> {
    let mut max_block_range: Option<u64> = None;  // uncapped unless an RPC error sets it
    let mut from_block = 18_600_000;

    while from_block <= snapshot_to_block {  // 18_601_000
        let to_block = clamp_to_block(from_block, snapshot_to_block, max_block_range);

        match fetch_logs(&provider, log_requests, from_block, to_block).await {
            Ok(logs) => {
                let logs_batch = logs_to_record_batch(&logs);
                let response = ArrowResponse::with_logs(logs_batch);
                tx.send(Ok(response)).await;   // blocks if channel is full (backpressure)

                // Advance: past last log's block_number, or past to_block if empty
                from_block = logs.last()
                    .and_then(|l| l.block_number)
                    .map_or(to_block + 1, |n| n + 1);

                // 10% random chance to reset max_block_range to None (recovers from temporary limits)
                if rng.random_bool(0.10) { max_block_range = None; }
            }
            Err(e) => {
                // Parse provider-specific error, shrink block range, retry
                if let Some(retry) = retry_with_block_range(&err_str, from_block, to_block, max_block_range) {
                    from_block = retry.from;
                    max_block_range = retry.max_block_range;
                    continue;  // retry without advancing
                }
                // Unknown error: halve range
                max_block_range = Some(halved_block_range(from_block, to_block) - from_block);
            }
        }
    }
    Ok(from_block)
}
```

**Block range clamping** — [`rust/src/convert/block_range.rs:25`](rust/src/convert/block_range.rs#L25):

```rust
pub fn clamp_to_block(from_block: u64, snapshot_to_block: u64, max_block_range: Option<u64>) -> u64 {
    if let Some(max) = max_block_range {
        (from_block + max).min(snapshot_to_block)
    } else {
        snapshot_to_block  // first request: send the entire range 18_600_000..18_601_000
    }
}
```

With `max_block_range = None` (initial state), the first request covers the full
range `18_600_000..18_601_000`. If the RPC returns a block-range error, the range is
narrowed and the loop retries.

---

## Log Fetching

[`rust/src/rpc/log_fetcher.rs:125`](rust/src/rpc/log_fetcher.rs#L125) — `fetch_logs()` iterates all `LogRequest`s (one in this test) and merges results, sorted by `(block_number, log_index)`:

```rust
pub async fn fetch_logs(provider, requests, from_block, to_block) -> Result<Vec<Log>> {
    let mut all_logs = Vec::new();
    for req in requests {
        let logs = fetch_logs_for_request(provider, req, from_block, to_block).await?;
        all_logs.extend(logs);
    }
    // Deterministic ordering across multiple LogRequests
    all_logs.sort_by(|a, b| {
        let block_cmp = a.block_number.cmp(&b.block_number);
        if block_cmp != Ordering::Equal { return block_cmp; }
        a.log_index.cmp(&b.log_index)
    });
    Ok(all_logs)
}
```

[`rust/src/rpc/log_fetcher.rs:66`](rust/src/rpc/log_fetcher.rs#L66) — `fetch_logs_for_request()` builds an alloy `Filter` and calls `eth_getLogs`. Since the query has only one address (rETH), a single RPC call suffices:

```rust
pub async fn fetch_logs_for_request(provider, req, from_block, to_block) -> Result<Vec<Log>> {
    if req.address.len() <= MAX_ADDRESSES_PER_REQUEST {  // 1 <= 1000, single call
        let filter = build_filter(req, from_block, to_block);
        let logs = provider.provider.get_logs(&filter).await?;
        return Ok(logs);
    }
    // > 1000 addresses: chunk into parallel tokio::spawn fetches (not used here)
}
```

For queries with more than 1000 addresses, `fetch_logs_for_request()` chunks the
address list and fires parallel `tokio::spawn` tasks — one `eth_getLogs` call per chunk.

[`rust/src/rpc/log_fetcher.rs:26`](rust/src/rpc/log_fetcher.rs#L26) — `build_filter()` constructs the alloy `Filter`:

```rust
pub fn build_filter(req: &LogRequest, from_block: u64, to_block: u64) -> Filter {
    Filter::new()
        .from_block(18_600_000)
        .to_block(18_601_000)
        .address(ValueOrArray::Array(vec![0xae78736c...]))
        .event_signature(ValueOrArray::Array(vec![0xddf252ad...]))
    // topic1, topic2, topic3 are empty → not added to the filter
}
```

---

## Error Recovery

[`rust/src/convert/block_range.rs:50`](rust/src/convert/block_range.rs#L50) — `retry_with_block_range()` encodes provider-specific block-range error handling. If `eth_getLogs` fails, the error message is pattern-matched against known provider formats:

| Provider | Error pattern | Action |
|---|---|---|
| Alchemy | `"this block range should work: [0x..., 0x...]"` | Use the suggested range directly |
| Infura / Tenderly / zkSync | `"try with this block range [0x..., 0x...]"` | Use the suggested range directly |
| Ankr | `"block range is too wide"` | Cap at 3000 blocks |
| QuickNode / 1RPC / BlockPI | `"limited to a 10,000"` | Use the numeric limit |
| Base | `"block range too large"` | Cap at 2000 blocks |
| Any provider (transient) | `"response is too big"` / `"error decoding response body"` | Halve the range |
| Unknown error | (fallback) | Step down the `FallbackBlockRange` ladder: 5000 → 500 → 75 → 50 → ... → 1 |

`halved_block_range()` ensures a minimum advance of 2 blocks even on near-zero ranges.

---

## Log-to-Arrow Conversion

[`rust/src/convert/arrow_convert.rs:8`](rust/src/convert/arrow_convert.rs#L8) — `logs_to_record_batch()` iterates the alloy `Log` slice and feeds each field into a `cherry_evm_schema::LogsBuilder`, then calls `builder.finish()`:

```rust
pub fn logs_to_record_batch(logs: &[Log]) -> RecordBatch {
    let mut builder = LogsBuilder::default();
    for log in logs {
        builder.removed.append_value(log.removed);
        builder.log_index.append_option(log.log_index);
        builder.transaction_index.append_option(log.transaction_index);
        builder.transaction_hash.append_option(log.transaction_hash.map(|h| h.as_slice()));
        builder.block_hash.append_option(log.block_hash.map(|h| h.as_slice()));
        builder.block_number.append_option(log.block_number);
        builder.address.append_value(log.address().as_slice());
        builder.data.append_value(log.data().data.as_ref());
        // topics 0-3: Some → append_value, None → append_null
        append_topic(&mut builder.topic0, topics.first());
        // ...
    }
    builder.finish()  // produces a RecordBatch conforming to logs_schema()
}
```

The output schema (12 columns) matches `cherry_evm_schema::logs_schema()`:
`removed`, `log_index`, `transaction_index`, `transaction_hash`, `block_hash`,
`block_number`, `address`, `data`, `topic0`, `topic1`, `topic2`, `topic3`.

---

## Response Assembly

[`rust/src/response.rs:55`](rust/src/response.rs#L55) — `ArrowResponse::with_logs()` populates only the logs table; the other three tables are zero-row batches with their canonical schemas:

```rust
pub fn with_logs(logs: RecordBatch) -> Self {
    Self {
        logs,
        ..Self::empty()  // blocks, transactions, traces: empty with correct schemas
    }
}
```

This is why the test assertions `resp.blocks.num_rows() == 0` and
`resp.transactions.num_rows() == 0` and `resp.traces.num_rows() == 0` always pass —
blocks/transactions/traces pipelines are not yet implemented.

---

## Consumer Side

[`rust/src/client.rs:355`](rust/src/client.rs#L355) — Back in the test, `stream.next().await` polls the `ReceiverStream` (backed by `tokio_stream::wrappers::ReceiverStream`) for each chunk:

```rust
while let Some(result) = stream.next().await {
    let resp = result.unwrap();

    assert_eq!(resp.logs.schema().as_ref(), &cherry_evm_schema::logs_schema());
    assert_eq!(resp.blocks.num_rows(), 0);
    assert_eq!(resp.transactions.num_rows(), 0);
    assert_eq!(resp.traces.num_rows(), 0);

    total_log_rows += resp.logs.num_rows() as u64;
    chunk_count += 1;
    all_responses.push(resp);
}
```

When `run_stream` finishes, the `tx` sender is dropped, closing the channel. The
`ReceiverStream` then yields `None`, ending the loop.

**Final assertions:**
- `total_log_rows > 0` — at least one rETH Transfer log was returned.
- `chunk_count > 0` — at least one chunk was sent through the stream.

**Parquet output** — each table's batches are collected and written to
`data/stream_<table>.parquet` via `write_parquet()` (non-empty tables only).

---

## Live Phase (not exercised by this test)

[`rust/src/rpc/stream.rs:175`](rust/src/rpc/stream.rs#L175) — If `stop_on_head` were `false`, `run_live()` would run after `run_historical()` returns. It polls for new blocks on a configurable interval (`head_poll_interval_millis`, default 1000 ms):

```rust
async fn run_live(provider, log_requests, start_from, stream_config, tx) -> Result<()> {
    let poll_interval = Duration::from_millis(stream_config.head_poll_interval_millis);
    let mut from_block = start_from;  // picks up where historical left off

    loop {
        tokio::time::sleep(poll_interval).await;

        let head = provider.get_block_number().await?;
        // Reorg-safe distance: don't process blocks too close to the head
        let safe_head = head.saturating_sub(stream_config.reorg_safe_distance);

        if from_block > safe_head {
            // At head — wait for next poll
            continue;
        }

        match fetch_logs(provider, log_requests, from_block, safe_head).await {
            Ok(logs) => {
                let response = ArrowResponse::with_logs(logs_to_record_batch(&logs));
                if tx.send(Ok(response)).await.is_err() { return Ok(()); }  // consumer dropped
                from_block = /* last log block + 1, or safe_head + 1 if empty */;
            }
            Err(e) => {
                // retry_with_block_range or 1-second backoff
            }
        }
    }
}
```

The loop runs until the consumer drops the receiver (channel send fails) or the task
is cancelled.

---

## Complete Function Call Chain Summary

```
Test entry:
  client.rs:stream_reth_transfer_logs()
    config.rs:ClientConfig::new()                  → default config, 5000 retries, 90s timeout
    client.rs:make_client()
      client.rs:Client::new()
        rpc/provider.rs:RpcProvider::new()         → HTTP + RetryBackoffLayer + AnyNetwork provider
    client.rs:reth_transfer_query()                → Query { logs: [rETH+Transfer], fields: all log fields }
    client.rs:Client::stream()
      rpc/stream.rs:start_log_stream()             → mpsc::channel(4) + tokio::spawn(run_stream)
        rpc/stream.rs:run_stream()
          query.rs:emit_unimplemented_warnings()   → warns: block.number, transaction.hash, trace.from
          rpc/stream.rs:run_historical()           → iterates 18_600_000..18_601_000
            convert/block_range.rs:clamp_to_block()  → to_block = 18_601_000 (no max_block_range)
            rpc/log_fetcher.rs:fetch_logs()
              rpc/log_fetcher.rs:fetch_logs_for_request()
                rpc/log_fetcher.rs:build_filter()  → Filter { from, to, address, topic0 }
                provider.get_logs(&filter)         [RPC: eth_getLogs]
            convert/arrow_convert.rs:logs_to_record_batch()  → RecordBatch (logs_schema, N rows)
            response.rs:ArrowResponse::with_logs() → ArrowResponse { logs: batch, rest: empty }
            tx.send(Ok(response))                  [backpressure: blocks if channel full]
            // advance from_block, optional 10% reset of max_block_range
          // run_live() SKIPPED because stop_on_head=true
          // tx dropped → channel closed → stream ends
      tokio_stream::ReceiverStream::new(rx)        → DataStream (Pin<Box<dyn Stream>>)
    stream.next().await loop
      // validates schema, counts rows, accumulates responses
    write_parquet()                                → data/stream_logs.parquet (+ other tables)
```

---

## Key Data Structures

[`rust/src/config.rs:6`](rust/src/config.rs#L6) — RPC transport configuration:

```rust
pub struct ClientConfig {
    pub url: String,
    pub max_num_retries: u32,           // 5000
    pub retry_backoff_ms: u64,          // 1000
    pub req_timeout_millis: u64,        // 90_000
    pub compute_units_per_second: Option<u64>,
    pub max_block_range: Option<u64>,   // None = uncapped
    // ...
}
```

[`rust/src/config.rs:41`](rust/src/config.rs#L41) — Streaming behavior configuration:

```rust
pub struct StreamConfig {
    pub stop_on_head: bool,              // true → historical only
    pub head_poll_interval_millis: u64,  // 1000 ms default
    pub buffer_size: usize,              // 4 (channel capacity / backpressure)
    pub reorg_safe_distance: u64,        // 0 default (live phase only)
}
```

[`rust/src/query.rs:21`](rust/src/query.rs#L21) — Top-level query descriptor:

```rust
pub struct Query {
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub logs: Vec<LogRequest>,
    pub transactions: Vec<TransactionRequest>,
    pub traces: Vec<TraceRequest>,
    pub fields: Fields,
}
```

[`rust/src/query.rs:31`](rust/src/query.rs#L31) — Per-contract log filter:

```rust
pub struct LogRequest {
    pub address: Vec<Address>,  // OR'd by provider
    pub topic0: Vec<Topic>,     // OR'd by provider
    pub topic1: Vec<Topic>,
    pub topic2: Vec<Topic>,
    pub topic3: Vec<Topic>,
}
```

[`rust/src/response.rs:11`](rust/src/response.rs#L11) — One chunk yielded by the stream:

```rust
pub struct ArrowResponse {
    pub blocks: RecordBatch,       // empty (unimplemented)
    pub transactions: RecordBatch, // empty (unimplemented)
    pub logs: RecordBatch,         // N rows, logs_schema()
    pub traces: RecordBatch,       // empty (unimplemented)
}
```

[`rust/src/convert/block_range.rs:12`](rust/src/convert/block_range.rs#L12) — Block range retry suggestion returned by `retry_with_block_range()`:

```rust
pub struct RetryBlockRange {
    pub from: u64,
    pub to: u64,
    pub max_block_range: Option<u64>,  // new cap for subsequent requests
}
```
