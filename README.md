# tiders-rpc-client

Rust library for fetching EVM blockchain data from any standard JSON-RPC endpoint and converting it to [Apache Arrow](https://arrow.apache.org/) format. Works with any Ethereum-compatible provider (Alchemy, Infura, QuickNode, local nodes, public endpoints, etc.).

Part of the [tiders](https://github.com/yulesa/tiders) data pipeline framework. Can be used directly as a Rust library or through the [tiders Python SDK](https://github.com/yulesa/tiders).

**[Read the full documentation](https://yulesa.github.io/tiders-docs/)**

## Features

- **Three independent pipelines** - Block (`eth_getBlockByNumber`), Log (`eth_getLogs`), and Trace (`trace_block` / `debug_traceBlockByNumber`)
- **Multi-pipeline streams** - Coordinated data fetching across pipelines in a single stream
- **Adaptive concurrency** - Automatically scales concurrency up on success and backs off on rate limits
- **Block range fallback** - Splits oversized ranges when providers reject them
- **Retry logic** - Configurable retries with exponential backoff
- **Field selection** - Choose exactly which block, transaction, log, and trace fields to fetch
- **Streaming** - Returns data as an async stream of Arrow `RecordBatch`es
- **Live mode** - Optionally follows the chain head, polling for new blocks after catching up

## Pipelines

Each pipeline has two phases: **historical** (concurrent fetching from `from_block` to chain head) and **live** (sequential polling for new blocks).

### Block Pipeline

Uses `eth_getBlockByNumber` in batches. Automatically fetches transaction receipts when receipt fields (e.g., `status`, `gas_used`) are selected.

### Log Pipeline

Uses `eth_getLogs` with server-side address and topic filters. Groups large address lists (max 1000 per request) and implements block range fallback for oversized responses.

### Trace Pipeline

Uses `trace_block` or `debug_traceBlockByNumber`. Auto-detects the correct trace method from the provider.

### Multi-Pipeline Stream

Divides the block range into fixed-size batches, runs selected pipelines sequentially within each batch, and merges results into a single response stream.

## Configuration

The `ClientConfig` struct controls RPC client behavior:

| Option | Description | Default |
|---|---|---|
| `url` | JSON-RPC endpoint URL | required |
| `bearer_token` | Optional bearer token for authentication | `None` |
| `max_num_retries` | Maximum retry attempts per request | `12` |
| `retry_backoff_ms` | Initial backoff between retries (ms) | `500` |
| `retry_base_ms` | Base for exponential backoff (ms) | `1000` |
| `retry_ceiling_ms` | Maximum backoff cap (ms) | `30000` |
| `http_req_timeout_millis` | HTTP request timeout (ms) | `30000` |
| `compute_units_per_second` | Rate limit in compute units | `None` |
| `batch_size` | Number of blocks per batch | `None` |
| `trace_method` | Override trace method (`trace_block` or `debug`) | auto-detect |
| `stop_on_head` | Stop streaming at chain head | `false` |
| `head_poll_interval_ms` | Polling interval in live mode (ms) | `1000` |
| `reorg_safe_distance` | Blocks behind head for reorg safety | `0` |

## Querying

Queries specify the block range and which data to fetch:

- **`from_block` / `to_block`** - Block range (inclusive)
- **`include_all_blocks`** - Whether to include all blocks or only those with matching data
- **Request types** - `LogRequest` (with address/topic filters), `TransactionRequest`, `TraceRequest`
- **Field selection** - `BlockFields`, `TransactionFields`, `LogFields`, `TraceFields`

### Available Fields

- **Block** - `number`, `hash`, `timestamp`, `gas_limit`, `gas_used`, `base_fee_per_gas`, `parent_hash`, `miner`, `nonce`, and more (23 fields total)
- **Transaction** - Standard fields plus receipt data (`status`, `gas_used`, `effective_gas_price`, etc.), 50+ fields total
- **Log** - `address`, `data`, `topic0`-`topic3`, `block_number`, `transaction_hash`, `log_index`, etc. (10 fields)
- **Trace** - `from`, `to`, `call_type`, `gas`, `gas_used`, `input`, `output`, `value`, etc. (21 fields)

## Adaptive Concurrency

Three independent controllers automatically tune concurrency:

| Controller | Task Range | Chunk Size |
|---|---|---|
| Block | 10 - 200 | 200 blocks |
| Log | 10 - 200 | 1000 blocks |
| Single-block (traces, receipts) | 100 - 2000 | 1 block |

The system scales up on consecutive successes and halves concurrency (with doubled backoff) on rate limits. Chunk sizes recover with 10% probability per successful call.

## Provider Compatibility

| Provider | Block | Log | Trace |
|---|---|---|---|
| Alchemy | Yes | Yes | Yes |
| Infura | Yes | Yes | No |
| QuickNode | Yes | Yes | Yes |
| Local node | Yes | Yes | Depends on client |
| Public endpoints | Yes | Yes | Rarely |

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
