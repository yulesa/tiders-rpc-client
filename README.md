# tiders-rpc-client

[![Crates.io](https://img.shields.io/badge/crates.io-orange?style=for-the-badge&logo=rust)](https://crates.io/crates/tiders-rpc-client)
[![GitHub](https://img.shields.io/badge/github-black?style=for-the-badge&logo=github)](https://github.com/yulesa/tiders)
[![Documentation](https://img.shields.io/badge/documentation-blue?style=for-the-badge&logo=readthedocs)](https://yulesa.github.io/tiders-docs/)

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

## Installation

### From crates.io

Add `tiders-rpc-client` to your `Cargo.toml`:

```toml
[dependencies]
tiders-rpc-client = "0.1.0"
```

Or install with Cargo:

```bash
cargo add tiders-rpc-client
```

### Build from source

```bash
git clone https://github.com/yulesa/tiders-rpc-client.git
cd tiders-rpc-client/rust
cargo build
```

### Using local `tiders-rpc-client` with `tiders-core`

If you're modifying this repo locally and want `tiders-core` to build against your local `tiders-rpc-client`, build `tiders-core` with a crates.io patch override:

```bash
cd tiders-core
cargo build --config 'patch.crates-io.tiders-rpc-client.path="../tiders-rpc-client/rust"'
```

When rebuilding Python bindings with `maturin`, pass the same patch to `maturin develop`:

```bash
cd tiders-core/python
maturin develop --config 'patch.crates-io.tiders-rpc-client.path="../../tiders-rpc-client/rust"'
```

For persistent local development, you can also put this in `tiders-core/.cargo/config.toml`:

```toml
[patch.crates-io]
tiders-rpc-client = { path = "../tiders-rpc-client/rust" }
```

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

```rust
use tiders_rpc_client::{Query, LogRequest, TransactionRequest, TraceRequest};
use tiders_rpc_client::{Fields, BlockFields, TransactionFields, LogFields, TraceFields};

let query = Query {
    from_block: 18_000_000,
    to_block: Some(18_001_000),
    include_all_blocks: false,
    logs: vec![LogRequest { .. }],
    transactions: vec![TransactionRequest { .. }],
    traces: vec![TraceRequest { .. }],
    fields: Fields {
        block: BlockFields { number: true, timestamp: true, ..Default::default() },
        transaction: TransactionFields { hash: true, ..Default::default() },
        log: LogFields { address: true, data: true, ..Default::default() },
        trace: TraceFields::default(),
    },
};
```

- **`from_block` / `to_block`** - Block range (inclusive)
- **`include_all_blocks`** - Whether to include all blocks or only those with matching data
- **Request types `logs`, `transactions`, `traces`** - Allows filtering on the request: `LogRequest` (with address/topic filters). `TransactionRequest`, `TraceRequest` only allows starting each one of the pipelines.
- **Field selection** - Select which fields the response should include.

## Adaptive Concurrency

Three independent controllers automatically tune concurrency:

| Controller | Task Range | Chunk Size |
|---|---|---|
| Block | 10 - 200 | 200 blocks |
| Log | 10 - 200 | 1000 blocks |
| Single-block (traces, receipts) | 100 - 2000 | 1 block |

The system scales up on consecutive successes and halves concurrency (with doubled backoff) on rate limits. Chunk sizes recover with 10% probability per successful call.

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
