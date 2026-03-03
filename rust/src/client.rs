use std::collections::BTreeMap;
use std::pin::Pin;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use futures_lite::Stream;
use log::{info, warn};

use crate::config::ClientConfig;
use crate::query::{analyze_query, Query};
use crate::response::ArrowResponse;
use crate::rpc::{
    start_block_stream, start_coordinated_stream, start_log_stream, start_trace_stream, RpcProvider,
};

/// An async stream of [`ArrowResponse`] chunks.
pub type DataStream = Pin<Box<dyn Stream<Item = Result<ArrowResponse>> + Send + Sync>>;

/// The main entry point for fetching EVM data over JSON-RPC.
///
/// Wraps an alloy RPC provider and routes queries to the appropriate
/// pipeline (blocks, logs, traces, or a coordinated combination).
#[derive(Debug, Clone)]
pub struct Client {
    config: ClientConfig,
    provider: RpcProvider,
}

impl Client {
    /// Create a new client from configuration.
    ///
    /// This sets up the alloy RPC provider with retry/backoff layers.
    pub fn new(config: ClientConfig) -> Result<Self> {
        let provider = RpcProvider::new(&config).context("failed to create RPC provider")?;
        Ok(Self { config, provider })
    }

    /// Return a reference to the client's configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Start a streaming query that yields `ArrowResponse` chunks.
    ///
    /// Selects the appropriate pipeline based on the query:
    /// - If the query requires, uses the coordinated multi-pipeline stream.
    /// - If only blocks and/or tx, uses the `eth_getBlockByNumber` pipeline.
    /// - If only log requests are present, uses the `eth_getLogs` pipeline.
    /// - If only trace requests are present, uses the `trace_block` pipeline.
    pub fn stream(&self, query: Query) -> Result<DataStream> {
        let pipelines = analyze_query(&query)?;

        let rx = if pipelines.needs_coordinator() {
            let mut config = self.config.clone();
            if config.batch_size.is_none() {
                config.batch_size = Some(100);
                warn!(
                    "Multi-pipeline stream requires a batch_size. \
                     Defaulting to 100 blocks per batch. The client accumulates all \
                     pipeline data for the full batch range before sending each response. \
                     Set batch_size in ClientConfig to control memory usage and response granularity."
                );
            }
            info!(
                "Starting multi-pipeline stream (blocks_transactions={}, logs={}, traces={})",
                pipelines.blocks_transactions, pipelines.logs, pipelines.traces
            );
            start_coordinated_stream(self.provider.clone(), query, config, pipelines)
        } else if pipelines.logs {
            info!("Starting log stream");
            start_log_stream(self.provider.clone(), query, self.config.clone())
        } else if pipelines.traces {
            info!("Starting trace stream");
            start_trace_stream(self.provider.clone(), query, self.config.clone())
        } else {
            info!("Starting block stream");
            start_block_stream(self.provider.clone(), query, self.config.clone())
        };
        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    /// Convert an `ArrowResponse` into the `BTreeMap<String, RecordBatch>`
    /// format expected by the tiders-core ingest pipeline.
    pub fn response_to_btree(response: ArrowResponse) -> BTreeMap<String, RecordBatch> {
        let mut map = BTreeMap::new();
        map.insert("blocks".to_owned(), response.blocks);
        map.insert("transactions".to_owned(), response.transactions);
        map.insert("logs".to_owned(), response.logs);
        map.insert("traces".to_owned(), response.traces);
        map
    }
}

#[cfg(test)]
#[expect(clippy::panic)]
mod tests {
    use std::fs::File;
    use std::path::Path;

    use alloy::primitives::{address, b256};
    use arrow::record_batch::RecordBatch;
    use futures_lite::StreamExt;
    use parquet::arrow::ArrowWriter;

    use super::*;
    use crate::config::ClientConfig;
    use crate::query::{
        Address, BlockFields, Fields, LogFields, LogRequest, Query, Topic, TraceFields,
        TraceRequest, TransactionFields, TransactionRequest,
    };

    /// Root of the tiders-rpc-client repository (one level above `rust/`).
    const REPO_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/..");

    /// Tenderly free Ethereum mainnet gateway (same default as rindexer examples).
    /// To run the traces test, you will need to get a free tenderly API key and
    /// set the RPC URL, e.g. `https://mainnet.gateway.tenderly.co/your_key_here`.
    const RPC_URL: &str = "https://mainnet.gateway.tenderly.co/";

    fn make_client() -> Client {
        Client::new(ClientConfig {
            stop_on_head: true,
            batch_size: Some(500),
            ..ClientConfig::new(RPC_URL.to_owned())
        })
        .unwrap_or_else(|e| panic!("Failed to create client: {e}"))
    }

    /// Streaming query: fetches the last 5000 blocks up to the current head
    /// (`stop_on_head: true`) and saves all log chunks to Parquet.
    /// Run with: cargo test stream_reth_transfer_logs -- --nocapture
    #[tokio::test]
    async fn stream_reth_transfer_logs() {
        // Initialise env_logger so log output is visible when running
        // tests with `--nocapture`. Defaults to `info`; override with `RUST_LOG`.
        // Silently ignores the error if a logger has already been set.
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .parse_default_env()
            .try_init();
        let client = make_client();

        // Resolve the current head and log it.
        let latest_block = client
            .provider
            .get_block_number()
            .await
            .unwrap_or_else(|e| panic!("failed to get block number: {e}"));
        log::info!("Latest block: {latest_block}");

        let from_block = latest_block.saturating_sub(5_000);

        let a = address!("ae78736cd615f374d3085123a210448e74fc6393");
        let reth_address = Address(a.0 .0);

        let t = b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
        let transfer_topic0 = Topic(t.0);

        let query = Query {
            from_block,
            to_block: Some(latest_block),
            include_all_blocks: false,
            logs: vec![LogRequest {
                address: vec![reth_address],
                topic0: vec![transfer_topic0],
                ..LogRequest::default()
            }],
            transactions: Vec::new(),
            traces: Vec::new(),
            fields: Fields {
                log: LogFields {
                    removed: true,
                    log_index: true,
                    transaction_index: true,
                    transaction_hash: true,
                    block_hash: true,
                    block_number: true,
                    address: true,
                    data: true,
                    topic0: true,
                    topic1: true,
                    topic2: true,
                    topic3: true,
                },
                block: BlockFields {
                    ..BlockFields::default()
                },
                transaction: TransactionFields {
                    ..TransactionFields::default()
                },
                trace: TraceFields {
                    ..TraceFields::default()
                },
            },
        };

        let mut stream = client
            .stream(query)
            .unwrap_or_else(|e| panic!("stream creation failed: {e}"));

        let mut total_log_rows = 0u64;
        let mut chunk_count = 0u64;
        let mut all_responses: Vec<ArrowResponse> = Vec::new();

        while let Some(result) = stream.next().await {
            let resp = result.unwrap_or_else(|e| panic!("stream chunk failed: {e}"));

            // Every chunk should carry the correct schemas.
            assert_eq!(
                resp.logs.schema().as_ref(),
                &tiders_evm_schema::logs_schema(),
            );

            // Unimplemented tables stay empty.
            assert_eq!(resp.blocks.num_rows(), 0);
            assert_eq!(resp.transactions.num_rows(), 0);
            assert_eq!(resp.traces.num_rows(), 0);

            total_log_rows += resp.logs.num_rows() as u64;
            chunk_count += 1;
            all_responses.push(resp);
        }

        assert!(
            total_log_rows > 0,
            "stream should have yielded Transfer logs"
        );
        assert!(
            chunk_count > 0,
            "stream should have yielded at least one chunk"
        );

        // Save all streamed chunks to parquet files.
        let root = Path::new(REPO_ROOT);
        for (name, extract) in [
            (
                "blocks",
                (|r: &ArrowResponse| r.blocks.clone()) as fn(&ArrowResponse) -> RecordBatch,
            ),
            ("transactions", |r| r.transactions.clone()),
            ("logs", |r| r.logs.clone()),
            ("traces", |r| r.traces.clone()),
        ] {
            let batches: Vec<RecordBatch> = all_responses.iter().map(extract).collect();
            let path = root.join(format!("data/stream_{name}.parquet"));
            write_parquet(&path, &batches);
        }
    }

    /// Streaming query: fetches the last 100 blocks up to the current head
    /// via the block fetcher pipeline (`eth_getBlockByNumber`)and their
    /// transaction receipts via `eth_getBlockReceipts` and saves
    /// blocks and transactions to Parquet.
    /// Run with: cargo test stream_blocks_and_transactions -- --nocapture
    #[tokio::test]
    async fn stream_blocks_and_transactions() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .parse_default_env()
            .try_init();

        let client = Client::new(ClientConfig {
            stop_on_head: true,
            batch_size: Some(10),
            reorg_safe_distance: 10,
            ..ClientConfig::new(RPC_URL.to_owned())
        })
        .unwrap_or_else(|e| panic!("Failed to create client: {e}"));

        let latest_block = client
            .provider
            .get_block_number()
            .await
            .unwrap_or_else(|e| panic!("failed to get block number: {e}"));
        log::info!("Latest block: {latest_block}");

        let from_block = latest_block.saturating_sub(20);

        let query = Query {
            from_block,
            to_block: None,
            include_all_blocks: true,
            logs: Vec::new(),
            transactions: vec![TransactionRequest {
                ..TransactionRequest::default()
            }],
            traces: Vec::new(),
            fields: Fields {
                block: BlockFields {
                    number: true,
                    hash: true,
                    timestamp: true,
                    gas_used: true,
                    gas_limit: true,
                    base_fee_per_gas: true,
                    miner: true,
                    ..BlockFields::default()
                },
                transaction: TransactionFields {
                    hash: true,
                    // Receipt-only fields:
                    gas_used: true,
                    status: true,
                    effective_gas_price: true,
                    cumulative_gas_used: true,
                    contract_address: true,
                    logs_bloom: true,
                    ..TransactionFields::default()
                },
                log: LogFields::default(),
                trace: TraceFields::default(),
            },
        };

        let mut stream = client
            .stream(query)
            .unwrap_or_else(|e| panic!("stream creation failed: {e}"));

        let mut total_block_rows = 0u64;
        let mut total_tx_rows = 0u64;
        let mut chunk_count = 0u64;
        let mut all_responses: Vec<ArrowResponse> = Vec::new();

        while let Some(result) = stream.next().await {
            let resp = result.unwrap_or_else(|e| panic!("stream chunk failed: {e}"));

            // Blocks batch should contain exactly the requested fields.
            let block_schema = resp.blocks.schema();
            assert!(block_schema.field_with_name("number").is_ok());
            assert!(block_schema.field_with_name("hash").is_ok());
            assert!(block_schema.field_with_name("timestamp").is_ok());
            assert!(block_schema.field_with_name("gas_used").is_ok());
            assert!(block_schema.field_with_name("gas_limit").is_ok());
            assert!(block_schema.field_with_name("base_fee_per_gas").is_ok());
            assert!(block_schema.field_with_name("miner").is_ok());
            assert_eq!(block_schema.fields().len(), 7);

            // Transactions batch should contain exactly the requested fields.
            let tx_schema = resp.transactions.schema();
            assert!(tx_schema.field_with_name("hash").is_ok());
            assert_eq!(tx_schema.fields().len(), 7);

            // Log and trace tables should be empty for the block pipeline.
            assert_eq!(resp.logs.num_rows(), 0);
            assert_eq!(resp.traces.num_rows(), 0);

            total_block_rows += resp.blocks.num_rows() as u64;
            total_tx_rows += resp.transactions.num_rows() as u64;
            chunk_count += 1;
            all_responses.push(resp);
        }

        assert!(
            total_block_rows > 0,
            "stream should have yielded block rows"
        );
        assert!(
            chunk_count > 0,
            "stream should have yielded at least one chunk"
        );

        log::info!(
            "Streamed {total_block_rows} block rows and {total_tx_rows} tx rows \
             in {chunk_count} chunks"
        );

        // Save all streamed chunks to parquet files.
        let root = Path::new(REPO_ROOT);
        for (name, extract) in [
            (
                "blocks",
                (|r: &ArrowResponse| r.blocks.clone()) as fn(&ArrowResponse) -> RecordBatch,
            ),
            ("transactions", |r| r.transactions.clone()),
            ("logs", |r| r.logs.clone()),
            ("traces", |r| r.traces.clone()),
        ] {
            let batches: Vec<RecordBatch> = all_responses.iter().map(extract).collect();
            let path = root.join(format!("data/block_stream_{name}.parquet"));
            write_parquet(&path, &batches);
        }
    }

    /// Multi-pipeline streaming query: fetches 100 blocks in 10-block batches
    /// via the coordinated stream, exercising blocks, transactions (with
    /// receipts), logs, and traces simultaneously.
    /// Run with: cargo test stream_multi_pipeline -- --nocapture
    #[tokio::test]
    async fn stream_multi_pipeline() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .parse_default_env()
            .try_init();

        let client = Client::new(ClientConfig {
            stop_on_head: true,
            batch_size: Some(10),
            ..ClientConfig::new(RPC_URL.to_owned())
        })
        .unwrap_or_else(|e| panic!("Failed to create client: {e}"));

        let latest_block = client
            .provider
            .get_block_number()
            .await
            .unwrap_or_else(|e| panic!("failed to get block number: {e}"));
        log::info!("Latest block: {latest_block}");

        let from_block = latest_block.saturating_sub(30);

        let query = Query {
            from_block,
            to_block: Some(latest_block),
            include_all_blocks: true,
            logs: Vec::new(),
            // TransactionRequest with include_logs + include_traces triggers
            // the coordinated multi-pipeline stream.
            transactions: vec![TransactionRequest {
                include_logs: true,
                // include_traces: true,
                ..TransactionRequest::default()
            }],
            traces: Vec::new(),
            fields: Fields {
                block: BlockFields {
                    number: true,
                    hash: true,
                    timestamp: true,
                    gas_used: true,
                    gas_limit: true,
                    base_fee_per_gas: true,
                    miner: true,
                    ..BlockFields::default()
                },
                transaction: TransactionFields {
                    hash: true,
                    from: true,
                    to: true,
                    value: true,
                    // Receipt fields:
                    gas_used: true,
                    status: true,
                    effective_gas_price: true,
                    ..TransactionFields::default()
                },
                log: LogFields {
                    block_number: true,
                    transaction_hash: true,
                    address: true,
                    topic0: true,
                    data: true,
                    ..LogFields::default()
                },
                trace: TraceFields {
                    //     block_number: true,
                    //     transaction_hash: true,
                    //     from: true,
                    //     to: true,
                    //     value: true,
                    //     call_type: true,
                    //     gas_used: true,
                    ..TraceFields::default()
                },
            },
        };

        let mut stream = client
            .stream(query)
            .unwrap_or_else(|e| panic!("stream creation failed: {e}"));

        let mut total_block_rows = 0u64;
        let mut total_tx_rows = 0u64;
        let mut total_log_rows = 0u64;
        let mut chunk_count = 0u64;
        let mut all_responses: Vec<ArrowResponse> = Vec::new();

        while let Some(result) = stream.next().await {
            let resp = result.unwrap_or_else(|e| panic!("stream chunk failed: {e}"));

            // All four tables must carry correct field counts.
            assert_eq!(resp.blocks.schema().fields().len(), 7);
            assert_eq!(resp.transactions.schema().fields().len(), 7);
            assert_eq!(resp.logs.schema().fields().len(), 5);

            total_block_rows += resp.blocks.num_rows() as u64;
            total_tx_rows += resp.transactions.num_rows() as u64;
            total_log_rows += resp.logs.num_rows() as u64;
            chunk_count += 1;
            all_responses.push(resp);
        }

        assert!(total_block_rows > 0, "should have yielded block rows");
        assert!(total_tx_rows > 0, "should have yielded transaction rows");
        assert!(total_log_rows > 0, "should have yielded log rows");
        assert!(chunk_count > 0, "should have yielded at least one chunk");

        log::info!(
            "Multi-pipeline: {total_block_rows} blocks, {total_tx_rows} txs, \
             {total_log_rows} logs, in {chunk_count} chunks"
        );

        // Save all streamed chunks to parquet files.
        let root = Path::new(REPO_ROOT);
        for (name, extract) in [
            (
                "blocks",
                (|r: &ArrowResponse| r.blocks.clone()) as fn(&ArrowResponse) -> RecordBatch,
            ),
            ("transactions", |r| r.transactions.clone()),
            ("logs", |r| r.logs.clone()),
        ] {
            let batches: Vec<RecordBatch> = all_responses.iter().map(extract).collect();
            let path = root.join(format!("data/multi_pipeline_{name}.parquet"));
            write_parquet(&path, &batches);
        }
    }

    /// Streaming query: fetches the last 20 blocks via the trace pipeline
    /// (`trace_block`) and saves traces to Parquet.
    /// To run the traces test, you will need to get a free tenderly API key and
    /// set the RPC URL, e.g. `https://mainnet.gateway.tenderly.co/your_key_here`
    /// Run with: cargo test stream_traces -- --nocapturecargo test stream_traces -- --nocapture
    #[tokio::test]
    async fn stream_traces() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .parse_default_env()
            .try_init();

        let client = Client::new(ClientConfig {
            stop_on_head: true,
            batch_size: Some(5),
            ..ClientConfig::new(RPC_URL.to_owned())
        })
        .unwrap_or_else(|e| panic!("Failed to create client: {e}"));

        let latest_block = client
            .provider
            .get_block_number()
            .await
            .unwrap_or_else(|e| panic!("failed to get block number: {e}"));
        log::info!("Latest block: {latest_block}");

        let from_block = latest_block.saturating_sub(10);

        let query = Query {
            from_block,
            to_block: Some(latest_block),
            include_all_blocks: false,
            logs: Vec::new(),
            transactions: Vec::new(),
            traces: vec![TraceRequest {
                ..TraceRequest::default()
            }],
            fields: Fields {
                block: BlockFields::default(),
                transaction: TransactionFields::default(),
                log: LogFields::default(),
                trace: TraceFields {
                    block_number: true,
                    transaction_hash: true,
                    from: true,
                    to: true,
                    value: true,
                    call_type: true,
                    gas: true,
                    gas_used: true,
                    input: true,
                    output: true,
                    subtraces: true,
                    trace_address: true,
                    type_: true,
                    error: true,
                    ..TraceFields::default()
                },
            },
        };

        let mut stream = client
            .stream(query)
            .unwrap_or_else(|e| panic!("stream creation failed: {e}"));

        let mut total_trace_rows = 0u64;
        let mut chunk_count = 0u64;
        let mut all_responses: Vec<ArrowResponse> = Vec::new();

        while let Some(result) = stream.next().await {
            let resp = result.unwrap_or_else(|e| panic!("stream chunk failed: {e}"));

            // Trace schema should contain exactly the requested fields.
            let trace_schema = resp.traces.schema();
            assert!(trace_schema.field_with_name("block_number").is_ok());
            assert!(trace_schema.field_with_name("transaction_hash").is_ok());
            assert!(trace_schema.field_with_name("from").is_ok());
            assert!(trace_schema.field_with_name("to").is_ok());
            assert!(trace_schema.field_with_name("call_type").is_ok());
            assert!(trace_schema.field_with_name("gas_used").is_ok());
            assert!(trace_schema.field_with_name("type").is_ok());
            assert_eq!(trace_schema.fields().len(), 14);

            // Block, transaction, and log tables should be empty for trace-only pipeline.
            assert_eq!(resp.blocks.num_rows(), 0);
            assert_eq!(resp.transactions.num_rows(), 0);
            assert_eq!(resp.logs.num_rows(), 0);

            total_trace_rows += resp.traces.num_rows() as u64;
            chunk_count += 1;
            all_responses.push(resp);
        }

        assert!(total_trace_rows > 0, "should have yielded trace rows");
        assert!(chunk_count > 0, "should have yielded at least one chunk");

        log::info!("Streamed {total_trace_rows} trace rows in {chunk_count} chunks");

        let root = Path::new(REPO_ROOT);
        for (name, extract) in [(
            "traces",
            (|r: &ArrowResponse| r.traces.clone()) as fn(&ArrowResponse) -> RecordBatch,
        )] {
            let batches: Vec<RecordBatch> = all_responses.iter().map(extract).collect();
            let path = root.join(format!("data/trace_stream_{name}.parquet"));
            write_parquet(&path, &batches);
        }
    }

    // helper function to write a sequence of RecordBatches to a Parquet file,
    // used to inspect streamed data in tests. Recommend using a vscode extension
    // like "Parquet Viewer" to open the resulting files.
    fn write_parquet(path: &Path, batches: &[RecordBatch]) {
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return;
        }
        let schema = batches[0].schema();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("create dir {}: {e}", parent.display()));
        }
        let file = File::create(path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .unwrap_or_else(|e| panic!("ArrowWriter::try_new: {e}"));
        for batch in batches {
            writer
                .write(batch)
                .unwrap_or_else(|e| panic!("ArrowWriter::write: {e}"));
        }
        writer
            .close()
            .unwrap_or_else(|e| panic!("ArrowWriter::close: {e}"));
    }
}
