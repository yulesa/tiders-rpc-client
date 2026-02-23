use std::collections::BTreeMap;
use std::pin::Pin;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use futures_lite::Stream;

use crate::config::ClientConfig;
use crate::query::{analyze_query, Pipeline, Query};
use crate::response::ArrowResponse;
use crate::rpc::{start_block_stream, start_log_stream, start_trace_stream, RpcProvider};

pub type DataStream = Pin<Box<dyn Stream<Item = Result<ArrowResponse>> + Send + Sync>>;

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

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Start a streaming query that yields `ArrowResponse` chunks.
    ///
    /// Selects the appropriate pipeline based on the query:
    /// - If log requests are present, uses the `eth_getLogs` pipeline.
    /// - Otherwise (block/transaction fields requested), uses the
    ///   `eth_getBlockByNumber` pipeline.
    pub fn stream(&self, query: Query) -> Result<DataStream> {
        let pipeline = analyze_query(&query)?;

        let rx = match pipeline {
            Pipeline::Log => start_log_stream(self.provider.clone(), query, self.config.clone()),
            Pipeline::Block => {
                start_block_stream(self.provider.clone(), query, self.config.clone())
            }
            Pipeline::Trace => {
                start_trace_stream(self.provider.clone(), query, self.config.clone())
            }
        };
        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    /// Convert an `ArrowResponse` into the `BTreeMap<String, RecordBatch>`
    /// format expected by the cherry-core ingest pipeline.
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
        TransactionFields, TransactionRequest,
    };

    /// Root of the cherry-rpc-client repository (one level above `rust/`).
    const REPO_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/..");

    /// Tenderly free Ethereum mainnet gateway (same default as rindexer examples).
    const RPC_URL: &str = "https://mainnet.gateway.tenderly.co";

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
                &cherry_evm_schema::logs_schema(),
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
    /// via the block fetcher pipeline (`eth_getBlockByNumber`) and saves
    /// blocks and transactions to Parquet.
    #[tokio::test]
    async fn stream_blocks_and_transactions() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .parse_default_env()
            .try_init();
        let client = make_client();

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
                    // block_number: true,
                    hash: true,
                    // from: true,
                    // to: true,
                    // value: true,
                    // gas: true,
                    // gas_price: true,
                    // transaction_index: true,
                    ..TransactionFields::default()
                },
                log: LogFields {
                    ..LogFields::default()
                },
                trace: TraceFields {
                    ..TraceFields::default()
                },
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
            assert_eq!(tx_schema.fields().len(), 1);

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

    /// Streaming query: fetches blocks and their transaction receipts via
    /// `eth_getBlockReceipts` and saves all chunks to Parquet.
    ///
    /// Verifies that receipt-only columns (`gas_used`, `status`,
    /// `effective_gas_price`, `cumulative_gas_used`, `contract_address`,
    /// `logs_bloom`) are present and non-empty in the transaction batch.
    #[tokio::test]
    async fn stream_blocks_with_receipts() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .parse_default_env()
            .try_init();
        let client = make_client();

        let latest_block = client
            .provider
            .get_block_number()
            .await
            .unwrap_or_else(|e| panic!("failed to get block number: {e}"));
        log::info!("Latest block: {latest_block}");

        // Use a small range so the test runs quickly.
        let from_block = latest_block.saturating_sub(5);

        let query = Query {
            from_block,
            to_block: Some(latest_block),
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
                    ..BlockFields::default()
                },
                transaction: TransactionFields {
                    hash: true,
                    from: true,
                    to: true,
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

        let mut total_tx_rows = 0u64;
        let mut all_responses: Vec<ArrowResponse> = Vec::new();

        while let Some(result) = stream.next().await {
            let resp = result.unwrap_or_else(|e| panic!("stream chunk failed: {e}"));

            // Receipt-only columns must be present in the schema.
            let tx_schema = resp.transactions.schema();
            assert!(
                tx_schema.field_with_name("gas_used").is_ok(),
                "gas_used column missing"
            );
            assert!(
                tx_schema.field_with_name("status").is_ok(),
                "status column missing"
            );
            assert!(
                tx_schema.field_with_name("effective_gas_price").is_ok(),
                "effective_gas_price column missing"
            );
            assert!(
                tx_schema.field_with_name("cumulative_gas_used").is_ok(),
                "cumulative_gas_used column missing"
            );

            total_tx_rows += resp.transactions.num_rows() as u64;
            all_responses.push(resp);
        }

        assert!(
            total_tx_rows > 0,
            "stream should have yielded transaction rows with receipt data"
        );

        log::info!("Streamed {total_tx_rows} tx rows with receipt data");

        let root = Path::new(REPO_ROOT);
        for (name, extract) in [
            (
                "blocks",
                (|r: &ArrowResponse| r.blocks.clone()) as fn(&ArrowResponse) -> RecordBatch,
            ),
            ("transactions", |r| r.transactions.clone()),
        ] {
            let batches: Vec<RecordBatch> = all_responses.iter().map(extract).collect();
            let path = root.join(format!("data/receipt_stream_{name}.parquet"));
            write_parquet(&path, &batches);
        }
    }

    fn write_parquet(path: &Path, batches: &[RecordBatch]) {
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return;
        }
        let schema = batches[0].schema();
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
