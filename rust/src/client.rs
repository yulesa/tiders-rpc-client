use std::collections::BTreeMap;
use std::pin::Pin;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use futures_lite::Stream;

use crate::config::ClientConfig;
use crate::query::Query;
use crate::response::ArrowResponse;
use crate::rpc::{start_block_stream, start_log_stream, RpcProvider};

pub type DataStream = Pin<Box<dyn Stream<Item = Result<ArrowResponse>> + Send + Sync>>;

/// Returns `true` if the query requires the `eth_getLogs` pipeline.
///
/// The log pipeline is selected when the query contains log requests.
/// Otherwise the block pipeline (`eth_getBlockByNumber`) is used.
fn needs_log_pipeline(query: &Query) -> bool {
    !query.logs.is_empty()
}

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
        let rx = if needs_log_pipeline(&query) {
            start_log_stream(self.provider.clone(), query, self.config.clone())
        } else {
            start_block_stream(self.provider.clone(), query, self.config.clone())
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
        TransactionFields,
    };

    /// Root of the cherry-rpc-client repository (one level above `rust/`).
    const REPO_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/..");

    /// Tenderly free Ethereum mainnet gateway (same default as rindexer examples).
    const RPC_URL: &str = "https://mainnet.gateway.tenderly.co";

    fn make_client() -> Client {
        Client::new(ClientConfig {
            stop_on_head: true,
            max_block_range: Some(500),
            ..ClientConfig::new(RPC_URL.to_owned())
        })
        .unwrap_or_else(|e| panic!("Failed to create client: {e}"))
    }

    /// Streaming query: fetches the last 5000 blocks up to the current head
    /// (`stop_on_head: true`) and saves all log chunks to Parquet.
    #[tokio::test]
    async fn stream_reth_transfer_logs() {
        /// Initialise env_logger so log output is visible when running
        /// tests with `--nocapture`. Defaults to `info`; override with `RUST_LOG`.
        /// Silently ignores the error if a logger has already been set.
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
                    number: true,
                    ..BlockFields::default()
                },
                transaction: TransactionFields {
                    hash: true,
                    ..TransactionFields::default()
                },
                trace: TraceFields {
                    from: true,
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
