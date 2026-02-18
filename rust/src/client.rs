use std::collections::BTreeMap;
use std::pin::Pin;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use futures_lite::Stream;

use crate::config::{ClientConfig, StreamConfig};
use crate::query::Query;
use crate::response::ArrowResponse;

pub type DataStream = Pin<Box<dyn Stream<Item = Result<ArrowResponse>> + Send + Sync>>;

#[derive(Debug, Clone)]
pub struct Client {
    config: ClientConfig,
}

impl Client {
    pub fn new(config: ClientConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Execute a one-shot query and return the Arrow response.
    ///
    /// In Part 1 this returns empty `RecordBatch`es with the correct
    /// `cherry-evm-schema` schemas. Later parts will fill in real data.
    #[expect(clippy::unused_async)]
    pub async fn query(&self, _query: &Query) -> Result<ArrowResponse> {
        Ok(ArrowResponse::empty())
    }

    /// Start a streaming query that yields `ArrowResponse` chunks.
    ///
    /// In Part 1 the stream emits a single empty response and then ends.
    /// Later parts will implement real block-range iteration and head tracking.
    pub fn stream(&self, _query: Query, _stream_config: StreamConfig) -> Result<DataStream> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ArrowResponse>>(1);

        tokio::spawn(async move {
            let _ = tx.send(Ok(ArrowResponse::empty())).await;
        });

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
mod tests {
    use futures_lite::StreamExt;

    use super::*;
    use crate::config::{ClientConfig, StreamConfig};
    use crate::query::Query;

    #[tokio::test]
    async fn query_and_response_to_btree() {
        let client = Client::new(ClientConfig::new("http://localhost:8545".to_owned()));

        // query() returns empty batches
        let resp = client.query(&Query::default()).await.unwrap();
        assert_eq!(resp.blocks.num_rows(), 0);
        assert_eq!(resp.transactions.num_rows(), 0);
        assert_eq!(resp.logs.num_rows(), 0);
        assert_eq!(resp.traces.num_rows(), 0);

        // response_to_btree() maps to the four canonical keys
        let btree = Client::response_to_btree(resp);
        assert_eq!(btree.len(), 4);
        assert!(btree.contains_key("blocks"));
        assert!(btree.contains_key("transactions"));
        assert!(btree.contains_key("logs"));
        assert!(btree.contains_key("traces"));
    }

    #[tokio::test]
    async fn stream_emits_one_empty_response_then_ends() {
        let client = Client::new(ClientConfig::new("http://localhost:8545".to_owned()));
        let mut stream = client
            .stream(Query::default(), StreamConfig::default())
            .unwrap();

        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.blocks.num_rows(), 0);
        assert!(stream.next().await.is_none());
    }
}
