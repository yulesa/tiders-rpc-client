//! RPC provider setup using alloy's `RpcClient` with `RetryBackoffLayer`.
//!
//! Adapted from rindexer's `create_client()` pattern (provider.rs:707-796).

use std::time::Duration;

use alloy::{
    network::AnyNetwork,
    providers::{
        fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller},
        Identity, Provider, ProviderBuilder, RootProvider,
    },
    rpc::client::RpcClient,
    transports::{
        http::{reqwest, Http},
        layers::RetryBackoffLayer,
    },
};
use anyhow::{Context, Result};
use url::Url;

use crate::config::ClientConfig;

/// The concrete alloy provider type with default fillers.
type AlloyProvider = FillProvider<
    JoinFill<
        Identity,
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
    >,
    RootProvider<AnyNetwork>,
    AnyNetwork,
>;

/// A thin wrapper around an alloy provider.
#[derive(Debug, Clone)]
pub struct RpcProvider {
    pub provider: AlloyProvider,
}

impl RpcProvider {
    /// Create a new RPC provider from client configuration.
    ///
    /// Sets up:
    /// - HTTP transport with configurable timeout
    /// - `RetryBackoffLayer` for automatic transport-level retries with
    ///   exponential backoff and compute-unit rate limiting
    pub fn new(config: &ClientConfig) -> Result<Self> {
        let rpc_url = Url::parse(&config.url).context("invalid RPC URL")?;

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.req_timeout_millis))
            .build()
            .context("failed to build HTTP client")?;

        let http = Http::with_client(http_client, rpc_url);

        let retry_layer = RetryBackoffLayer::new(
            config.max_num_retries,
            config.retry_backoff_ms,
            config.compute_units_per_second.unwrap_or(660),
        );

        let rpc_client = RpcClient::builder()
            .layer(retry_layer)
            .transport(http, false);

        let provider = ProviderBuilder::new()
            .network::<AnyNetwork>()
            .connect_client(rpc_client);

        Ok(Self { provider })
    }

    /// Fetch the latest block number from the provider.
    pub async fn get_block_number(&self) -> Result<u64> {
        let number = self
            .provider
            .get_block_number()
            .await
            .context("failed to get block number")?;
        Ok(number)
    }
}
