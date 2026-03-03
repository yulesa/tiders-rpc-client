use crate::query::TraceMethod;

/// All configuration for the RPC client, including both connection settings
/// and stream behaviour.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// JSON-RPC endpoint URL.
    pub url: String,
    /// Optional bearer token for authenticated providers.
    pub bearer_token: Option<String>,
    /// Maximum number of retries for a single RPC call.
    pub max_num_retries: u32,
    /// Fixed per-retry delay in milliseconds used by alloy's `RetryBackoffLayer`.
    pub retry_backoff_ms: u64,
    /// Base delay for exponential backoff (used by per-block retry loops, not by alloy).
    pub retry_base_ms: u64,
    /// Maximum delay for exponential backoff (used by per-block retry loops, not by alloy).
    pub retry_ceiling_ms: u64,
    /// Per-request HTTP timeout in milliseconds.
    pub req_timeout_millis: u64,
    /// Compute-unit rate limit for alloy's `RetryBackoffLayer`.
    pub compute_units_per_second: Option<u64>,
    /// Number of blocks per batch. Controls memory usage in multi-pipeline mode.
    pub batch_size: Option<usize>,
    /// Override the trace method for all trace requests.
    pub trace_method: Option<TraceMethod>,
    /// When `true`, stop the stream after reaching the chain head instead of
    /// entering live-polling mode.
    pub stop_on_head: bool,
    /// How often to poll for new blocks during live mode, in milliseconds.
    pub head_poll_interval_millis: u64,
    /// Bounded channel capacity for the `ArrowResponse` stream.
    pub buffer_size: usize,
    /// Number of blocks behind the head to stay, to avoid reorged data.
    pub reorg_safe_distance: u64,
}

impl ClientConfig {
    /// Create a new configuration with sensible defaults for the given RPC URL.
    pub fn new(url: String) -> Self {
        Self {
            url,
            bearer_token: None,
            max_num_retries: 5000,
            retry_backoff_ms: 1000,
            retry_base_ms: 300,
            retry_ceiling_ms: 10_000,
            req_timeout_millis: 30_000,
            compute_units_per_second: None,
            batch_size: None,
            trace_method: None,
            stop_on_head: false,
            head_poll_interval_millis: 1000,
            buffer_size: 10,
            reorg_safe_distance: 0,
        }
    }
}
