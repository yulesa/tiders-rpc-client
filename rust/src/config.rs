use crate::query::TraceMethod;

/// Configuration types for the RPC client.

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub url: String,
    pub max_num_retries: u32,
    pub retry_backoff_ms: u64,
    pub retry_base_ms: u64,
    pub retry_ceiling_ms: u64,
    pub req_timeout_millis: u64,
    pub compute_units_per_second: Option<u64>,
    pub max_concurrent_requests: Option<usize>,
    pub batch_size: Option<usize>,
    pub rpc_batch_size: Option<usize>,
    pub max_block_range: Option<u64>,
    pub trace_method: Option<TraceMethod>,
}

impl ClientConfig {
    pub fn new(url: String) -> Self {
        Self {
            url,
            max_num_retries: 5000,
            retry_backoff_ms: 1000,
            retry_base_ms: 300,
            retry_ceiling_ms: 10_000,
            req_timeout_millis: 90_000,
            compute_units_per_second: None,
            max_concurrent_requests: None,
            batch_size: None,
            rpc_batch_size: None,
            max_block_range: None,
            trace_method: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub stop_on_head: bool,
    pub head_poll_interval_millis: u64,
    pub buffer_size: usize,
    pub reorg_safe_distance: u64,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            stop_on_head: false,
            head_poll_interval_millis: 1000,
            buffer_size: 10,
            reorg_safe_distance: 0,
        }
    }
}
