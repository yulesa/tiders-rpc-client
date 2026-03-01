use crate::query::TraceMethod;

/// All configuration for the RPC client, including both connection settings
/// and stream behaviour.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub url: String,
    pub bearer_token: Option<String>,
    pub max_num_retries: u32,
    pub retry_backoff_ms: u64,
    pub retry_base_ms: u64,
    pub retry_ceiling_ms: u64,
    pub req_timeout_millis: u64,
    pub compute_units_per_second: Option<u64>,
    pub batch_size: Option<usize>,
    pub trace_method: Option<TraceMethod>,
    pub stop_on_head: bool,
    pub head_poll_interval_millis: u64,
    pub buffer_size: usize,
    pub reorg_safe_distance: u64,
}

impl ClientConfig {
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
