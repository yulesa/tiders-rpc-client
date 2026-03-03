//! RPC pipeline implementations: fetching, adaptive concurrency, Arrow conversion,
//! and streaming for blocks, logs, traces, and coordinated multi-pipeline queries.

pub(crate) mod arrow_convert;
pub(crate) mod block_adaptive_concurrency;
pub(crate) mod block_fetcher;
pub(crate) mod log_adaptive_concurrency;
mod log_fetcher;
mod multi_pipeline_stream;
mod provider;
pub(crate) mod shared_helpers;
pub(crate) mod single_block_adaptive_concurrency;
mod stream;
pub(crate) mod trace_fetcher;
pub(crate) mod tx_receipt_fetcher;

pub(crate) use multi_pipeline_stream::start_coordinated_stream;
pub(crate) use provider::RpcProvider;
pub(crate) use stream::{start_block_stream, start_log_stream, start_trace_stream};
