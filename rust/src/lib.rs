//! RPC client for fetching EVM blockchain data from JSON-RPC providers
//! and converting it into Apache Arrow format.
//!
//! Designed for use with the [tiders](https://github.com/yulesa/tiders)
//! data pipeline framework.

mod client;
pub mod config;
pub mod query;
mod response;
mod rpc;

pub use client::{Client, DataStream};
pub use config::ClientConfig;
pub use query::{
    Address, BlockFields, Fields, Hash, LogFields, LogRequest, Query, Sighash, Topic, TraceFields,
    TraceMethod, TraceRequest, TransactionFields, TransactionRequest,
};
pub use response::ArrowResponse;
