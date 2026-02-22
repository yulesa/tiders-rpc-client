//! EVM query types for the RPC client.
//!
//! These mirror `cherry_ingest::evm::Query` but are owned by this crate
//! so the RPC client can evolve independently.

use anyhow::{bail, Result};

#[derive(Default, Debug, Clone, Copy)]
pub struct Address(pub [u8; 20]);

#[derive(Default, Debug, Clone, Copy)]
pub struct Topic(pub [u8; 32]);

#[derive(Default, Debug, Clone, Copy)]
pub struct Sighash(pub [u8; 4]);

#[derive(Default, Debug, Clone, Copy)]
pub struct Hash(pub [u8; 32]);

#[derive(Default, Debug, Clone)]
pub struct Query {
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub include_all_blocks: bool,
    pub logs: Vec<LogRequest>,
    pub transactions: Vec<TransactionRequest>,
    pub traces: Vec<TraceRequest>,
    pub fields: Fields,
}

#[derive(Default, Debug, Clone)]
pub struct LogRequest {
    pub address: Vec<Address>,
    pub topic0: Vec<Topic>,
    pub topic1: Vec<Topic>,
    pub topic2: Vec<Topic>,
    pub topic3: Vec<Topic>,
}

#[derive(Default, Debug, Clone)]
pub struct TransactionRequest {
    pub from: Vec<Address>,
    pub to: Vec<Address>,
    pub sighash: Vec<Sighash>,
    pub status: Vec<u8>,
    pub type_: Vec<u8>,
    pub contract_deployment_address: Vec<Address>,
    pub hash: Vec<Hash>,
}

#[derive(Default, Debug, Clone)]
pub struct TraceRequest {
    pub from: Vec<Address>,
    pub to: Vec<Address>,
    pub address: Vec<Address>,
    pub call_type: Vec<String>,
    pub reward_type: Vec<String>,
    pub type_: Vec<String>,
    pub sighash: Vec<Sighash>,
    pub author: Vec<Address>,
    pub trace_method: TraceMethod,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum TraceMethod {
    #[default]
    TraceBlock,
    DebugTraceBlockByNumber,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct Fields {
    pub block: BlockFields,
    pub transaction: TransactionFields,
    pub log: LogFields,
    pub trace: TraceFields,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct BlockFields {
    pub number: bool,
    pub hash: bool,
    pub parent_hash: bool,
    pub nonce: bool,
    pub sha3_uncles: bool,
    pub logs_bloom: bool,
    pub transactions_root: bool,
    pub state_root: bool,
    pub receipts_root: bool,
    pub miner: bool,
    pub difficulty: bool,
    pub total_difficulty: bool,
    pub extra_data: bool,
    pub size: bool,
    pub gas_limit: bool,
    pub gas_used: bool,
    pub timestamp: bool,
    pub uncles: bool,
    pub base_fee_per_gas: bool,
    pub blob_gas_used: bool,
    pub excess_blob_gas: bool,
    pub parent_beacon_block_root: bool,
    pub withdrawals_root: bool,
    pub withdrawals: bool,
    pub l1_block_number: bool,
    pub send_count: bool,
    pub send_root: bool,
    pub mix_hash: bool,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct TransactionFields {
    pub block_hash: bool,
    pub block_number: bool,
    pub from: bool,
    pub gas: bool,
    pub gas_price: bool,
    pub hash: bool,
    pub input: bool,
    pub nonce: bool,
    pub to: bool,
    pub transaction_index: bool,
    pub value: bool,
    pub v: bool,
    pub r: bool,
    pub s: bool,
    pub max_priority_fee_per_gas: bool,
    pub max_fee_per_gas: bool,
    pub chain_id: bool,
    pub cumulative_gas_used: bool,
    pub effective_gas_price: bool,
    pub gas_used: bool,
    pub contract_address: bool,
    pub logs_bloom: bool,
    pub type_: bool,
    pub root: bool,
    pub status: bool,
    pub sighash: bool,
    pub y_parity: bool,
    pub access_list: bool,
    pub l1_fee: bool,
    pub l1_gas_price: bool,
    pub l1_fee_scalar: bool,
    pub gas_used_for_l1: bool,
    pub max_fee_per_blob_gas: bool,
    pub blob_versioned_hashes: bool,
    pub deposit_nonce: bool,
    pub blob_gas_price: bool,
    pub deposit_receipt_version: bool,
    pub blob_gas_used: bool,
    pub l1_base_fee_scalar: bool,
    pub l1_blob_base_fee: bool,
    pub l1_blob_base_fee_scalar: bool,
    pub l1_block_number: bool,
    pub mint: bool,
    pub source_hash: bool,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct LogFields {
    pub removed: bool,
    pub log_index: bool,
    pub transaction_index: bool,
    pub transaction_hash: bool,
    pub block_hash: bool,
    pub block_number: bool,
    pub address: bool,
    pub data: bool,
    pub topic0: bool,
    pub topic1: bool,
    pub topic2: bool,
    pub topic3: bool,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct TraceFields {
    pub from: bool,
    pub to: bool,
    pub call_type: bool,
    pub gas: bool,
    pub input: bool,
    pub init: bool,
    pub value: bool,
    pub author: bool,
    pub reward_type: bool,
    pub block_hash: bool,
    pub block_number: bool,
    pub address: bool,
    pub code: bool,
    pub gas_used: bool,
    pub output: bool,
    pub subtraces: bool,
    pub trace_address: bool,
    pub transaction_hash: bool,
    pub transaction_position: bool,
    pub type_: bool,
    pub error: bool,
    pub sighash: bool,
    pub action_address: bool,
    pub balance: bool,
    pub refund_address: bool,
}

/// Return `true` if any of the listed bool fields is set.
macro_rules! any_field_set {
    ($obj:expr, $( $field:ident ),+ $(,)?) => {
        $( $obj.$field )||+
    };
}

/// The RPC pipeline a validated query should use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Pipeline {
    /// `eth_getBlockByNumber` — fetches blocks and transactions.
    Block,
    /// `eth_getLogs` — fetches event logs.
    Log,
    /// `trace_block` or `debug_traceBlockByNumber` — fetches traces.
    Trace,
}

/// Validate a query and determine which RPC pipeline it requires.
///
/// Returns an error if the query:
/// - Selects fields that belong to different RPC pipelines (e.g. log fields
///   together with block/transaction fields).
/// - Includes any filter fields on a `TransactionRequest` or `TraceRequest`.
///   RPC has no server-side filtering for blocks, transactions, or traces: every
///   block in the requested range is fetched unconditionally. Filtering must be
///   done by the caller after data is ingested (post-indexing).
pub(crate) fn analyze_query(query: &Query) -> Result<Pipeline> {
    let has_log_fields = has_any_log_field(&query.fields.log);
    let has_block_fields = has_any_block_field(&query.fields.block);
    let has_tx_fields = has_any_transaction_field(&query.fields.transaction);
    let has_trace_fields = has_any_trace_field(&query.fields.trace);

    let uses_log_pipeline = !query.logs.is_empty() || has_log_fields;
    let uses_block_pipeline = has_block_fields || has_tx_fields || !query.transactions.is_empty();
    let uses_trace_pipeline = has_trace_fields || !query.traces.is_empty();

    // 1. Cross-pipeline field selection
    let pipeline_count =
        uses_log_pipeline as u8 + uses_block_pipeline as u8 + uses_trace_pipeline as u8;
    if pipeline_count > 1 {
        let mut pipelines = Vec::new();
        if uses_log_pipeline {
            pipelines.push("eth_getLogs (log fields/filters)");
        }
        if uses_block_pipeline {
            pipelines.push("eth_getBlockByNumber (block/transaction fields/filters)");
        }
        if uses_trace_pipeline {
            pipelines.push("trace_block (trace fields/filters)");
        }
        bail!(
            "Query mixes fields/filters from different RPC pipelines: [{}]. \
             RPC providers only support queries that target one pipeline. \
             Split your indexer into separate pipelines and filter/join post-indexing.",
            pipelines.join(", ")
        );
    }

    // 2. TransactionRequest filter fields are not supported.
    //
    // eth_getBlockByNumber returns every transaction in a block with no
    // server-side filtering. Applying filters inside the client would fetch
    // all blocks anyway and silently hide that cost. Instead, ingest all
    // transactions and filter in your database post-indexing.
    if !query.transactions.is_empty() {
        for (i, req) in query.transactions.iter().enumerate() {
            let mut unsupported: Vec<&str> = Vec::new();
            if !req.from.is_empty() { unsupported.push("from"); }
            if !req.to.is_empty() { unsupported.push("to"); }
            if !req.sighash.is_empty() { unsupported.push("sighash"); }
            if !req.type_.is_empty() { unsupported.push("type_"); }
            if !req.hash.is_empty() { unsupported.push("hash"); }
            if !req.status.is_empty() { unsupported.push("status"); }
            if !req.contract_deployment_address.is_empty() {
                unsupported.push("contract_deployment_address");
            }
            if !unsupported.is_empty() {
                bail!(
                    "transactions[{i}] sets filter fields [{}] which are not supported by the \
                     RPC block pipeline. eth_getBlockByNumber returns all transactions in a block \
                     with no server-side filtering — every block in the range is fetched \
                     regardless. Remove the filters and perform them post-indexing in your \
                     database instead or use a different cherry client that supports filtering data on source.",
                    unsupported.join(", ")
                );
            }
        }
    }

    // 3. TraceRequest filter fields are not supported.
    //
    // trace_block / debug_traceBlockByNumber returns every trace in a block with
    // no server-side filtering. Applying filters inside the client would fetch
    // all traces anyway and silently hide that cost. Instead, ingest all traces
    // and filter in your database post-indexing.
    if !query.traces.is_empty() {
        for (i, req) in query.traces.iter().enumerate() {
            let mut unsupported: Vec<&str> = Vec::new();
            if !req.from.is_empty() { unsupported.push("from"); }
            if !req.to.is_empty() { unsupported.push("to"); }
            if !req.address.is_empty() { unsupported.push("address"); }
            if !req.call_type.is_empty() { unsupported.push("call_type"); }
            if !req.reward_type.is_empty() { unsupported.push("reward_type"); }
            if !req.type_.is_empty() { unsupported.push("type_"); }
            if !req.sighash.is_empty() { unsupported.push("sighash"); }
            if !req.author.is_empty() { unsupported.push("author"); }
            if !unsupported.is_empty() {
                bail!(
                    "traces[{i}] sets filter fields [{}] which are not supported by the \
                     RPC trace pipeline. trace_block / debug_traceBlockByNumber returns all \
                     traces in a block with no server-side filtering — every block in the range \
                     is fetched regardless. Remove the filters and perform them post-indexing in \
                     your database instead or use a different cherry client that supports \
                     filtering data on source.",
                    unsupported.join(", ")
                );
            }
        }
    }

    // 4. Determine pipeline
    if uses_log_pipeline {
        Ok(Pipeline::Log)
    } else if uses_trace_pipeline {
        Ok(Pipeline::Trace)
    } else {
        Ok(Pipeline::Block)
    }
}

/// Return the `TraceMethod` to use for the query.
///
/// Uses the first `TraceRequest`'s method, defaulting to `TraceBlock`.
pub(crate) fn get_trace_method(query: &Query) -> TraceMethod {
    query
        .traces
        .first()
        .map(|r| r.trace_method)
        .unwrap_or_default()
}

fn has_any_log_field(f: &LogFields) -> bool {
    any_field_set!(
        f, removed, log_index, transaction_index, transaction_hash, block_hash, block_number,
        address, data, topic0, topic1, topic2, topic3,
    )
}

fn has_any_block_field(f: &BlockFields) -> bool {
    any_field_set!(
        f, number, hash, parent_hash, nonce, sha3_uncles, logs_bloom, transactions_root,
        state_root, receipts_root, miner, difficulty, total_difficulty, extra_data, size,
        gas_limit, gas_used, timestamp, uncles, base_fee_per_gas, blob_gas_used,
        excess_blob_gas, parent_beacon_block_root, withdrawals_root, withdrawals,
        l1_block_number, send_count, send_root, mix_hash,
    )
}

fn has_any_transaction_field(f: &TransactionFields) -> bool {
    any_field_set!(
        f, block_hash, block_number, from, gas, gas_price, hash, input, nonce, to,
        transaction_index, value, v, r, s, max_priority_fee_per_gas, max_fee_per_gas, chain_id,
        cumulative_gas_used, effective_gas_price, gas_used, contract_address, logs_bloom, type_,
        root, status, sighash, y_parity, access_list, l1_fee, l1_gas_price, l1_fee_scalar,
        gas_used_for_l1, max_fee_per_blob_gas, blob_versioned_hashes, deposit_nonce,
        blob_gas_price, deposit_receipt_version, blob_gas_used, l1_base_fee_scalar,
        l1_blob_base_fee, l1_blob_base_fee_scalar, l1_block_number, mint, source_hash,
    )
}

fn has_any_tx_receipt_field(f: &TransactionFields) -> bool {
    any_field_set!(
        f, cumulative_gas_used, effective_gas_price, gas_used,
        contract_address, logs_bloom, root, status,
    )
}

fn has_any_trace_field(f: &TraceFields) -> bool {
    any_field_set!(
        f, from, to, call_type, gas, input, init, value, author, reward_type,
        block_hash, block_number, address, code, gas_used, output, subtraces,
        trace_address, transaction_hash, transaction_position, type_, error,
        sighash, action_address, balance, refund_address,
    )
}

/// Return `true` if any transaction field *other than `hash`* is set.
///
/// Used to determine whether `eth_getBlockByNumber` must be called with `include_txs=true`.
fn has_tx_fields_except_hash(f: &TransactionFields) -> bool {
    any_field_set!(
        f, block_hash, block_number, from, gas, gas_price, input, nonce, to,
        transaction_index, value, v, r, s, max_priority_fee_per_gas, max_fee_per_gas, chain_id,
        cumulative_gas_used, effective_gas_price, gas_used, contract_address, logs_bloom, type_,
        root, status, sighash, y_parity, access_list, l1_fee, l1_gas_price, l1_fee_scalar,
        gas_used_for_l1, max_fee_per_blob_gas, blob_versioned_hashes, deposit_nonce,
        blob_gas_price, deposit_receipt_version, blob_gas_used, l1_base_fee_scalar,
        l1_blob_base_fee, l1_blob_base_fee_scalar, l1_block_number, mint, source_hash,
    )
}

/// Return `true` if `eth_getBlockByNumber` must be called with `include_txs=true`.
///
/// Full transaction objects are needed when any transaction field other than
/// `hash` is requested.
pub(crate) fn get_blocks_needs_full_txs(query: &Query) -> bool {
    has_tx_fields_except_hash(&query.fields.transaction)
}

/// Return `true` if the query requests any field that only comes from receipt data.
///
/// Used by the block pipeline to decide whether to call `eth_getBlockReceipts`.
pub(crate) fn needs_tx_receipts(query: &Query) -> bool {
    has_any_tx_receipt_field(&query.fields.transaction)
}

