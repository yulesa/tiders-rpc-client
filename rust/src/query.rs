//! EVM query types for the RPC client.
//!
//! These mirror `tiders_ingest::evm::Query` but are owned by this crate
//! so the RPC client can evolve independently.

use anyhow::{bail, Result};
use log::{warn};

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
    pub include_transactions: bool,
    pub include_transaction_logs: bool,
    pub include_transaction_traces: bool,
    pub include_blocks: bool,
}

#[derive(Default, Debug, Clone)]
pub struct TransactionRequest {
    pub from_: Vec<Address>,
    pub to: Vec<Address>,
    pub sighash: Vec<Sighash>,
    pub status: Vec<u8>,
    pub type_: Vec<u8>,
    pub contract_deployment_address: Vec<Address>,
    pub hash: Vec<Hash>,
    pub include_logs: bool,
    pub include_traces: bool,
    pub include_blocks: bool,
}

#[derive(Default, Debug, Clone)]
pub struct TraceRequest {
    pub from_: Vec<Address>,
    pub to: Vec<Address>,
    pub address: Vec<Address>,
    pub call_type: Vec<String>,
    pub reward_type: Vec<String>,
    pub type_: Vec<String>,
    pub sighash: Vec<Sighash>,
    pub author: Vec<Address>,
    pub trace_method: TraceMethod,
    pub include_transactions: bool,
    pub include_transaction_logs: bool,
    pub include_transaction_traces: bool,
    pub include_blocks: bool,
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

/// Describes which pipelines should be run for a query.
///
/// The three pipelines correspond to distinct RPC methods:
/// - `blocks_transactions`: `eth_getBlockByNumber` — fetches blocks and transactions.
/// - `logs`: `eth_getLogs` — fetches event logs.
/// - `traces`: `trace_block` or `debug_traceBlockByNumber` — fetches traces.
///
/// When more than one flag is set, the coordinator runs all pipelines over the
/// same block range and merges results into a single `ArrowResponse`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pipelines {
    pub blocks_transactions: bool,
    pub traces: bool,
    pub logs: bool,
}

impl Pipelines {
    /// Returns `true` if more than one pipeline is needed, requiring coordination.
    pub fn needs_coordinator(&self) -> bool {
        (self.blocks_transactions as u8 + self.logs as u8 + self.traces as u8) > 1
    }
}

/// Validate a query and determine which RPC pipelines it requires.
///
/// Returns an error if the query:
/// - Selects fields that belong to different RPC pipelines (e.g. log fields
///   together with block/transaction fields) **without** using `include_*` flags.
/// - Has a `LogRequest` with both `include_*` flags and non-empty address/topic
///   filters. Cross pipeline queries are only supported when the result is the full
///   block range with no client-side filtering.
/// - Includes any filter fields on a `TransactionRequest` or `TraceRequest`.
///   RPC has no server-side filtering for blocks, transactions, or traces: every
///   block in the requested range is fetched unconditionally. Filtering must be
///   done by the caller after data is ingested (post-indexing).
pub(crate) fn analyze_query(query: &Query) -> Result<Pipelines> {
    let has_log_fields = has_any_log_field(&query.fields.log);
    let has_block_fields = has_any_block_field(&query.fields.block);
    let has_tx_fields = has_any_transaction_field(&query.fields.transaction);
    let has_trace_fields = has_any_trace_field(&query.fields.trace);

    let uses_log_pipeline = !query.logs.is_empty() || has_log_fields;
    let uses_block_pipeline = !query.transactions.is_empty()
        || has_block_fields
        || has_tx_fields
        || query.include_all_blocks;
    let uses_trace_pipeline = !query.traces.is_empty() || has_trace_fields;

    // Collect cross-pipeline requests from include_* flags.
    let mut cross_blocks_transactions = false;
    let mut cross_logs = false;
    let mut cross_traces = false;

    for req in &query.logs {
        // include_transactions and include_blocks request the Block pipeline.
        cross_blocks_transactions |= req.include_transactions || req.include_blocks;
        cross_traces |= req.include_transaction_traces;
        if req.include_transaction_logs {
            warn!("Logs request include_transaction_logs=true is ineffective since logs filters are not allowed in RPC client cross pipeline queries.");
        }
    }

    for req in &query.transactions {
        cross_logs |= req.include_logs;
        cross_traces |= req.include_traces;
        // Note: include_blocks is NOT checked here because transactions
        // already come from the Block pipeline (eth_getBlockByNumber),
        // which inherently fetches both blocks and transactions.
    }

    for req in &query.traces {
        // include_transactions and include_blocks request the Block pipeline.
        cross_blocks_transactions |= req.include_transactions || req.include_blocks;
        cross_logs |= req.include_transaction_logs;
    }

    let has_cross_pipeline = cross_blocks_transactions || cross_logs || cross_traces;

    // 1. Cross-pipeline field selection.
    //
    // Mixing pipelines via field selectors is only allowed when the user has
    // opted in via `include_*` flags. Without those flags, a query that
    // touches multiple pipelines is an error.
    let pipeline_count =
        uses_log_pipeline as u8 + uses_block_pipeline as u8 + uses_trace_pipeline as u8;
    if pipeline_count > 1 && !has_cross_pipeline {
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
             Use include_* flags on your request types to enable cross-pipeline \
             coordination, or split your indexer into separate pipelines and \
             filter/join post-indexing.",
            pipelines.join(", ")
        );
    }

    // 2. LogRequest include_* flags conflict with address/topic filters.
    //
    // When include_* is set on a LogRequest, the secondary pipelines
    // (eth_getBlockByNumber, trace_block) return all data for the full block
    // range with no server-side filtering. Combining that with log-side filters
    // would make the log table a filtered subset while blocks/transactions/traces
    // are unfiltered — an inconsistency the RPC layer cannot resolve.
    if has_cross_pipeline {
        for (i, req) in query.logs.iter().enumerate() {
            let has_include = req.include_transactions
                || req.include_transaction_logs
                || req.include_transaction_traces
                || req.include_blocks;
            if has_include {
                let mut filters: Vec<&str> = Vec::new();
                if !req.address.is_empty() { filters.push("address"); }
                if !req.topic0.is_empty() { filters.push("topic0"); }
                if !req.topic1.is_empty() { filters.push("topic1"); }
                if !req.topic2.is_empty() { filters.push("topic2"); }
                if !req.topic3.is_empty() { filters.push("topic3"); }
                if !filters.is_empty() {
                    bail!(
                        "logs[{i}] sets both include_* flags ({}) and field filters [{}]. \
                         Cross pipelines queries can only be used when the result is the full block range with no client-side filtering. \
                         Remove the log filters to use cross-pipeline coordination.",
                        [
                            req.include_transactions.then_some("include_transactions"),
                            req.include_transaction_logs.then_some("include_transaction_logs"),
                            req.include_transaction_traces.then_some("include_transaction_traces"),
                            req.include_blocks.then_some("include_blocks"),
                        ]
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>()
                        .join(", "),
                        filters.join(", ")
                    );
                }
            }
        }
    }

    // 3. TransactionRequest filter fields are not supported.
    //
    // eth_getBlockByNumber returns every transaction in a block with no
    // server-side filtering. Applying filters inside the client would fetch
    // all blocks anyway and silently hide that cost. Instead, ingest all
    // transactions and filter in your database post-indexing.
    if !query.transactions.is_empty() {
        for (i, req) in query.transactions.iter().enumerate() {
            let mut unsupported: Vec<&str> = Vec::new();
            if !req.from_.is_empty() { unsupported.push("from_"); }
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
                     database instead or use a different tiders client that supports filtering data on source.",
                    unsupported.join(", ")
                );
            }
        }
    }

    // 4. TraceRequest filter fields are not supported.
    //
    // trace_block / debug_traceBlockByNumber returns every trace in a block with
    // no server-side filtering. Applying filters inside the client would fetch
    // all traces anyway and silently hide that cost. Instead, ingest all traces
    // and filter in your database post-indexing.
    if !query.traces.is_empty() {
        for (i, req) in query.traces.iter().enumerate() {
            let mut unsupported: Vec<&str> = Vec::new();
            if !req.from_.is_empty() { unsupported.push("from_"); }
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
                     your database instead or use a different tiders client that supports \
                     filtering data on source.",
                    unsupported.join(", ")
                );
            }
        }
    }

    // 5. Build the final Pipelines, combining direct query usage with
    //    cross-pipeline include_* requests.
    Ok(Pipelines {
        blocks_transactions: uses_block_pipeline || cross_blocks_transactions,
        logs: uses_log_pipeline || cross_logs,
        traces: uses_trace_pipeline || cross_traces,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn log_query(logs: Vec<LogRequest>, fields: Fields) -> Query {
        Query { logs, fields, ..Query::default() }
    }

    fn block_query(transactions: Vec<TransactionRequest>, fields: Fields) -> Query {
        Query { transactions, fields, ..Query::default() }
    }

    fn trace_query(traces: Vec<TraceRequest>, fields: Fields) -> Query {
        Query { traces, fields, ..Query::default() }
    }

    // -----------------------------------------------------------------------
    // analyze_query — pipeline selection
    // -----------------------------------------------------------------------

    #[test]
    fn log_pipeline_only() {
        let q = log_query(vec![LogRequest::default()], Fields::default());
        let p = analyze_query(&q).unwrap();
        assert!(p.logs);
        assert!(!p.blocks_transactions);
        assert!(!p.traces);
        assert!(!p.needs_coordinator());
    }

    #[test]
    fn block_pipeline_when_transactions_present() {
        let q = block_query(vec![TransactionRequest::default()], Fields::default());
        let p = analyze_query(&q).unwrap();
        assert!(p.blocks_transactions);
        assert!(!p.logs);
        assert!(!p.traces);
        assert!(!p.needs_coordinator());
    }

    #[test]
    fn block_pipeline_when_block_fields_set() {
        let fields = Fields {
            block: BlockFields { number: true, ..BlockFields::default() },
            ..Fields::default()
        };
        let q = Query { fields, ..Query::default() };
        let p = analyze_query(&q).unwrap();
        assert!(p.blocks_transactions);
        assert!(!p.needs_coordinator());
    }

    #[test]
    fn trace_pipeline_only() {
        let q = trace_query(vec![TraceRequest::default()], Fields::default());
        let p = analyze_query(&q).unwrap();
        assert!(p.traces);
        assert!(!p.blocks_transactions);
        assert!(!p.logs);
        assert!(!p.needs_coordinator());
    }

    // -----------------------------------------------------------------------
    // analyze_query — cross-pipeline via include_* flags
    // -----------------------------------------------------------------------

    #[test]
    fn log_include_transactions_triggers_coordinator() {
        let req = LogRequest { include_transactions: true, ..LogRequest::default() };
        let q = log_query(vec![req], Fields::default());
        let p = analyze_query(&q).unwrap();
        assert!(p.logs);
        assert!(p.blocks_transactions);
        assert!(!p.traces);
        assert!(p.needs_coordinator());
    }

    #[test]
    fn log_include_blocks_triggers_coordinator() {
        let req = LogRequest { include_blocks: true, ..LogRequest::default() };
        let q = log_query(vec![req], Fields::default());
        let p = analyze_query(&q).unwrap();
        assert!(p.logs);
        assert!(p.blocks_transactions);
        assert!(p.needs_coordinator());
    }

    #[test]
    fn log_include_traces_triggers_coordinator() {
        let req = LogRequest { include_transaction_traces: true, ..LogRequest::default() };
        let q = log_query(vec![req], Fields::default());
        let p = analyze_query(&q).unwrap();
        assert!(p.logs);
        assert!(p.traces);
        assert!(p.needs_coordinator());
    }

    #[test]
    fn transaction_include_logs_triggers_coordinator() {
        let req = TransactionRequest { include_logs: true, ..TransactionRequest::default() };
        let q = block_query(vec![req], Fields::default());
        let p = analyze_query(&q).unwrap();
        assert!(p.blocks_transactions);
        assert!(p.logs);
        assert!(p.needs_coordinator());
    }

    #[test]
    fn transaction_include_traces_triggers_coordinator() {
        let req = TransactionRequest { include_traces: true, ..TransactionRequest::default() };
        let q = block_query(vec![req], Fields::default());
        let p = analyze_query(&q).unwrap();
        assert!(p.blocks_transactions);
        assert!(p.traces);
        assert!(p.needs_coordinator());
    }

    #[test]
    fn transaction_include_blocks_does_not_trigger_coordinator() {
        // Blocks and transactions are the same pipeline — no coordination needed.
        let req = TransactionRequest { include_blocks: true, ..TransactionRequest::default() };
        let q = block_query(vec![req], Fields::default());
        let p = analyze_query(&q).unwrap();
        assert!(p.blocks_transactions);
        assert!(!p.needs_coordinator());
    }

    #[test]
    fn trace_include_transactions_triggers_coordinator() {
        let req = TraceRequest { include_transactions: true, ..TraceRequest::default() };
        let q = trace_query(vec![req], Fields::default());
        let p = analyze_query(&q).unwrap();
        assert!(p.traces);
        assert!(p.blocks_transactions);
        assert!(p.needs_coordinator());
    }

    #[test]
    fn trace_include_blocks_triggers_coordinator() {
        let req = TraceRequest { include_blocks: true, ..TraceRequest::default() };
        let q = trace_query(vec![req], Fields::default());
        let p = analyze_query(&q).unwrap();
        assert!(p.traces);
        assert!(p.blocks_transactions);
        assert!(p.needs_coordinator());
    }

    // -----------------------------------------------------------------------
    // analyze_query — cross-pipeline error (no include_* flags)
    // -----------------------------------------------------------------------

    #[test]
    fn cross_pipeline_error_without_include_flags() {
        // Log fields + block fields without any include_* flag → error.
        let fields = Fields {
            log: LogFields { block_number: true, ..LogFields::default() },
            block: BlockFields { number: true, ..BlockFields::default() },
            ..Fields::default()
        };
        let q = Query { fields, ..Query::default() };
        assert!(
            analyze_query(&q).is_err(),
            "should error when mixing log and block fields without include_* flags"
        );
    }

    #[test]
    fn cross_pipeline_log_and_trace_fields_without_include_flags_errors() {
        let fields = Fields {
            log: LogFields { block_number: true, ..LogFields::default() },
            trace: TraceFields { from: true, ..TraceFields::default() },
            ..Fields::default()
        };
        let q = Query { fields, ..Query::default() };
        assert!(analyze_query(&q).is_err());
    }

    // -----------------------------------------------------------------------
    // analyze_query — LogRequest include_* + filters conflict
    // -----------------------------------------------------------------------

    #[test]
    fn log_include_with_empty_filters_ok() {
        let req = LogRequest { include_transactions: true, ..LogRequest::default() };
        let q = log_query(vec![req], Fields::default());
        // Should succeed (no filters set).
        assert!(analyze_query(&q).is_ok());
    }

    #[test]
    fn log_include_with_address_filter_errors() {
        let req = LogRequest {
            include_transactions: true,
            address: vec![Address([0u8; 20])],
            ..LogRequest::default()
        };
        let q = log_query(vec![req], Fields::default());
        let err = analyze_query(&q).unwrap_err();
        assert!(
            err.to_string().contains("include_transactions"),
            "error should mention include_transactions"
        );
        assert!(
            err.to_string().contains("address"),
            "error should mention the offending filter"
        );
    }

    #[test]
    fn log_include_with_topic0_filter_errors() {
        let req = LogRequest {
            include_blocks: true,
            topic0: vec![Topic([0u8; 32])],
            ..LogRequest::default()
        };
        let q = log_query(vec![req], Fields::default());
        assert!(analyze_query(&q).is_err());
    }

    // -----------------------------------------------------------------------
    // analyze_query — TransactionRequest and TraceRequest filter validation
    // -----------------------------------------------------------------------

    #[test]
    fn transaction_request_with_filter_fields_errors() {
        let req = TransactionRequest {
            from_: vec![Address([1u8; 20])],
            ..TransactionRequest::default()
        };
        let q = block_query(vec![req], Fields::default());
        let err = analyze_query(&q).unwrap_err();
        assert!(err.to_string().contains("from_"));
    }

    #[test]
    fn trace_request_with_filter_fields_errors() {
        let req = TraceRequest {
            call_type: vec!["call".to_string()],
            ..TraceRequest::default()
        };
        let q = trace_query(vec![req], Fields::default());
        let err = analyze_query(&q).unwrap_err();
        assert!(err.to_string().contains("call_type"));
    }

    #[test]
    fn transaction_request_with_include_and_no_filters_ok() {
        // include_logs on a TransactionRequest should be fine — existing filter
        // validation already guards against filter fields.
        let req = TransactionRequest { include_logs: true, ..TransactionRequest::default() };
        let q = block_query(vec![req], Fields::default());
        assert!(analyze_query(&q).is_ok());
    }
}
