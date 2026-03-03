//! Convert alloy RPC types to Arrow `RecordBatch` using `tiders_evm_schema` builders.

use std::collections::HashMap;

use alloy::consensus::{Transaction as TransactionTrait, TxReceipt};
use alloy::eips::Typed2718;
use alloy::network::primitives::BlockTransactions;
use alloy::network::{AnyRpcBlock, AnyTransactionReceipt, TransactionResponse};
use alloy::primitives::{TxHash, B256, U128, U256};
use alloy::rpc::types::trace::parity::{Action, LocalizedTransactionTrace, TraceOutput};
use alloy::rpc::types::Log;
use arrow::array::{
    builder::{BinaryBuilder, Decimal256Builder},
    Array,
};
use arrow::datatypes::i256;
use arrow::record_batch::RecordBatch;
use tiders_evm_schema::{BlocksBuilder, LogsBuilder, TracesBuilder, TransactionsBuilder};

use crate::query::{BlockFields, LogFields, TraceFields, TransactionFields};

/// Convert a `U256` (unsigned 256-bit) to Arrow's `i256` (signed 256-bit).
///
/// The `U256` value is stored in big-endian byte form and reinterpreted as a
/// two's-complement signed integer.  Values that set the highest bit will
/// appear negative, but for Ethereum quantities this is not expected to occur.
fn u256_to_i256(val: U256) -> i256 {
    let be_bytes: [u8; 32] = val.to_be_bytes();
    i256::from_be_bytes(be_bytes)
}

/// Convert `u128` to `i256` for Decimal256 columns.
#[expect(clippy::cast_possible_wrap)]
fn u128_to_i256(val: u128) -> i256 {
    // Ethereum u128 values (gas prices, fees) never exceed i128::MAX in practice.
    i256::from_i128(val as i128)
}

/// Convert `u64` to `i256` for Decimal256 columns.
fn u64_to_i256(val: u64) -> i256 {
    i256::from_i128(i128::from(val))
}

/// Append an optional `B256` to a `BinaryBuilder`.
fn append_optional_b256(builder: &mut BinaryBuilder, val: Option<B256>) {
    match val {
        Some(v) => builder.append_value(v.as_slice()),
        None => builder.append_null(),
    }
}

/// Append an optional `U256` to a `Decimal256Builder`.
fn append_optional_u256(builder: &mut Decimal256Builder, val: Option<U256>) {
    match val {
        Some(v) => builder.append_value(u256_to_i256(v)),
        None => builder.append_null(),
    }
}

/// Append an optional `u128` to a `Decimal256Builder`.
fn append_optional_u128(builder: &mut Decimal256Builder, val: Option<u128>) {
    match val {
        Some(v) => builder.append_value(u128_to_i256(v)),
        None => builder.append_null(),
    }
}

/// Append an optional `u64` to a `Decimal256Builder`.
fn append_optional_u64_decimal(builder: &mut Decimal256Builder, val: Option<u64>) {
    match val {
        Some(v) => builder.append_value(u64_to_i256(v)),
        None => builder.append_null(),
    }
}

// ---------------------------------------------------------------------------
// Functions for Logs conversion to RecordBatch
// ---------------------------------------------------------------------------

/// Convert a slice of alloy `Log`s into a `RecordBatch` matching `logs_schema()`.
pub fn logs_to_record_batch(logs: &[Log]) -> RecordBatch {
    let mut builder = LogsBuilder::default();

    for log in logs {
        // removed
        builder.removed.append_value(log.removed);

        // log_index
        if let Some(idx) = log.log_index {
            builder.log_index.append_value(idx);
        } else {
            builder.log_index.append_null();
        }

        // transaction_index
        if let Some(idx) = log.transaction_index {
            builder.transaction_index.append_value(idx);
        } else {
            builder.transaction_index.append_null();
        }

        // transaction_hash
        if let Some(hash) = log.transaction_hash {
            builder.transaction_hash.append_value(hash.as_slice());
        } else {
            builder.transaction_hash.append_null();
        }

        // block_hash
        if let Some(hash) = log.block_hash {
            builder.block_hash.append_value(hash.as_slice());
        } else {
            builder.block_hash.append_null();
        }

        // block_number
        if let Some(num) = log.block_number {
            builder.block_number.append_value(num);
        } else {
            builder.block_number.append_null();
        }

        // address (always present)
        builder.address.append_value(log.address().as_slice());

        // data
        builder.data.append_value(log.data().data.as_ref());

        // topics: topic0-3
        let topics = log.data().topics();
        append_topic(&mut builder.topic0, topics.first());
        append_topic(&mut builder.topic1, topics.get(1));
        append_topic(&mut builder.topic2, topics.get(2));
        append_topic(&mut builder.topic3, topics.get(3));
    }

    builder.finish()
}

fn append_topic(builder: &mut BinaryBuilder, topic: Option<&B256>) {
    if let Some(t) = topic {
        builder.append_value(t.as_slice());
    } else {
        builder.append_null();
    }
}

// ---------------------------------------------------------------------------
// Functions for Blocks conversion to RecordBatch
// ---------------------------------------------------------------------------

/// Convert a slice of `AnyRpcBlock` into a `RecordBatch` matching `blocks_schema()`.
///
/// Each block produces one row.  Header fields are extracted from the
/// consensus header accessible via `block.header.inner`.
pub fn blocks_to_record_batch(blocks: &[AnyRpcBlock]) -> RecordBatch {
    let mut b = BlocksBuilder::default();

    for block in blocks {
        let hdr = &block.inner.header.inner;

        // number (u64)
        b.number.append_value(hdr.number);

        // hash (Binary) — from the RPC wrapper, not the consensus header
        b.hash.append_value(block.inner.header.hash.as_slice());

        // parent_hash
        b.parent_hash.append_value(hdr.parent_hash.as_slice());

        // nonce (B64 → Binary, optional on AnyHeader)
        match hdr.nonce {
            Some(nonce) => b.nonce.append_value(nonce.as_slice()),
            None => b.nonce.append_null(),
        }

        // sha3_uncles (ommers_hash)
        b.sha3_uncles.append_value(hdr.ommers_hash.as_slice());

        // logs_bloom
        b.logs_bloom.append_value(hdr.logs_bloom.as_slice());

        // transactions_root
        b.transactions_root
            .append_value(hdr.transactions_root.as_slice());

        // state_root
        b.state_root.append_value(hdr.state_root.as_slice());

        // receipts_root
        b.receipts_root.append_value(hdr.receipts_root.as_slice());

        // miner (beneficiary)
        b.miner.append_value(hdr.beneficiary.as_slice());

        // difficulty (U256 → Decimal256)
        b.difficulty.append_value(u256_to_i256(hdr.difficulty));

        // total_difficulty (Option<U256>)
        append_optional_u256(&mut b.total_difficulty, block.inner.header.total_difficulty);

        // extra_data
        b.extra_data.append_value(hdr.extra_data.as_ref());

        // size (Option<U256>)
        append_optional_u256(&mut b.size, block.inner.header.size);

        // gas_limit (u64 → Decimal256)
        b.gas_limit.append_value(u64_to_i256(hdr.gas_limit));

        // gas_used (u64 → Decimal256)
        b.gas_used.append_value(u64_to_i256(hdr.gas_used));

        // timestamp (u64 → Decimal256)
        b.timestamp.append_value(u64_to_i256(hdr.timestamp));

        // uncles (List<Binary>)
        for uncle in &block.inner.uncles {
            b.uncles.values().append_value(uncle.as_slice());
        }
        b.uncles.append(true);

        // base_fee_per_gas (Option<u64> → Decimal256)
        append_optional_u64_decimal(&mut b.base_fee_per_gas, hdr.base_fee_per_gas);

        // blob_gas_used (Option<u64> → Decimal256)
        append_optional_u64_decimal(&mut b.blob_gas_used, hdr.blob_gas_used);

        // excess_blob_gas (Option<u64> → Decimal256)
        append_optional_u64_decimal(&mut b.excess_blob_gas, hdr.excess_blob_gas);

        // parent_beacon_block_root (Option<B256>)
        append_optional_b256(
            &mut b.parent_beacon_block_root,
            hdr.parent_beacon_block_root,
        );

        // withdrawals_root (Option<B256>)
        append_optional_b256(&mut b.withdrawals_root, hdr.withdrawals_root);

        // withdrawals (Option<Withdrawals>)
        append_withdrawals(&mut b.withdrawals, block);

        // l1_block_number — not available from standard RPC, null
        b.l1_block_number.append_null();

        // send_count — not available from standard RPC, null
        b.send_count.append_null();

        // send_root — not available from standard RPC, null
        b.send_root.append_null();

        // mix_hash (Option<B256>)
        append_optional_b256(&mut b.mix_hash, hdr.mix_hash);
    }

    b.finish()
}

/// Append one row to the withdrawals list builder.
fn append_withdrawals(wb: &mut tiders_evm_schema::WithdrawalsBuilder, block: &AnyRpcBlock) {
    let list_builder = &mut wb.0;

    if let Some(withdrawals) = &block.inner.withdrawals {
        for w in withdrawals {
            // The struct builder has sub-builders in order:
            // 0: index (UInt64), 1: validator_index (UInt64),
            // 2: address (Binary), 3: amount (Decimal256)
            if let Some(fb) = list_builder
                .values()
                .field_builder::<arrow::array::builder::UInt64Builder>(0)
            {
                fb.append_value(w.index);
            }
            if let Some(fb) = list_builder
                .values()
                .field_builder::<arrow::array::builder::UInt64Builder>(1)
            {
                fb.append_value(w.validator_index);
            }
            if let Some(fb) = list_builder.values().field_builder::<BinaryBuilder>(2) {
                fb.append_value(w.address.as_slice());
            }
            if let Some(fb) = list_builder.values().field_builder::<Decimal256Builder>(3) {
                fb.append_value(u64_to_i256(w.amount));
            }
            list_builder.values().append(true);
        }
        list_builder.append(true);
    } else {
        list_builder.append(false); // null list
    }
}

// ---------------------------------------------------------------------------
// Function for Transactions conversion to RecordBatch
// ---------------------------------------------------------------------------

/// Convert blocks (with full transaction objects) into a `RecordBatch`
/// matching `transactions_schema()`.
///
/// Every transaction from every block produces one row.  Fields that require
/// receipt data (`cumulative_gas_used`, `gas_used`, `status`, `contract_address`,
/// `logs_bloom`, `root`) are left null — they will be populated by a future
/// receipt-fetching pipeline.
pub fn transactions_to_record_batch(blocks: &[AnyRpcBlock]) -> RecordBatch {
    let mut t = TransactionsBuilder::default();

    for block in blocks {
        let block_hash = block.inner.header.hash;
        let block_number = block.inner.header.inner.number;

        match &block.inner.transactions {
            // include_txs=false: only tx hashes are available; emit one row per
            // hash with all other fields null.
            BlockTransactions::Hashes(hashes) => {
                for hash in hashes {
                    t.block_hash.append_value(block_hash.as_slice());
                    t.block_number.append_value(block_number);
                    t.from.append_null();
                    t.gas.append_null();
                    t.gas_price.append_null();
                    t.hash.append_value(hash.as_slice());
                    t.input.append_null();
                    t.nonce.append_null();
                    t.to.append_null();
                    t.transaction_index.append_null();
                    t.value.append_null();
                    t.v.append_null();
                    t.r.append_null();
                    t.s.append_null();
                    t.max_priority_fee_per_gas.append_null();
                    t.max_fee_per_gas.append_null();
                    t.chain_id.append_null();
                    t.cumulative_gas_used.append_null();
                    t.effective_gas_price.append_null();
                    t.gas_used.append_null();
                    t.contract_address.append_null();
                    t.logs_bloom.append_null();
                    t.type_.append_null();
                    t.root.append_null();
                    t.status.append_null();
                    t.sighash.append_null();
                    t.y_parity.append_null();
                    t.access_list.0.append(false);
                    t.l1_fee.append_null();
                    t.l1_gas_price.append_null();
                    t.l1_gas_used.append_null();
                    t.l1_fee_scalar.append_null();
                    t.gas_used_for_l1.append_null();
                    t.max_fee_per_blob_gas.append_null();
                    t.blob_versioned_hashes.append(false);
                    t.deposit_nonce.append_null();
                    t.blob_gas_price.append_null();
                    t.deposit_receipt_version.append_null();
                    t.blob_gas_used.append_null();
                    t.l1_base_fee_scalar.append_null();
                    t.l1_blob_base_fee.append_null();
                    t.l1_blob_base_fee_scalar.append_null();
                    t.l1_block_number.append_null();
                    t.mint.append_null();
                    t.source_hash.append_null();
                }
            }
            BlockTransactions::Uncle => {}
            BlockTransactions::Full(txns) => {
                for tx in txns {
                    // block_hash
                    t.block_hash.append_value(block_hash.as_slice());

                    // block_number
                    t.block_number.append_value(block_number);

                    // from (sender)
                    t.from
                        .append_value(TransactionResponse::from(tx).as_slice());

                    // gas (gas_limit: u64 → Decimal256)
                    t.gas.append_value(u64_to_i256(tx.gas_limit()));

                    // gas_price (Option<u128>) — use the Transaction trait version
                    append_optional_u128(&mut t.gas_price, TransactionTrait::gas_price(tx));

                    // hash
                    t.hash.append_value(tx.tx_hash().as_slice());

                    // input
                    t.input.append_value(tx.input().as_ref());

                    // nonce (u64 → Decimal256)
                    t.nonce.append_value(u64_to_i256(tx.nonce()));

                    // to (TxKind: Create → null, Call(addr) → addr)
                    match TransactionTrait::to(tx) {
                        Some(addr) => t.to.append_value(addr.as_slice()),
                        None => t.to.append_null(),
                    }

                    // transaction_index
                    if let Some(idx) = tx.transaction_index() {
                        t.transaction_index.append_value(idx);
                    } else {
                        t.transaction_index.append_null();
                    }

                    // value (U256 → Decimal256)
                    t.value.append_value(u256_to_i256(tx.value()));

                    // Signature fields
                    append_signature_fields(&mut t, tx);

                    // max_priority_fee_per_gas (Option<u128>)
                    append_optional_u128(
                        &mut t.max_priority_fee_per_gas,
                        tx.max_priority_fee_per_gas(),
                    );

                    // max_fee_per_gas — only present on EIP-1559+ txs.
                    // The consensus trait returns u128 unconditionally; for legacy txs
                    // gas_price is Some while max_priority_fee_per_gas is None.
                    let gas_price = TransactionTrait::gas_price(tx);
                    if gas_price.is_none() {
                        // EIP-1559+ transaction (no legacy gas_price)
                        t.max_fee_per_gas
                            .append_value(u128_to_i256(TransactionTrait::max_fee_per_gas(tx)));
                    } else if tx.max_priority_fee_per_gas().is_some() {
                        // Has both gas_price and max_priority — EIP-1559 where
                        // gas_price is filled as effective_gas_price by the RPC.
                        t.max_fee_per_gas
                            .append_value(u128_to_i256(TransactionTrait::max_fee_per_gas(tx)));
                    } else {
                        t.max_fee_per_gas.append_null();
                    }

                    // chain_id (Option<u64> → Decimal256)
                    append_optional_u64_decimal(&mut t.chain_id, tx.chain_id());

                    // Receipt-only fields — null until receipt pipeline is implemented
                    t.cumulative_gas_used.append_null();
                    t.effective_gas_price.append_null();
                    t.gas_used.append_null();
                    t.contract_address.append_null();
                    t.logs_bloom.append_null();
                    t.root.append_null();
                    t.status.append_null();

                    // type (tx type byte)
                    t.type_.append_value(Typed2718::ty(tx));

                    // sighash (first 4 bytes of input, or null if input < 4 bytes)
                    let input = tx.input();
                    if input.len() >= 4 {
                        t.sighash.append_value(&input[..4]);
                    } else {
                        t.sighash.append_null();
                    }

                    // y_parity
                    append_y_parity(&mut t, tx);

                    // access_list
                    append_access_list(&mut t.access_list, tx);

                    // max_fee_per_blob_gas (Option<u128>)
                    append_optional_u128(&mut t.max_fee_per_blob_gas, tx.max_fee_per_blob_gas());

                    // blob_versioned_hashes
                    if let Some(hashes) = tx.blob_versioned_hashes() {
                        for h in hashes {
                            t.blob_versioned_hashes.values().append_value(h.as_slice());
                        }
                        t.blob_versioned_hashes.append(true);
                    } else {
                        t.blob_versioned_hashes.append(false);
                    }

                    // L1/OP fields — decoded from WithOtherFields if present
                    append_op_fields(&mut t, tx);
                }
            }
        }
    }

    t.finish()
}

/// Append v, r, s signature fields from the transaction.
///
/// For `AnyTxEnvelope::Ethereum` we can access the signature directly.
/// For unknown envelope types, we append nulls.
fn append_signature_fields(t: &mut TransactionsBuilder, tx: &alloy::network::AnyRpcTransaction) {
    // tx.inner = WithOtherFields<Transaction<AnyTxEnvelope>>
    // tx.inner.inner = Recovered<AnyTxEnvelope> (Deref → AnyTxEnvelope)
    // We go through as_envelope() when available, otherwise append nulls.
    if let Some(envelope) = tx.as_envelope() {
        let sig = envelope.signature();
        // v: bool → u8 (0 or 1)
        t.v.append_value(u8::from(sig.v()));
        // r: U256 → Binary (32 bytes big-endian)
        let r_bytes: [u8; 32] = sig.r().to_be_bytes();
        t.r.append_value(r_bytes);
        // s: U256 → Binary (32 bytes big-endian)
        let s_bytes: [u8; 32] = sig.s().to_be_bytes();
        t.s.append_value(s_bytes);
    } else {
        // Unknown envelope type — no signature available
        t.v.append_null();
        t.r.append_null();
        t.s.append_null();
    }
}

/// Append y_parity from the signature.
fn append_y_parity(t: &mut TransactionsBuilder, tx: &alloy::network::AnyRpcTransaction) {
    match tx.as_envelope() {
        Some(envelope) => {
            t.y_parity.append_value(envelope.signature().v());
        }
        None => {
            t.y_parity.append_null();
        }
    }
}

/// Append access_list for the transaction.
fn append_access_list(
    al_builder: &mut tiders_evm_schema::AccessListBuilder,
    tx: &alloy::network::AnyRpcTransaction,
) {
    let list_builder = &mut al_builder.0;

    if let Some(access_list) = tx.access_list() {
        for item in access_list.iter() {
            // Sub-builders: 0 = address (Binary), 1 = storage_keys (List<Binary>)
            if let Some(fb) = list_builder.values().field_builder::<BinaryBuilder>(0) {
                fb.append_value(item.address.as_slice());
            }

            let storage_keys_builder = list_builder
                .values()
                .field_builder::<arrow::array::builder::ListBuilder<BinaryBuilder>>(1);
            if let Some(sk_builder) = storage_keys_builder {
                for key in &item.storage_keys {
                    sk_builder.values().append_value(key.as_slice());
                }
                sk_builder.append(true);
            }
            list_builder.values().append(true);
        }
        list_builder.append(true);
    } else {
        list_builder.append(false); // null list
    }
}

/// Append L1/OP-chain fields from the transaction's `other` fields map.
///
/// These fields are not part of the standard Ethereum JSON-RPC transaction
/// object. On OP-stack chains they are returned as extra keys alongside the
/// standard fields and land in `WithOtherFields::other` after alloy
/// deserialisation. Each field is appended as null when absent (e.g. on
/// mainnet) or when the value cannot be parsed.
fn append_op_fields(t: &mut TransactionsBuilder, tx: &alloy::network::AnyRpcTransaction) {
    /// Read an optional hex-encoded U128 from `other` and convert to i256.
    fn get_u128(other: &alloy::serde::OtherFields, key: &str) -> Option<i256> {
        let v: U128 = other.get_deserialized::<U128>(key)?.ok()?;
        Some(u128_to_i256(v.to()))
    }

    /// Read an optional hex-encoded U256 from `other` and convert to i256.
    fn get_u256(other: &alloy::serde::OtherFields, key: &str) -> Option<i256> {
        let v: U256 = other.get_deserialized::<U256>(key)?.ok()?;
        Some(u256_to_i256(v))
    }

    /// Read an optional hex-encoded u64 from `other`.
    fn get_u64(other: &alloy::serde::OtherFields, key: &str) -> Option<u64> {
        let v: U256 = other.get_deserialized::<U256>(key)?.ok()?;
        v.try_into().ok()
    }

    /// Read an optional hex-encoded B256 from `other`.
    fn get_b256(other: &alloy::serde::OtherFields, key: &str) -> Option<B256> {
        other.get_deserialized::<B256>(key)?.ok()
    }

    let o = &tx.other;

    match get_u128(o, "l1Fee") {
        Some(v) => t.l1_fee.append_value(v),
        None => t.l1_fee.append_null(),
    }
    match get_u128(o, "l1GasPrice") {
        Some(v) => t.l1_gas_price.append_value(v),
        None => t.l1_gas_price.append_null(),
    }
    match get_u256(o, "l1GasUsed") {
        Some(v) => t.l1_gas_used.append_value(v),
        None => t.l1_gas_used.append_null(),
    }
    match get_u128(o, "l1FeeScalar") {
        Some(v) => t.l1_fee_scalar.append_value(v),
        None => t.l1_fee_scalar.append_null(),
    }
    match get_u256(o, "gasUsedForL1") {
        Some(v) => t.gas_used_for_l1.append_value(v),
        None => t.gas_used_for_l1.append_null(),
    }
    match get_u256(o, "depositNonce") {
        Some(v) => t.deposit_nonce.append_value(v),
        None => t.deposit_nonce.append_null(),
    }
    match get_u128(o, "blobGasPrice") {
        Some(v) => t.blob_gas_price.append_value(v),
        None => t.blob_gas_price.append_null(),
    }
    match get_u256(o, "depositReceiptVersion") {
        Some(v) => t.deposit_receipt_version.append_value(v),
        None => t.deposit_receipt_version.append_null(),
    }
    match get_u256(o, "blobGasUsed") {
        Some(v) => t.blob_gas_used.append_value(v),
        None => t.blob_gas_used.append_null(),
    }
    match get_u128(o, "l1BaseFeeScalar") {
        Some(v) => t.l1_base_fee_scalar.append_value(v),
        None => t.l1_base_fee_scalar.append_null(),
    }
    match get_u128(o, "l1BlobBaseFee") {
        Some(v) => t.l1_blob_base_fee.append_value(v),
        None => t.l1_blob_base_fee.append_null(),
    }
    match get_u128(o, "l1BlobBaseFeeScalar") {
        Some(v) => t.l1_blob_base_fee_scalar.append_value(v),
        None => t.l1_blob_base_fee_scalar.append_null(),
    }
    match get_u64(o, "l1BlockNumber") {
        Some(v) => t.l1_block_number.append_value(v),
        None => t.l1_block_number.append_null(),
    }
    match get_u256(o, "mint") {
        Some(v) => t.mint.append_value(v),
        None => t.mint.append_null(),
    }
    match get_b256(o, "sourceHash") {
        Some(v) => t.source_hash.append_value(v.as_slice()),
        None => t.source_hash.append_null(),
    }
}

// ---------------------------------------------------------------------------
// Functions for Traces conversion to RecordBatch
// ---------------------------------------------------------------------------

/// Convert a slice of `LocalizedTransactionTrace` into a `RecordBatch`
/// matching `traces_schema()`.
///
/// Each trace produces one row. Fields that are not available for a given
/// trace type (e.g. `input` on a reward action) are left null.
pub fn traces_to_record_batch(traces: &[LocalizedTransactionTrace]) -> RecordBatch {
    let mut t = TracesBuilder::default();

    for trace in traces {
        // Collect all per-row field values up-front so each builder gets
        // exactly one append call per row.

        // --- action-derived fields ---
        // `action_address` = the selfdestruct contract address (schema column).
        // `address`        = selfdestruct contract OR created contract (from result).
        struct ActionFields {
            from: Option<Vec<u8>>,
            to: Option<Vec<u8>>,
            call_type: Option<String>,
            gas: Option<i256>,
            input: Option<Vec<u8>>,
            init: Option<Vec<u8>>,
            value: Option<i256>,
            author: Option<Vec<u8>>,
            reward_type: Option<String>,
            /// `action_address` schema column (selfdestruct contract addr).
            action_address: Option<Vec<u8>>,
            balance: Option<i256>,
            refund_address: Option<Vec<u8>>,
        }

        let af = match &trace.trace.action {
            Action::Call(call) => ActionFields {
                from: Some(call.from.as_slice().to_vec()),
                to: Some(call.to.as_slice().to_vec()),
                call_type: Some(format!("{:?}", call.call_type).to_lowercase()),
                gas: Some(u64_to_i256(call.gas)),
                input: Some(call.input.to_vec()),
                init: None,
                value: Some(u256_to_i256(call.value)),
                author: None,
                reward_type: None,
                action_address: None,
                balance: None,
                refund_address: None,
            },
            Action::Create(create) => ActionFields {
                from: Some(create.from.as_slice().to_vec()),
                to: None,
                call_type: Some("create".to_owned()),
                gas: Some(u64_to_i256(create.gas)),
                input: None,
                init: Some(create.init.to_vec()),
                value: Some(u256_to_i256(create.value)),
                author: None,
                reward_type: None,
                action_address: None,
                balance: None,
                refund_address: None,
            },
            Action::Selfdestruct(suicide) => ActionFields {
                from: Some(suicide.address.as_slice().to_vec()),
                to: Some(suicide.refund_address.as_slice().to_vec()),
                call_type: Some("selfdestruct".to_owned()),
                gas: None,
                input: None,
                init: None,
                value: Some(u256_to_i256(suicide.balance)),
                author: None,
                reward_type: None,
                action_address: Some(suicide.address.as_slice().to_vec()),
                balance: Some(u256_to_i256(suicide.balance)),
                refund_address: Some(suicide.refund_address.as_slice().to_vec()),
            },
            Action::Reward(reward) => ActionFields {
                from: None,
                to: None,
                call_type: None,
                gas: None,
                input: None,
                init: None,
                value: Some(u256_to_i256(reward.value)),
                author: Some(reward.author.as_slice().to_vec()),
                reward_type: Some(format!("{:?}", reward.reward_type).to_lowercase()),
                action_address: None,
                balance: None,
                refund_address: None,
            },
        };

        // --- result-derived fields ---
        let (gas_used, output, result_address, code) = match &trace.trace.result {
            Some(TraceOutput::Call(call_out)) => (
                Some(u64_to_i256(call_out.gas_used)),
                Some(call_out.output.to_vec()),
                None::<Vec<u8>>,
                None::<Vec<u8>>,
            ),
            Some(TraceOutput::Create(create_out)) => (
                Some(u64_to_i256(create_out.gas_used)),
                None,
                Some(create_out.address.as_slice().to_vec()),
                Some(create_out.code.to_vec()),
            ),
            None => (None, None, None, None),
        };

        // `address` schema column: created contract address (from result) for
        // create actions, or selfdestruct contract for selfdestruct actions.
        let address = result_address.or_else(|| af.action_address.clone());

        // --- append one value per field (schema column order) ---
        append_opt_binary(&mut t.from, af.from.as_deref());
        append_opt_binary(&mut t.to, af.to.as_deref());
        match af.call_type {
            Some(s) => t.call_type.append_value(s),
            None => t.call_type.append_null(),
        }
        match af.gas {
            Some(v) => t.gas.append_value(v),
            None => t.gas.append_null(),
        }
        append_opt_binary(&mut t.input, af.input.as_deref());
        append_opt_binary(&mut t.init, af.init.as_deref());
        match af.value {
            Some(v) => t.value.append_value(v),
            None => t.value.append_null(),
        }
        append_opt_binary(&mut t.author, af.author.as_deref());
        match af.reward_type {
            Some(s) => t.reward_type.append_value(s),
            None => t.reward_type.append_null(),
        }
        match trace.block_hash {
            Some(h) => t.block_hash.append_value(h.as_slice()),
            None => t.block_hash.append_null(),
        }
        match trace.block_number {
            Some(n) => t.block_number.append_value(n),
            None => t.block_number.append_null(),
        }
        append_opt_binary(&mut t.address, address.as_deref());
        append_opt_binary(&mut t.code, code.as_deref());
        match gas_used {
            Some(v) => t.gas_used.append_value(v),
            None => t.gas_used.append_null(),
        }
        append_opt_binary(&mut t.output, output.as_deref());
        t.subtraces.append_value(trace.trace.subtraces as u64);
        for idx in &trace.trace.trace_address {
            t.trace_address.values().append_value(*idx as u64);
        }
        t.trace_address.append(true);
        match trace.transaction_hash {
            Some(h) => t.transaction_hash.append_value(h.as_slice()),
            None => t.transaction_hash.append_null(),
        }
        match trace.transaction_position {
            Some(p) => t.transaction_position.append_value(p),
            None => t.transaction_position.append_null(),
        }
        let type_str = match &trace.trace.action {
            Action::Call(_) => "call",
            Action::Create(_) => "create",
            Action::Selfdestruct(_) => "suicide",
            Action::Reward(_) => "reward",
        };
        t.type_.append_value(type_str);
        match &trace.trace.error {
            Some(e) => t.error.append_value(e),
            None => t.error.append_null(),
        }
        // sighash: first 4 bytes of input for call actions
        let sighash = match &trace.trace.action {
            Action::Call(call) if call.input.len() >= 4 => Some(&call.input[..4]),
            _ => None,
        };
        match sighash {
            Some(s) => t.sighash.append_value(s),
            None => t.sighash.append_null(),
        }
        append_opt_binary(&mut t.action_address, af.action_address.as_deref());
        match af.balance {
            Some(v) => t.balance.append_value(v),
            None => t.balance.append_null(),
        }
        append_opt_binary(&mut t.refund_address, af.refund_address.as_deref());
    }

    t.finish()
}

/// Append an optional byte slice to a `BinaryBuilder`.
fn append_opt_binary(builder: &mut BinaryBuilder, val: Option<&[u8]>) {
    match val {
        Some(b) => builder.append_value(b),
        None => builder.append_null(),
    }
}

// ---------------------------------------------------------------------------
// Tx Receipts handlers: patch receipt-only columns into a transactions RecordBatch
// ---------------------------------------------------------------------------
// Blocks, Logs, and Transactions are build-from-scratch to a RecordBatch, but
// for Tx Receipts needs to be patched into the transactions batch.
// It first build a compact index of receipt data by transaction hash, then
// iterates through the transactions batch and fills in receipt fields when a matching receipt is found.

/// A compact, indexable view of receipt data needed to populate receipt-only
/// transaction columns.
struct TxReceiptRow {
    cumulative_gas_used: i256,
    effective_gas_price: i256,
    gas_used: i256,
    contract_address: Option<[u8; 20]>,
    logs_bloom: [u8; 256],
    /// `Some(bytes)` for pre-EIP-658 state-root receipts, `None` otherwise.
    root: Option<[u8; 32]>,
    /// `Some(0 or 1)` for post-EIP-658 status receipts, `None` for pre-EIP-658.
    status: Option<u8>,
}

/// Build a `tx_hash → TxReceiptRow` index from a slice of receipts.
fn index_tx_receipts(receipts: &[AnyTransactionReceipt]) -> HashMap<TxHash, TxReceiptRow> {
    let mut map = HashMap::with_capacity(receipts.len());
    for r in receipts {
        // r       : WithOtherFields<TransactionReceipt<AnyReceiptEnvelope<Log>>>
        // r.inner : TransactionReceipt<AnyReceiptEnvelope<Log>>
        // r.inner.inner : AnyReceiptEnvelope<Log>  — implements TxReceipt
        let envelope = &r.inner.inner;

        let status_or_root = envelope.status_or_post_state();

        let (status, root) = if status_or_root.is_post_state() {
            // Pre-EIP-658: no boolean status, just a state root
            let post_state = status_or_root.as_post_state().map(|b| b.0);
            (None, post_state)
        } else {
            // Post-EIP-658: boolean status (true = success = 1)
            let s: u8 = u8::from(status_or_root.coerce_status());
            (Some(s), None)
        };

        let logs_bloom: [u8; 256] = *envelope.bloom().0;

        let row = TxReceiptRow {
            cumulative_gas_used: u64_to_i256(envelope.cumulative_gas_used()),
            effective_gas_price: u128_to_i256(r.inner.effective_gas_price),
            gas_used: u64_to_i256(r.inner.gas_used),
            contract_address: r.inner.contract_address.map(|a| a.0 .0),
            logs_bloom,
            root,
            status,
        };
        map.insert(r.inner.transaction_hash, row);
    }
    map
}

/// Merge receipt data into a transactions `RecordBatch`.
///
/// The input `txs_batch` must have been produced by [`transactions_to_record_batch`]
/// and therefore contains the full `transactions_schema()` with null receipt columns.
///
/// For each row in `txs_batch`, we look up the receipt by transaction hash and
/// fill in the tx receipt fields.
///
/// Rows whose hash is not found in `receipts` keep null receipt columns
/// (this can happen for the `BlockTransactions::Hashes` path when
/// `include_txs = false`).
pub fn merge_tx_receipts_into_batch(
    txs_batch: RecordBatch,
    receipts: &[AnyTransactionReceipt],
) -> RecordBatch {
    if txs_batch.num_rows() == 0 || receipts.is_empty() {
        return txs_batch;
    }

    let index = index_tx_receipts(receipts);

    // Collect the tx hashes column to drive the join.
    let schema = txs_batch.schema();
    let Ok(hash_col_idx) = schema.index_of("hash") else {
        return txs_batch; // no hash column — nothing to merge
    };

    let hash_col = txs_batch
        .column(hash_col_idx)
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>();

    let Some(hash_col) = hash_col else {
        return txs_batch;
    };

    // Collect per-row receipt values (or None when no receipt found).
    let n = txs_batch.num_rows();
    let mut cum_gas: Vec<Option<i256>> = Vec::with_capacity(n);
    let mut eff_price: Vec<Option<i256>> = Vec::with_capacity(n);
    let mut gas_used: Vec<Option<i256>> = Vec::with_capacity(n);
    let mut contract_addr: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
    let mut bloom: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
    let mut root: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
    let mut status: Vec<Option<u8>> = Vec::with_capacity(n);

    for row in 0..n {
        if hash_col.is_null(row) {
            cum_gas.push(None);
            eff_price.push(None);
            gas_used.push(None);
            contract_addr.push(None);
            bloom.push(None);
            root.push(None);
            status.push(None);
            continue;
        }
        let hash_bytes = hash_col.value(row);
        let hash: TxHash = if hash_bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(hash_bytes);
            TxHash::from(arr)
        } else {
            cum_gas.push(None);
            eff_price.push(None);
            gas_used.push(None);
            contract_addr.push(None);
            bloom.push(None);
            root.push(None);
            status.push(None);
            continue;
        };

        if let Some(r) = index.get(&hash) {
            cum_gas.push(Some(r.cumulative_gas_used));
            eff_price.push(Some(r.effective_gas_price));
            gas_used.push(Some(r.gas_used));
            contract_addr.push(r.contract_address.map(|a| a.to_vec()));
            bloom.push(Some(r.logs_bloom.to_vec()));
            root.push(r.root.map(|b| b.to_vec()));
            status.push(r.status);
        } else {
            cum_gas.push(None);
            eff_price.push(None);
            gas_used.push(None);
            contract_addr.push(None);
            bloom.push(None);
            root.push(None);
            status.push(None);
        }
    }

    // Build a patched RecordBatch: replace the null receipt columns with filled arrays.
    let mut columns: Vec<std::sync::Arc<dyn arrow::array::Array>> = txs_batch.columns().to_vec();

    let patch_decimal = |vals: &[Option<i256>]| -> std::sync::Arc<dyn arrow::array::Array> {
        #[expect(
            clippy::expect_used,
            reason = "precision 76 is always valid for Decimal256"
        )]
        let mut b = Decimal256Builder::new()
            .with_precision_and_scale(76, 0)
            .expect("valid decimal precision");
        for v in vals {
            match v {
                Some(i) => b.append_value(*i),
                None => b.append_null(),
            }
        }
        std::sync::Arc::new(b.finish())
    };

    let patch_binary = |vals: &[Option<Vec<u8>>]| -> std::sync::Arc<dyn arrow::array::Array> {
        let mut b = BinaryBuilder::new();
        for v in vals {
            match v {
                Some(bytes) => b.append_value(bytes),
                None => b.append_null(),
            }
        }
        std::sync::Arc::new(b.finish())
    };

    let patch_u8 = |vals: &[Option<u8>]| -> std::sync::Arc<dyn arrow::array::Array> {
        let mut b = arrow::array::builder::UInt8Builder::new();
        for v in vals {
            match v {
                Some(i) => b.append_value(*i),
                None => b.append_null(),
            }
        }
        std::sync::Arc::new(b.finish())
    };

    // Map column name → new array.
    let patches: &[(&str, std::sync::Arc<dyn arrow::array::Array>)] = &[
        ("cumulative_gas_used", patch_decimal(&cum_gas)),
        ("effective_gas_price", patch_decimal(&eff_price)),
        ("gas_used", patch_decimal(&gas_used)),
        ("contract_address", patch_binary(&contract_addr)),
        ("logs_bloom", patch_binary(&bloom)),
        ("root", patch_binary(&root)),
        ("status", patch_u8(&status)),
    ];

    for (name, new_col) in patches {
        if let Ok(col_idx) = schema.index_of(name) {
            columns[col_idx] = new_col.clone();
        }
    }

    #[expect(
        clippy::expect_used,
        reason = "columns were taken from the same batch schema; type mismatch is a programmer error"
    )]
    RecordBatch::try_new(txs_batch.schema(), columns)
        .expect("merge_tx_receipts_into_batch: column count/type mismatch")
}

// ---------------------------------------------------------------------------
// Functions for selecting columns based on query field selection
// ---------------------------------------------------------------------------

/// Select the log columns requested by the query's [`LogFields`].
///
/// Each `true` field in `fields` corresponds to a column that the caller wants
/// in the output. Columns not requested are dropped from the batch. If no
/// fields are set (the query did not specify any log field selection), the
/// batch is returned unchanged. Unknown field names are silently skipped.
pub fn select_log_columns(batch: RecordBatch, fields: &LogFields) -> RecordBatch {
    let requested: &[(&str, bool)] = &[
        ("removed", fields.removed),
        ("log_index", fields.log_index),
        ("transaction_index", fields.transaction_index),
        ("transaction_hash", fields.transaction_hash),
        ("block_hash", fields.block_hash),
        ("block_number", fields.block_number),
        ("address", fields.address),
        ("data", fields.data),
        ("topic0", fields.topic0),
        ("topic1", fields.topic1),
        ("topic2", fields.topic2),
        ("topic3", fields.topic3),
    ];
    select_columns(batch, requested)
}

/// Select the block columns requested by the query's [`BlockFields`].
///
/// Each `true` field in `fields` corresponds to a column that the caller wants
/// in the output. Columns not requested are dropped from the batch. If no
/// fields are set (the query did not specify any block field selection), the
/// batch is returned unchanged. Unknown field names are silently skipped.
pub fn select_block_columns(batch: RecordBatch, fields: &BlockFields) -> RecordBatch {
    let requested: &[(&str, bool)] = &[
        ("number", fields.number),
        ("hash", fields.hash),
        ("parent_hash", fields.parent_hash),
        ("nonce", fields.nonce),
        ("sha3_uncles", fields.sha3_uncles),
        ("logs_bloom", fields.logs_bloom),
        ("transactions_root", fields.transactions_root),
        ("state_root", fields.state_root),
        ("receipts_root", fields.receipts_root),
        ("miner", fields.miner),
        ("difficulty", fields.difficulty),
        ("total_difficulty", fields.total_difficulty),
        ("extra_data", fields.extra_data),
        ("size", fields.size),
        ("gas_limit", fields.gas_limit),
        ("gas_used", fields.gas_used),
        ("timestamp", fields.timestamp),
        ("uncles", fields.uncles),
        ("base_fee_per_gas", fields.base_fee_per_gas),
        ("blob_gas_used", fields.blob_gas_used),
        ("excess_blob_gas", fields.excess_blob_gas),
        ("parent_beacon_block_root", fields.parent_beacon_block_root),
        ("withdrawals_root", fields.withdrawals_root),
        ("withdrawals", fields.withdrawals),
        ("l1_block_number", fields.l1_block_number),
        ("send_count", fields.send_count),
        ("send_root", fields.send_root),
        ("mix_hash", fields.mix_hash),
    ];
    select_columns(batch, requested)
}

/// Select the transaction columns requested by the query's [`TransactionFields`].
///
/// Each `true` field in `fields` corresponds to a column that the caller wants
/// in the output. Columns not requested are dropped from the batch. If no
/// fields are set (the query did not specify any transaction field selection),
/// the batch is returned unchanged. Unknown field names are silently skipped.
pub fn select_transaction_columns(batch: RecordBatch, fields: &TransactionFields) -> RecordBatch {
    let requested: &[(&str, bool)] = &[
        ("block_hash", fields.block_hash),
        ("block_number", fields.block_number),
        ("from", fields.from),
        ("gas", fields.gas),
        ("gas_price", fields.gas_price),
        ("hash", fields.hash),
        ("input", fields.input),
        ("nonce", fields.nonce),
        ("to", fields.to),
        ("transaction_index", fields.transaction_index),
        ("value", fields.value),
        ("v", fields.v),
        ("r", fields.r),
        ("s", fields.s),
        ("max_priority_fee_per_gas", fields.max_priority_fee_per_gas),
        ("max_fee_per_gas", fields.max_fee_per_gas),
        ("chain_id", fields.chain_id),
        ("cumulative_gas_used", fields.cumulative_gas_used),
        ("effective_gas_price", fields.effective_gas_price),
        ("gas_used", fields.gas_used),
        ("contract_address", fields.contract_address),
        ("logs_bloom", fields.logs_bloom),
        ("type", fields.type_),
        ("root", fields.root),
        ("status", fields.status),
        ("sighash", fields.sighash),
        ("y_parity", fields.y_parity),
        ("access_list", fields.access_list),
        ("l1_fee", fields.l1_fee),
        ("l1_gas_price", fields.l1_gas_price),
        ("l1_fee_scalar", fields.l1_fee_scalar),
        ("gas_used_for_l1", fields.gas_used_for_l1),
        ("max_fee_per_blob_gas", fields.max_fee_per_blob_gas),
        ("blob_versioned_hashes", fields.blob_versioned_hashes),
        ("deposit_nonce", fields.deposit_nonce),
        ("blob_gas_price", fields.blob_gas_price),
        ("deposit_receipt_version", fields.deposit_receipt_version),
        ("blob_gas_used", fields.blob_gas_used),
        ("l1_base_fee_scalar", fields.l1_base_fee_scalar),
        ("l1_blob_base_fee", fields.l1_blob_base_fee),
        ("l1_blob_base_fee_scalar", fields.l1_blob_base_fee_scalar),
        ("l1_block_number", fields.l1_block_number),
        ("mint", fields.mint),
        ("source_hash", fields.source_hash),
    ];
    select_columns(batch, requested)
}

/// Select the trace columns requested by the query's [`TraceFields`].
pub fn select_trace_columns(batch: RecordBatch, fields: &TraceFields) -> RecordBatch {
    let requested: &[(&str, bool)] = &[
        ("from", fields.from),
        ("to", fields.to),
        ("call_type", fields.call_type),
        ("gas", fields.gas),
        ("input", fields.input),
        ("init", fields.init),
        ("value", fields.value),
        ("author", fields.author),
        ("reward_type", fields.reward_type),
        ("block_hash", fields.block_hash),
        ("block_number", fields.block_number),
        ("address", fields.address),
        ("code", fields.code),
        ("gas_used", fields.gas_used),
        ("output", fields.output),
        ("subtraces", fields.subtraces),
        ("trace_address", fields.trace_address),
        ("transaction_hash", fields.transaction_hash),
        ("transaction_position", fields.transaction_position),
        ("type", fields.type_),
        ("error", fields.error),
        ("sighash", fields.sighash),
        ("action_address", fields.action_address),
        ("balance", fields.balance),
        ("refund_address", fields.refund_address),
    ];
    select_columns(batch, requested)
}

/// Retain only the columns whose name maps to `true` in `requested`.
///
/// Called by the typed `select_*_columns` functions after translating a
/// `Fields` struct into a `(column_name, is_requested)` slice. Returns the
/// batch unchanged if no entry is `true`.
fn select_columns(batch: RecordBatch, requested: &[(&str, bool)]) -> RecordBatch {
    let any_set = requested.iter().any(|(_, v)| *v);
    if !any_set {
        return batch;
    }

    let schema = batch.schema();
    let indices: Vec<usize> = requested
        .iter()
        .filter(|(_, v)| *v)
        .filter_map(|(name, _)| schema.index_of(name).ok())
        .collect();

    #[expect(
        clippy::expect_used,
        reason = "indices are built from schema.index_of which only returns valid indices"
    )]
    batch
        .project(&indices)
        .expect("project_columns: column index out of range")
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn empty_logs_produce_valid_batch() {
        let batch = logs_to_record_batch(&[]);
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().as_ref(), &tiders_evm_schema::logs_schema());
    }

    #[test]
    fn empty_blocks_produce_valid_batch() {
        let batch = blocks_to_record_batch(&[]);
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().as_ref(), &tiders_evm_schema::blocks_schema());
    }

    #[test]
    fn empty_transactions_produce_valid_batch() {
        let batch = transactions_to_record_batch(&[]);
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(
            batch.schema().as_ref(),
            &tiders_evm_schema::transactions_schema()
        );
    }

    #[test]
    fn u256_to_i256_basic() {
        let zero = u256_to_i256(U256::ZERO);
        assert_eq!(zero, i256::ZERO);

        let one = u256_to_i256(U256::from(1u64));
        assert_eq!(one, i256::ONE);

        let large = u256_to_i256(U256::from(1_000_000u64));
        assert_eq!(large, i256::from_i128(1_000_000));
    }
}
