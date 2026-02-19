//! Convert alloy `Log` objects to Arrow `RecordBatch` using `cherry_evm_schema::LogsBuilder`.

use alloy::rpc::types::Log;
use arrow::record_batch::RecordBatch;
use cherry_evm_schema::LogsBuilder;

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

fn append_topic(
    builder: &mut arrow::array::BinaryBuilder,
    topic: Option<&alloy::primitives::B256>,
) {
    if let Some(t) = topic {
        builder.append_value(t.as_slice());
    } else {
        builder.append_null();
    }
}
