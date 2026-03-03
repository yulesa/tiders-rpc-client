use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;

/// A response containing Arrow `RecordBatch`es for each EVM table.
///
/// Each batch uses the canonical `tiders-evm-schema` schemas so the data
/// can be passed directly into the tiders-core pipeline.
#[derive(Debug, Clone)]
pub struct ArrowResponse {
    /// Block header rows.
    pub blocks: RecordBatch,
    /// Transaction rows (may include receipt data if requested).
    pub transactions: RecordBatch,
    /// Event log rows.
    pub logs: RecordBatch,
    /// Execution trace rows.
    pub traces: RecordBatch,
}

impl ArrowResponse {
    /// Returns the next block number that should be requested, derived from
    /// the maximum block number present across all tables in this response.
    ///
    /// Returns `None` if every table is empty (zero rows).
    pub fn next_block(&self) -> Result<Option<u64>> {
        let mut max_block: Option<u64> = None;

        for (batch, col_name) in [
            (&self.blocks, "number"),
            (&self.transactions, "block_number"),
            (&self.logs, "block_number"),
            (&self.traces, "block_number"),
        ] {
            if batch.num_rows() == 0 {
                continue;
            }
            if let Some(block_num) = max_block_in_batch(batch, col_name)? {
                max_block = Some(max_block.map_or(block_num, |m: u64| m.max(block_num)));
            }
        }

        Ok(max_block.map(|b| b + 1))
    }

    /// Build an empty response (zero rows, correct schemas).
    pub fn empty() -> Self {
        Self {
            blocks: empty_batch(&tiders_evm_schema::blocks_schema()),
            transactions: empty_batch(&tiders_evm_schema::transactions_schema()),
            logs: empty_batch(&tiders_evm_schema::logs_schema()),
            traces: empty_batch(&tiders_evm_schema::traces_schema()),
        }
    }

    /// Build a response with only the `logs` RecordBatch populated.
    /// Other tables are empty with correct schemas.
    pub fn with_logs(logs: RecordBatch) -> Self {
        Self {
            logs,
            ..Self::empty()
        }
    }

    /// Build a response with `blocks` and `transactions` populated.
    /// Other tables (logs, traces) are empty with correct schemas.
    pub fn with_blocks(blocks: RecordBatch, transactions: RecordBatch) -> Self {
        Self {
            blocks,
            transactions,
            ..Self::empty()
        }
    }

    /// Build a response with only the `traces` RecordBatch populated.
    /// Other tables are empty with correct schemas.
    pub fn with_traces(traces: RecordBatch) -> Self {
        Self {
            traces,
            ..Self::empty()
        }
    }
}

fn empty_batch(schema: &arrow::datatypes::Schema) -> RecordBatch {
    RecordBatch::new_empty(Arc::new(schema.clone()))
}

fn max_block_in_batch(batch: &RecordBatch, col_name: &str) -> Result<Option<u64>> {
    let col = batch
        .column_by_name(col_name)
        .with_context(|| format!("column '{col_name}' not found in batch"))?;

    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .with_context(|| format!("column '{col_name}' is not UInt64"))?;

    Ok(arrow::compute::max(arr))
}
