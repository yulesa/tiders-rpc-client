use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;

/// A response containing Arrow `RecordBatch`es for each EVM table.
///
/// Each batch uses the canonical `cherry-evm-schema` schemas so the data
/// can be passed directly into the cherry-core pipeline.
#[derive(Debug, Clone)]
pub struct ArrowResponse {
    pub blocks: RecordBatch,
    pub transactions: RecordBatch,
    pub logs: RecordBatch,
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
            blocks: empty_batch(&cherry_evm_schema::blocks_schema()),
            transactions: empty_batch(&cherry_evm_schema::transactions_schema()),
            logs: empty_batch(&cherry_evm_schema::logs_schema()),
            traces: empty_batch(&cherry_evm_schema::traces_schema()),
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

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::UInt64Array;
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn batch_with_block_number(
        schema: Arc<arrow::datatypes::Schema>,
        col: &str,
        num: u64,
    ) -> RecordBatch {
        let columns: Vec<Arc<dyn arrow::array::Array>> = schema
            .fields()
            .iter()
            .map(|f| {
                if f.name() == col {
                    Arc::new(UInt64Array::from(vec![num])) as Arc<dyn arrow::array::Array>
                } else {
                    arrow::array::new_null_array(f.data_type(), 1)
                }
            })
            .collect();
        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[test]
    fn arrow_response_empty_and_next_block() {
        // empty: correct schemas, zero rows, next_block = None
        let empty = ArrowResponse::empty();
        assert_eq!(
            empty.blocks.schema().as_ref(),
            &cherry_evm_schema::blocks_schema()
        );
        assert_eq!(
            empty.transactions.schema().as_ref(),
            &cherry_evm_schema::transactions_schema()
        );
        assert_eq!(
            empty.logs.schema().as_ref(),
            &cherry_evm_schema::logs_schema()
        );
        assert_eq!(
            empty.traces.schema().as_ref(),
            &cherry_evm_schema::traces_schema()
        );
        assert_eq!(empty.next_block().unwrap(), None);

        // blocks only: next_block = max + 1
        let with_blocks = ArrowResponse {
            blocks: batch_with_block_number(
                Arc::new(cherry_evm_schema::blocks_schema()),
                "number",
                20,
            ),
            ..ArrowResponse::empty()
        };
        assert_eq!(with_blocks.next_block().unwrap(), Some(21));

        // logs wins over blocks: next_block = logs max + 1
        let with_logs = ArrowResponse {
            blocks: batch_with_block_number(
                Arc::new(cherry_evm_schema::blocks_schema()),
                "number",
                10,
            ),
            logs: batch_with_block_number(
                Arc::new(cherry_evm_schema::logs_schema()),
                "block_number",
                30,
            ),
            ..ArrowResponse::empty()
        };
        assert_eq!(with_logs.next_block().unwrap(), Some(31));
    }
}
