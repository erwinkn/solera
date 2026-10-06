//! Entries as Arrow columns, the one in-memory shape both formats decode to:
//! key (binary), commit (u64 as i64), new and replaced versions (nullable).

use arrow_array::{Array, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Binary, false),
        Field::new("commit", DataType::Int64, false),
        Field::new("new", DataType::Int64, true),
        Field::new("replaced", DataType::Int64, true),
    ]))
}

/// A decoded run of entries, sorted by key then commit, newest first.
#[derive(Clone)]
pub struct Run {
    pub key: BinaryArray,
    pub commit: Int64Array,
    pub new: Int64Array,
    pub replaced: Int64Array,
}

impl Run {
    pub fn of(b: &RecordBatch) -> Run {
        let c = |i: usize| b.column(i).clone();
        Run {
            key: c(0).as_any().downcast_ref::<BinaryArray>().unwrap().clone(),
            commit: c(1).as_any().downcast_ref::<Int64Array>().unwrap().clone(),
            new: c(2).as_any().downcast_ref::<Int64Array>().unwrap().clone(),
            replaced: c(3).as_any().downcast_ref::<Int64Array>().unwrap().clone(),
        }
    }
    pub fn batch(&self) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(self.key.clone()),
                Arc::new(self.commit.clone()),
                Arc::new(self.new.clone()),
                Arc::new(self.replaced.clone()),
            ],
        )
        .unwrap()
    }
    pub fn len(&self) -> usize {
        self.key.len()
    }
    #[inline]
    pub fn opt(a: &Int64Array, i: usize) -> Option<u64> {
        if a.is_null(i) {
            None
        } else {
            Some(a.value(i) as u64)
        }
    }
}
