use mosaicod_core::types;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct TimestampRange {
    pub start_ns: i64,
    pub end_ns: i64,
}

impl From<types::TimestampRange> for TimestampRange {
    fn from(value: types::TimestampRange) -> Self {
        Self {
            start_ns: value.start.as_i64(),
            end_ns: value.end.as_i64(),
        }
    }
}

impl From<TimestampRange> for types::TimestampRange {
    fn from(value: TimestampRange) -> Self {
        Self {
            start: value.start_ns.into(),
            end: value.end_ns.into(),
        }
    }
}
