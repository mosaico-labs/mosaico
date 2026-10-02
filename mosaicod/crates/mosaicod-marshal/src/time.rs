use mosaicod_core::types;
pub use mosaicod_proto::v1::time::TimestampRange;

pub fn timestamp_range_from_proto(value: TimestampRange) -> types::TimestampRange {
    types::TimestampRange {
        start: value
            .start_ns
            .map_or_else(types::Timestamp::unbounded_neg, Into::into),
        end: value
            .end_ns
            .map_or_else(types::Timestamp::unbounded_pos, Into::into),
    }
}

pub fn timestamp_range_to_proto(value: types::TimestampRange) -> TimestampRange {
    TimestampRange {
        start_ns: (!value.start.is_unbounded_neg()).then(|| value.start.as_i64()),
        end_ns: (!value.end.is_unbounded_pos()).then(|| value.end.as_i64()),
    }
}
