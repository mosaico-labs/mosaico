use mosaicod_core::types;
pub use mosaicod_proto::v1::time::TimestampRange;

pub fn timestamp_range_from_proto(value: &TimestampRange) -> types::TimestampRange {
    types::TimestampRange {
        start: value.start_ns.into(),
        end: value.end_ns.into(),
    }
}

pub(crate) fn timestamp_range_to_proto(value: &types::TimestampRange) -> TimestampRange {
    TimestampRange {
        start_ns: value.start.as_i64(),
        end_ns: value.end.as_i64(),
    }
}
