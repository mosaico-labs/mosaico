use mosaicod_core::types;
pub use mosaicod_proto::v1::core::Format;

pub(crate) fn format_to_proto(value: types::Format) -> Format {
    match value {
        types::Format::Default => Format::Default,
        types::Format::Ragged => Format::Ragged,
        types::Format::Image => Format::Image,
    }
}

pub(crate) fn format_from_proto(value: Format) -> types::Format {
    match value {
        Format::Default => types::Format::Default,
        Format::Ragged => types::Format::Ragged,
        Format::Image => types::Format::Image,
    }
}

pub fn format_from_i32(value: i32) -> types::Format {
    let proto_format = Format::try_from(value).unwrap_or(Format::Default);
    format_from_proto(proto_format)
}
