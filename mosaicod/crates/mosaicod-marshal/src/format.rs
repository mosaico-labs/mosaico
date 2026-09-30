use crate::Error;
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

pub fn try_format_from_i32(value: i32) -> Result<types::Format, Error> {
    Format::try_from(value)
        .map(format_from_proto)
        .map_err(|_| Error::DeserializationError(format!("unknown serialization format: {value}")))
}
