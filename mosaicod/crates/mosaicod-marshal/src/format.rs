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

pub fn format_to_i32(value: types::Format) -> i32 {
    format_to_proto(value) as i32
}

fn try_format_from_proto(value: Format) -> Result<types::Format, Error> {
    Ok(match value {
        Format::Unspecified => {
            return Err(Error::DeserializationError(
                "unknown serialization format".to_owned(),
            ));
        }
        Format::Default => types::Format::Default,
        Format::Ragged => types::Format::Ragged,
        Format::Image => types::Format::Image,
    })
}

pub fn try_format_from_i32(value: i32) -> Result<types::Format, Error> {
    Format::try_from(value)
        .map_err(|_| Error::DeserializationError(format!("unknown serialization format: {value}")))
        .map(try_format_from_proto)?
}
