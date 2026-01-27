use mosaicod_core::types;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Default,
    Ragged,
    Image,
}

impl From<types::Format> for Format {
    fn from(value: types::Format) -> Self {
        match value {
            types::Format::Default => Self::Default,
            types::Format::Ragged => Self::Ragged,
            types::Format::Image => Self::Image,
        }
    }
}

impl From<Format> for types::Format {
    fn from(value: Format) -> Self {
        match value {
            Format::Default => types::Format::Default,
            Format::Ragged => types::Format::Ragged,
            Format::Image => types::Format::Image,
        }
    }
}
