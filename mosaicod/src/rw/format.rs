use serde::{Deserialize, Serialize};

use crate::{params, rw::Error, traits};

/// This enum allows choosing the appropriate storage strategy based on the
/// structure of the data being written.
#[derive(Debug, Serialize, Deserialize, PartialEq, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    /// Serialization format used to store data in a columnar format.
    /// This is suitable for structured data where each row has a fixed number of columns.
    Default,
    /// Serialization format for ragged data, where each record can contain a
    /// variable number of items. This is ideal for representing nested or list-like
    /// structures.
    Ragged,

    /// Serialization format for images and dense multi-dimensional arrays.
    /// This format is optimized for storing high-dimensional data efficiently.
    Image,
}

impl traits::AsExtension for Format {
    fn as_extension(&self) -> String {
        match self {
            Self::Default => params::ext::PARQUET.to_owned(),
            Self::Ragged => params::ext::PARQUET.to_owned(),
            Self::Image => params::ext::PARQUET.to_owned(),
        }
    }
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Default => write!(f, "default"),
            Self::Ragged => write!(f, "ragged"),
            Self::Image => write!(f, "image"),
        }
    }
}

impl std::str::FromStr for Format {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "default" => Ok(Self::Default),
            "ragged" => Ok(Self::Ragged),
            "image" => Ok(Self::Image),
            _ => Err(Error::UnkownFormat(value.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::traits::AsExtension;
    use std::str::FromStr;

    use super::*;

    #[test]
    fn from_str() {
        let default = Format::from_str("default");
        assert!(default.is_ok());
        assert_eq!(default.as_ref().unwrap(), &Format::Default);
        assert_eq!(default.unwrap().as_extension(), params::ext::PARQUET);

        let ragged = Format::from_str("ragged");
        assert!(ragged.is_ok());
        assert_eq!(ragged.as_ref().unwrap(), &Format::Ragged);
        assert_eq!(ragged.unwrap().as_extension(), params::ext::PARQUET);

        let image = Format::from_str("image");
        assert!(image.is_ok());
        assert_eq!(image.as_ref().unwrap(), &Format::Image);
        assert_eq!(image.unwrap().as_extension(), params::ext::PARQUET);
    }

    #[test]
    fn to_str() {
        assert_eq!("ragged", Format::Ragged.to_string());
        assert_eq!("default", Format::Default.to_string());
        assert_eq!("image", Format::Image.to_string());
    }
}
