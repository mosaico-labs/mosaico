use thiserror::Error;

#[derive(Debug, Error)]
pub enum FormatError {
    #[error("unknown format :: {0}")]
    UnknownFormat(String),
}

impl FormatError {
    pub fn unknown_format(format_name: &str) -> Self {
        Self::UnknownFormat(format_name.to_owned())
    }
}

/// This enum allows choosing the appropriate resource format based on the
/// structure of the data being written.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Format {
    /// Resource format used to store data in a columnar format.
    /// This is suitable for structured data where each row has a fixed number of columns.
    Default,

    /// Resource format for ragged data, where each record can contain a
    /// variable number of items. This is ideal for representing nested or list-like
    /// structures.
    Ragged,

    /// Resource format for images and dense multi-dimensional arrays.
    /// This format is optimized for storing high-dimensional data efficiently.
    Image,
}

impl Format {
    /// Returns the format name.
    fn name(&self) -> &'static str {
        match self {
            Format::Default => "default",
            Format::Ragged => "ragged",
            Format::Image => "image",
        }
    }
}

impl std::str::FromStr for Format {
    type Err = FormatError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "default" => Ok(Self::Default),
            "ragged" => Ok(Self::Ragged),
            "image" => Ok(Self::Image),
            _ => Err(FormatError::unknown_format(value)),
        }
    }
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn from_str() {
        let default = Format::from_str("default");
        assert!(default.is_ok());
        assert_eq!(default.as_ref().unwrap(), &Format::Default);

        let ragged = Format::from_str("ragged");
        assert!(ragged.is_ok());
        assert_eq!(ragged.as_ref().unwrap(), &Format::Ragged);

        let image = Format::from_str("image");
        assert!(image.is_ok());
        assert_eq!(image.as_ref().unwrap(), &Format::Image);
    }

    #[test]
    fn to_str() {
        assert_eq!("ragged", Format::Ragged.to_string());
        assert_eq!("default", Format::Default.to_string());
        assert_eq!("image", Format::Image.to_string());
    }
}
