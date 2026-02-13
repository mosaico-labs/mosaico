use std::str::FromStr;
use thiserror::Error;

/// Errors that can occur when working with [`Uuid`].
#[derive(Error, Debug)]
pub enum UuidError {
    #[error("error while parsing uuid from string :: {0}")]
    FromStringParse(String),
}

/// A wrapper around the [`uuid::Uuid`] type to provide a Mosaico-specific UUID implementation.
///
/// This struct simplifies UUID creation and validation within the Mosaico ecosystem.
/// It defaults to creating a v4 UUID.
#[derive(PartialEq)]
pub struct Uuid(uuid::Uuid);

impl Default for Uuid {
    /// Creates a new v4 UUID.
    fn default() -> Self {
        Self::new()
    }
}

impl Uuid {
    /// Creates a new v4 UUID.
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    /// Checks if the UUID is in a valid range.
    pub fn is_valid(&self) -> bool {
        !(self.0.is_nil() || self.0.is_max())
    }
}

impl AsRef<uuid::Uuid> for Uuid {
    fn as_ref(&self) -> &uuid::Uuid {
        &self.0
    }
}

impl std::fmt::Display for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<uuid::Uuid> for Uuid {
    fn from(value: uuid::Uuid) -> Self {
        Self(value)
    }
}

impl From<Uuid> for uuid::Uuid {
    fn from(value: Uuid) -> Self {
        value.0
    }
}

impl FromStr for Uuid {
    type Err = UuidError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            uuid::Uuid::try_parse(s).map_err(|e| UuidError::FromStringParse(e.to_string()))?,
        ))
    }
}
