use crate::error;

#[derive(thiserror::Error, Debug)]
pub enum MetadataError {
    #[error("deserialization error")]
    DeserializationError(String),
    #[error("serialization error")]
    SerializationError(String),
    #[error("invalid json key: {0}")]
    InvalidJsonKey(String),
}

impl error::PublicError for MetadataError {
    /// Metadata serialization/deserialization errors will always be converted to
    /// internal errors, since are completely handled by the platform
    fn error(&self) -> error::Error {
        match self {
            MetadataError::InvalidJsonKey(msg) => {
                error::Error::bad_request(format!("invalid json key: {}", msg))
            }
            _ => error::Error::internal(Some("marshalling failed".to_owned())),
        }
    }
}

pub trait MetadataBlob {
    fn try_to_string(&self) -> Result<String, MetadataError>;
    fn try_from_str(v: &str) -> Result<impl MetadataBlob, MetadataError>;
    fn to_bytes(&self) -> Result<Vec<u8>, MetadataError>;
}
