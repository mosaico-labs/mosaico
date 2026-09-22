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
    /// `DeserializationError`/`InvalidJsonKey` come from parsing client-supplied
    /// text (`try_from_str`) and are the client's fault: `bad_request`.
    /// `SerializationError` comes from re-serializing already-validated in-memory data,
    /// if that fails, it's an internal bug, not bad input.
    fn error(&self) -> error::Error {
        match self {
            MetadataError::InvalidJsonKey(msg) => {
                error::Error::bad_request(format!("invalid json key: {}", msg))
            }
            MetadataError::DeserializationError(msg) => {
                error::Error::bad_request(format!("invalid metadata json: {}", msg))
            }
            MetadataError::SerializationError(_) => {
                error::Error::internal(Some("marshalling failed".to_owned()))
            }
        }
    }
}

pub trait MetadataBlob {
    fn try_to_string(&self) -> Result<String, MetadataError>;
    fn try_from_str(v: &str) -> Result<impl MetadataBlob, MetadataError>;
    fn to_bytes(&self) -> Result<Vec<u8>, MetadataError>;
    fn try_from_slice(bytes: &[u8]) -> Result<impl MetadataBlob, MetadataError>;
}
