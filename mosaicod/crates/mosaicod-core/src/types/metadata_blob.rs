#[derive(thiserror::Error, Debug)]
pub enum MetadataError {
    #[error("deserialization error :: {0}")]
    DeserializationError(String),
    #[error("serialization error :: {0}")]
    SerializationError(String),
}

pub trait MetadataBlob {
    fn try_to_string(&self) -> Result<String, MetadataError>;
    fn try_from_str(v: &str) -> Result<impl MetadataBlob, MetadataError>;
    fn to_bytes(&self) -> Result<Vec<u8>, MetadataError>;
}
