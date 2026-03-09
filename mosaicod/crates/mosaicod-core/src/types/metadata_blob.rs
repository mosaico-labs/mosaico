#[derive(thiserror::Error, Debug)]
pub enum MetadataError {
    #[error("deserialization error")]
    DeserializationError(String),
    #[error("serialization error")]
    SerializationError(String),
}

pub trait MetadataBlob {
    fn try_to_string(&self) -> Result<String, MetadataError>;
    fn try_from_str(v: &str) -> Result<impl MetadataBlob, MetadataError>;
    fn to_bytes(&self) -> Result<Vec<u8>, MetadataError>;
}
