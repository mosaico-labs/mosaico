#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("serialization error :: {0}")]
    SerializationError(String),
    #[error("deserialization error :: {0}")]
    DeserializationError(String),
}
