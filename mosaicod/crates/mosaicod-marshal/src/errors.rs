#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("serialization error")]
    SerializationError(String),
    #[error("deserialization error")]
    DeserializationError(String),
}
