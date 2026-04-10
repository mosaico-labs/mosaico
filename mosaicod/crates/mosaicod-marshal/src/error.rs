use mosaicod_core as core;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("serialization error")]
    SerializationError(String),
    #[error("deserialization error")]
    DeserializationError(String),
}

impl core::error::PublicError for Error {
    fn error_kind(&self) -> core::error::ErrorKind {
        match self {
            Self::DeserializationError(msg) => core::error::bad_request(msg.to_owned()),
            Self::SerializationError(_) => core::error::internal(),
        }
    }
}
