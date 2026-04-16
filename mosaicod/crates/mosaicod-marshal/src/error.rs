use mosaicod_core as core;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("serialization error")]
    SerializationError(String),
    #[error("deserialization error")]
    DeserializationError(String),
}

impl core::error::PublicError for Error {
    fn error(&self) -> core::Error {
        match self {
            Self::DeserializationError(msg) => core::Error::bad_request(msg.to_owned()),
            Self::SerializationError(_) => {
                core::Error::internal(Some("internal serialization failed".to_owned()))
            }
        }
    }
}
