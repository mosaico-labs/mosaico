use mosaicod_core::{self as core};

#[derive(Debug)]
pub enum Error {
    MissingDbData(String),
    Query(mosaicod_query::Error),
    Internal(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingDbData(msg) => write!(f, "{msg}"),
            Self::Query(e) => write!(f, "{e}"),
            Self::Internal(_) => write!(f, "internal"),
        }
    }
}

impl From<String> for Error {
    fn from(err: String) -> Self {
        Self::Internal(Box::new(StringError(err)))
    }
}

impl From<mosaicod_db::Error> for Error {
    fn from(err: mosaicod_db::Error) -> Self {
        Self::Internal(Box::new(err))
    }
}

impl From<mosaicod_query::Error> for Error {
    fn from(err: mosaicod_query::Error) -> Self {
        Self::Query(err)
    }
}

impl From<tokio::sync::AcquireError> for Error {
    fn from(err: tokio::sync::AcquireError) -> Self {
        Self::Internal(Box::new(err))
    }
}

impl core::error::PublicError for Error {
    fn error(&self) -> core::Error {
        match self {
            Self::Query(e) => e.error(),
            _ => core::Error::internal(None),
        }
    }
}

/// Internal type used to map string to [`Error::Internal`]
#[derive(Debug)]
struct StringError(String);

impl std::fmt::Display for StringError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StringError {}
