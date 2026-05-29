use mosaicod_core as core;

/// Errors that can occur during the construction or deserialization of a Query.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Wraps errors occurring during the JSON parsing/deserialization phase.
    #[error("query deserialization error: {0}")]
    DeserializationError(String),

    #[error("operation error :: field `{field}` has {err}")]
    OpError { field: String, err: super::OpError },

    #[error("bad field `{field}`")]
    BadField { field: String },

    #[error("datafusion backend error")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("bad path")]
    BadPath(#[from] url::ParseError),

    #[error("store error")]
    StoreError(#[from] mosaicod_store::Error),
}

impl Error {
    pub fn unsupported_op(field_name: String) -> Self {
        Self::OpError {
            field: field_name,
            err: super::OpError::UnsupportedOperation,
        }
    }

    pub fn empty_in(field_name: String) -> Self {
        Self::OpError {
            field: field_name,
            err: super::OpError::EmptyIn,
        }
    }

    pub fn empty_pattern(field_name: String) -> Self {
        Self::OpError {
            field: field_name,
            err: super::OpError::EmptyPattern,
        }
    }

    pub fn bad_field(field_name: String) -> Self {
        Self::BadField { field: field_name }
    }
}

impl core::error::PublicError for Error {
    fn error(&self) -> core::Error {
        match self {
            Self::BadField { field } => core::Error::bad_request(field.to_owned()),
            Self::OpError { field, err } => core::Error::bad_request(format!("{field} : {err}")),
            Self::StoreError(e) => e.error(),
            Self::DeserializationError(msg) => core::Error::bad_request(msg.to_owned()),
            Self::DataFusion(_) | Self::BadPath(_) => {
                core::Error::internal(Some("query engine failed".to_owned()))
            }
        }
    }
}
