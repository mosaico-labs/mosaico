use mosaicod_core as core;

/// Errors that can occur during the construction or deserialization of a Query.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Wraps errors occurring during the JSON parsing/deserialization phase.
    #[error("query deserialization error: {0}")]
    DeserializationError(String),

    #[error("operation error :: field `{field}` has {err}")]
    OpError { field: String, err: super::OpError },

    #[error("bad field `{field}`: {msg}")]
    BadField { field: String, msg: String },

    #[error("datafusion backend error")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("bad path")]
    BadPath(#[from] url::ParseError),

    #[error("store error")]
    StoreError(#[from] mosaicod_store::Error),

    #[error("retrieved null min or max timestamp")]
    NullMinMaxTimestamps,
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

    pub fn pattern_too_long(field_name: String) -> Self {
        Self::OpError {
            field: field_name,
            err: super::OpError::PatternTooLong,
        }
    }

    pub fn malformed_pattern(field_name: String, msg: String) -> Self {
        Self::OpError {
            field: field_name,
            err: super::OpError::MalformedPattern(msg),
        }
    }

    pub fn bad_field(field_name: String) -> Self {
        Self::BadField {
            field: field_name,
            msg: String::new(),
        }
    }

    pub fn bad_field_with_message(field: String, msg: String) -> Self {
        Self::BadField { field, msg }
    }

    pub fn list_too_large(field_name: String, max: usize) -> Self {
        Self::OpError {
            field: field_name,
            err: super::OpError::ListTooLarge { max },
        }
    }
}

impl core::error::PublicError for Error {
    fn error(&self) -> core::Error {
        match self {
            Self::BadField { field: _, msg: _ } => core::Error::bad_request(self.to_string()),
            Self::OpError { field, err } => core::Error::bad_request(format!("{field} : {err}")),
            Self::StoreError(e) => e.error(),
            Self::DeserializationError(msg) => core::Error::bad_request(msg.to_owned()),
            Self::DataFusion(_) | Self::BadPath(_) | Self::NullMinMaxTimestamps => {
                core::Error::internal(Some("query engine failed".to_owned()))
            }
        }
    }
}

/// Converts a [`query::regex::Error`] into an [`Error`] adding the field name to the error description.
pub fn regex_to_query_error(regex_err: super::regex::Error, field: String) -> Error {
    match regex_err {
        super::regex::Error::EmptyPattern => Error::empty_pattern(field),
        super::regex::Error::PatternTooLong => Error::pattern_too_long(field),
        super::regex::Error::MalformedPattern(err) => {
            Error::malformed_pattern(field.to_owned(), err.to_string())
        }
    }
}
