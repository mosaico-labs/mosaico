#[derive(thiserror::Error, Debug, Clone)]
pub enum ErrorKind {
    #[error("Not found")]
    NotFound,
    #[error("Already exists")]
    AlreadyExists,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Unimplemented")]
    Unimplemented,
    #[error("Unable to fullfill request, session `{0}` is locked.")]
    LockedSession(String),
    #[error("Unable to fullfill request, topic `{0}` is locked.")]
    LockedTopic(String),
    #[error("Unable to fullfill request, topic `{0}` is unlocked.")]
    UnlockedTopic(String),
    #[error("Locator contains unsupported characters")]
    BadLocator,
    #[error("Bad UUID")]
    BadUuid,
    #[error("Bad request: {0}")]
    BadRequest(String),
    #[error("Bad header: {0}")]
    BadHeader(String),
    #[error("Stream error: {0}")]
    StreamError(String),
    #[error("Missing API key in request header")]
    MissingApiKey,
    #[error("Missing schema")]
    MissingSchema,
    #[error("Missing header")]
    MissingHeader,
    #[error("Missing descriptor")]
    MissingDescriptor,
    #[error("Unsupported Arrow Flight descriptor")]
    UnsupportedDescriptor,
    #[error("Unsupported message")]
    UnsupportedMessage,
    #[error("Unsupported locator")]
    UnsupportedLocator,
    #[error("Unsupported operation")]
    UnsupportedOperation,
    #[error("Unsupported schema: {0}")]
    UnsupportedSchema(String),
    #[error("Internal error")]
    Internal,
}

pub fn not_found() -> ErrorKind {
    ErrorKind::NotFound
}

pub fn already_exists() -> ErrorKind {
    ErrorKind::AlreadyExists
}

pub fn locked_session(locator: String) -> ErrorKind {
    ErrorKind::LockedSession(locator)
}

pub fn locked_topic(locator: String) -> ErrorKind {
    ErrorKind::LockedTopic(locator)
}

pub fn unlocked_topic(locator: String) -> ErrorKind {
    ErrorKind::UnlockedTopic(locator)
}

pub fn stream_error(err: impl std::error::Error) -> ErrorKind {
    ErrorKind::StreamError(err.to_string())
}

pub fn unauthorized() -> ErrorKind {
    ErrorKind::Unauthorized
}

pub fn unimplemented() -> ErrorKind {
    ErrorKind::Unimplemented
}

pub fn bad_locator() -> ErrorKind {
    ErrorKind::BadLocator
}

pub fn bad_uuid() -> ErrorKind {
    ErrorKind::BadUuid
}

pub fn bad_request(msg: String) -> ErrorKind {
    ErrorKind::BadRequest(msg)
}

pub fn bad_header(msg: String) -> ErrorKind {
    ErrorKind::BadHeader(msg)
}

pub fn missing_api_key() -> ErrorKind {
    ErrorKind::MissingApiKey
}

pub fn missing_schema() -> ErrorKind {
    ErrorKind::MissingSchema
}

pub fn missing_header() -> ErrorKind {
    ErrorKind::MissingHeader
}

pub fn missing_descriptor() -> ErrorKind {
    ErrorKind::MissingDescriptor
}

pub fn unsupported_descriptor() -> ErrorKind {
    ErrorKind::UnsupportedDescriptor
}

pub fn unsupported_message() -> ErrorKind {
    ErrorKind::UnsupportedMessage
}

pub fn unsupported_locator() -> ErrorKind {
    ErrorKind::UnsupportedLocator
}

pub fn unsupported_operation() -> ErrorKind {
    ErrorKind::UnsupportedOperation
}

pub fn unsupported_schema(msg: String) -> ErrorKind {
    ErrorKind::UnsupportedSchema(msg)
}

pub fn internal() -> ErrorKind {
    ErrorKind::Internal
}

impl ErrorKind {
    pub fn to_public_error(self) -> BoxPublicError {
        self.into()
    }
}

pub trait PublicError: std::fmt::Display + std::fmt::Debug + std::error::Error {
    fn error_kind(&self) -> ErrorKind;

    /// Optionally a documentation link can be provided.
    fn documentation_link(&self) -> Option<url::Url> {
        None
    }

    fn public_error(&self) -> String {
        let code = self.error_kind();
        format!("{}", code)
    }
}

impl PublicError for ErrorKind {
    fn error_kind(&self) -> ErrorKind {
        self.clone()
    }
}

pub type BoxPublicError = Box<dyn PublicError + Send + Sync + 'static>;

pub type Result<T> = std::result::Result<T, BoxPublicError>;

/// Blanket implementation
/// Used to automatically box an error with .into()
impl<E> From<E> for Box<dyn PublicError + Send + Sync>
where
    E: PublicError + Send + Sync + 'static,
{
    fn from(error: E) -> Self {
        Box::new(error)
    }
}
