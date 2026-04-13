/// A trait for errors that can be safely exposed to external clients.
///
/// `PublicError` serves as a translation layer between internal system failures
/// and meaningful, user-facing error responses. It ensures that every error
/// returned by the server provides a consistent message and, where applicable,
/// helpful debugging resources like documentation links.
///
/// This trait is designed for minimal boilerplate. Implementers *only need to
/// define the [`PublicError::error()`] method*. The documentation links and string
/// formatting are handled by default implementations, though they can be
/// overridden if specific customization is required.
///
/// ```rust
/// use mosaicod_core::{Error, error::PublicError};
///
/// #[derive(Debug)]
/// struct MyCustomError();
///
/// impl PublicError for MyCustomError {
///     fn error(&self) -> Error {
///         Error::internal()
///     }
/// }
/// ```
pub trait PublicError: std::fmt::Debug {
    /// Returns the inner [`Error`] variant.
    ///
    /// This is used internally to build the error string representation
    fn error(&self) -> Error;

    /// Returns an optional URL pointing to detailed documentation about the error.
    fn documentation_link(&self) -> Option<url::Url> {
        let err = self.error();
        match err.kind() {
            ErrorKind::Internal => Some("https://c.xkcd.com/random/comic/".parse().unwrap()),
            _ => None,
        }
    }
}

impl std::fmt::Display for dyn PublicError + Send + Sync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut client_string = self.error().to_string();

        // Ensure that the line ends with a `.`
        if !client_string.ends_with('.') {
            client_string.push('.');
        }

        if let Some(link) = self.documentation_link() {
            client_string.push_str(&format!(" See {link} for more details."));
        }

        write!(f, "{}", client_string)
    }
}

/// Shorthand alias for a boxed public error
pub type BoxPublicError = Box<dyn PublicError + Send + Sync + 'static>;

/// mosaicod custom public error result alias
pub type PublicResult<T> = std::result::Result<T, BoxPublicError>;

/// Blanket implementation for auto-boxing
impl<E> From<E> for Box<dyn PublicError + Send + Sync>
where
    E: PublicError + Send + Sync + 'static,
{
    fn from(error: E) -> Self {
        Box::new(error)
    }
}

/// Represents an error than can occur in mosaicod.
///
/// This is the public error type that can be returned to clients.
/// It is designed to be easily converted from internal errors,
/// and to provide a consistent error interface for clients.
///
/// The macro `#[error(msg)]` is used to define the error message
/// for each variant.
///
/// [`Error`] can be converted into a [`PublicError`] using the
/// [`Error::to_public_error`] method, which allows for additional
/// information to be provided about the error such as a documentation link.
#[derive(thiserror::Error, Debug, Clone)]
pub enum ErrorKind {
    #[error("Not found")]
    NotFound,
    #[error("Already exists")]
    AlreadyExists,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Unauthenticated")]
    Unauthenticated,
    #[error("Unimplemented")]
    Unimplemented,
    #[error("Session `{0}` is locked.")]
    LockedSession(String),
    #[error("Topic `{0}` is locked.")]
    LockedTopic(String),
    #[error("Topic `{0}` is unlocked.")]
    UnlockedTopic(String),
    #[error("Session `{0} is empty.`")]
    EmptySession(String),
    #[error("Locator contains unsupported characters")]
    BadLocator,
    #[error("Bad UUID: are you sure it's a valid UUID?")]
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
    #[error("Missing header in request")]
    MissingHeader,
    #[error("Request has no descriptor")]
    MissingDescriptor,
    #[error("Unsupported descriptor")]
    UnsupportedDescriptor,
    #[error("Unsupported stream message, stream aborted.")]
    UnsupportedStreamMessage,
    #[error("Unsupported locator")]
    UnsupportedLocator,
    #[error("Unsupported operation")]
    UnsupportedOperation,
    #[error("Unsupported schema: {0}")]
    UnsupportedSchema(String),
    #[error("Internal error")]
    Internal,
}

#[derive(Debug, Clone)]
pub struct Error(ErrorKind);

impl Error {
    pub fn kind(&self) -> &ErrorKind {
        &self.0
    }

    pub fn not_found() -> Self {
        Self(ErrorKind::NotFound)
    }

    pub fn already_exists() -> Self {
        Self(ErrorKind::AlreadyExists)
    }

    pub fn locked_session(locator: String) -> Self {
        Self(ErrorKind::LockedSession(locator))
    }

    pub fn locked_topic(locator: String) -> Self {
        Self(ErrorKind::LockedTopic(locator))
    }

    pub fn unlocked_topic(locator: String) -> Self {
        Self(ErrorKind::UnlockedTopic(locator))
    }

    pub fn empty_session(locator: String) -> Self {
        Self(ErrorKind::EmptySession(locator))
    }

    pub fn stream_error(err: impl std::error::Error) -> Self {
        Self(ErrorKind::StreamError(err.to_string()))
    }

    pub fn unauthorized() -> Self {
        Self(ErrorKind::Unauthorized)
    }

    pub fn unauthenticated() -> Self {
        Self(ErrorKind::Unauthenticated)
    }

    pub fn unimplemented() -> Self {
        Self(ErrorKind::Unimplemented)
    }

    pub fn bad_locator() -> Self {
        Self(ErrorKind::BadLocator)
    }

    pub fn bad_uuid() -> Self {
        Self(ErrorKind::BadUuid)
    }

    pub fn bad_request(msg: String) -> Self {
        Self(ErrorKind::BadRequest(msg))
    }

    pub fn bad_header(msg: String) -> Self {
        Self(ErrorKind::BadHeader(msg))
    }

    pub fn missing_api_key() -> Self {
        Self(ErrorKind::MissingApiKey)
    }

    pub fn missing_schema() -> Self {
        Self(ErrorKind::MissingSchema)
    }

    pub fn missing_header() -> Self {
        Self(ErrorKind::MissingHeader)
    }

    pub fn missing_descriptor() -> Self {
        Self(ErrorKind::MissingDescriptor)
    }

    pub fn unsupported_descriptor() -> Self {
        Self(ErrorKind::UnsupportedDescriptor)
    }

    pub fn unsupported_stream_message() -> Self {
        Self(ErrorKind::UnsupportedStreamMessage)
    }

    pub fn unsupported_locator() -> Self {
        Self(ErrorKind::UnsupportedLocator)
    }

    pub fn unsupported_operation() -> Self {
        Self(ErrorKind::UnsupportedOperation)
    }

    pub fn unsupported_schema(msg: String) -> Self {
        Self(ErrorKind::UnsupportedSchema(msg))
    }

    pub fn internal() -> Self {
        Self(ErrorKind::Internal)
    }

    pub fn to_public_error(self) -> BoxPublicError {
        self.into()
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{err}", err = self.0)
    }
}

impl PublicError for Error {
    fn error(&self) -> Error {
        self.clone()
    }
}
