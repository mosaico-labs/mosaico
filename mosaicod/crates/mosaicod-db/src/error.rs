use mosaicod_core as core;

#[derive(Debug)]
pub enum Error {
    /// An error occurred in the underlying SQL database backend (e.g., connection, query execution).
    BackendError(sqlx::Error),

    /// The record already exists
    AlreadyExists,

    /// An error occurred during database schema migration.
    MigrationError(sqlx::migrate::MigrateError),

    /// An error occurred during serialization or deserialization of data,
    /// typically to or from JSON in the database.
    SerializationError(serde_json::Error),

    /// Found some bad data inside the database
    BadData(String),

    /// An attempt was made to handle an unrecognized or unsupported report type.
    UnknownNotificationType(String),

    /// A required field was found to be empty.
    EmptyField,

    /// The received query is empty
    EmptyQuery,

    // Not found
    NotFound,

    /// The query received contains an unsupported operation
    QueryError(mosaicod_query::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BackendError(_) => write!(f, "backend error"),
            Self::AlreadyExists => write!(f, "already exists"),
            Self::MigrationError(_) => write!(f, "migration error"),
            Self::SerializationError(_) => write!(f, "serialization error"),
            Self::BadData(msg) => write!(f, "bad data: {0}", msg),
            Self::UnknownNotificationType(_) => write!(f, "unknown notification type"),
            Self::EmptyField => write!(f, "empty field"),
            Self::EmptyQuery => write!(f, "empty query"),
            Self::NotFound => write!(f, "not found"),
            Self::QueryError(_) => write!(f, "query error"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BackendError(err) => Some(err),
            Self::MigrationError(err) => Some(err),
            Self::SerializationError(err) => Some(err),
            Self::QueryError(err) => Some(err),

            _ => None,
        }
    }
}

impl From<sqlx::Error> for Error {
    fn from(value: sqlx::Error) -> Self {
        match &value {
            sqlx::Error::Database(err) => {
                if err.is_unique_violation() {
                    Self::AlreadyExists
                } else {
                    Self::BackendError(value)
                }
            }
            sqlx::Error::RowNotFound => Self::NotFound,
            _ => Self::BackendError(value),
        }
    }
}

impl From<sqlx::migrate::MigrateError> for Error {
    fn from(value: sqlx::migrate::MigrateError) -> Self {
        Self::MigrationError(value)
    }
}

impl From<mosaicod_query::Error> for Error {
    fn from(value: mosaicod_query::Error) -> Self {
        Self::QueryError(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::SerializationError(value)
    }
}

impl core::error::PublicError for Error {
    fn error(&self) -> core::Error {
        match self {
            Self::NotFound => core::Error::not_found(String::new()),
            Self::AlreadyExists => core::Error::already_exists(String::new()),
            _ => core::Error::internal(Some("database failure".to_owned())),
        }
    }
}
