use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    /// An error occurred in the underlying SQL database backend (e.g., connection, query execution).
    #[error("backend error")]
    BackendError(#[from] sqlx::Error),
    /// An error occurred during database schema migration.
    #[error("migration error")]
    MigrationError(#[from] sqlx::migrate::MigrateError),
    /// An error occurred during serialization or deserialization of data,
    /// typically to or from JSON in the database.
    #[error("serialization error")]
    SerializationError(#[from] serde_json::Error),
    /// An attempt was made to handle an unrecognized or unsupported report type.
    #[error("unknown notification type")]
    UnknownNotificationType(String),
    /// A required field was found to be empty.
    #[error("empty field")]
    EmptyField,
    /// The received query is empty
    #[error("empty query")]
    EmptyQuery,
    // Not found
    #[error("not found")]
    NotFound,
    /// The query received contains an unsupported operation
    #[error("query error")]
    QueryError(#[from] mosaicod_query::Error),
}
