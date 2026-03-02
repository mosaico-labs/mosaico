use mosaicod_core::types;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unable to find `{0}`")]
    NotFound(String),
    #[error("missing metadata field `{0}`")]
    MissingMetadataField(String),
    #[error("missing serialization format for resource {0}")]
    MissingSerializationFormat(String),
    #[error("operation failed, sequence notification `{0}` has been added")]
    FailedAndNotified(types::Uuid),
    #[error(
        "an error occurred, but the system was unable to notify it. The original message is: {0}"
    )]
    FailedAndUnableToNotify(String),
    #[error("store error")]
    StoreError(#[from] mosaicod_store::Error),
    #[error("data serialization error")]
    DataSerializationError(#[from] mosaicod_rw::Error),
    #[error("metadata error")]
    MetadataError(#[from] types::MetadataError),
    #[error("database error")]
    DatabaseError(#[from] mosaicod_db::Error),
    #[error("sequence locked, unable to perform modifications")]
    SequenceLocked,
    #[error("concurrecy error")]
    ConcurrencyError(String),
    #[error("query error")]
    QueryError(#[from] mosaicod_query::Error),
    #[error("marshalling error")]
    MarshallingError(#[from] mosaicod_marshal::Error),
    #[error("topic locked, unable to perform modifications")]
    TopicLocked,
    #[error("session locked, unable to perform modifications")]
    SessionLocked,
    #[error("topic unlocked, unable to perform the requested operation over an unlocked topic")]
    TopicUnlocked,
    #[error("unimplemented")]
    Unimplemented,
    #[error("unauthorized")]
    Unauthorized,
    #[error("missing data")]
    MissingData(String),
}

impl Error {
    /// Report an error due to some missing data, `msg` is used to
    /// give additional infos about the missing data (e.g. which data are missing).
    pub fn missing_data(msg: String) -> Self {
        Self::MissingData(msg)
    }

    /// The requested resource was not found
    pub fn not_found(msg: String) -> Self {
        Self::NotFound(msg)
    }

    /// Used to report a failure and a corresponding notification,
    /// the notification will be used by the users to see advanced
    /// details about the error.
    pub fn failed_and_notified(notification_uuid: types::Uuid) -> Self {
        Self::FailedAndNotified(notification_uuid)
    }

    /// Used when something has failed, similar to [`Error::failed_and_notified`],
    /// but a notification has not been created.
    pub fn failed_and_unable_to_notify(msg: String) -> Self {
        Self::FailedAndUnableToNotify(msg)
    }
}
