#[derive(thiserror::Error, Debug)]
pub enum FacadeError {
    #[error("unable to find `{0}`")]
    NotFound(String),
    #[error("missing metadata field `{0}`")]
    MissingMetadataField(String),
    #[error("missing serialization format for resource {0}")]
    MissingSerializationFormat(String),
    #[error("store error :: {0}")]
    StoreError(#[from] mosaicod_store::Error),
    #[error("data serialization error :: {0}")]
    DataSerializationError(#[from] mosaicod_rw::Error),
    #[error("metadata error :: {0}")]
    MetadataError(#[from] mosaicod_core::types::MetadataError),
    #[error("repository error :: {0}")]
    RepositoryError(#[from] crate::Error),
    #[error("sequence locked, unable to perform modifications")]
    SequenceLocked,
    #[error("concurrecy error :: {0}")]
    ConcurrencyError(String),
    #[error("query error :: {0}")]
    QueryError(#[from] mosaicod_query::Error),
    #[error("marshalling error :: {0}")]
    MarshallingError(#[from] mosaicod_marshal::Error),
    #[error("topic locked, unable to perform modifications")]
    TopicLocked,
    #[error("topic unlocked, unable to perform the requested operation over an unlocked topic")]
    TopicUnlocked,
    #[error("unimplemented")]
    Unimplemented,
    #[error("unauthorized")]
    Unauthorized,
    #[error("missing data :: {0}")]
    MissingData(String),
}

impl FacadeError {
    pub fn missing_data(msg: String) -> Self {
        Self::MissingData(msg)
    }

    pub fn not_found(msg: String) -> Self {
        Self::NotFound(msg)
    }
}
