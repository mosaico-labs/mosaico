use thiserror::Error;

use crate::{query, rw};

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("error during data streaming :: {0}")]
    StreamError(String),

    #[error("missing descriptor in request")]
    MissingDescriptior,

    #[error("missing `ontology_tag`")]
    MissingOntologyTag,

    #[error("missing `serialization_format`")]
    MissingSerializationFormat,

    #[error("unsupported descriptor type")]
    UnsupportedDescriptor,

    #[error("multiple path in descriptor not supported")]
    MultiplePathUnsupported,

    #[error("missing schema")]
    MissingSchema,

    #[error("resource error :: {0}")]
    ResourceError(#[from] crate::types::ResourceError),

    /// This error is produced when the flight header message is missing.
    ///
    /// The flight header message is the first message sent by flight in a `do_put` call,
    /// this message carryies the schema and the descriptor.
    #[error("missing header message")]
    MissingDoPutHeaderMessage,

    #[error("not found")]
    NotFound,

    #[error("received duplicate schema in payload")]
    DuplicateSchemaInPayload,

    #[error("sequence `{0}` already exists")]
    SequenceAlreadyExists(String),

    #[error("sequence is locked")]
    SequenceLocked,

    #[error("topic `{0}` already exists")]
    TopicAlreadyExists(String),

    #[error("no data received")]
    NoData,

    #[error("unimplemented")]
    Unimplemented,

    #[error("bad ticket, unable to convert ticket to string (maybe not utf8?)")]
    BadTicket(String),

    #[error("bad key")]
    BadKey,

    #[error("io error :: {0}")]
    IOError(#[from] std::io::Error),

    #[error("sanitization error :: {0}")]
    SchemaError(#[from] crate::arrow::SchemaError),

    #[error("malformed key :: {0}")]
    MalformedKey(#[from] uuid::Error),

    #[error("bad command :: {0}")]
    BadCommand(#[from] serde_json::Error),

    #[error("rw error :: {0}")]
    RwError(#[from] rw::Error),

    #[error("arrow error :: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("marshal error :: {0}")]
    MarshalError(#[from] crate::marshal::Error),

    #[error("action error :: {0}")]
    ActionError(#[from] crate::marshal::ActionError),

    #[error("facade error :: {0}")]
    FacadeError(#[from] crate::repo::FacadeError),

    #[error("repository error :: {0}")]
    RepositoryError(#[from] crate::repo::Error),

    #[error("query error :: {0}")]
    QueryError(#[from] query::Error),

    #[error("internal error :: {0}")]
    InternalError(String),
}

impl ServerError {
    pub fn internal_error(msg: &str) -> Self {
        Self::InternalError(msg.to_owned())
    }
}

impl From<ServerError> for tonic::Status {
    fn from(value: ServerError) -> Self {
        use tonic::Status;
        match value {
            ServerError::MultiplePathUnsupported => Status::invalid_argument(value.to_string()),
            ServerError::MissingDescriptior => Status::invalid_argument(value.to_string()),
            ServerError::BadTicket(_) => Status::invalid_argument(value.to_string()),

            _ => Status::internal(value.to_string()),
        }
    }
}
