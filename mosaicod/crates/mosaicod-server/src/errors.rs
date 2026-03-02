use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("error during data streaming")]
    StreamError(String),

    #[error("missing descriptor in request")]
    MissingDescriptor,

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

    #[error("resource error")]
    ResourceError(#[from] mosaicod_core::types::ResourceError),

    /// This error is produced when the flight header message is missing.
    ///
    /// The flight header message is the first message sent by flight in a `do_put` call,
    /// this message carries the schema and the descriptor.
    #[error("missing header message")]
    MissingDoPutHeaderMessage,

    #[error("not found")]
    NotFound,

    #[error("received duplicate schema in payload")]
    DuplicateSchemaInPayload,

    #[error("sequence `{0}` already exists")]
    SequenceAlreadyExists(String),

    #[error("topic is locked")]
    TopicLocked,

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

    #[error("io error")]
    IOError(#[from] std::io::Error),

    #[error("sanitization error")]
    SchemaError(#[from] mosaicod_ext::arrow::SchemaError),

    #[error("malformed uuid")]
    MalformedUuid(#[from] mosaicod_core::types::UuidError),

    #[error("bad command")]
    BadCommand(#[from] serde_json::Error),

    #[error("rw error")]
    RwError(#[from] mosaicod_rw::Error),

    #[error("arrow error")]
    ArrowError(#[from] mosaicod_ext::arrow::Error),

    #[error("marshal error")]
    MarshalError(#[from] mosaicod_marshal::Error),

    #[error("action error")]
    ActionError(#[from] mosaicod_marshal::ActionError),

    #[error("facade error")]
    FacadeError(#[from] mosaicod_facade::Error),

    #[error("database error")]
    DatabaseError(#[from] mosaicod_db::Error),

    #[error("query error")]
    QueryError(#[from] mosaicod_query::Error),

    #[error("internal error: {0}")]
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
            ServerError::MissingDescriptor => Status::invalid_argument(value.to_string()),
            ServerError::BadTicket(_) => Status::invalid_argument(value.to_string()),
            ServerError::BadKey => Status::invalid_argument(value.to_string()),
            ServerError::NotFound => Status::not_found(value.to_string()),
            ServerError::Unimplemented => Status::unimplemented(value.to_string()),
            ServerError::BadCommand(_) => Status::invalid_argument(value.to_string()),
            ServerError::DuplicateSchemaInPayload => Status::invalid_argument(value.to_string()),
            ServerError::SequenceAlreadyExists(_) => Status::already_exists(value.to_string()),
            ServerError::TopicLocked => Status::permission_denied(value.to_string()),
            ServerError::TopicAlreadyExists(_) => Status::already_exists(value.to_string()),
            ServerError::NoData => Status::not_found(value.to_string()),
            ServerError::SchemaError(_) => Status::invalid_argument(value.to_string()),
            ServerError::MalformedUuid(_) => Status::invalid_argument(value.to_string()),
            ServerError::MissingDoPutHeaderMessage => Status::invalid_argument(value.to_string()),
            ServerError::MissingSerializationFormat => Status::invalid_argument(value.to_string()),
            ServerError::UnsupportedDescriptor => Status::invalid_argument(value.to_string()),
            ServerError::MissingOntologyTag => Status::invalid_argument(value.to_string()),
            ServerError::MissingSchema => Status::invalid_argument(value.to_string()),

            _ => Status::internal(value.to_string()),
        }
    }
}
