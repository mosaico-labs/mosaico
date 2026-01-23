use parquet::errors::ParquetError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("unknown serialization format `{0}`")]
    UnkownFormat(String),
    #[error("parquet error :: {0}")]
    ParquetError(#[from] ParquetError),
    #[error("arrow error :: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("io error :: {0}")]
    IOError(#[from] std::io::Error),
    #[error("chunk creation callback error with message `{0}`")]
    ChunkCreationCallbackError(String),
    #[error("unsupported write format")]
    Unsupported,
    #[error("blocking operation failed: {0}")]
    BlockingOperationError(String),
}
