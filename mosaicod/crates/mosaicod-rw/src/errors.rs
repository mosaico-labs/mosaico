use parquet::errors::ParquetError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    // #[error("unknown serialization format `{0}`")]
    // UnkownFormat(String),
    #[error("parquet error")]
    ParquetError(#[from] ParquetError),
    #[error("arrow error")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("io error")]
    IOError(#[from] std::io::Error),
    #[error("chunk creation callback error with message `{0}`")]
    ChunkCreationCallbackError(String),
    #[error("unsupported write format")]
    Unsupported,
    #[error("blocking operation failed: {0}")]
    BlockingOperationError(String),
}
