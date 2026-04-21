use mosaicod_core as core;
use mosaicod_ext as ext;
use parquet::errors::ParquetError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("parquet error")]
    ParquetError(#[from] ParquetError),
    #[error("arrow error")]
    ArrowError(#[from] ext::arrow::Error),
    #[error("io error")]
    IOError(#[from] std::io::Error),
    #[error("unsupported write format")]
    Unsupported,
    #[error("blocking operation failed: {0}")]
    BlockingOperationError(String),
}

impl core::error::PublicError for Error {
    fn error(&self) -> core::Error {
        core::Error::internal(None)
    }
}
