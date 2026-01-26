//! Module for reading and writing data in various formats.

pub mod errors;
pub use errors::Error;

pub mod format;
pub use format::*;

pub mod chunk_writer;
pub use chunk_writer::{ChunkMetadata, ChunkWriter};

mod writer;

pub mod chunked_writer;
pub use chunked_writer::{ChunkedWriter, ChunkedWriterSummary};

pub mod chunk_reader;
pub use chunk_reader::ChunkReader;
