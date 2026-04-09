//! Module for reading and writing data in various formats.

pub mod errors;
pub use errors::Error;

pub mod format;
pub use format::*;

pub mod chunk_encoder;
pub use chunk_encoder::{ChunkMetadata, InMemoryChunkEncoder};

mod writer;

pub mod chunk_writer;
pub use chunk_writer::{ChunkWriter, SerializedChunk};

pub mod chunk_reader;
pub use chunk_reader::ChunkReader;
