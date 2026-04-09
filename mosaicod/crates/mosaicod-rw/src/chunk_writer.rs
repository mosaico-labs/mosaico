use super::*;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use mosaicod_core::{traits, types};
use std::time::Instant;
use tracing::debug;

pub struct SerializedChunk {
    pub path: std::path::PathBuf,
    pub ontology_stats: types::OntologyModelStats,
    pub metadata: ChunkMetadata,
}

/// Writes [`RecordBatch`] into multiple chunks to a location. A location is a path-like structure.
/// Internally the [`ChunkWriter`] can subdivide the batches in multiple files.
///
/// When `max_chunk_size` is set and the internal buffer exceeds this size
/// after writing a batch, the current chunk is automatically finalized and
/// written to storage. A new chunk is started for subsequent writes.
pub struct ChunkWriter<W> {
    format: types::Format,

    /// Schema is here, even if can be deducted from a record batch to impose that a chunked writer
    /// can work on a single schema.
    schema: SchemaRef,

    /// Target of the write operation for this writer
    write_target: W,

    /// Number of chunks serialized
    pub chunk_count: usize,

    /// Callback used to format file path when new data needs to be serialized
    path_provider: Box<dyn Fn(usize) -> std::path::PathBuf + Send>,
}

impl<W> ChunkWriter<W> {
    /// Creates a new [`ChunkWriter`].
    /// This writer will split data when a `max_chunk_size` is reached (see
    /// [`ChunkedWriter::with_max_chunk_size`] for more details.
    pub fn new<F>(target: W, format: types::Format, schema: SchemaRef, path_provider: F) -> Self
    where
        F: Fn(usize) -> std::path::PathBuf + Send + 'static,
    {
        Self {
            write_target: target,
            format,
            schema,
            chunk_count: 0,
            path_provider: Box::new(path_provider),
        }
    }

    /// Writes a [`RecordBatch`] into the chunked writer.
    ///
    /// The [`ChunkWriter`] will internally manage the creation of chunks
    /// based on the serialization format and the maximum chunk size (if any).
    pub async fn write<A>(&mut self, batch: RecordBatch) -> Result<SerializedChunk, Error>
    where
        A: traits::AsyncWriteToPath,
        W: AsRef<A>,
    {
        let mut writer = InMemoryChunkEncoder::try_new(self.schema.clone(), self.format)?;

        let encoding_time = Instant::now();

        // Offload CPU-intensive parquet encoding/compression to blocking thread pool
        let (buffer, stats, chunk_metadata) = tokio::task::spawn_blocking(move || {
            writer.write(&batch)?;
            writer.finalize()
        })
        .await
        .map_err(|e| Error::BlockingOperationError(e.to_string()))??;

        let buffer_len = buffer.len();
        let encoding_time_ms = encoding_time.elapsed().as_millis();
        let store_time = Instant::now();

        let target_path = self.store(buffer).await?;
        self.chunk_count += 1;

        debug!(
            target = "chunk encoding",
            encoding_ms = encoding_time_ms,
            store_ms = store_time.elapsed().as_millis(),
            store_path = target_path.to_string_lossy().to_string(),
            buffer_size_kb = buffer_len / 1000
        );

        Ok(SerializedChunk {
            path: target_path,
            ontology_stats: stats,
            metadata: chunk_metadata,
        })
    }

    /// Finalize the writing process of a single chunk
    async fn store<A>(&mut self, buffer: Vec<u8>) -> Result<std::path::PathBuf, Error>
    where
        A: traits::AsyncWriteToPath,
        W: AsRef<A>,
    {
        let path = (self.path_provider)(self.chunk_count);

        self.write_target
            .as_ref()
            .write_to_path(&path, buffer)
            .await?;

        Ok(path)
    }
}
