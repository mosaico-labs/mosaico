use std::path::PathBuf;
use std::pin::Pin;

use arrow::array::RecordBatch;
use log::{debug, trace};

use crate::{traits, types};

use super::Error;
use super::Format;
use super::chunk_writer::{ChunkMetadata, ChunkWriter};

/// Callback called just before file serialization
type OnChunkCallback = Box<
    dyn Fn(
            std::path::PathBuf,
            types::ColumnsStats,
            ChunkMetadata,
        ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>>
        + Send
        + Sync,
>;

/// Callback used to define a format function for files
type OnFileFormat = Box<dyn Fn(&std::path::Path, &Format, usize) -> std::path::PathBuf + Send>;

/// Writes [`RecordBatch`] into multiple chunks to a location. A location is a path like structure.
/// Internally the [`ChunkedWriter`] can subdivide the batches in multiple files
pub struct ChunkedWriter<'a, W>
where
    W: traits::AsyncWriteToPath,
{
    /// The current active writer. If this is [`None`], a new writer will be
    /// initialized on the first call to [`ChunkedWriter::write`].
    ///
    /// When the chunk-size constraint is reached, a new writer will be created.
    writer: Option<ChunkWriter>,
    format: Format,
    write_target: &'a W,
    /// Target path where the data will be serialized (e.g., `my/target/path`).
    ///
    /// Do not include a file extension or filename. The [`ChunkedWriter`] will
    /// automatically split the data into multiple files if the maximum chunk-size
    /// constraint is reached.
    path: PathBuf,
    /// Number of chunks serialized
    chunk_serialized_number: usize,
    /// Function called just before the chunk finalization (and serialization)
    on_chunk_created_clbk: Option<OnChunkCallback>,
    /// Callback used to format data when written
    on_file_format: OnFileFormat,
}

impl<'a, W> ChunkedWriter<'a, W>
where
    W: traits::AsyncWriteToPath,
{
    /// Creates a new [`ChunkedWriter`] that saves file to a given path on a given target writer
    pub fn new<F>(
        target: &'a W,
        path: impl AsRef<std::path::Path>,
        format: Format,
        format_callback: F,
    ) -> Self
    where
        F: Fn(&std::path::Path, &Format, usize) -> std::path::PathBuf + Send + 'static,
    {
        Self {
            writer: None,
            write_target: target,
            format,
            path: path.as_ref().to_path_buf(),
            chunk_serialized_number: 0,
            on_chunk_created_clbk: None,
            on_file_format: Box::new(format_callback),
        }
    }

    /// Sets a callback function that will be called every time a chunk is produced just before
    /// serialization.
    pub fn on_chunk_created<F1, Fut>(mut self, clbk: F1) -> Self
    where
        F1: Fn(std::path::PathBuf, types::ColumnsStats, ChunkMetadata) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = Result<(), Box<dyn std::error::Error>>> + Send + 'static,
    {
        let wrapped = move |path, stats, metadata| {
            let fut = clbk(path, stats, metadata);
            Box::pin(fut)
                as Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>>
        };

        self.on_chunk_created_clbk = Some(Box::new(wrapped));
        self
    }

    /// Writes a [`RecordBatch`] into the chunked writer.
    ///
    /// The [`ChunkedWriter`] will internally manage the creation of chunks
    /// based on the serialization format and the maximum chunk size (if any).
    /// To perform custom actions when a chunk is produced, use the
    /// [`on_chunk_produced`] method to set a callback function.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), Error> {
        // Take the writer and if not inizialized creates a new one.
        // At the end the writer will be put back.
        //
        // If the maximum chunk size is surpassed the current writer will be consumed by the finalization
        // method, and the next itertation a new writer will be instantiated. Also if defined the
        // chunk produced callback will be triggered
        let mut writer = match self.writer.take() {
            Some(w) => w,
            None => ChunkWriter::try_new(batch.schema(), self.format)?,
        };

        // Clone batch for spawn_blocking (requires 'static)
        let batch = batch.clone();

        // Offload CPU-intensive parquet encoding/compression to blocking thread pool
        writer = tokio::task::spawn_blocking(move || {
            writer.write(&batch)?;
            Ok::<_, Error>(writer)
        })
        .await
        .map_err(|e| Error::SpawnBlockingError(e.to_string()))??;

        self.writer = Some(writer);

        Ok(())
    }

    /// Finalizes any pending reading, writing operation.
    ///
    /// It is important to call this method to ensure that an open chunk is properly finalized
    /// and written.
    pub async fn finalize(&mut self) -> Result<(), Error> {
        // Calling this function will "consume" the current writer.
        // If another write_batch willl be called after this function call
        // will cause the instantiation of another writer.
        if let Some(writer) = self.writer.take() {
            let path =
                (self.on_file_format)(&self.path, &writer.format, self.chunk_serialized_number);
            self.chunk_serialized_number += 1;

            // Offload CPU-intensive parquet finalization to blocking thread pool
            let (buffer, stats, metadata) = tokio::task::spawn_blocking(move || writer.finalize())
                .await
                .map_err(|e| Error::SpawnBlockingError(e.to_string()))??;

            self.write_target.write_to_path(&path, buffer).await?;

            trace!(
                "on_chunk_created_clbk present: {}",
                self.on_chunk_created_clbk.is_some()
            );

            return self
                .on_chunk_created_clbk
                .as_ref()
                .map(async move |clbk| {
                    debug!("calling chunk serialization callback");
                    return clbk(path, stats, metadata).await;
                })
                .unwrap()
                .await
                .map_err(|e| Error::ChunkCreationCallbackError(e.to_string()));
        }
        Ok(())
    }
}
