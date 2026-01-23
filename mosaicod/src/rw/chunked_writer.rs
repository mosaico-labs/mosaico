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
            types::OntologyModelStats,
            ChunkMetadata,
        ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>>
        + Send
        + Sync,
>;

/// Callback used to define a format function for files
type OnFileFormat = Box<dyn Fn(&std::path::Path, &Format, usize) -> std::path::PathBuf + Send>;

/// Summary data produced after write completion
#[derive(Default, Debug)]
pub struct ChunkedWriterSummary {
    /// Total number of chunks created during write
    pub number_of_chunks_created: usize,
}

/// Writes [`RecordBatch`] into multiple chunks to a location. A location is a path like structure.
/// Internally the [`ChunkedWriter`] can subdivide the batches in multiple files.
///
/// When `max_chunk_size` is set and the internal buffer exceeds this size
/// after writing a batch, the current chunk is automatically finalized and
/// written to storage. A new chunk is started for subsequent writes.
pub struct ChunkedWriter<W> {
    /// The current active writer. If this is [`None`], a new writer will be
    /// initialized on the first call to [`ChunkedWriter::write`].
    ///
    /// When the chunk-size constraint is reached, a new writer will be created.
    writer: Option<ChunkWriter>,

    format: Format,

    /// Target of the write operation for this writer
    write_target: W,

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

    /// Maximum chunk size in bytes. When exceeded after writing a batch,
    /// the current chunk is finalized and a new one is started.
    /// `None` means no limit (current behavior preserved).
    max_chunk_size: Option<usize>,
}

impl<W> ChunkedWriter<W> {
    /// Creates a new [`ChunkedWriter`] that saves file to a given path on a given target writer
    pub fn new<F>(
        target: W,
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
            max_chunk_size: None,
        }
    }

    /// Sets the maximum chunk size in bytes.
    ///
    /// When the internal buffer exceeds this size after writing a batch,
    /// the current chunk is automatically finalized and written to storage,
    /// and a new chunk is started for subsequent writes.
    ///
    /// # Arguments
    /// * `size` - Maximum size in bytes. Pass `None` for unlimited (default).
    pub fn with_max_chunk_size(mut self, size: Option<usize>) -> Self {
        self.max_chunk_size = size;
        self
    }

    /// Sets a callback function that will be called every time a chunk is produced just before
    /// serialization.
    pub fn on_chunk_created<F1, Fut>(&mut self, clbk: F1)
    where
        F1: Fn(std::path::PathBuf, types::OntologyModelStats, ChunkMetadata) -> Fut
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
    }

    /// Writes a [`RecordBatch`] into the chunked writer.
    ///
    /// The [`ChunkedWriter`] will internally manage the creation of chunks
    /// based on the serialization format and the maximum chunk size (if any).
    /// To perform custom actions when a chunk is produced, use the
    /// [`on_chunk_produced`] method to set a callback function.
    pub async fn write<A>(&mut self, batch: &RecordBatch) -> Result<(), Error>
    where
        A: traits::AsyncWriteToPath,
        W: AsRef<A>,
    {
        trace!("AAAAAAAAAAA");
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
        .map_err(|e| Error::BlockingOperationError(e.to_string()))??;

        // Check if we should auto-finalize based on chunk size threshold
        if let Some(max) = self.max_chunk_size {
            let current_size = writer.memory_size();

            if current_size >= max {
                trace!(
                    "chunk size {} bytes exceeds max {} bytes, auto-finalizing chunk {}",
                    current_size, max, self.chunk_serialized_number
                );

                // Put the writer back for finalize() to consume it
                self.writer = Some(writer);
                self.finalize_chunk().await?;
                // After finalize(), self.writer is None, ready for next chunk
            } else {
                self.writer = Some(writer);
            }
        } else {
            self.writer = Some(writer);
        }

        Ok(())
    }

    /// Finalizes any pending reading, writing operation.
    ///
    /// It is important to call this method to ensure that an open chunk is properly finalized
    /// and written.
    pub async fn finalize<A>(mut self) -> Result<ChunkedWriterSummary, Error>
    where
        A: traits::AsyncWriteToPath,
        W: AsRef<A>,
    {
        self.finalize_chunk().await?;

        Ok(ChunkedWriterSummary {
            number_of_chunks_created: self.chunk_serialized_number,
        })
    }

    /// Finalize the writing process of a single chunk
    async fn finalize_chunk<A>(&mut self) -> Result<(), Error>
    where
        A: traits::AsyncWriteToPath,
        W: AsRef<A>,
    {
        // Calling this function will "consume" the current writer.
        // If another write_batch will be called after this function call
        // will cause the instantiation of another writer.
        if let Some(writer) = self.writer.take() {
            let path =
                (self.on_file_format)(&self.path, &writer.format, self.chunk_serialized_number);
            self.chunk_serialized_number += 1;

            // Offload CPU-intensive parquet finalization to blocking thread pool
            let (buffer, om_stats, metadata) =
                tokio::task::spawn_blocking(move || writer.finalize())
                    .await
                    .map_err(|e| Error::BlockingOperationError(e.to_string()))??;

            self.write_target
                .as_ref()
                .write_to_path(&path, buffer)
                .await?;

            trace!(
                "chunked writer callback: {}",
                self.on_chunk_created_clbk.is_some()
            );

            return self
                .on_chunk_created_clbk
                .as_ref()
                .map(async move |clbk| {
                    debug!("calling chunk serialization callback");
                    return clbk(path, om_stats, metadata).await;
                })
                .unwrap()
                .await
                .map_err(|e| Error::ChunkCreationCallbackError(e.to_string()));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, BinaryArray, Int64Array};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Mock store that counts write operations
    struct MockStore {
        write_count: Arc<AtomicUsize>,
    }

    impl MockStore {
        fn new() -> Self {
            Self {
                write_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn get_write_count(&self) -> usize {
            self.write_count.load(Ordering::SeqCst)
        }
    }

    impl traits::AsyncWriteToPath for MockStore {
        fn write_to_path(
            &self,
            _path: impl AsRef<std::path::Path>,
            _buf: impl Into<bytes::Bytes>,
        ) -> impl Future<Output = std::io::Result<()>> {
            self.write_count.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        }
    }

    /// Create a test batch with binary data to inflate the size
    fn create_test_batch_with_size(rows: usize, blob_size: usize) -> RecordBatch {
        use rand::Rng;

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", arrow_schema::DataType::Int64, false),
            Field::new("data", arrow_schema::DataType::Binary, false),
        ]));

        let timestamps: Vec<i64> = (0..rows as i64).collect();
        let timestamp_array: ArrayRef = Arc::new(Int64Array::from(timestamps));

        // Create random binary data (doesn't compress well, unlike zeros)
        let mut rng = rand::rng();
        let blobs: Vec<Vec<u8>> = (0..rows)
            .map(|_| (0..blob_size).map(|_| rng.random::<u8>()).collect())
            .collect();
        let blob_refs: Vec<&[u8]> = blobs.iter().map(|b| b.as_slice()).collect();
        let data_array: ArrayRef = Arc::new(BinaryArray::from(blob_refs));

        RecordBatch::try_new(schema, vec![timestamp_array, data_array])
            .expect("Failed to create test batch")
    }

    #[tokio::test]
    async fn test_auto_split_creates_multiple_chunks() {
        let store = Arc::new(MockStore::new());

        // Use a small max_chunk_size to trigger splitting
        let mut writer = ChunkedWriter::new(
            store.clone(),
            "test/path",
            Format::Default,
            |path, _, idx| path.join(format!("chunk_{}.parquet", idx)),
        )
        .with_max_chunk_size(Some(1024)); // 1 KiB threshold

        writer.on_chunk_created(|_, _, _| async { Ok(()) });

        // Write multiple batches with enough data to exceed threshold multiple times
        // Each batch ~500 bytes of binary data + overhead
        for _ in 0..10 {
            let batch = create_test_batch_with_size(5, 100);
            writer.write(&batch).await.expect("Write failed");
        }

        // Finalize to write any remaining data
        writer.finalize().await.expect("Finalize failed");

        // Should have created multiple chunks due to auto-split
        let chunk_count = store.get_write_count();
        assert!(
            chunk_count > 1,
            "Expected multiple chunks due to auto-split, got {}",
            chunk_count
        );
    }

    #[tokio::test]
    async fn test_no_split_when_under_threshold() {
        let store = Arc::new(MockStore::new());

        // Use a large max_chunk_size that won't be exceeded
        let mut writer =
            ChunkedWriter::new(&store, "test/path", Format::Default, |path, _, idx| {
                path.join(format!("chunk_{}.parquet", idx))
            })
            .with_max_chunk_size(Some(100 * 1024 * 1024)); // 100 MiB threshold

        writer.on_chunk_created(|_, _, _| async { Ok(()) });

        // Write small batches
        for _ in 0..5 {
            let batch = create_test_batch_with_size(3, 10);
            writer.write(&batch).await.expect("Write failed");
        }

        writer.finalize().await.expect("Finalize failed");

        // Should have created only one chunk
        assert_eq!(
            store.get_write_count(),
            1,
            "Expected single chunk when under threshold"
        );
    }

    #[tokio::test]
    async fn test_unlimited_creates_single_chunk() {
        let store = Arc::new(MockStore::new());

        // No max_chunk_size (unlimited)
        let mut writer =
            ChunkedWriter::new(&store, "test/path", Format::Default, |path, _, idx| {
                path.join(format!("chunk_{}.parquet", idx))
            })
            .with_max_chunk_size(None); // Unlimited

        writer.on_chunk_created(|_, _, _| async { Ok(()) });

        // Write many batches
        for _ in 0..20 {
            let batch = create_test_batch_with_size(10, 100);
            writer.write(&batch).await.expect("Write failed");
        }

        writer.finalize().await.expect("Finalize failed");

        // Should have created only one chunk (no auto-split)
        assert_eq!(
            store.get_write_count(),
            1,
            "Expected single chunk with unlimited size"
        );
    }

    #[tokio::test]
    async fn test_large_data_auto_split() {
        let store = Arc::new(MockStore::new());

        // 5 MiB threshold - large enough to fit multiple batches
        let max_chunk_size = 5 * 1024 * 1024;

        let mut writer =
            ChunkedWriter::new(&store, "test/path", Format::Default, |path, _, idx| {
                path.join(format!("chunk_{}.parquet", idx))
            })
            .with_max_chunk_size(Some(max_chunk_size));

        writer.on_chunk_created(|_, _, _| async { Ok(()) });

        // Write 10 batches of ~1 MiB each (10 rows * 100KB random blob)
        // Random data doesn't compress, so total ~10 MiB
        // With 5 MiB threshold, should create 2-4 chunks
        let blob_size = 100 * 1024; // 100 KiB per blob
        let rows_per_batch = 10;
        let num_batches = 10;

        for _ in 0..num_batches {
            let batch = create_test_batch_with_size(rows_per_batch, blob_size);
            writer.write(&batch).await.expect("Write failed");
        }

        writer.finalize().await.expect("Finalize failed");

        let chunk_count = store.get_write_count();

        // With ~1 MiB per batch (random data) and 5 MiB threshold:
        // Expect multiple chunks (exact count depends on Parquet overhead)
        assert!(
            chunk_count >= 2,
            "Expected at least 2 chunks for ~10 MiB data with 5 MiB threshold, got {}",
            chunk_count
        );

        println!(
            "Large data test: wrote ~{} MiB of random data in {} chunks (threshold: {} MiB)",
            (rows_per_batch * blob_size * num_batches) / (1024 * 1024),
            chunk_count,
            max_chunk_size / (1024 * 1024)
        );
    }

    #[tokio::test]
    async fn test_single_large_batch_exceeds_threshold() {
        let store = Arc::new(MockStore::new());

        // 512 KiB threshold
        let max_chunk_size = 512 * 1024;

        let mut writer =
            ChunkedWriter::new(&store, "test/path", Format::Default, |path, _, idx| {
                path.join(format!("chunk_{}.parquet", idx))
            })
            .with_max_chunk_size(Some(max_chunk_size));

        writer.on_chunk_created(|_, _, _| async { Ok(()) });

        // Write a single batch that exceeds the threshold (~1 MiB)
        // This tests that we handle the case where a single batch > max_chunk_size
        let batch = create_test_batch_with_size(10, 100 * 1024); // ~1 MiB
        writer.write(&batch).await.expect("Write failed");

        writer.finalize().await.expect("Finalize failed");

        // Should still create exactly 1 chunk (can't split a single batch)
        assert_eq!(
            store.get_write_count(),
            1,
            "Single batch exceeding threshold should still create 1 chunk"
        );
    }
}
