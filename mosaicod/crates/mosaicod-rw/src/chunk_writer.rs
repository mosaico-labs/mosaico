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

    /// Format of the images already written, used to impose that an image
    /// sequence holds images of a single format.
    image_format: Option<String>,
}

impl<W> ChunkWriter<W> {
    /// Creates a new [`ChunkWriter`].
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
            image_format: None,
        }
    }

    /// Checks that `batch` holds images of the same format as the ones
    /// already written.
    fn check_image_format(&mut self, batch: &RecordBatch) -> Result<(), Error> {
        let Some(found) = ImageSequenceFormatProperties::image_format(batch)? else {
            return Ok(());
        };

        match &self.image_format {
            Some(expected) if *expected != found => Err(Error::MixedImageSequenceFormats {
                expected: expected.clone(),
                found,
            }),
            Some(_) => Ok(()),
            None => {
                self.image_format = Some(found);
                Ok(())
            }
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
        if self.format == types::Format::ImageSequence {
            self.check_image_format(&batch)?;
        }

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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::{BinaryArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use mosaicod_config::params as config_params;
    use mosaicod_core::constants;

    use super::*;

    /// Write target keeping the paths of the stored chunks in memory insteadù
    /// of writing something in the storage.
    #[derive(Default)]
    struct MemoryTarget {
        paths: Mutex<Vec<std::path::PathBuf>>,
    }

    impl traits::AsyncWriteToPath for MemoryTarget {
        async fn write_to_path(
            &self,
            path: impl AsRef<std::path::Path>,
            _buf: impl Into<bytes::Bytes>,
        ) -> std::io::Result<()> {
            self.paths
                .lock()
                .map_err(|e| std::io::Error::other(e.to_string()))?
                .push(path.as_ref().to_path_buf());
            Ok(())
        }
    }

    fn image_sequence_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
                DataType::Int64,
                false,
            ),
            Field::new("format", DataType::Utf8, false),
            Field::new("capture_timestamp_ns", DataType::Int64, false),
            Field::new("data", DataType::Binary, false),
        ]))
    }

    fn image_sequence_batch(formats: Vec<&str>) -> RecordBatch {
        let timestamps = vec![0; formats.len()];
        let images = vec![[0u8].as_slice(); formats.len()];
        RecordBatch::try_new(
            image_sequence_schema(),
            vec![
                Arc::new(Int64Array::from(timestamps.clone())),
                Arc::new(StringArray::from(formats)),
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(BinaryArray::from(images)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn image_sequence_rejects_image_format_change_across_batches() {
        config_params::load_params(config_params::ParamsLoadOptions::testing()).unwrap();

        let target = Arc::new(MemoryTarget::default());
        let mut writer = ChunkWriter::new(
            target.clone(),
            types::Format::ImageSequence,
            image_sequence_schema(),
            |chunk_number| std::path::PathBuf::from(chunk_number.to_string()),
        );

        writer
            .write::<MemoryTarget>(image_sequence_batch(vec!["png", "png"]))
            .await
            .unwrap();
        writer
            .write::<MemoryTarget>(image_sequence_batch(vec![]))
            .await
            .unwrap();
        writer
            .write::<MemoryTarget>(image_sequence_batch(vec!["png"]))
            .await
            .unwrap();

        assert!(matches!(
            writer
                .write::<MemoryTarget>(image_sequence_batch(vec!["tiff"]))
                .await,
            Err(Error::MixedImageSequenceFormats { expected, found }) if expected == "png" && found == "tiff"
        ));
        // The rejected batch is not stored
        assert_eq!(target.paths.lock().unwrap().len(), 3);
    }
}
