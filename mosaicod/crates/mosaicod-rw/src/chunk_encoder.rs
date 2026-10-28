use super::{Error, writer::Writer};
use arrow::{array::RecordBatch, datatypes::Schema, datatypes::SchemaRef};
use mosaicod_core::types;
use mosaicod_ext;
use std::sync::Arc;

/// Metadata about a finalized chunk, including size and row count.
#[derive(Debug, Clone)]
pub struct ChunkMetadata {
    pub size_bytes: usize,
    /// Uncompressed in-memory Arrow footprint of the rows written to this chunk, as
    /// opposed to [`Self::size_bytes`], which measures the encoded, compressed buffer.
    pub arrow_size_bytes: usize,
    pub row_count: usize,
}

/// The [`InMemoryChunkEncoder`] is used to encode [`RecordBatch`] instances into a single in-memory block,
/// supporting multiple serialization formats. It encapsulates the underlying writer and manages the encoding
/// process based on the specified format.
///
/// During the writing process, it also collects statistics about the data being serialized, which can be
/// accessed via the [`ChunkBlockEncoder::statistics`] method.
pub struct InMemoryChunkEncoder {
    pub format: types::Format,
    writer: Writer,
    stats: types::OntologyModelStats,
    schema: SchemaRef,
    row_count: usize,
    arrow_size_bytes: usize,
}

impl InMemoryChunkEncoder {
    /// Creates a new [`InMemoryChunkEncoder`] configured for a specific serialization format.
    ///
    /// This fallible constructor initializes an appropriate underlying writer
    /// based on the provided `format`.
    pub fn try_new(schema: Arc<Schema>, format: types::Format) -> Result<Self, Error> {
        Ok(InMemoryChunkEncoder {
            writer: Writer::new(&schema, format)?,
            format,
            stats: mosaicod_ext::arrow::ontology_model_stats_from_schema(&schema),
            schema,
            row_count: 0,
            arrow_size_bytes: 0,
        })
    }

    /// Writes the provided [`RecordBatch`].
    ///
    /// The [`RecordBatch`] is serialized according to the writer's format, and the internal statistics
    /// are updated based on the data in the batch. The method returns an error if the serialization fails
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), Error> {
        match &mut self.writer {
            Writer::Parquet(writer) => {
                mosaicod_ext::arrow::ontology_model_stats_inspect_record_batch(
                    &mut self.stats,
                    batch,
                )?;

                writer.write(batch)?;
            }
        }
        self.row_count += batch.num_rows();
        self.arrow_size_bytes += batch.get_array_memory_size();
        Ok(())
    }

    /// Returns a reference to the current statistics of the serialized data.
    pub fn statistics(&self) -> &types::OntologyModelStats {
        &self.stats
    }

    pub fn take_statistics(&mut self) -> types::OntologyModelStats {
        std::mem::replace(
            &mut self.stats,
            mosaicod_ext::arrow::ontology_model_stats_from_schema(&self.schema),
        )
    }

    /// Returns a mutable reference to the internal buffer containing the serialized data.
    pub fn buffer_mut(&mut self) -> &mut Vec<u8> {
        match &mut self.writer {
            Writer::Parquet(writer) => writer.inner_mut(),
        }
    }

    /// Returns a reference to the internal buffer containing the serialized data.
    pub fn buffer(&self) -> &Vec<u8> {
        match &self.writer {
            Writer::Parquet(writer) => writer.inner(),
        }
    }

    pub fn memory_size(&self) -> usize {
        match &self.writer {
            Writer::Parquet(writer) => writer.memory_size(),
        }
    }

    pub fn bytes_written(&self) -> usize {
        match &self.writer {
            Writer::Parquet(writer) => writer.bytes_written(),
        }
    }

    pub fn in_progress_size(&self) -> usize {
        match &self.writer {
            Writer::Parquet(writer) => writer.in_progress_size(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Finalizes the writer, ensuring all buffered data and metadata are written to the file.
    ///
    /// This method must be called to complete the writing process. It consumes the writer object,
    /// preventing any further writes.
    ///
    /// Returns the serialized buffer, column statistics, and chunk metadata (size and row count).
    pub fn finalize(self) -> Result<(Vec<u8>, types::OntologyModelStats, ChunkMetadata), Error> {
        // We are calling `finish` since the implementation is the same as
        // close but takes no ownership of the writer. And we return the internal data buffer.
        let row_count = self.row_count;
        let arrow_size_bytes = self.arrow_size_bytes;
        let buffer = match self.writer {
            Writer::Parquet(w) => w.buffer()?,
        };
        let metadata = ChunkMetadata {
            size_bytes: buffer.len(),
            arrow_size_bytes,
            row_count,
        };
        Ok((buffer, self.stats, metadata))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray, StructArray,
    };
    use arrow::datatypes::Field;
    use arrow_schema::DataType;
    use mosaicod_config::params as config_params;
    use mosaicod_core::constants;
    use parquet::basic::{Compression, Encoding};
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::*;

    /// Helper function to create a test RecordBatch.
    /// This provides a schema with numeric, string, and nested struct types,
    /// along with nulls and NaN values to test the statistics generation.
    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
            Field::new("is_braking", DataType::Boolean, false),
            Field::new(
                "pose",
                DataType::Struct(
                    vec![
                        Field::new("x", DataType::Float64, false),
                        Field::new("y", DataType::Float64, false),
                    ]
                    .into(),
                ),
                false,
            ),
            Field::new("image", DataType::Binary, false),
        ]));

        // Create arrays for each column
        let timestamp_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let label_array: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")]));
        let image_array: ArrayRef = Arc::new(BinaryArray::from(vec![
            b"BLOB-01" as &[u8],
            b"BLOB-02",
            b"BLOB-03",
        ]));

        let braking_array: ArrayRef = Arc::new(BooleanArray::from(vec![true, true, false]));
        let x_array: ArrayRef = Arc::new(Float64Array::from(vec![0.1, 0.2, 0.3]));
        let y_array: ArrayRef = Arc::new(Float64Array::from(vec![1.1, 1.2, 1.3]));
        let pose_array: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("x", DataType::Float64, false)), x_array),
            (Arc::new(Field::new("y", DataType::Float64, false)), y_array),
        ]));

        RecordBatch::try_new(
            schema.clone(),
            vec![
                timestamp_array,
                label_array,
                braking_array,
                pose_array,
                image_array,
            ],
        )
        .expect("Fail during record batch creation")
    }

    #[test]
    fn chunk_writer_statistics() {
        config_params::load_params(config_params::ParamsLoadOptions::testing()).unwrap();

        let batch = create_test_batch();
        let schema = batch.schema();

        let mut writer = InMemoryChunkEncoder::try_new(schema, types::Format::Default)
            .expect("Failed to create ChunkWriter");

        writer.write(&batch).expect("Failed to write batch");
        let cstats = writer.statistics();

        dbg!(cstats);

        // The schema should be flattened into 5 fields: timestamp, label, is_braking, pose.x, pose.y, image
        assert_eq!(cstats.cols.len(), 6);

        // Check stats for "timestamp"
        if let Some(types::Stats::Numeric(s)) = cstats.cols.get("timestamp") {
            assert_eq!(s.min, 1.0);
            assert_eq!(s.max, 3.0);
            assert!(!s.has_null);
            assert!(!s.has_nan);
        } else {
            panic!("Missing or incorrect type for `timestamp` stats");
        }

        // Check  stats for "is_braking"
        if let Some(types::Stats::Numeric(v)) = cstats.cols.get("is_braking") {
            assert_eq!(v.min, 0.0);
            assert_eq!(v.max, 1.0);
            assert!(!v.has_null);
            assert!(!v.has_nan);
        } else {
            panic!("Missing or incorrect type for `is_braking` stats");
        }

        // Check stats for "label", and check that the computed bloom filter has
        // all the values in the set
        if let Some(types::Stats::Textual(s)) = cstats.cols.get("label") {
            assert_eq!(s.min.as_deref(), Some("a"));
            assert_eq!(s.max.as_deref(), Some("c"));

            assert!(s.has_null);
        } else {
            panic!("Missing or incorrect type for label stats");
        }

        // Check stats for "pose.x"
        if let Some(types::Stats::Numeric(s)) = cstats.cols.get("pose.x") {
            assert!((s.min - 0.1).abs() < constants::EPSILON);
            assert!((s.max - 0.3).abs() < constants::EPSILON);
            assert!(!s.has_null);
            assert!(!s.has_nan);
        } else {
            panic!("Missing or incorrect type for pose.x stats");
        }

        // Check stats for "pose.y"
        if let Some(types::Stats::Numeric(s)) = cstats.cols.get("pose.y") {
            assert!((s.min - 1.1).abs() < constants::EPSILON);
            assert!((s.max - 1.3).abs() < constants::EPSILON);
            assert!(!s.has_null);
            assert!(!s.has_nan);
        } else {
            panic!("Missing or incorrect type for pose.y stats");
        }

        // Check stats for "image", since this column holds binary data no statistics
        // needs to be computed
        assert_eq!(cstats.cols.get("image"), Some(&types::Stats::Unsupported));

        // Finalize the writer (optional in test, but good practice)
        let (buffer, _, metadata) = writer.finalize().expect("Failed to finalize writer");

        // Ensure that buffer is not empty
        dbg!(buffer.len());
        assert!(!buffer.is_empty());

        // Check metadata
        assert_eq!(metadata.row_count, 3);
        assert_eq!(metadata.size_bytes, buffer.len());

        assert_eq!(metadata.arrow_size_bytes, batch.get_array_memory_size());
    }

    #[test]
    fn image_sequence_column_encoding() {
        config_params::load_params(config_params::ParamsLoadOptions::testing()).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
                DataType::Int64,
                false,
            ),
            Field::new("format", DataType::Utf8, false),
            Field::new("capture_timestamp_ns", DataType::Int64, false),
            Field::new("data", DataType::Binary, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_100, 1_200, 1_300])),
                Arc::new(StringArray::from(vec!["png", "png", "png"])),
                Arc::new(Int64Array::from(vec![1_000, 1_033, 1_066])),
                Arc::new(BinaryArray::from(vec![
                    b"\x89PNG first".as_ref(),
                    b"\x89PNG second".as_ref(),
                    b"\x89PNG third".as_ref(),
                ])),
            ],
        )
        .unwrap();

        let mut encoder = InMemoryChunkEncoder::try_new(schema, types::Format::ImageSequence)
            .expect("Failed to create encoder");
        encoder.write(&batch).expect("Failed to write batch");
        let (buffer, _, _) = encoder.finalize().expect("Failed to finalize");

        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        let row_group = reader.metadata().row_group(0);
        let column = |name: &str| {
            row_group
                .columns()
                .iter()
                .find(|c| c.column_path().string() == name)
                .unwrap()
        };

        let ts = column(constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP);
        assert_eq!(ts.compression(), Compression::UNCOMPRESSED);
        assert!(ts.statistics().is_some());
        assert!(ts.bloom_filter_offset().is_some());

        let capture = column("capture_timestamp_ns");
        assert!(matches!(capture.compression(), Compression::ZSTD(_)));
        let stats = capture
            .statistics()
            .expect("capture_timestamp_ns statistics");
        assert_eq!(stats.min_bytes_opt(), Some(1_000i64.to_le_bytes().as_ref()));
        assert_eq!(stats.max_bytes_opt(), Some(1_066i64.to_le_bytes().as_ref()));

        let format = column("format");
        assert!(matches!(format.compression(), Compression::ZSTD(_)));
        assert!(format.encodings().any(|e| e == Encoding::RLE_DICTIONARY));
        let stats = format.statistics().expect("format statistics");
        assert_eq!(stats.min_bytes_opt(), Some(b"png".as_ref()));
        assert_eq!(stats.max_bytes_opt(), Some(b"png".as_ref()));

        let data = column("data");
        assert!(matches!(data.compression(), Compression::ZSTD(_)));
        assert!(data.statistics().is_none());
        assert!(data.encodings().all(|e| e != Encoding::RLE_DICTIONARY));
    }
}
