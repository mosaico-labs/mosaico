use super::{Error, Format, writer::Writer};
use crate::types;
use arrow::{array::RecordBatch, datatypes::Schema, datatypes::SchemaRef};
use std::sync::Arc;

/// Metadata about a finalized chunk, including size and row count.
#[derive(Debug, Clone)]
pub struct ChunkMetadata {
    pub size_bytes: usize,
    pub row_count: usize,
}

/// The [`ChunkWriter`] is used to serialize [`RecordBatch`] instances into a single memory chunk,
/// supporting multiple serialization formats. It encapsulates the underlying writer and manages the serialization
/// process based on the specified format.
///
/// During the writing process, it also collects statistics about the data being serialized, which can be
/// accessed via the [`ChunkStats::statistics`] method.
pub struct ChunkWriter {
    pub format: Format,
    writer: Writer,
    stats: types::ColumnsStats,
    schema: SchemaRef,
    row_count: usize,
}

impl ChunkWriter {
    /// Creates a new [`ChunkWriter`] configured for a specific serialization format.
    ///
    /// This fallible constructor initializes an appropriate underlying writer
    /// based on the provided `format`.
    pub fn try_new(schema: Arc<Schema>, format: Format) -> Result<Self, Error> {
        Ok(ChunkWriter {
            writer: Writer::new(&schema, format)?,
            format,
            stats: crate::arrow::column_stats_from_schema(&schema),
            schema,
            row_count: 0,
        })
    }

    /// Wrties the provided [`RecordBatch`].
    ///
    /// The `RecordBatch` is serialized according to the writer's format, and the internal statistics
    /// are updated based on the data in the batch. The method returns an error if the serialization fails
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), Error> {
        match &mut self.writer {
            Writer::Parquet(writer) => {
                crate::arrow::column_stats_inspect_record_batch(&mut self.stats, batch)?;
                writer.write(batch)?;
            }
        }
        self.row_count += batch.num_rows();
        Ok(())
    }

    /// Returns a reference to the current statistics of the serialized data.
    pub fn statistics(&self) -> &types::ColumnsStats {
        &self.stats
    }

    pub fn take_statistics(&mut self) -> types::ColumnsStats {
        std::mem::replace(
            &mut self.stats,
            crate::arrow::column_stats_from_schema(&self.schema),
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

    /// Finalizes the writer, ensuring all buffered data and metadata are written to the file.
    ///
    /// This method must be called to complete the writing process. It consumes the writer object,
    /// preventing any further writes.
    ///
    /// Returns the serialized buffer, column statistics, and chunk metadata (size and row count).
    pub fn finalize(self) -> Result<(Vec<u8>, types::ColumnsStats, ChunkMetadata), Error> {
        // We are calling `finish`` since the implementation is the same as
        // close but takes no ownership of the writer. And we return the internal data buffer.
        let row_count = self.row_count;
        let buffer = match self.writer {
            Writer::Parquet(w) => w.into_inner()?,
        };
        let metadata = ChunkMetadata {
            size_bytes: buffer.len(),
            row_count,
        };
        Ok((buffer, self.stats, metadata))
    }
}

#[cfg(test)]
mod tests {
    use crate::{params, types};

    use super::*;
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray, StructArray,
    };
    use arrow::datatypes::Field;
    use arrow_schema::DataType;

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
        let batch = create_test_batch();
        let schema = batch.schema();

        let mut writer =
            ChunkWriter::try_new(schema, Format::Default).expect("Failed to create ChunkWriter");
        writer.write(&batch).expect("Failed to write batch");
        let cstats = writer.statistics();

        dbg!(cstats);

        // The schema should be flattened into 5 fields: timestamp, label, is_braking, pose.x, pose.y, image
        assert_eq!(cstats.stats.len(), 6);

        // Check stats for "timestamp"
        if let Some(types::Stats::Numeric(s)) = cstats.stats.get("timestamp") {
            assert_eq!(s.min, 1.0);
            assert_eq!(s.max, 3.0);
            assert!(!s.has_null);
            assert!(!s.has_nan);
        } else {
            panic!("Missing or incorrect type for `timestamp` stats");
        }

        // Check  stats for "is_braking"
        if let Some(types::Stats::Numeric(v)) = cstats.stats.get("is_braking") {
            assert_eq!(v.min, 0.0);
            assert_eq!(v.max, 1.0);
            assert!(!v.has_null);
            assert!(!v.has_nan);
        } else {
            panic!("Missing or incorrect type for `is_braking` stats");
        }

        // Check stats for "label", and check that the computed bloom filter has
        // all the values in the set
        if let Some(types::Stats::Text(s)) = cstats.stats.get("label") {
            assert_eq!(s.min, "a");
            assert_eq!(s.max, "c");
            assert!(s.has_null);
        } else {
            panic!("Missing or incorrect type for label stats");
        }

        // Check stats for "pose.x"
        if let Some(types::Stats::Numeric(s)) = cstats.stats.get("pose.x") {
            assert!((s.min - 0.1).abs() < params::EPSILON);
            assert!((s.max - 0.3).abs() < params::EPSILON);
            assert!(!s.has_null);
            assert!(!s.has_nan);
        } else {
            panic!("Missing or incorrect type for pose.x stats");
        }

        // Check stats for "pose.y"
        if let Some(types::Stats::Numeric(s)) = cstats.stats.get("pose.y") {
            assert!((s.min - 1.1).abs() < params::EPSILON);
            assert!((s.max - 1.3).abs() < params::EPSILON);
            assert!(!s.has_null);
            assert!(!s.has_nan);
        } else {
            panic!("Missing or incorrect type for pose.y stats");
        }

        // Check stats for "image", since this column holds binary data no statistics
        // needs to be computed
        assert_eq!(cstats.stats.get("image"), Some(&types::Stats::Unsupported));

        // Finalize the writer (optional in test, but good practice)
        let (buffer, _, metadata) = writer.finalize().expect("Failed to finalize writer");

        // Ensure that buffer is not empty
        dbg!(buffer.len());
        assert!(!buffer.is_empty());

        // Check metadata
        assert_eq!(metadata.row_count, 3);
        assert_eq!(metadata.size_bytes, buffer.len());
    }
}
