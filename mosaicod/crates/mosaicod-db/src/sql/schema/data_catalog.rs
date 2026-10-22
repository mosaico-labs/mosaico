use crate as db;
use mosaicod_core::types;

#[derive(Debug)]
pub struct ColumnRecord {
    pub column_id: i32,
    pub column_name: String,
    pub ontology_tag: String,
}

impl ColumnRecord {
    pub fn new(ontology_tag: String, name: String) -> Self {
        Self {
            column_id: db::UNREGISTERED,
            column_name: name,
            ontology_tag,
        }
    }
}

/// Chunk of data associated with a topic.
///
/// Each chunk represents a partition of data for a given topic.
/// Chunks are entity indexed by the platform for fast retrieval and storage.
#[derive(Debug)]
pub struct ChunkRecord {
    pub chunk_id: i32,
    pub chunk_uuid: uuid::Uuid,
    pub topic_id: i32,
    pub(crate) data_file: String,
    pub size_bytes: i64,
    /// Uncompressed in-memory Arrow footprint of the chunk, as opposed to
    /// [`Self::size_bytes`], which is the size of the compressed Parquet buffer.
    pub arrow_size_bytes: Option<i64>,
    pub row_count: i64,

    pub first_sample_timestamp_ns: i64,
    pub last_sample_timestamp_ns: i64,
}

impl ChunkRecord {
    pub fn new(
        topic_id: i32,
        data_file: impl AsRef<std::path::Path>,
        size_bytes: i64,
        arrow_size_bytes: i64,
        row_count: i64,
        first_sample_timestamp_ns: i64,
        last_sample_timestamp_ns: i64,
    ) -> Self {
        Self {
            chunk_id: db::UNREGISTERED,
            chunk_uuid: uuid::Uuid::new_v4(),
            topic_id,
            data_file: data_file.as_ref().to_string_lossy().to_string(),
            size_bytes,
            arrow_size_bytes: Some(arrow_size_bytes),
            row_count,
            first_sample_timestamp_ns,
            last_sample_timestamp_ns,
        }
    }

    pub fn data_file(&self) -> &std::path::Path {
        std::path::Path::new(&self.data_file)
    }
}

/// Chunk of textual data associated with a column.
#[derive(Debug)]
pub struct ColumnChunkTextualRecord {
    pub column_id: i32,
    pub chunk_id: i32,

    /// Min value (in lexycographic sense) in the chunk
    pub min_value: String,
    /// Max value (in lexycographic sense) in the chunk
    pub max_value: String,

    pub null_count: i64,

    pub first_sample_value: Option<String>,
    pub last_sample_value: Option<String>,

    /// The stats are computed on the elements of a list column (temporary)
    pub is_list: bool,
}

impl ColumnChunkTextualRecord {
    pub fn new(column_id: i32, chunk_id: i32, stats: types::TextualStats, is_list: bool) -> Self {
        Self {
            column_id,
            chunk_id,
            min_value: stats.min.unwrap_or_default(),
            max_value: stats.max.unwrap_or_default(),
            null_count: stats.null_count,
            first_sample_value: stats.first_sample_value,
            last_sample_value: stats.last_sample_value,
            is_list,
        }
    }
}

/// Chunk of textual data associated with a column.
#[derive(Debug)]
pub struct ColumnChunkNumericRecord {
    pub column_id: i32,
    pub chunk_id: i32,

    /// Min value (in lexycographic sense) in the chunk
    pub min_value: f64,
    /// Max value (in lexycographic sense) in the chunk
    pub max_value: f64,

    pub nan_count: i64,
    pub null_count: i64,

    pub first_sample_value: Option<f64>,
    pub last_sample_value: Option<f64>,

    pub mean_value: Option<f64>,
    pub m2_value: Option<f64>,

    /// The stats are computed on the elements of a list column (temporary)
    pub is_list: bool,
}

impl ColumnChunkNumericRecord {
    pub fn new(column_id: i32, chunk_id: i32, stats: &types::NumericStats, is_list: bool) -> Self {
        Self {
            column_id,
            chunk_id,
            min_value: stats.min,
            max_value: stats.max,
            nan_count: stats.nan_count,
            null_count: stats.null_count,
            first_sample_value: stats.first_sample_value,
            last_sample_value: stats.last_sample_value,
            mean_value: stats.mean_value,
            m2_value: stats.m2_value,
            is_list,
        }
    }
}
