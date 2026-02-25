use crate as db;

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
    pub row_count: i64,
}

impl ChunkRecord {
    pub fn new(
        topic_id: i32,
        data_file: impl AsRef<std::path::Path>,
        size_bytes: i64,
        row_count: i64,
    ) -> Self {
        Self {
            chunk_id: db::UNREGISTERED,
            chunk_uuid: uuid::Uuid::new_v4(),
            topic_id,
            data_file: data_file.as_ref().to_string_lossy().to_string(),
            size_bytes,
            row_count,
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

    pub has_null: bool,
}

impl ColumnChunkTextualRecord {
    pub fn try_new(
        column_id: i32,
        chunk_id: i32,
        min_value: String,
        max_value: String,
        has_null: bool,
    ) -> Result<Self, db::Error> {
        Ok(Self {
            column_id,
            chunk_id,
            min_value,
            max_value,
            has_null,
        })
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

    pub has_null: bool,
    pub has_nan: bool,
}

impl ColumnChunkNumericRecord {
    pub fn new(
        column_id: i32,
        chunk_id: i32,
        min: f64,
        max: f64,
        has_null: bool,
        has_nan: bool,
    ) -> Self {
        Self {
            column_id,
            chunk_id,
            min_value: min,
            max_value: max,
            has_null,
            has_nan,
        }
    }
}
