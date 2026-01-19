use chrono::{DateTime, Utc};

/// Entry in the chunks outbox table.
///
/// Represents a pending chunk upload to S3. The parquet data is temporarily
/// stored here until the background worker successfully writes it to S3.
#[derive(Debug)]
pub struct ChunkOutboxEntry {
    pub outbox_id: i32,
    pub chunk_id: i32,
    pub s3_path: String,
    pub parquet_data: Vec<u8>,
    pub created_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub retry_count: i32,
}

impl ChunkOutboxEntry {
    /// Creates a new outbox entry for a chunk upload.
    pub fn new(chunk_id: i32, s3_path: impl AsRef<std::path::Path>, parquet_data: Vec<u8>) -> Self {
        Self {
            outbox_id: crate::repo::UNREGISTERED,
            chunk_id,
            s3_path: s3_path.as_ref().to_string_lossy().to_string(),
            parquet_data,
            created_at: Utc::now(),
            processed_at: None,
            error_message: None,
            retry_count: 0,
        }
    }
}
