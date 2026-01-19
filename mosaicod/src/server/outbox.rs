//! Background worker for processing the chunks outbox.
//!
//! This module implements the outbox pattern for reliable S3 uploads.
//! The worker polls the outbox table for pending entries and writes
//! the parquet data to S3, then marks entries as processed.

use std::sync::Arc;
use std::time::Duration;

use log::{debug, error, info, trace, warn};
use tokio::sync::Notify;

use crate::{params, repo, store};

/// Configuration for the outbox worker.
#[derive(Debug, Clone)]
pub struct OutboxConfig {
    /// How often to poll for pending entries (in milliseconds)
    pub poll_interval_ms: u64,
    /// Maximum number of entries to process per batch
    pub batch_size: i32,
    /// Maximum number of retries before giving up on an entry
    pub max_retries: i32,
    /// How old (in hours) processed entries must be before cleanup
    pub cleanup_after_hours: i32,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 100,
            batch_size: 50,
            max_retries: 5,
            cleanup_after_hours: 24,
        }
    }
}

impl OutboxConfig {
    /// Creates a config from environment variables with defaults.
    pub fn from_env() -> Self {
        let params = params::configurables();
        Self {
            poll_interval_ms: params.outbox_poll_interval_ms,
            batch_size: params.outbox_batch_size as i32,
            max_retries: params.outbox_max_retries as i32,
            cleanup_after_hours: 24,
        }
    }
}

/// Background worker that processes the chunks outbox.
pub struct OutboxWorker {
    store: store::StoreRef,
    repo: repo::Repository,
    shutdown: Arc<Notify>,
    config: OutboxConfig,
}

impl OutboxWorker {
    /// Creates a new outbox worker.
    pub fn new(
        store: store::StoreRef,
        repo: repo::Repository,
        shutdown: Arc<Notify>,
        config: OutboxConfig,
    ) -> Self {
        Self {
            store,
            repo,
            shutdown,
            config,
        }
    }

    /// Runs the outbox worker until shutdown is signaled.
    pub async fn run(&self) {
        info!(
            "outbox worker started (poll_interval={}ms, batch_size={}, max_retries={})",
            self.config.poll_interval_ms, self.config.batch_size, self.config.max_retries
        );

        let poll_interval = Duration::from_millis(self.config.poll_interval_ms);
        let mut cleanup_counter = 0u32;

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => {
                    info!("outbox worker received shutdown signal");
                    break;
                }
                _ = tokio::time::sleep(poll_interval) => {
                    if let Err(e) = self.process_batch().await {
                        error!("outbox processing error: {}", e);
                    }

                    // Periodic cleanup (every ~100 iterations)
                    cleanup_counter += 1;
                    if cleanup_counter >= 100 {
                        cleanup_counter = 0;
                        if let Err(e) = self.cleanup_processed().await {
                            warn!("outbox cleanup error: {}", e);
                        }
                    }
                }
            }
        }

        info!("outbox worker stopped");
    }

    /// Processes a batch of pending outbox entries.
    async fn process_batch(&self) -> Result<(), OutboxError> {
        let mut tx = self.repo.transaction().await?;

        // Fetch pending entries with row-level locking
        let entries =
            repo::outbox_fetch_pending(&mut tx, self.config.batch_size, self.config.max_retries)
                .await?;

        if entries.is_empty() {
            return Ok(());
        }

        trace!("processing {} outbox entries", entries.len());

        for entry in entries {
            match self.process_entry(&entry).await {
                Ok(()) => {
                    // Mark as processed
                    repo::outbox_mark_processed(&mut tx, entry.outbox_id).await?;
                    debug!(
                        "outbox entry {} processed: {}",
                        entry.outbox_id, entry.s3_path
                    );
                }
                Err(e) => {
                    // Record failure
                    let error_msg = e.to_string();
                    repo::outbox_mark_failed(&mut tx, entry.outbox_id, &error_msg).await?;
                    warn!(
                        "outbox entry {} failed (retry {}): {}",
                        entry.outbox_id,
                        entry.retry_count + 1,
                        error_msg
                    );
                }
            }
        }

        tx.commit().await?;
        Ok(())
    }

    /// Processes a single outbox entry by writing to S3.
    async fn process_entry(&self, entry: &repo::ChunkOutboxEntry) -> Result<(), OutboxError> {
        // Write parquet data to S3 (idempotent operation)
        self.store
            .write_bytes(&entry.s3_path, entry.parquet_data.clone())
            .await
            .map_err(|e| OutboxError::StoreWrite(e.to_string()))?;

        trace!(
            "wrote {} bytes to {}",
            entry.parquet_data.len(),
            entry.s3_path
        );
        Ok(())
    }

    /// Cleans up old processed entries.
    async fn cleanup_processed(&self) -> Result<(), OutboxError> {
        let mut cx = self.repo.connection();
        let deleted =
            repo::outbox_cleanup_processed(&mut cx, self.config.cleanup_after_hours).await?;

        if deleted > 0 {
            debug!("cleaned up {} processed outbox entries", deleted);
        }

        Ok(())
    }
}

/// Errors that can occur during outbox processing.
#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    #[error("repository error: {0}")]
    Repository(#[from] repo::Error),

    #[error("store write error: {0}")]
    StoreWrite(String),
}

#[cfg(test)]
mod tests {
    use crate::repo;
    use sqlx::Pool;

    /// Helper to create a chunk and return its ID for outbox testing.
    /// Uses internal repo functions that are available in test context.
    async fn create_test_chunk(repo: &repo::testing::Repository) -> i32 {
        // chunk_create and Chunk are already publicly exported
        // sequence_create, topic_create, SequenceRecord, TopicRecord are test-only exports
        use crate::repo::{
            Chunk, SequenceRecord, TopicRecord, chunk_create, sequence_create, topic_create,
        };

        let mut tx = repo.transaction().await.unwrap();

        // First create a sequence
        let seq = SequenceRecord::new("test-outbox-seq");
        let seq = sequence_create(&mut tx, &seq).await.unwrap();

        // Then create a topic
        let topic = TopicRecord::new("test-outbox-seq/test-topic", seq.sequence_id);
        let topic = topic_create(&mut tx, &topic).await.unwrap();

        // Finally create a chunk
        let chunk = Chunk::new(topic.topic_id, "test/path/data.parquet", 1000, 100);
        let chunk = chunk_create(&mut tx, &chunk).await.unwrap();

        tx.commit().await.unwrap();

        chunk.chunk_id
    }

    #[sqlx::test]
    async fn test_outbox_create_and_fetch(pool: Pool<repo::Database>) {
        let repo = repo::testing::Repository::new(pool);
        let chunk_id = create_test_chunk(&repo).await;

        let mut tx = repo.transaction().await.unwrap();

        // Create outbox entry
        let entry = repo::ChunkOutboxEntry::new(chunk_id, "test/path.parquet", vec![1, 2, 3, 4]);
        let created = repo::outbox_create(&mut tx, &entry).await.unwrap();

        assert_eq!(created.chunk_id, chunk_id);
        assert_eq!(created.s3_path, "test/path.parquet");
        assert_eq!(created.parquet_data, vec![1, 2, 3, 4]);
        assert_eq!(created.retry_count, 0);
        assert!(created.processed_at.is_none());

        tx.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn test_outbox_fetch_respects_max_retries(pool: Pool<repo::Database>) {
        let repo = repo::testing::Repository::new(pool);
        let chunk_id = create_test_chunk(&repo).await;

        let mut tx = repo.transaction().await.unwrap();

        // Create outbox entry
        let entry = repo::ChunkOutboxEntry::new(chunk_id, "test/path.parquet", vec![1, 2, 3]);
        let created = repo::outbox_create(&mut tx, &entry).await.unwrap();

        // Simulate failures up to max_retries
        let max_retries = 3;
        for i in 0..max_retries {
            repo::outbox_mark_failed(&mut tx, created.outbox_id, &format!("error {}", i))
                .await
                .unwrap();
        }

        tx.commit().await.unwrap();

        // Fetch with max_retries=3 should NOT return the entry (retry_count >= max_retries)
        let mut tx2 = repo.transaction().await.unwrap();
        let entries = repo::outbox_fetch_pending(&mut tx2, 10, max_retries)
            .await
            .unwrap();

        assert!(
            entries.is_empty(),
            "Entry with retry_count >= max_retries should not be fetched"
        );
    }

    #[sqlx::test]
    async fn test_outbox_fetch_returns_entries_under_max_retries(pool: Pool<repo::Database>) {
        let repo = repo::testing::Repository::new(pool);
        let chunk_id = create_test_chunk(&repo).await;

        let mut tx = repo.transaction().await.unwrap();

        // Create outbox entry
        let entry = repo::ChunkOutboxEntry::new(chunk_id, "test/path.parquet", vec![1, 2, 3]);
        let created = repo::outbox_create(&mut tx, &entry).await.unwrap();

        // Simulate 2 failures (under max_retries=3)
        repo::outbox_mark_failed(&mut tx, created.outbox_id, "error 1")
            .await
            .unwrap();
        repo::outbox_mark_failed(&mut tx, created.outbox_id, "error 2")
            .await
            .unwrap();

        tx.commit().await.unwrap();

        // Fetch with max_retries=3 SHOULD return the entry (retry_count=2 < 3)
        let mut tx2 = repo.transaction().await.unwrap();
        let entries = repo::outbox_fetch_pending(&mut tx2, 10, 3).await.unwrap();

        assert_eq!(
            entries.len(),
            1,
            "Entry with retry_count < max_retries should be fetched"
        );
        assert_eq!(entries[0].retry_count, 2);
    }

    #[sqlx::test]
    async fn test_outbox_mark_processed_clears_data(pool: Pool<repo::Database>) {
        let repo = repo::testing::Repository::new(pool);
        let chunk_id = create_test_chunk(&repo).await;

        let mut tx = repo.transaction().await.unwrap();

        // Create outbox entry with data
        let entry = repo::ChunkOutboxEntry::new(chunk_id, "test/path.parquet", vec![1, 2, 3, 4, 5]);
        let created = repo::outbox_create(&mut tx, &entry).await.unwrap();

        // Mark as processed
        repo::outbox_mark_processed(&mut tx, created.outbox_id)
            .await
            .unwrap();

        tx.commit().await.unwrap();

        // Verify entry is no longer fetched (processed_at is set)
        let mut tx2 = repo.transaction().await.unwrap();
        let entries = repo::outbox_fetch_pending(&mut tx2, 10, 5).await.unwrap();

        assert!(entries.is_empty(), "Processed entry should not be fetched");
    }

    #[sqlx::test]
    async fn test_outbox_empty_returns_early(pool: Pool<repo::Database>) {
        let repo = repo::testing::Repository::new(pool);
        let mut tx = repo.transaction().await.unwrap();

        // Fetch from empty outbox
        let entries = repo::outbox_fetch_pending(&mut tx, 10, 5).await.unwrap();

        assert!(entries.is_empty(), "Empty outbox should return empty vec");
    }

    #[sqlx::test]
    async fn test_outbox_batch_size_limit(pool: Pool<repo::Database>) {
        let repo = repo::testing::Repository::new(pool);
        let chunk_id = create_test_chunk(&repo).await;

        let mut tx = repo.transaction().await.unwrap();

        // Create 5 outbox entries
        for i in 0..5 {
            let entry =
                repo::ChunkOutboxEntry::new(chunk_id, format!("test/path{}.parquet", i), vec![i]);
            repo::outbox_create(&mut tx, &entry).await.unwrap();
        }

        tx.commit().await.unwrap();

        // Fetch with batch_size=2 should return only 2 entries
        let mut tx2 = repo.transaction().await.unwrap();
        let entries = repo::outbox_fetch_pending(&mut tx2, 2, 5).await.unwrap();

        assert_eq!(entries.len(), 2, "Should respect batch_size limit");
    }

    #[sqlx::test]
    async fn test_outbox_pending_count(pool: Pool<repo::Database>) {
        let repo = repo::testing::Repository::new(pool);

        // Initially empty
        let mut cx = repo.connection();
        let count = repo::outbox_count_pending(&mut cx).await.unwrap();
        assert_eq!(count, 0);

        let chunk_id = create_test_chunk(&repo).await;

        let mut tx = repo.transaction().await.unwrap();

        // Create 3 entries
        for i in 0..3 {
            let entry =
                repo::ChunkOutboxEntry::new(chunk_id, format!("test/path{}.parquet", i), vec![i]);
            repo::outbox_create(&mut tx, &entry).await.unwrap();
        }

        let count = repo::outbox_count_pending(&mut tx).await.unwrap();
        assert_eq!(count, 3);

        tx.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn test_outbox_error_message_recorded(pool: Pool<repo::Database>) {
        let repo = repo::testing::Repository::new(pool);
        let chunk_id = create_test_chunk(&repo).await;

        let mut tx = repo.transaction().await.unwrap();

        let entry = repo::ChunkOutboxEntry::new(chunk_id, "test/path.parquet", vec![1, 2, 3]);
        let created = repo::outbox_create(&mut tx, &entry).await.unwrap();

        // Mark as failed with specific error
        repo::outbox_mark_failed(&mut tx, created.outbox_id, "S3 connection timeout")
            .await
            .unwrap();

        tx.commit().await.unwrap();

        // Fetch and verify error message
        let mut tx2 = repo.transaction().await.unwrap();
        let entries = repo::outbox_fetch_pending(&mut tx2, 10, 5).await.unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].error_message.as_deref(),
            Some("S3 connection timeout")
        );
        assert_eq!(entries[0].retry_count, 1);
    }
}
