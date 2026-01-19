-- Outbox table for transactional chunk upload to S3
-- This table temporarily stores parquet data until it's successfully written to S3

CREATE TABLE chunks_outbox (
    outbox_id SERIAL PRIMARY KEY,
    -- Reference to the chunk being uploaded (inserted in same transaction)
    chunk_id INTEGER NOT NULL REFERENCES chunk_t(chunk_id) ON DELETE CASCADE,
    -- S3 path where the parquet file should be written
    s3_path TEXT NOT NULL,
    -- Parquet data bytes (temporary storage until S3 write succeeds)
    -- NULL after processing to free storage
    parquet_data BYTEA,
    -- Timestamps for tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    -- Error tracking for failed attempts
    error_message TEXT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0
);

-- Index for efficient polling of pending entries
CREATE INDEX idx_chunks_outbox_pending ON chunks_outbox (created_at)
    WHERE processed_at IS NULL;

-- Index for cleanup of processed entries
CREATE INDEX idx_chunks_outbox_processed ON chunks_outbox (processed_at)
    WHERE processed_at IS NOT NULL;
