-- Add size and row count metadata to chunks for optimal batch size calculation
-- This avoids O(N) HEAD requests to object storage when reading topics

ALTER TABLE chunk_t ADD COLUMN size_bytes BIGINT NOT NULL;
ALTER TABLE chunk_t ADD COLUMN row_count BIGINT NOT NULL;
