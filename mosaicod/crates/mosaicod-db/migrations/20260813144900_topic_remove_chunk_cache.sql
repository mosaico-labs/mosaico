-- Remove chunks number and total bytes from topic_t table for they can be easily obtained from chunk_t table.
ALTER TABLE topic_t
DROP COLUMN chunks_number,
DROP COLUMN total_bytes;