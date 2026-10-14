-- Index foreign keys: PostgreSQL does not create them automatically. They speed up joins
-- and lookups by parent id, and avoid full scans on `ON DELETE CASCADE` / `SET NULL`.
CREATE INDEX session_sequence_id_idx ON session_t (sequence_id);

CREATE INDEX topic_sequence_id_idx ON topic_t (sequence_id);
CREATE INDEX topic_session_id_idx  ON topic_t (session_id);

CREATE INDEX sequence_notification_sequence_id_idx ON sequence_notification_t (sequence_id);
CREATE INDEX topic_notification_topic_id_idx       ON topic_notification_t (topic_id);

CREATE INDEX chunk_topic_id_idx ON chunk_t (topic_id);

CREATE INDEX column_chunk_textual_chunk_id_idx ON column_chunk_textual_t (chunk_id);
CREATE INDEX column_chunk_numeric_chunk_id_idx ON column_chunk_numeric_t (chunk_id);

CREATE INDEX cleanup_log_instance_id_idx ON cleanup_log_t (instance_id);
