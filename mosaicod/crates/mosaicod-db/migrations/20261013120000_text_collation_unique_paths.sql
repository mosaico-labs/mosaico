ALTER TABLE sequence_t
    ALTER COLUMN locator_name  TYPE TEXT COLLATE "C",
    ALTER COLUMN path_in_store TYPE TEXT COLLATE "C";

ALTER TABLE session_t
    ALTER COLUMN locator_name TYPE TEXT COLLATE "C";

ALTER TABLE topic_t
    ALTER COLUMN locator_name  TYPE TEXT COLLATE "C",
    ALTER COLUMN path_in_store TYPE TEXT COLLATE "C",
    ALTER COLUMN ontology_tag  TYPE TEXT COLLATE "C";

ALTER TABLE topic_optimization_t
    ALTER COLUMN opt_path_in_store TYPE TEXT COLLATE "C";

ALTER TABLE column_t
    ALTER COLUMN column_name  TYPE TEXT COLLATE "C",
    ALTER COLUMN ontology_tag TYPE TEXT COLLATE "C";

ALTER TABLE column_chunk_textual_t
    ALTER COLUMN min_value TYPE TEXT COLLATE "C",
    ALTER COLUMN max_value TYPE TEXT COLLATE "C";

-- path_in_store values are random ULID folders, so they must be unique.
ALTER TABLE sequence_t
    ADD CONSTRAINT sequence_t_path_in_store_key UNIQUE (path_in_store);

ALTER TABLE topic_t
    ADD CONSTRAINT topic_t_path_in_store_key UNIQUE (path_in_store);

ALTER TABLE topic_optimization_t
    ADD CONSTRAINT topic_optimization_t_opt_path_in_store_key UNIQUE (opt_path_in_store);

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
