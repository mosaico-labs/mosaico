-- The chunks written before this migration don't have the stats added here, which can't be
-- computed from the db: their column stats are removed and every topic is queued again (see
-- the end of the file), so that the store optimizer rewrites its chunks recomputing every
-- stat. Until then, these topics don't match the ontology queries.
DELETE FROM column_chunk_numeric_t;
DELETE FROM column_chunk_textual_t;

ALTER TABLE column_chunk_numeric_t
    -- replaced by null_count > 0 and nan_count > 0
    DROP COLUMN has_null,
    DROP COLUMN has_nan,
    -- count = chunk_t.row_count - null_count - nan_count
    ADD COLUMN nan_count BIGINT NOT NULL,
    ADD COLUMN null_count BIGINT NOT NULL,
    -- first and last (NULL if the value at the first/last timestamp is null)
    ADD COLUMN first_sample_value DOUBLE PRECISION,
    ADD COLUMN last_sample_value DOUBLE PRECISION,
    -- mean and stddev -> mean and M2 = sum((x - mean)^2) of valid values (Welford/Chan)
    ADD COLUMN mean_value DOUBLE PRECISION,
    ADD COLUMN m2_value DOUBLE PRECISION,
    ADD CONSTRAINT column_chunk_numeric_null_count_non_negative CHECK (null_count >= 0),
    ADD CONSTRAINT column_chunk_numeric_nan_count_non_negative CHECK (nan_count >= 0);

ALTER TABLE column_chunk_textual_t
    -- replaced by null_count > 0
    DROP COLUMN has_null,
    -- count = chunk_t.row_count - null_count
    ADD COLUMN null_count BIGINT NOT NULL,
    -- first and last (NULL if the value at the first/last timestamp is null)
    ADD COLUMN first_sample_value TEXT COLLATE "C",
    ADD COLUMN last_sample_value TEXT COLLATE "C",
    ADD CONSTRAINT column_chunk_textual_null_count_non_negative CHECK (null_count >= 0);

-- Timestamps of the first and last sample of the chunk. A NOT NULL column needs a value on
-- the existing chunks too, but their real timestamps can't be computed from the db: they get
-- 0, a placeholder that is never read, because the queries reach chunk_t through the column
-- stats (removed above for these chunks), and the store optimizer rewrites these chunks with
-- the real timestamps.
ALTER TABLE chunk_t
    ADD COLUMN first_sample_timestamp_ns BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN last_sample_timestamp_ns BIGINT NOT NULL DEFAULT 0;

-- The default is only for the existing chunks: the new ones must always set the timestamps,
-- and an insert without them must fail instead of silently writing 0.
ALTER TABLE chunk_t
    ALTER COLUMN first_sample_timestamp_ns DROP DEFAULT,
    ALTER COLUMN last_sample_timestamp_ns DROP DEFAULT;

-- every topic is queued for the store optimizer, which rewrites its chunks
UPDATE topic_t SET optimization_end_unix_tstamp = NULL;
