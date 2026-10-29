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

ALTER TABLE chunk_t
    -- first and last 
    ADD COLUMN first_sample_timestamp_ns BIGINT NOT NULL,
    ADD COLUMN last_sample_timestamp_ns BIGINT NOT NULL;
