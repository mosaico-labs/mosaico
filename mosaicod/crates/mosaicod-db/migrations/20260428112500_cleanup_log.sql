CREATE TABLE cleanup_log_t (
    cleanup_id              SERIAL PRIMARY KEY,
    start_unix_tstamp_secs  BIGINT NOT NULL,
    end_unix_tstamp_secs    BIGINT
);