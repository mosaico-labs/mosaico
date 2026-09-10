-- Add topic_optimization table to keep a list of topic not yet optimized.

CREATE TABLE topic_optimization_t (
    topic_id INT PRIMARY KEY,
    opt_path_in_store TEXT,
    start_unix_tstamp BIGINT,

    CONSTRAINT fk_topic
        FOREIGN KEY (topic_id)
            REFERENCES topic_t (topic_id)
            ON DELETE CASCADE
);

-- Add optimization_end_unix_tstamp field into topic_t table to track when the optimization has finished.

ALTER TABLE topic_t ADD COLUMN optimization_end_unix_tstamp BIGINT;
