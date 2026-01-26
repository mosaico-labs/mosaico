CREATE TABLE sequence_notify_t(
  sequence_notify_id   SERIAL PRIMARY KEY,
  sequence_id          INTEGER NOT NULL, -- Constraint on sequences defined below
  notify_type          TEXT NOT NULL,
  msg                  TEXT,
  creation_unix_tstamp BIGINT NOT NULL,

  -- This constraint will cause the deletion of all 
  -- reports of a sequence if the related sequence 
  -- entry is deleted.
  CONSTRAINT fk_sequence
    FOREIGN KEY (sequence_id)
    REFERENCES sequence_t(sequence_id)
    ON DELETE CASCADE
);

CREATE TABLE topic_notify_t(
  topic_notify_id      SERIAL PRIMARY KEY,
  topic_id             INTEGER NOT NULL, -- Constraint on topics defined below
  notify_type          TEXT NOT NULL,
  msg                  TEXT,
  creation_unix_tstamp BIGINT NOT NULL,

  -- This constraint will cause the deletion of all 
  -- reports of a topic if the related topic 
  -- entry is deleted.
  CONSTRAINT fk_topic
    FOREIGN KEY (topic_id)
    REFERENCES topic_t(topic_id)
    ON DELETE CASCADE
);