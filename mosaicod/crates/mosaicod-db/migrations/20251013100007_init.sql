CREATE TABLE sequence_t(
  sequence_id   SERIAL PRIMARY KEY,
  sequence_uuid UUID UNIQUE NOT NULL,
  locator_name  TEXT UNIQUE NOT NULL,
  user_metadata JSONB,

  path_in_store TEXT NOT NULL,

  creation_unix_tstamp BIGINT NOT NULL
);

CREATE TABLE session_t(
  session_id    SERIAL PRIMARY KEY,
  session_uuid  UUID UNIQUE NOT NULL,
  sequence_id   INTEGER NOT NULL,

  creation_unix_tstamp    BIGINT NOT NULL,
  completion_unix_tstamp  BIGINT,

  CONSTRAINT fk_sequence
      FOREIGN KEY (sequence_id)
      REFERENCES sequence_t (sequence_id)
      ON DELETE CASCADE
);

CREATE TABLE topic_t(
  topic_id      SERIAL PRIMARY KEY,
  topic_uuid    UUID UNIQUE NOT NULL,
  sequence_id   INTEGER NOT NULL,
  session_id    INTEGER NOT NULL,
  locator_name  TEXT UNIQUE NOT NULL,
  user_metadata JSONB,

  path_in_store TEXT,

  serialization_format  TEXT NOT NULL,
  ontology_tag          TEXT NOT NULL,

  creation_unix_tstamp   BIGINT NOT NULL,
  completion_unix_tstamp BIGINT,

  -- These fields store unsigned int 64bit numbers. Casting is required before usage.
  chunks_number     BIGINT,
  total_bytes       BIGINT,

  start_index_timestamp   BIGINT,
  end_index_timestamp     BIGINT,

  CONSTRAINT fk_sequence
      FOREIGN KEY (sequence_id)
      REFERENCES sequence_t (sequence_id)
      ON DELETE CASCADE,

  CONSTRAINT fk_session
      FOREIGN KEY (session_id)
      REFERENCES session_t (session_id)
      ON DELETE CASCADE
);
