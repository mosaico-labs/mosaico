CREATE TABLE sequence_t(
  sequence_id   SERIAL PRIMARY KEY,
  sequence_uuid UUID UNIQUE NOT NULL,
  locator_name  TEXT UNIQUE NOT NULL,
  user_metadata JSONB,
  
  creation_unix_tstamp BIGINT NOT NULL
);

CREATE TABLE session_t(
  session_id    SERIAL PRIMARY KEY,
  session_uuid  UUID UNIQUE NOT NULL,
  sequence_id   INTEGER REFERENCES sequence_t(sequence_id) NOT NULL,

  creation_unix_tstamp    BIGINT NOT NULL,
  completion_unix_tstamp  BIGINT
);

CREATE TABLE topic_t(
  topic_id      SERIAL PRIMARY KEY,
  topic_uuid    UUID UNIQUE NOT NULL,
  sequence_id   INTEGER REFERENCES sequence_t(sequence_id) NOT NULL,
  session_id    INTEGER REFERENCES session_t(session_id) NOT NULL,
  locator_name  TEXT UNIQUE NOT NULL,
  user_metadata JSONB,
  
  serialization_format  TEXT NOT NULL,
  ontology_tag          TEXT NOT NULL,

  creation_unix_tstamp BIGINT NOT NULL,

  -- These fields store unsigned int 64bit numbers. Casting is required before usage.
  chunks_number     BIGINT,
  total_bytes       BIGINT,

  start_index_timestamp   BIGINT,
  end_index_timestamp     BIGINT
);
