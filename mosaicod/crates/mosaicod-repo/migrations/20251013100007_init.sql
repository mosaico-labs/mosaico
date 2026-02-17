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
  locked        BOOL NOT NULL DEFAULT FALSE,

  creation_unix_tstamp    BIGINT NOT NULL,
  completion_unix_tstamp  BIGINT
);

CREATE TABLE topic_t(
  topic_id      SERIAL PRIMARY KEY,
  topic_uuid    UUID UNIQUE NOT NULL,
  sequence_id   INTEGER REFERENCES sequence_t(sequence_id) NOT NULL,
  session_id    INTEGER REFERENCES session_t(session_id) NOT NULL,
  locator_name  TEXT UNIQUE NOT NULL,
  locked        BOOL NOT NULL DEFAULT FALSE,
  user_metadata JSONB,
  
  serialization_format  TEXT,
  ontology_tag          TEXT,
  
  creation_unix_tstamp BIGINT NOT NULL
);
