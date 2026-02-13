CREATE TABLE session_t(
  session_id    SERIAL PRIMARY KEY,
  session_uuid  UUID UNIQUE NOT NULL,
  sequence_id   INTEGER REFERENCES sequence_t(sequence_id) NOT NULL, 
  locked        BOOL NOT NULL DEFAULT FALSE,

  creation_unix_timestamp   BIGINT NOT NULL,
  completion_unix_timestamp BIGINT
);
