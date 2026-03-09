CREATE TABLE column_t(
  column_id    SERIAL PRIMARY KEY,
  column_name  TEXT NOT NULL,
  ontology_tag TEXT NOT NULL,

  UNIQUE(column_name, ontology_tag)
);

CREATE TABLE chunk_t(
  chunk_id        SERIAL  PRIMARY KEY,
  chunk_uuid UUID UNIQUE  NOT NULL,
  topic_id        INTEGER NOT NULL, -- Constraint on topics defined below
  data_file       TEXT    NOT NULL,


  -- This constraint will cause the deletion of all 
  -- reports of a topic if the related topic 
  -- entry is deleted.
  CONSTRAINT fk_topic
    FOREIGN KEY (topic_id)
    REFERENCES topic_t(topic_id)
    ON DELETE CASCADE
);

CREATE TABLE column_chunk_textual_t(
  column_id    INTEGER REFERENCES column_t(column_id) NOT NULL,
  chunk_id     INTEGER NOT NULL, -- Constraint on chunks defined below

  min_value        TEXT NOT NULL,
  max_value        TEXT NOT NULL,
  has_null         BOOL NOT NULL,

  PRIMARY KEY (column_id, chunk_id),


  -- This constraint will cause the deletion of all 
  -- chunks if the related column chunk entry is deleted.
  CONSTRAINT fk_chunk
    FOREIGN KEY (chunk_id)
    REFERENCES chunk_t(chunk_id)
    ON DELETE CASCADE
);

CREATE TABLE column_chunk_numeric_t(
  column_id    INTEGER REFERENCES column_t(column_id) NOT NULL,
  chunk_id     INTEGER NOT NULL, -- Constraint on chunks defined below

  min_value        DOUBLE PRECISION NOT NULL,
  max_value        DOUBLE PRECISION NOT NULL,
  has_null         BOOL NOT NULL,
  has_nan          BOOL NOT NULL,

  PRIMARY KEY (column_id, chunk_id),

  -- This constraint will cause the deletion of all 
  -- chunks if the related column chunk entry is deleted.
  CONSTRAINT fk_chunk
    FOREIGN KEY (chunk_id)
    REFERENCES chunk_t(chunk_id)
    ON DELETE CASCADE
);
