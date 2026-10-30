-- Serves `list_flights` pages ordered by creation time.
CREATE INDEX sequence_creation_idx ON sequence_t (creation_unix_tstamp, sequence_id);
