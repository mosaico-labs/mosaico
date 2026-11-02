-- TEMPORARY: marks the stats computed on the elements of a list column, so that the
-- aggregators not defined on lists (e.g. mean, first, last) can be rejected at query time.
-- This change is temporary, until lists are properly supported by the aggregators.
ALTER TABLE column_chunk_numeric_t
    ADD COLUMN is_list BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE column_chunk_textual_t
    ADD COLUMN is_list BOOLEAN NOT NULL DEFAULT FALSE;
