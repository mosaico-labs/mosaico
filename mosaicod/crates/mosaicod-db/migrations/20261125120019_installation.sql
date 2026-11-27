-- The identity of this installation, sent with every usage report. Every mosaicod process
-- connected to the same database shares it, and users quote it to have their data deleted.
CREATE TABLE installation_t
(
    -- Always true: a primary key on a constant column keeps the table to a single row.
    singleton            BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    installation_uuid    UUID   NOT NULL,
    creation_unix_tstamp BIGINT NOT NULL
);
