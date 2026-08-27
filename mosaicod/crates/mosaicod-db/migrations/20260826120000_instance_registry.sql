-- Registry of running `mosaicod` processes (server, cleanup, ...), used to answer "how many
-- instances are running, and where" (see `mosaicod ps`).
--
-- Liveness is not tracked explicitly: it is derived at read time from how recent
-- `last_heartbeat_unix_tstamp_secs` is, compared against a staleness threshold. A separate,
-- much larger threshold is used to decide when a row is old enough to be garbage collected.

CREATE TABLE instance_registry_t
(
    instance_id                     SERIAL PRIMARY KEY,
    kind                            TEXT    NOT NULL,
    hostname                        TEXT    NOT NULL,
    pid                             INTEGER NOT NULL,
    started_unix_tstamp_secs        BIGINT  NOT NULL,
    last_heartbeat_unix_tstamp_secs BIGINT  NOT NULL,
    -- True for a routine that performs a single run and exits (e.g. `mosaicod cleanup` with the
    -- default `--time-interval 0`) rather than looping until shut down. Always false for `server`,
    -- which has no such concept.
    one_shot                        BOOLEAN NOT NULL DEFAULT false
);

-- Speeds up both `mosaicod ps` (listing, ordered by staleness) and the periodic garbage
-- collection of long-expired entries.
CREATE INDEX instance_registry_last_heartbeat_idx ON instance_registry_t (last_heartbeat_unix_tstamp_secs);

-- Attributes each cleanup run to the instance that performed it. `ON DELETE SET NULL` (rather
-- than this codebase's usual `ON DELETE CASCADE`) is deliberate: cleanup_log_t is a historical
-- log that must survive its originating instance eventually being garbage collected from
-- instance_registry_t.
ALTER TABLE cleanup_log_t
    ADD COLUMN instance_id INTEGER REFERENCES instance_registry_t (instance_id) ON DELETE SET NULL;
