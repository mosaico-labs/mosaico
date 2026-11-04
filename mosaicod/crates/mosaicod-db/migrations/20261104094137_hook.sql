-- HTTP endpoints that mosaicod sends an event to when a topic is uploaded or a session is
-- finalized (see `mosaicod hook`).
CREATE TABLE hook_t
(
    hook_id              SERIAL PRIMARY KEY,
    -- Stored normalized. UNIQUE because a URL registered twice would get every event twice.
    url                  TEXT COLLATE "C" NOT NULL UNIQUE,
    creation_unix_tstamp BIGINT NOT NULL
);
