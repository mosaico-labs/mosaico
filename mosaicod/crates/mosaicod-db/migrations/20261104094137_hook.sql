-- HTTP endpoints that mosaicod sends an event to (see `mosaicod hook`).
CREATE TABLE hook_t
(
    hook_id              SERIAL PRIMARY KEY,
    -- Stored normalized. UNIQUE because a URL registered twice would get every event twice.
    url                  TEXT COLLATE "C" NOT NULL UNIQUE,
    creation_unix_tstamp BIGINT NOT NULL
);
