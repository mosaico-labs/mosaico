CREATE TABLE api_key_t (
    fingerprint BYTEA PRIMARY KEY, -- 8 bytes
    payload BYTEA NOT NULL, -- 32 bytes.

    permissions SMALLINT NOT NULL CHECK (permissions >= 0 AND permissions <= 255), -- 8 bytes
    description TEXT NOT NULL,

    creation_unix_timestamp BIGINT NOT NULL,
    expiration_unix_timestamp BIGINT
);

