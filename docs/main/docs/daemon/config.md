---
title: Config File
sidebar_position: 12
description: "How to configure mosaicod with a config.toml file: file resolution order, the TOML schema, secret handling, and how it interacts with CLI flags and environment variables."
---

In addition to [CLI flags](cli.md) and [environment variables](env.md), `mosaicod` can be configured with an optional `config.toml` file. It is the lowest-priority configuration source, useful for setting stable, host-wide defaults without having to repeat CLI flags or export environment variables.

For every setting, the resolution order is:

1. CLI flag (if the command exposes one)
2. Environment variable
3. `config.toml` value
4. Hard-coded default

Every field in the config file is optional. An absent section, or an absent key within a section, simply means "not set at this tier" and falls through to the next one below it.

## Locating the file

`mosaicod` looks for the config file in the following order, stopping at the first one it finds:

1. The path passed to the global `--config <PATH>` CLI flag.
2. The path in the `MOSAICOD_CONFIG_FILE` environment variable.
3. `$XDG_CONFIG_HOME/mosaicod/config.toml`, or `~/.config/mosaicod/config.toml` if `XDG_CONFIG_HOME` is unset.
4. `/etc/mosaicod/config.toml`.

If none of these exist, `mosaicod` starts with no config file loaded (every field falls through to the environment/default tiers).

:::warning
    If a path is set explicitly via `--config` or `MOSAICOD_CONFIG_FILE` and that file does not exist, `mosaicod` fails to start with an error. The `$XDG_CONFIG_HOME`/`/etc` defaults, on the other hand, are silently skipped when absent, since the config file is entirely optional at those locations.
:::

```bash
mosaicod --config /path/to/config.toml server
```

## Schema

Unknown keys and unknown sections are rejected: a typo in the config file causes `mosaicod` to fail at startup rather than silently ignoring the setting.

```toml
[server]
host = "127.0.0.1"
port = 6726
gzip_enabled = false
api_key_enabled = false
max_batch_size = 8192
default_parallelism = 8
max_concurrent_writes = 8

[server.tls]
enabled = false
certificate_file = "/path/to/cert.pem"
private_key_file = "/path/to/key.pem"

[server.grpc]
max_message_size = 50000000

[database]
url = "postgresql://localhost:5432/mosaico"
user = "postgres"
password_file = "/run/secrets/mosaicod_db_password"
max_connections = 19

[store]
endpoint = "https://s3.example.com"
bucket = "mosaico"
access_key = "access-key"
secret_key_file = "/run/secrets/mosaicod_store_secret_key"

[query_engine]
memory_pool_size = 0
max_concurrent_chunk_queries = 4
max_size_plain_list_eq = 1024

[encoding]
parquet_in_memory_encoding_buffer_size = 75000000

[store_optimizer]
time_interval = 0
max_chunk_size = 256000000
memory_pool_size = 0

[cleanup]
time_interval = 0
retention_duration = 86400

[logging]
level = "warning"
format = "pretty"
```

The table below maps each config key to the [environment variable](env.md) it corresponds to, for cross-reference.

<div className="plain-links">

| Config key | Environment variable |
| :--- | :--- |
| `server.host` | [`MOSAICOD_HOST`](env.md#mosaicod-host) |
| `server.port` | [`MOSAICOD_PORT`](env.md#mosaicod-port) |
| `server.gzip_enabled` | [`MOSAICOD_GZIP_ENABLED`](env.md#mosaicod-gzip-enabled) |
| `server.api_key_enabled` | [`MOSAICOD_API_KEY_ENABLED`](env.md#mosaicod-api-key-enabled) |
| `server.max_batch_size` | [`MOSAICOD_MAX_BATCH_SIZE`](env.md#mosaicod-max-batch-size) |
| `server.default_parallelism` | [`MOSAICOD_DEFAULT_PARALLELISM`](env.md#mosaicod-default-parallelism) |
| `server.max_concurrent_writes` | [`MOSAICOD_MAX_CONCURRENT_WRITES`](env.md#mosaicod-max-concurrent-writes) |
| `server.tls.enabled` | [`MOSAICOD_TLS_ENABLED`](env.md#mosaicod-tls-enabled) |
| `server.tls.certificate_file` | [`MOSAICOD_TLS_CERT_FILE`](env.md#mosaicod-tls-cert-file) |
| `server.tls.private_key_file` | [`MOSAICOD_TLS_PRIVATE_KEY_FILE`](env.md#mosaicod-tls-private-key-file) |
| `server.grpc.max_message_size` | [`MOSAICOD_MAX_GRPC_MESSAGE_SIZE`](env.md#mosaicod-max-grpc-message-size) |
| `database.url` | [`MOSAICOD_DB_URL`](env.md#mosaicod-db-url) |
| `database.user` | [`MOSAICOD_DB_USER`](env.md#mosaicod-db-user) |
| `database.password_file` | [`MOSAICOD_DB_PASSWORD_FILE`](env.md#mosaicod-db-password) |
| `database.max_connections` | [`MOSAICOD_MAX_DB_CONNECTIONS`](env.md#mosaicod-max-db-connections) |
| `store.endpoint` | [`MOSAICOD_STORE_ENDPOINT`](env.md#mosaicod-store-endpoint) |
| `store.bucket` | [`MOSAICOD_STORE_BUCKET`](env.md#mosaicod-store-bucket) |
| `store.access_key` | [`MOSAICOD_STORE_ACCESS_KEY`](env.md#mosaicod-store-access-key) |
| `store.secret_key_file` | [`MOSAICOD_STORE_SECRET_KEY_FILE`](env.md#mosaicod-store-secret-key) |
| `query_engine.memory_pool_size` | [`MOSAICOD_QUERY_ENGINE_MEMORY_POOL_SIZE`](env.md#mosaicod-query-engine-memory-pool-size) |
| `query_engine.max_concurrent_chunk_queries` | [`MOSAICOD_MAX_CONCURRENT_CHUNK_QUERIES`](env.md#mosaicod-max-concurrent-chunk-queries) |
| `query_engine.max_size_plain_list_eq` | [`MOSAICOD_MAX_SIZE_PLAIN_LIST_EQ`](env.md#mosaicod-max-size-plain-list-eq) |
| `encoding.parquet_in_memory_encoding_buffer_size` | [`MOSAICOD_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE`](env.md#mosaicod-parquet-in-memory-encoding-buffer-size) |
| `store_optimizer.time_interval` | [`MOSAICOD_STORE_OPTIMIZER_TIME_INTERVAL`](env.md#mosaicod-store-optimizer-time-interval) |
| `store_optimizer.max_chunk_size` | [`MOSAICOD_STORE_OPTIMIZER_MAX_CHUNK_SIZE`](env.md#mosaicod-store-optimizer-max-chunk-size) |
| `store_optimizer.memory_pool_size` | [`MOSAICOD_STORE_OPTIMIZER_MEMORY_POOL_SIZE`](env.md#mosaicod-store-optimizer-memory-pool-size) |
| `cleanup.time_interval` | [`MOSAICOD_CLEANUP_TIME_INTERVAL`](env.md#mosaicod-cleanup-time-interval) |
| `cleanup.retention_duration` | [`MOSAICOD_CLEANUP_RETENTION_DURATION`](env.md#mosaicod-cleanup-retention-duration) |
| `logging.level` | [`MOSAICOD_LOG_LEVEL`](env.md#mosaicod-log-level) |
| `logging.format` | [`MOSAICOD_LOG_FORMAT`](env.md#mosaicod-log-format) |

</div>

## Secrets

The database password and the object store secret key cannot be set directly in the config file: there is no `database.password` or `store.secret_key` field. Instead, use `database.password_file` / `store.secret_key_file` to point at a file containing the secret, mirroring the `MOSAICOD_DB_PASSWORD_FILE` / `MOSAICOD_STORE_SECRET_KEY_FILE` environment variables. This keeps secrets out of the config file itself, which may be checked into configuration management or otherwise more widely readable than a runtime environment.

If the plain environment variable (`MOSAICOD_DB_PASSWORD` or `MOSAICOD_STORE_SECRET_KEY`) is set, it takes priority over the config file's `_file` path, following the same precedence as every other setting.
