---
title: Environment Variables
sidebar_position: 12
description: "Complete reference for all environment variables accepted by mosaicod. Covers storage path, gRPC port, TLS certificate paths, log level, retention policy, and other runtime configuration options."
---

Here we provide a complete list of environment variables that can be used to configure the daemon. These variables allow you to customize various aspects of the daemon's behavior, including database connections, storage options and more.

## General

- `MOSAICOD_MAX_GRPC_MESSAGE_SIZE`: The maximum allowed [gRPC](https://grpc.io/) message size in bytes. If a message exceeds this size, a protocol error will be returned. Default is `50 MiB`. Must be between `4 MiB` and `128 MiB`. If you need to update this value be aware that this value is tipically smaller than `MOSAICOD_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE`.

- `MOSAICOD_TARGET_MESSAGE_SIZE`: Target message size in bytes used during data streaming. The daemon will try to aggregate a number of [`RecordBatches`](https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html) to create a sufficiently large message. If the resulting batch size exceeds the limit, it will be capped by `MOSAICOD_MAX_BATCH_SIZE`. Not itself configurable via an environment variable; it is always set to half of `MOSAICOD_MAX_GRPC_MESSAGE_SIZE`. Defaults to `25 MiB`.

- `MOSAICOD_MAX_CONCURRENT_WRITES`: The maximum number of concurrent encoding and serialization operations. This setting controls how many data batches can be processed and sent to the object store simultaneously. It is important to note that this does not limit the number of topics the server can handle; rather, it constrains the parallel execution of the encoding/serialization pipeline. Each operation runs in a dedicated thread to handle CPU-bound compression and I/O-bound storage tasks. This value should be tuned based on available RAM and CPU. Excessive parallelism may lead to scheduler thrashing or memory exhaustion. Defaults to `MOSAICOD_DEFAULT_PARALLELISM`.

- `MOSAICOD_MAX_CONCURRENT_CHUNK_QUERIES`: Maximum number of concurrent queries that can be executed against data chunks. Default is `4`.

- `MOSAICOD_MAX_BATCH_SIZE`: Maximum batch size (number of elements inside an Arrow `RecordBatch`) used during data streaming. Defaults to [DataFusion default batch size](https://datafusion.apache.org/user-guide/configs.html#via-sql) `8192`.

- `MOSAICOD_DEFAULT_PARALLELISM`: Sets the degree of parallelism. While this is typically detected automatically based on available hardware, this field allows for a manual override in environments where automatic detection might fail or be inaccurate.

- `MOSAICOD_QUERY_ENGINE_MEMORY_POOL_SIZE`: Defines the amount of memory (in bytes) used by the query engine. Set this value to a number greater than 0 to enforce a hard limit on the memory allocated by the query engine. Use this setting if mosaicod encounters OOM (Out Of Memory) errors or you plan to use `mosaicod` in a memory constrained environment. Defaults to `0` (no limit).

- `MOSAICOD_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE`: Size (in bytes) of the in-memory buffer used for encoding parquet data. Defaults to `70 MiB`.

- `MOSAICOD_MAX_SIZE_PLAIN_LIST_EQ`: Maximum number of elements unrolled when decomposing a list `eq`/`neq` filter into per-element scalar comparisons. Filters whose value array exceeds this limit are skipped to avoid generating pathologically large query expressions. Defaults to `1024`.

## Logging

- `MOSAICOD_LOG_LEVEL`: Log verbosity level. One of `warning`, `info`, `debug`. Defaults to `warning`. Overridden by the `--log-level` CLI argument if set, and ignored if `RUST_LOG` is set.

- `MOSAICOD_LOG_FORMAT`: Log output format. One of `json`, `pretty`, `plain`. Defaults to `pretty`. Overridden by the `--log-format` CLI argument if set.

## Server

- `MOSAICOD_HOST`: Host address the server listens on. Defaults to the loopback address `127.0.0.1`. Overridden by the `--host` CLI argument of the `server` command if set.

- `MOSAICOD_PORT`: Port the server listens on. Defaults to `6726`. Overridden by the `--port` CLI argument of the `server` command if set.

- `MOSAICOD_TLS_ENABLED`: Whether TLS is enabled for the server. Requires `MOSAICOD_TLS_CERT_FILE` and `MOSAICOD_TLS_PRIVATE_KEY_FILE` to be set. Default is `false`.

- `MOSAICOD_GZIP_ENABLED`: Whether gzip compression is enabled for gRPC requests and responses. Default is `false`.

- `MOSAICOD_API_KEY_ENABLED`: Whether API key enforcement is enabled. See the `mosaicod api-key` command. Default is `false`.

## TLS

- `MOSAICOD_TLS_CERT_FILE`: Path to the TLS certificate file used for secure communication. Default is an empty string.

- `MOSAICOD_TLS_PRIVATE_KEY_FILE`: Path to the TLS private key file used for secure communication. Default is an empty string.

## DBMS

- `MOSAICOD_DB_URL`: Database connection URL, without credentials (e.g. `postgresql://host:port/dbname`). Use `MOSAICOD_DB_USER` and `MOSAICOD_DB_PASSWORD` to provide credentials. **Required**.

- `MOSAICOD_DB_USER`: Database user used to establish the connection. Default is an empty string, as some databases do not require a user to be specified.

- `MOSAICOD_DB_PASSWORD`: Database password used to establish the connection. Default is an empty string, as some databases do not require a password to be specified. May also be provided via `MOSAICOD_DB_PASSWORD_FILE`, set to the path of a file containing the password, instead of setting it directly; setting both `MOSAICOD_DB_PASSWORD` and `MOSAICOD_DB_PASSWORD_FILE` is an error.

- `MOSAICOD_MAX_DB_CONNECTIONS`: Maximum number of database connections that can be established. Default is `10`.

## Store

- `MOSAICOD_STORE_ENDPOINT`: Endpoint URL for the object storage service (e.g., S3). Use `file:///some/absolute/path` to set up a local storage directory. **Required**.
- `MOSAICOD_STORE_BUCKET`: Name of the bucket in the object storage service where data will be stored. When using the local filesystem endpoint, the system creates a new directory named after the bucket within the endpoint path. **Required**.
- `MOSAICOD_STORE_ACCESS_KEY`: Access key for the object storage service. Default is an empty string.
- `MOSAICOD_STORE_SECRET_KEY`: Secret key for the object storage service. Default is an empty string. May also be provided via `MOSAICOD_STORE_SECRET_KEY_FILE`, set to the path of a file containing the secret key, instead of setting it directly; setting both `MOSAICOD_STORE_SECRET_KEY` and `MOSAICOD_STORE_SECRET_KEY_FILE` is an error.

## Store optimizer

- `MOSAICOD_STORE_OPTIMIZER_TIME_INTERVAL`: Minimum interval, in seconds, that must pass between a store optimization run and the next one. When set to `0` a single run is performed and then the process terminates; any value greater than `0` runs the routine in a loop. Defaults to `0`. Overridden by the `--time-interval` CLI argument of the `store-optimizer` command if set.

- `MOSAICOD_STORE_OPTIMIZER_MAX_CHUNK_SIZE`: Maximum size (in bytes) for an output chunk after optimization. This is a soft limit, actual files on store may exceed this value slightly. Defaults to `256 MiB`. Overridden by the `--max-chunk-size` CLI argument of the `store-optimizer` command if set.

- `MOSAICOD_STORE_OPTIMIZER_MEMORY_POOL_SIZE`: Defines the amount of memory (in bytes) used by the store optimizer's query engine. Set this value to a number greater than 0 to enforce a hard limit on the memory allocated. Use this setting if `mosaicod` encounters OOM (Out Of Memory) errors during optimization. Defaults to `0` (no limit).

## Cleanup

- `MOSAICOD_CLEANUP_TIME_INTERVAL`: Minimum interval, in seconds, that must pass between a cleanup run and the next one. When set to `0` a single cleanup is performed and then the process terminates; any value greater than `0` runs the cleanup routine in a loop. Defaults to `0`. Overridden by the `--time-interval` CLI argument of the `cleanup` command if set.

- `MOSAICOD_CLEANUP_RETENTION_DURATION`: Maximum period, in seconds, an obsolete file is kept in the store before being permanently deleted. Defaults to `86400`. Overridden by the `--retention-duration` CLI argument of the `cleanup` command if set.
