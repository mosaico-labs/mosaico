# Environment Variables

Here we provide a complete list of environment variables that can be used to configure the daemon. These variables allow you to customize various aspects of the daemon's behavior, including database connections, storage options and more.

## General

- `MOSAICOD_MAX_MESSAGE_SIZE_IN_BYTES`: The maximum allowed gRPC message size in bytes. If a message exceeds this size, a protocol error will be returned. Default is `50 MiB`.

- `MOSAICOD_TARGET_MESSAGE_SIZE_IN_BYTES`: Target message size used during data streaming. Mosaicod will try to aggregate a number of `RecordBatches`(https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html) to create a sufficiently large message. If the resulting batch size exceeds the limit, it will be capped by `MOSAICOD_MAX_BATCH_SIZE`. Defaults to `25MiB`.

- `MOSAICOD_MAX_CONCURRENT_CHUNK_QUERIES`: Maximum number of concurrent queries that can be executed against data chunks. Default is `4`.

- `MOSAICOD_MAX_DB_CONNECTIONS`: Maximum number of database connections that can be established. Default is `10`.

- `MOSAICOD_MAX_CHUNK_SIZE_IN_BYTES`: Maximum chunk size in bytes before automatic splitting during upload. When a chunk exceeds this size, it is finalized and a new chunk is started. A value of `0` means unlimited chunk size and no automatic splitting. Defaults to `250 MiB`. 

- `MOSAICOD_MAX_BATCH_SIZE`: Maximum batch size (number of elements inside an Arrow `RecordBatch`) used during data streaming. Defaults to [default DataFusion](https://datafusion.apache.org/user-guide/configs.html#via-sql) batch size `8192`.

- `QUERY_ENGINE_MEMORY_POOL_SIZE_IN_BYTES`: Defines the amount of memory used by the query engine. Set this value to a number greater than 0 to enforce a hard limit on the memory allocated by the query engine. Use this setting if mosaicod encounters OOM (Out Of Memory) errors or you plan to use `mosaicod` in a memory constrained environment. Defaults to `0` (no limit).

## TLS

- `MOSAICOD_TLS_CERT_FILE`: Path to the TLS certificate file used for secure communication. Default is an empty string.

- `MOSAICOD_TLS_PRIVATE_KEY_FILE`: Path to the TLS private key file used for secure communication. Default is an empty string.

## DBMS

- `MOSAICOD_DB_URL`: Database connection URL. This should be in the format expected by the database driver being used. **Required**.

## Store

- `MOSAICOD_STORE_ENDPOINT`: Endpoint URL for the object storage service (e.g., S3). Default is an empty string.
- `MOSAICOD_STORE_ACCESS_KEY`: Access key for the object storage service. Default is an empty string.
- `MOSAICOD_STORE_SECRET_KEY`: Secret key for the object storage service. Default is an empty string.
- `MOSAICOD_STORE_BUCKET`: Name of the bucket in the object storage service where data will be stored. Default is an empty string.
