# Environment Variables

Here we provide a complete list of environment variables that can be used to configure the daemon. These variables allow you to customize various aspects of the daemon's behavior, including database connections, storage options and more.

## General

- `MOSAICOD_MAX_GRPC_MESSAGE_SIZE`: The maximum allowed [gRPC](https://grpc.io/) message size in bytes. If a message exceeds this size, a protocol error will be returned. Default is `25 MB`. *If you need to update this value be aware that this value is tipically smaller than `Params::parquet_in_memory_encoding_buffer_size`.

- `MOSAICOD_TARGET_MESSAGE_SIZE`: Target message size in bytes used during data streaming. The daemon will try to aggregate a number of [`RecordBatches`](https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html) to create a sufficiently large message. If the resulting batch size exceeds the limit, it will be capped by `MOSAICOD_MAX_BATCH_SIZE`. Defaults to `25MB`.

- `MOSAICOD_MAX_CONCURRENT_WRITES`: The maximum number of concurrent encoding and serialization operations. This setting controls how many data batches can be processed and sent to the object store simultaneously. It is important to note that this does not limit the number of topics the server can handle; rather, it constrains the parallel execution of the encoding/serialization pipeline. Each operation runs in a dedicated thread to handle CPU-bound compression and I/O-bound storage tasks. This value should be tuned based on available RAM and CPU. Excessive parallelism may lead to scheduler thrashing or memory exhaustion. Defaults to `MOSAICOD_DEFAULT_PARALLELISM`.

- `MOSAICOD_MAX_CONCURRENT_CHUNK_QUERIES`: Maximum number of concurrent queries that can be executed against data chunks. Default is `4`.

- `MOSAICOD_MAX_BATCH_SIZE`: Maximum batch size (number of elements inside an Arrow `RecordBatch`) used during data streaming. Defaults to [DataFusion default batch size](https://datafusion.apache.org/user-guide/configs.html#via-sql) `8192`.

- `MOSAICOD_DEFAULT_PARALLELISM`: Sets the degree of parallelism. While this is typically detected automatically based on available hardware, this field allows for a manual override in environments where automatic detection might fail or be inaccurate.

- `MOSAICOD_QUERY_ENGINE_MEMORY_POOL_SIZE`: Defines the amount of memory (in bytes) used by the query engine. Set this value to a number greater than 0 to enforce a hard limit on the memory allocated by the query engine. Use this setting if mosaicod encounters OOM (Out Of Memory) errors or you plan to use `mosaicod` in a memory constrained environment. Defaults to `0` (no limit).

- `MOSAICOD_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE`: Size (in bytes) of the in-memory buffer used for encoding parquet data. Defaults to `50MB`.

## TLS

- `MOSAICOD_TLS_CERT_FILE`: Path to the TLS certificate file used for secure communication. Default is an empty string.

- `MOSAICOD_TLS_PRIVATE_KEY_FILE`: Path to the TLS private key file used for secure communication. Default is an empty string.

## DBMS

- `MOSAICOD_DB_URL`: Database connection URL. This should be in the format expected by the database driver being used. **Required**.

- `MOSAICOD_MAX_DB_CONNECTIONS`: Maximum number of database connections that can be established. Default is `10`.

## Store

- `MOSAICOD_STORE_ENDPOINT`: Endpoint URL for the object storage service (e.g., S3). Default is an empty string.
- `MOSAICOD_STORE_ACCESS_KEY`: Access key for the object storage service. Default is an empty string.
- `MOSAICOD_STORE_SECRET_KEY`: Secret key for the object storage service. Default is an empty string.
- `MOSAICOD_STORE_BUCKET`: Name of the bucket in the object storage service where data will be stored. Default is an empty string.
