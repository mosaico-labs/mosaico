//! [`Params`], mosaicod's own parameters, and the defaults they fall back to.

use crate::param::{Hidden, Param};
use mosaicod_core::error;
use mosaicod_core::types::{LogFormat, LogLevel};
use std::net::{IpAddr, Ipv4Addr};
use std::num::NonZero;

pub const GRPC_MSG_MIN_SIZE_BYTES: usize = 4 * 1024 * 1024; // 4MiB. Default grpc message size.
pub const GRPC_MSG_MAX_SIZE_BYTES: usize = 512 * 1024 * 1024; // 512MiB

// Default values for Params.
pub const DEFAULT_GRPC_MAX_DECODE_MESSAGE_SIZE: usize = 50 * 1024 * 1024; // 50MiB
pub const DEFAULT_GRPC_TARGET_ENCODE_MESSAGE_SIZE: usize = 25 * 1024 * 1024; // 25MiB
pub const DEFAULT_MAX_CONCURRENT_CHUNK_QUERIES: NonZero<usize> = NonZero::new(4).unwrap();
pub const DEFAULT_MAX_SIZE_PLAIN_LIST_EQ: NonZero<usize> = NonZero::new(1024).unwrap();
pub const DEFAULT_MAX_DB_CONNECTIONS: NonZero<u32> = NonZero::new(10).unwrap();
pub const DEFAULT_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE: usize = 70 * 1024 * 1024; // 70MiB
pub const DEFAULT_MAX_BATCH_SIZE: NonZero<usize> = NonZero::new(8192).unwrap();
pub const DEFAULT_QUERY_ENGINE_MEMORY_POOL_SIZE: usize = 0; // No memory restriction.
pub const DEFAULT_TLS_CERT_FILE: &str = "";
pub const DEFAULT_TLS_PRIVATE_KEY_FILE: &str = "";
pub const DEFAULT_STORE_ENDPOINT: &str = "";
pub const DEFAULT_STORE_BUCKET: &str = "";
pub const DEFAULT_STORE_REGION: &str = "";
pub const DEFAULT_STORE_SECRET_KEY: &str = "";
pub const DEFAULT_STORE_ACCESS_KEY: &str = "";
pub const DEFAULT_STORE_OPTIMIZER_MEMORY_POOL_SIZE: usize = 0;
pub const DEFAULT_HOST: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
pub const DEFAULT_PORT: NonZero<u16> = NonZero::new(6726).unwrap();
pub const DEFAULT_TLS_ENABLED: bool = false;
pub const DEFAULT_GZIP: bool = false;
pub const DEFAULT_API_KEY_ENABLED: bool = false;
pub const DEFAULT_CLEANUP_TIME_INTERVAL: u32 = 0;
pub const DEFAULT_CLEANUP_RETENTION_DURATION: u32 = 86400;
pub const DEFAULT_STORE_OPTIMIZER_TIME_INTERVAL: u32 = 0;
pub const DEFAULT_STORE_OPTIMIZER_MAX_CHUNK_SIZE: NonZero<usize> =
    NonZero::new(256 * 1024 * 1024).unwrap(); // 256 MiB
pub const DEFAULT_LOG_LEVEL: LogLevel = LogLevel::Warning;
pub const DEFAULT_LOG_FORMAT: LogFormat = LogFormat::Pretty;

/// Required and configurables parameters of mosaico.
#[derive(Debug)]
pub struct Params {
    /// Maximum incoming message size (in bytes) accepted by the gRPC protocol.
    /// The server's outgoing message size is fixed at [`GRPC_MSG_MAX_SIZE_BYTES`]
    /// regardless of this value, so lowering this param can never make previously-accepted
    /// data unretrievable.
    ///
    /// If you need to update this value be aware that it is usually
    /// smaller than [`Params::parquet_in_memory_encoding_buffer_size`].
    ///
    /// Defaults to 50 MB.
    pub grpc_max_decode_message_size: Param<usize>,

    /// Target message size (in bytes) used during data streaming. Mosaicod will try to
    /// aggregate a number of Arrow RecordBatches to create a sufficiently large
    /// message. If the resulting batch size exceeds the limit, it will be capped by
    /// [`Params::max_batch_size`].
    pub grpc_target_encode_message_size: Param<usize>,

    /// Maximum number of concurrent chunk queries during data catalog filtering.
    pub max_concurrent_chunk_queries: Param<NonZero<usize>>,

    /// Maximum number of elements unrolled when decomposing a list `eq`/`neq`
    /// filter into per-element scalar comparisons. Filters whose value array
    /// exceeds this limit are skipped to avoid generating pathologically large
    /// query expressions.
    ///
    /// Defaults to 1024.
    pub max_size_plain_list_eq: Param<NonZero<usize>>,

    /// The maximum number of concurrent encoding and serialization operations.
    ///
    /// This setting controls how many data batches can be processed and sent to the object
    /// store simultaneously. It is important to note that this does not limit the number
    /// of topics the server can handle; rather, it constrains the parallel execution of
    /// the encoding/serialization pipeline.
    ///
    /// Each operation runs in a dedicated thread to handle CPU-bound compression and
    /// I/O-bound storage tasks. This value should be tuned based on available RAM and CPU.
    /// Excessive parallelism may lead to scheduler thrashing or memory exhaustion.
    ///
    /// Defaults to `MOSAICOD_DEFAULT_PARALLELISM`.
    pub max_concurrent_writes: Param<NonZero<usize>>,

    /// Maximum batch size (number of elements inside an arrow record batch) used during data
    /// streaming
    ///
    /// Defaults to default data fusion batch size 8192.
    pub max_batch_size: Param<NonZero<usize>>,

    /// Sets the degree of parallelism.
    ///
    /// While this is typically detected automatically based on available hardware,
    /// this field allows for a manual override in environments where automatic
    /// detection might fail or be inaccurate.
    ///
    /// Default is computed at runtime based on the machine.
    pub default_parallelism: Param<NonZero<usize>>,

    /// Defines the amount of memory (in bytes) used by the query engine (DataFusion).
    /// Set this value to a number greater than 0 to enforce a hard limit
    /// on the memory allocated by the query engine. Use this setting if
    /// mosaicod encounters OOM (Out Of Memory) errors.
    ///
    /// Defaults to 0 (no limit).
    pub query_engine_memory_pool_size: Param<usize>,

    /// Defines the amount of memory (in bytes) used by the store optimizer (DataFusion).
    /// Set this value to a number greater than 0 to enforce a hard limit
    /// on the memory allocated. Use this setting if mosaicod encounters OOM (Out Of Memory) errors.
    ///
    /// Defaults to 0 (no limit).
    pub store_optimizer_memory_pool_size: Param<usize>,

    /// Minimum interval, in seconds, between store optimization runs. `0` runs once.
    pub store_optimizer_time_interval: Param<u32>,

    /// Maximum size (in bytes) for an output chunk after optimization.
    pub store_optimizer_max_chunk_size: Param<NonZero<usize>>,

    /// Minimum interval, in seconds, between cleanup runs. `0` runs once.
    pub cleanup_time_interval: Param<u32>,

    /// Maximum period, in seconds, an obsolete file is kept in the store before deletion.
    pub cleanup_retention_duration: Param<u32>,

    /// Size (in bytes) of the in-memory buffer used for encoding parquet data.
    ///
    /// Defaults to 75 MB.
    pub parquet_in_memory_encoding_buffer_size: Param<usize>,

    /// Log verbosity level. Note: if `RUST_LOG` is set, it takes priority over this
    /// (see [`mosaicod_core::types::init_logger`]).
    pub log_level: Param<LogLevel>,

    /// Log output format.
    pub log_format: Param<LogFormat>,

    /// Host address the server listens on.
    pub host: Param<IpAddr>,

    /// Port the server listens on.
    pub port: Param<NonZero<u16>>,

    /// Whether TLS is enabled for the server. Requires [`Params::tls_certificate_file`]
    /// and [`Params::tls_private_key_file`] to be set.
    pub tls_enabled: Param<bool>,

    /// Path of the `cert.pem` file used as TLS certificate.
    pub tls_certificate_file: Param<String>,

    /// Path of the `key.pem` file used as private key for TLS.
    pub tls_private_key_file: Param<String>,

    /// Whether gzip compression is enabled for gRPC requests and responses.
    pub gzip_enabled: Param<bool>,

    /// Whether API key enforcement is enabled. See command `mosaicod api-key`.
    pub api_key_enabled: Param<bool>,

    /// Database URL, without credentials (e.g. `postgresql://host:port/dbname`).
    pub db_url: Param<String>,

    pub db_user: Param<String>,

    /// May also be set via `MOSAICOD_DB_PASSWORD_FILE` (see [`Param::optional_or_file`]).
    pub db_password: Param<String, Hidden>,

    /// Maximum number of database connections in the pool.
    pub max_db_connections: Param<NonZero<u32>>,

    pub store_endpoint: Param<String>,
    pub store_bucket: Param<String>,

    /// Region of the S3-compatible object store. When empty, the object store client
    /// default is used. Ignored by the local filesystem backend.
    pub store_region: Param<String>,

    /// May also be set via `MOSAICOD_STORE_SECRET_KEY_FILE` (see [`Param::optional_or_file`]).
    pub store_secret_key: Param<String, Hidden>,

    pub store_access_key: Param<String>,
}

impl Params {
    pub(super) fn validate(&self) -> Result<(), error::Error> {
        if self.grpc_max_decode_message_size.value < GRPC_MSG_MIN_SIZE_BYTES
            || self.grpc_max_decode_message_size.value > GRPC_MSG_MAX_SIZE_BYTES
        {
            let err_msg = format!(
                "must be in the range: [{}, {}]",
                GRPC_MSG_MIN_SIZE_BYTES, GRPC_MSG_MAX_SIZE_BYTES
            );

            Err(error::Error::invalid_configuration(
                self.grpc_max_decode_message_size.env.clone(),
                err_msg,
            ))?;
        }

        if self.grpc_target_encode_message_size.value == 0
            || self.grpc_target_encode_message_size.value > GRPC_MSG_MAX_SIZE_BYTES
        {
            let err_msg = format!("must be in the range: (0, {}]", GRPC_MSG_MAX_SIZE_BYTES);

            Err(error::Error::invalid_configuration(
                self.grpc_target_encode_message_size.env.clone(),
                err_msg,
            ))?;
        }

        Ok(())
    }
}

/// mosaicod's own parameters, built by [`super::DefaultConfig`].
pub fn params() -> &'static Params {
    crate::get::<Params>()
}

/// Returns mosaicod version.
pub fn version() -> String {
    let mut version = env!("CARGO_PKG_VERSION").to_owned();
    if cfg!(debug_assertions) {
        version.push_str("-devel");
    }
    version
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::Env;
    use mosaicod_core::error::ErrorKind;

    fn param<T, V>(value: T) -> Param<T, V>
    where
        T: std::str::FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Display,
        V: crate::param::ParamVisibility,
    {
        Param::optional("TEST_ENV_VAR", &Env::ignored(), None, value).unwrap()
    }

    /// Builds a `Params` instance that passes `validate()`, so individual
    /// fields can be overridden to exercise a single validation rule at a time.
    fn valid_params() -> Params {
        Params {
            grpc_max_decode_message_size: param(GRPC_MSG_MIN_SIZE_BYTES),
            grpc_target_encode_message_size: param(DEFAULT_GRPC_TARGET_ENCODE_MESSAGE_SIZE),
            max_concurrent_chunk_queries: param(DEFAULT_MAX_CONCURRENT_CHUNK_QUERIES),
            max_size_plain_list_eq: param(DEFAULT_MAX_SIZE_PLAIN_LIST_EQ),
            max_concurrent_writes: param(NonZero::<usize>::MIN),
            max_batch_size: param(DEFAULT_MAX_BATCH_SIZE),
            default_parallelism: param(NonZero::<usize>::MIN),
            query_engine_memory_pool_size: param(DEFAULT_QUERY_ENGINE_MEMORY_POOL_SIZE),
            parquet_in_memory_encoding_buffer_size: param(
                DEFAULT_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE,
            ),
            log_level: param(DEFAULT_LOG_LEVEL),
            log_format: param(DEFAULT_LOG_FORMAT),
            host: param(DEFAULT_HOST),
            port: param(DEFAULT_PORT),
            tls_enabled: param(DEFAULT_TLS_ENABLED),
            tls_certificate_file: param(DEFAULT_TLS_CERT_FILE.to_owned()),
            tls_private_key_file: param(DEFAULT_TLS_PRIVATE_KEY_FILE.to_owned()),
            gzip_enabled: param(DEFAULT_GZIP),
            api_key_enabled: param(DEFAULT_API_KEY_ENABLED),
            db_url: param("".to_owned()),
            db_user: param("".to_owned()),
            db_password: param("".to_owned()),
            max_db_connections: param(DEFAULT_MAX_DB_CONNECTIONS),
            store_endpoint: param(DEFAULT_STORE_ENDPOINT.to_owned()),
            store_bucket: param(DEFAULT_STORE_BUCKET.to_owned()),
            store_region: param(DEFAULT_STORE_REGION.to_owned()),
            store_secret_key: param(DEFAULT_STORE_SECRET_KEY.to_owned()),
            store_access_key: param(DEFAULT_STORE_ACCESS_KEY.to_owned()),
            store_optimizer_memory_pool_size: param(DEFAULT_STORE_OPTIMIZER_MEMORY_POOL_SIZE),
            store_optimizer_time_interval: param(DEFAULT_STORE_OPTIMIZER_TIME_INTERVAL),
            store_optimizer_max_chunk_size: param(DEFAULT_STORE_OPTIMIZER_MAX_CHUNK_SIZE),
            cleanup_time_interval: param(DEFAULT_CLEANUP_TIME_INTERVAL),
            cleanup_retention_duration: param(DEFAULT_CLEANUP_RETENTION_DURATION),
        }
    }

    #[test]
    fn validate_accepts_valid_params() {
        assert!(valid_params().validate().is_ok());
    }

    #[test]
    fn validate_rejects_grpc_max_decode_message_size_below_min() {
        let mut params = valid_params();
        params.grpc_max_decode_message_size = param(GRPC_MSG_MIN_SIZE_BYTES - 1);

        let err = params.validate().unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidConfiguration(_)));
    }

    #[test]
    fn validate_rejects_grpc_max_decode_message_size_above_max() {
        let mut params = valid_params();
        params.grpc_max_decode_message_size = param(GRPC_MSG_MAX_SIZE_BYTES + 1);

        let err = params.validate().unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidConfiguration(_)));
    }

    #[test]
    fn validate_accepts_grpc_max_decode_message_size_at_bounds() {
        let mut params = valid_params();

        params.grpc_max_decode_message_size = param(GRPC_MSG_MIN_SIZE_BYTES);
        assert!(params.validate().is_ok());

        params.grpc_max_decode_message_size = param(GRPC_MSG_MAX_SIZE_BYTES);
        assert!(params.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_grpc_target_encode_message_size() {
        let mut params = valid_params();
        params.grpc_target_encode_message_size = param(0);

        let err = params.validate().unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidConfiguration(_)));
    }

    #[test]
    fn validate_rejects_grpc_target_encode_message_size_above_max() {
        let mut params = valid_params();
        params.grpc_target_encode_message_size = param(GRPC_MSG_MAX_SIZE_BYTES + 1);

        let err = params.validate().unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidConfiguration(_)));
    }

    #[test]
    fn validate_accepts_grpc_target_encode_message_size_at_max() {
        let mut params = valid_params();
        params.grpc_target_encode_message_size = param(GRPC_MSG_MAX_SIZE_BYTES);

        assert!(params.validate().is_ok());
    }
}
