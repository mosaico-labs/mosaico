//! Module containing several parameters used across the codebase
//!
//! These parameter can be either constants or configurable via environment variables
//! but they are not expected to change during runtime.
//! For retrieving parameters that can be configured during startup (with env variables),
//! see the [`load_configurables_from_env`] function and the [`configurables`] accessor.

use super::error;
use std::marker::PhantomData;

/// Header name for client requests
pub const MOSAICO_API_KEY_HEADER: &str = "mosaico-api-key-token";

/// Defines the name of the index timestamp column in the arrow schema
pub const ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP: &str = "timestamp_ns";

/// Defines schema name for mosaico resources
pub const MOSAICO_URL_SCHEMA: &str = "mosaico";

/// Internal resolution for floating point comparisons
pub const EPSILON: f64 = 1.0e-06;

pub const MAX_BUFFERED_FUTURES: usize = 8;

// MIN/MAX values admissible for grpc message size.
pub const GRPC_MSG_MIN_SIZE_BYTES: usize = 4 * 1024 * 1024; // 4MB. Default grpc message size.
pub const GRPC_MSG_MAX_SIZE_BYTES: usize = 128 * 1024 * 1024; // 128MB

// Default values for Params.
pub const DEFAULT_MAX_GRPC_MESSAGE_SIZE: usize = 50 * 1_000_000; // 50MB
pub const DEFAULT_TARGET_MESSAGE_SIZE: usize = DEFAULT_MAX_GRPC_MESSAGE_SIZE / 2; // 25MB
pub const DEFAULT_MAX_CONCURRENT_CHUNK_QUERIES: usize = 4;
pub const DEFAULT_MAX_SIZE_PLAIN_LIST_EQ: usize = 1024;
pub const DEFAULT_MAX_DB_CONNECTIONS: u32 = 19;
pub const DEFAULT_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE: usize = 70 * 1_000_000;
pub const DEFAULT_MAX_BATCH_SIZE: usize = 8192;
pub const DEFAULT_QUERY_ENGINE_MEMORY_POOL_SIZE: usize = 0; // No memory restriction.
pub const DEFAULT_TLS_CERT_FILE: &str = "";
pub const DEFAULT_TLS_PRIVATE_KEY_FILE: &str = "";
pub const DEFAULT_STORE_ENDPOINT: &str = "";
pub const DEFAULT_STORE_BUCKET: &str = "";
pub const DEFAULT_STORE_SECRET_KEY: &str = "";
pub const DEFAULT_STORE_ACCESS_KEY: &str = "";
pub const DEFAULT_STORE_OPTIMIZER_MEMORY_POOL_SIZE: usize = 0;

/// Module containing several file extensions
pub mod ext {
    /// Json file extension
    pub const JSON: &str = "json";
    pub const PARQUET: &str = "parquet";
}

use std::{env, str::FromStr, sync::OnceLock};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unable to parse variable `{0}`")]
    UnableToParse(String),
    #[error("variable `{0}` missing: {1}.")]
    RetrieveError(String, String),
}

/// Marker trait for parameters visibility
pub trait ParamVisibility {}

/// Marker used to specify that a parameers needs to be hidden from prints
#[derive(Default)]
pub struct Hidden;
impl ParamVisibility for Hidden {}

/// Marker for default parameter visibility
#[derive(Default)]
pub struct Plain;
impl ParamVisibility for Plain {}

#[derive(Default)]
pub struct Param<T, V = Plain>
where
    V: ParamVisibility,
{
    /// Name of the environment variable
    pub env: String,

    /// Value
    pub value: T,

    _visibility: PhantomData<V>,
}

impl<T, V> Param<T, V>
where
    V: ParamVisibility,
{
    pub fn optional(name: &str, default: T) -> Self
    where
        T: std::str::FromStr,
        <T as FromStr>::Err: std::fmt::Debug,
    {
        let value = match env::var(name) {
            Ok(value) => value
                .parse()
                .unwrap_or_else(|_| panic!("unable to parse variable `{}`", name)),
            Err(_) => default,
        };

        Self {
            value,
            env: name.to_owned(),
            _visibility: PhantomData,
        }
    }

    pub fn required(name: &str) -> error::PublicResult<Param<T, V>>
    where
        T: std::str::FromStr,
        <T as FromStr>::Err: std::fmt::Debug,
    {
        let value = env::var(name)
            .map_err(|e| error::Error::invalid_configuration(name.to_owned(), e.to_string()))?;

        let t = value.parse().map_err(|_| {
            error::Error::invalid_configuration(name.into(), "unable to parse".to_owned())
        })?;

        Ok(Self {
            value: t,
            env: name.to_owned(),
            _visibility: PhantomData,
        })
    }
}

impl<T> std::fmt::Debug for Param<T, Hidden>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "*********")
    }
}

impl<T> std::fmt::Debug for Param<T, Plain>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.value)
    }
}

/// Required and configurables parameters of mosaico
#[derive(Debug)]
pub struct Params {
    /// Maximum allowed message size (in bytes) by the gRPC protocol.
    ///
    /// If you need to update this value be aware that it is usually
    /// smaller than [`Params::parquet_in_memory_encoding_buffer_size`].
    ///
    /// Defaults to 50 MB.
    pub max_grpc_message_size: Param<usize>,

    /// Target message size (in bytes) used during data streaming. Mosaicod will try to
    /// aggregate a number of Arrow RecordBatches to create a sufficiently large
    /// message. If the resulting batch size exceeds the limit, it will be capped by
    /// [`Params::max_batch_size`].
    ///
    /// This param does not have a corresponding ENV var associated,
    /// but it is directly set to half of [`Params::max_grpx_message_size`] instead.
    pub target_message_size: usize,

    /// Maximum number of concurrent chunk queries during data catalog filtering.
    pub max_concurrent_chunk_queries: Param<usize>,

    /// Maximum number of elements unrolled when decomposing a list `eq`/`neq`
    /// filter into per-element scalar comparisons. Filters whose value array
    /// exceeds this limit are skipped to avoid generating pathologically large
    /// query expressions.
    ///
    /// Defaults to 1024.
    pub max_size_plain_list_eq: Param<usize>,

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
    pub max_concurrent_writes: Param<usize>,

    /// Maximum batch size (number of elements inside a arrow record batch) used during data
    /// streaming
    ///
    /// Must be greater than 0.
    ///
    /// Defaults to default data fusion batch size 8192.
    pub max_batch_size: Param<usize>,

    /// Sets the degree of parallelism.
    ///
    /// While this is typically detected automatically based on available hardware,
    /// this field allows for a manual override in environments where automatic
    /// detection might fail or be inaccurate.
    ///
    /// Default is computed at runtime based on the machine.
    pub default_parallelism: Param<usize>,

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

    /// Size (in bytes) of the in-memory buffer used for encoding parquet data.
    ///
    /// Defaults to 75 MB
    pub parquet_in_memory_encoding_buffer_size: Param<usize>,

    /// Path of the `cert.pem` file used as TLS certificate
    pub tls_certificate_file: Param<String>,

    /// Path of the `key.pem` file used as private key for TLS
    pub tls_private_key_file: Param<String>,

    pub db_url: Param<String>,

    /// Maximum number of database connections in the pool
    pub max_db_connections: Param<u32>,

    pub store_endpoint: Param<String>,
    pub store_bucket: Param<String>,
    pub store_secret_key: Param<String, Hidden>,
    pub store_access_key: Param<String>,
}

impl Params {
    fn validate(&self) -> Result<(), error::Error> {
        if self.max_batch_size.value == 0 {
            Err(error::Error::invalid_configuration(
                self.max_batch_size.env.clone(),
                "must be greater than 0".to_owned(),
            ))?;
        }

        if self.max_grpc_message_size.value < GRPC_MSG_MIN_SIZE_BYTES
            || self.max_grpc_message_size.value > GRPC_MSG_MAX_SIZE_BYTES
        {
            let err_msg = format!(
                "must be in the range: [{}, {}]",
                GRPC_MSG_MIN_SIZE_BYTES, GRPC_MSG_MAX_SIZE_BYTES
            );

            Err(error::Error::invalid_configuration(
                self.max_grpc_message_size.env.clone(),
                err_msg,
            ))?;
        }

        Ok(())
    }
}

/// Options for loading parameters from environment variables
pub struct ParamsLoadOptions {
    /// Avoid parsing `MOSICOD_DB_URL` env variable
    pub skip_db_url: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for ParamsLoadOptions {
    fn default() -> Self {
        Self { skip_db_url: false }
    }
}

impl ParamsLoadOptions {
    /// Load parameters with options suitable for testing
    ///
    /// This will skip the loading of database URL in the environment variables.
    pub fn testing() -> Self {
        Self { skip_db_url: true }
    }
}

pub fn load_params_from_env(config: ParamsLoadOptions) -> error::PublicResult<()> {
    let default_parallelism = std::thread::available_parallelism()
        .expect("Unable to detect default parallelism, please define MOSAICOD_DEFAULT_PARALLELISM")
        .get();

    let max_grpc_message_size = Param::optional(
        "MOSAICOD_MAX_GRPC_MESSAGE_SIZE",
        DEFAULT_MAX_GRPC_MESSAGE_SIZE,
    );
    let target_message_size = max_grpc_message_size.value / 2;

    let ev = Params {
        // general
        max_grpc_message_size,
        target_message_size,
        max_concurrent_chunk_queries: Param::optional(
            "MOSAICOD_MAX_CONCURRENT_CHUNK_QUERIES",
            DEFAULT_MAX_CONCURRENT_CHUNK_QUERIES,
        ),
        max_size_plain_list_eq: Param::optional(
            "MOSAICOD_MAX_SIZE_PLAIN_LIST_EQ",
            DEFAULT_MAX_SIZE_PLAIN_LIST_EQ,
        ),
        max_db_connections: Param::optional(
            "MOSAICOD_MAX_DB_CONNECTIONS",
            DEFAULT_MAX_DB_CONNECTIONS,
        ),
        max_concurrent_writes: Param::optional(
            "MOSAICOD_MAX_CONCURRENT_WRITES",
            default_parallelism,
        ),
        default_parallelism: Param::optional("MOSAICOD_DEFAULT_PARALLELISM", default_parallelism),
        parquet_in_memory_encoding_buffer_size: Param::optional(
            "MOSAICOD_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE",
            DEFAULT_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE,
        ),
        max_batch_size: Param::optional("MOSAICOD_MAX_BATCH_SIZE", DEFAULT_MAX_BATCH_SIZE),
        query_engine_memory_pool_size: Param::optional(
            "MOSAICOD_QUERY_ENGINE_MEMORY_POOL_SIZE",
            DEFAULT_QUERY_ENGINE_MEMORY_POOL_SIZE,
        ),

        // tls
        tls_certificate_file: Param::optional(
            "MOSAICOD_TLS_CERT_FILE",
            DEFAULT_TLS_CERT_FILE.to_owned(),
        ),
        tls_private_key_file: Param::optional(
            "MOSAICOD_TLS_PRIVATE_KEY_FILE",
            DEFAULT_TLS_PRIVATE_KEY_FILE.to_owned(),
        ),

        // database
        db_url: if config.skip_db_url {
            Param::default()
        } else {
            Param::required("MOSAICOD_DB_URL")?
        },

        // store
        store_endpoint: Param::optional(
            "MOSAICOD_STORE_ENDPOINT",
            DEFAULT_STORE_ENDPOINT.to_owned(),
        ),
        store_bucket: Param::optional("MOSAICOD_STORE_BUCKET", DEFAULT_STORE_BUCKET.to_owned()),
        store_secret_key: Param::optional(
            "MOSAICOD_STORE_SECRET_KEY",
            DEFAULT_STORE_SECRET_KEY.to_owned(),
        ),
        store_access_key: Param::optional(
            "MOSAICOD_STORE_ACCESS_KEY",
            DEFAULT_STORE_ACCESS_KEY.to_owned(),
        ),

        // store optimizer
        store_optimizer_memory_pool_size: Param::optional(
            "MOSAICOD_STORE_OPTIMIZER_MEMORY_POOL_SIZE",
            DEFAULT_STORE_OPTIMIZER_MEMORY_POOL_SIZE,
        ),
    };

    ev.validate()?;

    let _ = ENV.set(ev);

    Ok(())
}

static ENV: OnceLock<Params> = OnceLock::new();

pub fn params() -> &'static Params {
    ENV.get().expect("parameters not initialized, please call `params::load_params_from_env()` before accessing an env variable.")
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
    use crate::error::ErrorKind;

    fn param<T>(value: T) -> Param<T> {
        Param {
            env: "TEST_ENV_VAR".to_owned(),
            value,
            _visibility: PhantomData,
        }
    }

    fn param_hidden<T>(value: T) -> Param<T, Hidden> {
        Param {
            env: "TEST_ENV_VAR".to_owned(),
            value,
            _visibility: PhantomData,
        }
    }

    /// Builds a `Params` instance that passes `validate()`, so individual
    /// fields can be overridden to exercise a single validation rule at a time.
    fn valid_params() -> Params {
        Params {
            max_grpc_message_size: param(GRPC_MSG_MIN_SIZE_BYTES),
            target_message_size: GRPC_MSG_MIN_SIZE_BYTES / 2,
            max_concurrent_chunk_queries: param(DEFAULT_MAX_CONCURRENT_CHUNK_QUERIES),
            max_size_plain_list_eq: param(DEFAULT_MAX_SIZE_PLAIN_LIST_EQ),
            max_concurrent_writes: param(1),
            max_batch_size: param(DEFAULT_MAX_BATCH_SIZE),
            default_parallelism: param(1),
            query_engine_memory_pool_size: param(DEFAULT_QUERY_ENGINE_MEMORY_POOL_SIZE),
            parquet_in_memory_encoding_buffer_size: param(
                DEFAULT_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE,
            ),
            tls_certificate_file: param(DEFAULT_TLS_CERT_FILE.to_owned()),
            tls_private_key_file: param(DEFAULT_TLS_PRIVATE_KEY_FILE.to_owned()),
            db_url: param("".to_owned()),
            max_db_connections: param(DEFAULT_MAX_DB_CONNECTIONS),
            store_endpoint: param(DEFAULT_STORE_ENDPOINT.to_owned()),
            store_bucket: param(DEFAULT_STORE_BUCKET.to_owned()),
            store_secret_key: param_hidden(DEFAULT_STORE_SECRET_KEY.to_owned()),
            store_access_key: param(DEFAULT_STORE_ACCESS_KEY.to_owned()),
            store_optimizer_memory_pool_size: param(DEFAULT_STORE_OPTIMIZER_MEMORY_POOL_SIZE),
        }
    }

    #[test]
    fn validate_accepts_valid_params() {
        assert!(valid_params().validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_max_batch_size() {
        let mut params = valid_params();
        params.max_batch_size = param(0);

        let err = params.validate().unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidConfiguration(_)));
    }

    #[test]
    fn validate_accepts_max_batch_size_of_one() {
        let mut params = valid_params();
        params.max_batch_size = param(1);

        assert!(params.validate().is_ok());
    }

    #[test]
    fn validate_rejects_max_grpc_message_size_below_min() {
        let mut params = valid_params();
        params.max_grpc_message_size = param(GRPC_MSG_MIN_SIZE_BYTES - 1);

        let err = params.validate().unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidConfiguration(_)));
    }

    #[test]
    fn validate_rejects_max_grpc_message_size_above_max() {
        let mut params = valid_params();
        params.max_grpc_message_size = param(GRPC_MSG_MAX_SIZE_BYTES + 1);

        let err = params.validate().unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidConfiguration(_)));
    }

    #[test]
    fn validate_accepts_max_grpc_message_size_at_bounds() {
        let mut params = valid_params();

        params.max_grpc_message_size = param(GRPC_MSG_MIN_SIZE_BYTES);
        assert!(params.validate().is_ok());

        params.max_grpc_message_size = param(GRPC_MSG_MAX_SIZE_BYTES);
        assert!(params.validate().is_ok());
    }
}
