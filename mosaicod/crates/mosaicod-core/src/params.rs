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
    /// If you need to update this value be aware that this value is tipically
    /// smaller than [`Params::parquet_in_memory_encoding_buffer_size`].
    ///
    /// Defaults to 50 MB.
    pub max_grpc_message_size: Param<usize>,

    /// Target message size (in bytes) used during data streaming. Mosaicod will try to
    /// aggregate a number of Arrow RecordBatches to create a sufficiently large
    /// message. If the resulting batch size exceeds the limit, it will be capped by
    /// [`Params::max_batch_size`].
    ///
    /// Defaults to 25MB.
    pub target_message_size: Param<usize>,

    /// Maximum number of concurrent chunk queries during data catalog filtering.
    pub max_concurrent_chunk_queries: Param<usize>,

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

    /// Size (in bytes) of the in-memory buffer used for encoding parquet data.
    ///
    /// Default to 75 MB
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

    let ev = Params {
        // general
        max_grpc_message_size: Param::optional("MOSAICOD_MAX_GRPC_MESSAGE_SIZE", 50 * 1_000_000),
        target_message_size: Param::optional("MOSAICOD_TARGET_MESSAGE_SIZE", 25 * 1_000_000),
        max_concurrent_chunk_queries: Param::optional("MOSAICOD_MAX_CONCURRENT_CHUNK_QUERIES", 4),
        max_db_connections: Param::optional("MOSAICOD_MAX_DB_CONNECTIONS", 10),
        max_concurrent_writes: Param::optional(
            "MOSAICOD_MAX_CONCURRENT_WRITES",
            default_parallelism,
        ),
        default_parallelism: Param::optional("MOSAICOD_DEFAULT_PARALLELISM", default_parallelism),
        parquet_in_memory_encoding_buffer_size: Param::optional(
            "MOSAICOD_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE",
            75 * 1_000_000,
        ),
        max_batch_size: Param::optional("MOSAICOD_MAX_BATCH_SIZE", 8192),
        query_engine_memory_pool_size: Param::optional("MOSAICOD_QUERY_ENGINE_MEMORY_POOL_SIZE", 0),

        // tls
        tls_certificate_file: Param::optional("MOSAICOD_TLS_CERT_FILE", "".to_owned()),
        tls_private_key_file: Param::optional("MOSAICOD_TLS_PRIVATE_KEY_FILE", "".to_owned()),

        // database
        db_url: if config.skip_db_url {
            Param::default()
        } else {
            Param::required("MOSAICOD_DB_URL")?
        },

        // store
        store_endpoint: Param::optional("MOSAICOD_STORE_ENDPOINT", "".to_owned()),
        store_bucket: Param::optional("MOSAICOD_STORE_BUCKET", "".to_owned()),
        store_secret_key: Param::optional("MOSAICOD_STORE_SECRET_KEY", "".to_owned()),
        store_access_key: Param::optional("MOSAICOD_STORE_ACCESS_KEY", "".to_owned()),
    };

    let _ = ENV.set(ev);

    Ok(())
}

static ENV: OnceLock<Params> = OnceLock::new();

pub fn params() -> &'static Params {
    ENV.get().expect("paramenters not initialized, plase call `params::load_params_from_env()` before accessing an env variable.")
}

/// Returns mosaicod version.
pub fn version() -> String {
    let mut version = env!("CARGO_PKG_VERSION").to_owned();
    if cfg!(debug_assertions) {
        version.push_str("-devel");
    }
    version
}
