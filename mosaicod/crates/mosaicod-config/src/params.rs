//! Runtime configuration registry: [`Params`], populated by merging CLI args (by the
//! caller), environment variables, [`crate::ConfigFile`], and hard-coded defaults, in that
//! precedence order.

use crate::config;
use mosaicod_core::error;
use mosaicod_core::types::{LogFormat, LogLevel};
use std::marker::PhantomData;
use std::net::{IpAddr, Ipv4Addr};
use std::{env, str::FromStr, sync::OnceLock};

// MIN/MAX values admissible for grpc message size (incoming/outgoing).

pub const GRPC_MSG_MIN_SIZE_BYTES: usize = 4 * 1024 * 1024; // 4MiB. Default grpc message size.
pub const GRPC_MSG_MAX_SIZE_BYTES: usize = 512 * 1024 * 1024; // 512MiB

// Default values for Params.
pub const DEFAULT_MAX_GRPC_DECODE_MESSAGE_SIZE: usize = 50 * 1024 * 1024; // 50MiB
pub const DEFAULT_TARGET_GRPC_ENCODE_MESSAGE_SIZE: usize = 25 * 1024 * 1024; // 25MiB
pub const DEFAULT_MAX_CONCURRENT_CHUNK_QUERIES: usize = 4;
pub const DEFAULT_MAX_SIZE_PLAIN_LIST_EQ: usize = 1024;
pub const DEFAULT_MAX_DB_CONNECTIONS: u32 = 19;
pub const DEFAULT_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE: usize = 70 * 1024 * 1024; // 70MiB
pub const DEFAULT_MAX_BATCH_SIZE: usize = 8192;
pub const DEFAULT_QUERY_ENGINE_MEMORY_POOL_SIZE: usize = 0; // No memory restriction.
pub const DEFAULT_TLS_CERT_FILE: &str = "";
pub const DEFAULT_TLS_PRIVATE_KEY_FILE: &str = "";
pub const DEFAULT_STORE_ENDPOINT: &str = "";
pub const DEFAULT_STORE_BUCKET: &str = "";
pub const DEFAULT_STORE_SECRET_KEY: &str = "";
pub const DEFAULT_STORE_ACCESS_KEY: &str = "";
pub const DEFAULT_STORE_OPTIMIZER_MEMORY_POOL_SIZE: usize = 0;
pub const DEFAULT_HOST: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
pub const DEFAULT_PORT: u16 = 6726;
pub const DEFAULT_TLS_ENABLED: bool = false;
pub const DEFAULT_GZIP: bool = false;
pub const DEFAULT_API_KEY_ENABLED: bool = false;
pub const DEFAULT_CLEANUP_TIME_INTERVAL: u32 = 0;
pub const DEFAULT_CLEANUP_RETENTION_DURATION: u32 = 86400;
pub const DEFAULT_STORE_OPTIMIZER_TIME_INTERVAL: u32 = 0;
pub const DEFAULT_STORE_OPTIMIZER_MAX_CHUNK_SIZE: usize = 256 * 1024 * 1024; // 256 MiB
pub const DEFAULT_LOG_LEVEL: LogLevel = LogLevel::Warning;
pub const DEFAULT_LOG_FORMAT: LogFormat = LogFormat::Pretty;

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
    /// Precedence: `<NAME>` env var, then `file_value` (config file), then `default`.
    /// If `ignore_env` is `true`, the environment tier is skipped entirely, as if
    /// `<NAME>` were unset.
    pub fn optional(env_name: &str, ignore_env: bool, file_value: Option<T>, default: T) -> Self
    where
        T: std::str::FromStr,
        <T as FromStr>::Err: std::fmt::Debug,
    {
        let value = match env_var(env_name, ignore_env) {
            Ok(value) => value
                .parse()
                .unwrap_or_else(|_| panic!("unable to parse variable `{}`", env_name)),
            Err(_) => file_value.unwrap_or(default),
        };

        Self {
            value,
            env: env_name.to_owned(),
            _visibility: PhantomData,
        }
    }

    /// Precedence: `<NAME>` env var, then `file_value` (config file); errors if neither is set.
    /// If `ignore_env` is `true`, the environment tier is skipped entirely, as if
    /// `<NAME>` were unset.
    pub fn required(
        env_name: &str,
        ignore_env: bool,
        file_value: Option<T>,
    ) -> error::PublicResult<Param<T, V>>
    where
        T: std::str::FromStr,
        <T as FromStr>::Err: std::fmt::Debug,
    {
        let value = match env_var(env_name, ignore_env) {
            Ok(raw) => raw.parse().map_err(|_| {
                error::Error::invalid_configuration(
                    env_name.to_owned(),
                    "unable to parse".to_owned(),
                )
            })?,
            Err(_) => file_value.ok_or_else(|| {
                error::Error::invalid_configuration(env_name.to_owned(), "missing".to_owned())
            })?,
        };

        Ok(Self {
            value,
            env: env_name.to_owned(),
            _visibility: PhantomData,
        })
    }

    /// Like [`Param::optional`], but the value may instead be provided via a
    /// `<NAME>_FILE` environment variable pointing at a file containing it, or via
    /// `file_path` (a config-file-provided path to such a file).
    /// At most one of `<NAME>` or `<NAME>_FILE` may be set; setting both is an error.
    /// Precedence: `<NAME>`/`<NAME>_FILE` env, then `file_path` (config file), then `default`.
    /// If `ignore_env` is `true`, both env forms are skipped entirely, as if neither
    /// `<NAME>` nor `<NAME>_FILE` were set.
    pub fn optional_or_file(
        env_name: &str,
        ignore_env: bool,
        file_path: Option<String>,
        default: T,
    ) -> error::PublicResult<Param<T, V>>
    where
        T: std::str::FromStr,
        <T as FromStr>::Err: std::fmt::Debug,
    {
        let raw = match resolve_plain_or_file(env_name, ignore_env)? {
            Some(raw) => Some(raw),
            None => file_path.map(|path| read_file_trimmed(&path)).transpose()?,
        };

        let value = match raw {
            Some(raw) => raw.parse().map_err(|_| {
                error::Error::invalid_configuration(
                    env_name.to_owned(),
                    "unable to parse".to_owned(),
                )
            })?,
            None => default,
        };

        Ok(Self {
            value,
            env: env_name.to_owned(),
            _visibility: PhantomData,
        })
    }

    /// Like [`Param::required`], but the value may instead be provided via a
    /// `<NAME>_FILE` environment variable pointing at a file containing it, or via
    /// `file_path` (a config-file-provided path to such a file).
    /// Exactly one of `<NAME>` or `<NAME>_FILE` may be set; setting both is an error.
    /// Precedence: `<NAME>`/`<NAME>_FILE` env, then `file_path` (config file); errors if
    /// none of these resolve to a value.
    /// If `ignore_env` is `true`, both env forms are skipped entirely, as if neither
    /// `<NAME>` nor `<NAME>_FILE` were set.
    pub fn required_or_file(
        env_name: &str,
        ignore_env: bool,
        file_path: Option<String>,
    ) -> error::PublicResult<Param<T, V>>
    where
        T: std::str::FromStr,
        <T as FromStr>::Err: std::fmt::Debug,
    {
        let raw = match resolve_plain_or_file(env_name, ignore_env)? {
            Some(raw) => raw,
            None => match file_path {
                Some(path) => read_file_trimmed(&path)?,
                None => Err(error::Error::invalid_configuration(
                    env_name.to_owned(),
                    format!("missing: set `{env_name}` or `{env_name}_FILE`"),
                ))?,
            },
        };

        let value = raw.parse().map_err(|_| {
            error::Error::invalid_configuration(env_name.to_owned(), "unable to parse".to_owned())
        })?;

        Ok(Self {
            value,
            env: env_name.to_owned(),
            _visibility: PhantomData,
        })
    }
}

/// Reads env var `name`, unless `ignore_env` is `true`, in which case it behaves as if
/// the variable were unset (used to bypass the environment tier entirely, e.g. for
/// deterministic tests — see [`ParamsLoadOptions::ignore_env`]).
fn env_var(name: &str, ignore_env: bool) -> Result<String, env::VarError> {
    if ignore_env {
        Err(env::VarError::NotPresent)
    } else {
        env::var(name)
    }
}

/// Reads a file's content and trims surrounding whitespace (e.g. a trailing newline).
fn read_file_trimmed(path: &str) -> error::PublicResult<String> {
    Ok(std::fs::read_to_string(path)
        .map_err(|e| error::Error::invalid_configuration(path.to_owned(), e.to_string()))?
        .trim()
        .to_owned())
}

/// Resolves a value that may be set either directly via `<NAME>` or indirectly via
/// `<NAME>_FILE` (a path to a file containing it, read and trailing-newline-trimmed).
/// Returns `Ok(None)` if neither is set. Setting both is an error. If `ignore_env` is
/// `true`, both forms are skipped entirely, as if neither were set.
fn resolve_plain_or_file(name: &str, ignore_env: bool) -> error::PublicResult<Option<String>> {
    let file_env = format!("{name}_FILE");

    let plain = env_var(name, ignore_env).ok();
    let file_path = env_var(&file_env, ignore_env).ok();

    match (plain, file_path) {
        (Some(_), Some(_)) => Err(error::Error::invalid_configuration(
            name.to_owned(),
            format!("`{name}` and `{file_env}` are mutually exclusive, set only one"),
        ))?,
        (Some(value), None) => Ok(Some(value)),
        (None, Some(path)) => Ok(Some(read_file_trimmed(&path)?)),
        (None, None) => Ok(None),
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
    /// Maximum incoming message size (in bytes) accepted by the gRPC protocol.
    /// The server's outgoing message size is fixed at [`GRPC_MSG_MAX_SIZE_BYTES`]
    /// regardless of this value, so lowering this param can never make previously-accepted
    /// data unretrievable.
    ///
    /// If you need to update this value be aware that it is usually
    /// smaller than [`Params::parquet_in_memory_encoding_buffer_size`].
    ///
    /// Defaults to 50 MB.
    pub max_grpc_decode_message_size: Param<usize>,

    /// Target message size (in bytes) used during data streaming. Mosaicod will try to
    /// aggregate a number of Arrow RecordBatches to create a sufficiently large
    /// message. If the resulting batch size exceeds the limit, it will be capped by
    /// [`Params::max_batch_size`].
    pub target_grpc_encode_message_size: Param<usize>,

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

    /// Minimum interval, in seconds, between store optimization runs. `0` runs once.
    pub store_optimizer_time_interval: Param<u32>,

    /// Maximum size (in bytes) for an output chunk after optimization.
    pub store_optimizer_max_chunk_size: Param<usize>,

    /// Minimum interval, in seconds, between cleanup runs. `0` runs once.
    pub cleanup_time_interval: Param<u32>,

    /// Maximum period, in seconds, an obsolete file is kept in the store before deletion.
    pub cleanup_retention_duration: Param<u32>,

    /// Size (in bytes) of the in-memory buffer used for encoding parquet data.
    ///
    /// Defaults to 75 MB
    pub parquet_in_memory_encoding_buffer_size: Param<usize>,

    /// Log verbosity level. Note: if `RUST_LOG` is set, it takes priority over this
    /// (see [`mosaicod_core::types::init_logger`]).
    pub log_level: Param<LogLevel>,

    /// Log output format
    pub log_format: Param<LogFormat>,

    /// Host address the server listens on
    pub host: Param<IpAddr>,

    /// Port the server listens on
    pub port: Param<u16>,

    /// Whether TLS is enabled for the server. Requires [`Params::tls_certificate_file`]
    /// and [`Params::tls_private_key_file`] to be set.
    pub tls_enabled: Param<bool>,

    /// Path of the `cert.pem` file used as TLS certificate
    pub tls_certificate_file: Param<String>,

    /// Path of the `key.pem` file used as private key for TLS
    pub tls_private_key_file: Param<String>,

    /// Whether gzip compression is enabled for gRPC requests and responses
    pub gzip_enabled: Param<bool>,

    /// Whether API key enforcement is enabled. See command `mosaicod api-key`.
    pub api_key_enabled: Param<bool>,

    /// Database URL, without credentials (e.g. `postgresql://host:port/dbname`)
    pub db_url: Param<String>,

    pub db_user: Param<String>,

    /// May also be set via `MOSAICOD_DB_PASSWORD_FILE` (see [`Param::optional_or_file`])
    pub db_password: Param<String, Hidden>,

    /// Maximum number of database connections in the pool
    pub max_db_connections: Param<u32>,

    pub store_endpoint: Param<String>,
    pub store_bucket: Param<String>,

    /// May also be set via `MOSAICOD_STORE_SECRET_KEY_FILE` (see [`Param::optional_or_file`])
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

        if self.max_grpc_decode_message_size.value < GRPC_MSG_MIN_SIZE_BYTES
            || self.max_grpc_decode_message_size.value > GRPC_MSG_MAX_SIZE_BYTES
        {
            let err_msg = format!(
                "must be in the range: [{}, {}]",
                GRPC_MSG_MIN_SIZE_BYTES, GRPC_MSG_MAX_SIZE_BYTES
            );

            Err(error::Error::invalid_configuration(
                self.max_grpc_decode_message_size.env.clone(),
                err_msg,
            ))?;
        }

        if self.target_grpc_encode_message_size.value == 0
            || self.target_grpc_encode_message_size.value > GRPC_MSG_MAX_SIZE_BYTES
        {
            let err_msg = format!("must be in the range: (0, {}]", GRPC_MSG_MAX_SIZE_BYTES);

            Err(error::Error::invalid_configuration(
                self.target_grpc_encode_message_size.env.clone(),
                err_msg,
            ))?;
        }

        Ok(())
    }
}

/// Options for loading parameters from environment variables
#[derive(Default)]
pub struct ParamsLoadOptions {
    /// Avoid requiring the `MOSAICOD_DB_*` env variables
    pub skip_db_config: bool,

    /// Skip the process-environment tier entirely for every field, resolving purely
    /// from `config_file` and hard-coded defaults. Useful for deterministic tests that
    /// shouldn't be affected by whatever `MOSAICOD_*` variables happen to be set in the
    /// developer's shell or CI.
    pub ignore_env: bool,

    /// Parsed config file (third precedence tier, below env vars). Defaults to
    /// "no config file loaded" (every field absent), which falls through unchanged.
    pub config_file: config::ConfigFile,
}

impl ParamsLoadOptions {
    /// Load parameters with options suitable for testing
    ///
    /// This will skip the loading of database connection settings from the environment variables.
    pub fn testing() -> Self {
        Self {
            skip_db_config: true,
            ignore_env: false,
            config_file: Default::default(),
        }
    }
}

/// Resolves and stores the global [`Params`], with precedence:
/// env var > `config.toml` > hard-coded default for each field (or, if `config.ignore_env` is set,
/// config file > default, bypassing the environment tier entirely).
///
/// Must be called once before [`params()`] is used. A second call within the same
/// process is a no-op: its result is silently discarded and the first-loaded `Params`
/// remains active for the rest of the process lifetime.
pub fn load_params(config: ParamsLoadOptions) -> error::PublicResult<()> {
    let cfg = &config.config_file;

    let default_parallelism = std::thread::available_parallelism()
        .expect("Unable to detect default parallelism, please define MOSAICOD_DEFAULT_PARALLELISM")
        .get();

    let ignore_env = config.ignore_env;

    let max_grpc_decode_message_size = Param::optional(
        "MOSAICOD_MAX_GRPC_DECODE_MESSAGE_SIZE",
        ignore_env,
        cfg.server.grpc.max_decode_message_size,
        DEFAULT_MAX_GRPC_DECODE_MESSAGE_SIZE,
    );
    let target_grpc_encode_message_size = Param::optional(
        "MOSAICOD_TARGET_GRPC_ENCODE_MESSAGE_SIZE",
        ignore_env,
        cfg.server.grpc.target_encode_message_size,
        DEFAULT_TARGET_GRPC_ENCODE_MESSAGE_SIZE,
    );

    let ev = Params {
        // general
        max_grpc_decode_message_size,
        target_grpc_encode_message_size,
        max_concurrent_chunk_queries: Param::optional(
            "MOSAICOD_MAX_CONCURRENT_CHUNK_QUERIES",
            ignore_env,
            cfg.query_engine.max_concurrent_chunk_queries,
            DEFAULT_MAX_CONCURRENT_CHUNK_QUERIES,
        ),
        max_size_plain_list_eq: Param::optional(
            "MOSAICOD_MAX_SIZE_PLAIN_LIST_EQ",
            ignore_env,
            cfg.query_engine.max_size_plain_list_eq,
            DEFAULT_MAX_SIZE_PLAIN_LIST_EQ,
        ),
        max_db_connections: Param::optional(
            "MOSAICOD_MAX_DB_CONNECTIONS",
            ignore_env,
            cfg.database.max_connections,
            DEFAULT_MAX_DB_CONNECTIONS,
        ),
        max_concurrent_writes: Param::optional(
            "MOSAICOD_MAX_CONCURRENT_WRITES",
            ignore_env,
            cfg.server.max_concurrent_writes,
            default_parallelism,
        ),
        default_parallelism: Param::optional(
            "MOSAICOD_DEFAULT_PARALLELISM",
            ignore_env,
            cfg.server.default_parallelism,
            default_parallelism,
        ),
        parquet_in_memory_encoding_buffer_size: Param::optional(
            "MOSAICOD_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE",
            ignore_env,
            cfg.encoding.parquet_in_memory_encoding_buffer_size,
            DEFAULT_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE,
        ),
        max_batch_size: Param::optional(
            "MOSAICOD_MAX_BATCH_SIZE",
            ignore_env,
            cfg.server.max_batch_size,
            DEFAULT_MAX_BATCH_SIZE,
        ),
        query_engine_memory_pool_size: Param::optional(
            "MOSAICOD_QUERY_ENGINE_MEMORY_POOL_SIZE",
            ignore_env,
            cfg.query_engine.memory_pool_size,
            DEFAULT_QUERY_ENGINE_MEMORY_POOL_SIZE,
        ),

        // logging
        log_level: Param::optional(
            "MOSAICOD_LOG_LEVEL",
            ignore_env,
            cfg.logging.level,
            DEFAULT_LOG_LEVEL,
        ),
        log_format: Param::optional(
            "MOSAICOD_LOG_FORMAT",
            ignore_env,
            cfg.logging.format,
            DEFAULT_LOG_FORMAT,
        ),

        // server
        host: Param::optional("MOSAICOD_HOST", ignore_env, cfg.server.host, DEFAULT_HOST),
        port: Param::optional("MOSAICOD_PORT", ignore_env, cfg.server.port, DEFAULT_PORT),
        tls_enabled: Param::optional(
            "MOSAICOD_TLS_ENABLED",
            ignore_env,
            cfg.server.tls.enabled,
            DEFAULT_TLS_ENABLED,
        ),
        gzip_enabled: Param::optional(
            "MOSAICOD_GZIP_ENABLED",
            ignore_env,
            cfg.server.gzip_enabled,
            DEFAULT_GZIP,
        ),
        api_key_enabled: Param::optional(
            "MOSAICOD_API_KEY_ENABLED",
            ignore_env,
            cfg.server.api_key_enabled,
            DEFAULT_API_KEY_ENABLED,
        ),

        // tls
        tls_certificate_file: Param::optional(
            "MOSAICOD_TLS_CERT_FILE",
            ignore_env,
            cfg.server.tls.certificate_file.clone(),
            DEFAULT_TLS_CERT_FILE.to_owned(),
        ),
        tls_private_key_file: Param::optional(
            "MOSAICOD_TLS_PRIVATE_KEY_FILE",
            ignore_env,
            cfg.server.tls.private_key_file.clone(),
            DEFAULT_TLS_PRIVATE_KEY_FILE.to_owned(),
        ),

        // database
        db_url: if config.skip_db_config {
            Param::default()
        } else {
            Param::required("MOSAICOD_DB_URL", ignore_env, cfg.database.url.clone())?
        },
        db_user: if config.skip_db_config {
            Param::default()
        } else {
            // Some databases do not require to specify a user for the connection.
            Param::optional(
                "MOSAICOD_DB_USER",
                ignore_env,
                cfg.database.user.clone(),
                String::new(),
            )
        },
        db_password: if config.skip_db_config {
            Param::default()
        } else {
            // Some databases do not require to specify a password for the connection.
            Param::optional_or_file(
                "MOSAICOD_DB_PASSWORD",
                ignore_env,
                cfg.database.password_file.clone(),
                String::new(),
            )?
        },

        // store
        store_endpoint: Param::optional(
            "MOSAICOD_STORE_ENDPOINT",
            ignore_env,
            cfg.store.endpoint.clone(),
            DEFAULT_STORE_ENDPOINT.to_owned(),
        ),
        store_bucket: Param::optional(
            "MOSAICOD_STORE_BUCKET",
            ignore_env,
            cfg.store.bucket.clone(),
            DEFAULT_STORE_BUCKET.to_owned(),
        ),
        store_secret_key: Param::optional_or_file(
            "MOSAICOD_STORE_SECRET_KEY",
            ignore_env,
            cfg.store.secret_key_file.clone(),
            DEFAULT_STORE_SECRET_KEY.to_owned(),
        )?,
        store_access_key: Param::optional(
            "MOSAICOD_STORE_ACCESS_KEY",
            ignore_env,
            cfg.store.access_key.clone(),
            DEFAULT_STORE_ACCESS_KEY.to_owned(),
        ),

        // store optimizer
        store_optimizer_memory_pool_size: Param::optional(
            "MOSAICOD_STORE_OPTIMIZER_MEMORY_POOL_SIZE",
            ignore_env,
            cfg.store_optimizer.memory_pool_size,
            DEFAULT_STORE_OPTIMIZER_MEMORY_POOL_SIZE,
        ),
        store_optimizer_time_interval: Param::optional(
            "MOSAICOD_STORE_OPTIMIZER_TIME_INTERVAL",
            ignore_env,
            cfg.store_optimizer.time_interval,
            DEFAULT_STORE_OPTIMIZER_TIME_INTERVAL,
        ),
        store_optimizer_max_chunk_size: Param::optional(
            "MOSAICOD_STORE_OPTIMIZER_MAX_CHUNK_SIZE",
            ignore_env,
            cfg.store_optimizer.max_chunk_size,
            DEFAULT_STORE_OPTIMIZER_MAX_CHUNK_SIZE,
        ),

        // cleanup
        cleanup_time_interval: Param::optional(
            "MOSAICOD_CLEANUP_TIME_INTERVAL",
            ignore_env,
            cfg.cleanup.time_interval,
            DEFAULT_CLEANUP_TIME_INTERVAL,
        ),
        cleanup_retention_duration: Param::optional(
            "MOSAICOD_CLEANUP_RETENTION_DURATION",
            ignore_env,
            cfg.cleanup.retention_duration,
            DEFAULT_CLEANUP_RETENTION_DURATION,
        ),
    };

    ev.validate()?;

    let _ = ENV.set(ev);

    Ok(())
}

static ENV: OnceLock<Params> = OnceLock::new();

pub fn params() -> &'static Params {
    ENV.get().expect("parameters not initialized, please call `params::load_params()` before accessing an env variable.")
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
    use crate::config;
    use mosaicod_core::error::ErrorKind;

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
            max_grpc_decode_message_size: param(GRPC_MSG_MIN_SIZE_BYTES),
            target_grpc_encode_message_size: param(DEFAULT_TARGET_GRPC_ENCODE_MESSAGE_SIZE),
            max_concurrent_chunk_queries: param(DEFAULT_MAX_CONCURRENT_CHUNK_QUERIES),
            max_size_plain_list_eq: param(DEFAULT_MAX_SIZE_PLAIN_LIST_EQ),
            max_concurrent_writes: param(1),
            max_batch_size: param(DEFAULT_MAX_BATCH_SIZE),
            default_parallelism: param(1),
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
            db_password: param_hidden("".to_owned()),
            max_db_connections: param(DEFAULT_MAX_DB_CONNECTIONS),
            store_endpoint: param(DEFAULT_STORE_ENDPOINT.to_owned()),
            store_bucket: param(DEFAULT_STORE_BUCKET.to_owned()),
            store_secret_key: param_hidden(DEFAULT_STORE_SECRET_KEY.to_owned()),
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
    fn validate_rejects_max_grpc_decode_message_size_below_min() {
        let mut params = valid_params();
        params.max_grpc_decode_message_size = param(GRPC_MSG_MIN_SIZE_BYTES - 1);

        let err = params.validate().unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidConfiguration(_)));
    }

    #[test]
    fn validate_rejects_max_grpc_decode_message_size_above_max() {
        let mut params = valid_params();
        params.max_grpc_decode_message_size = param(GRPC_MSG_MAX_SIZE_BYTES + 1);

        let err = params.validate().unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidConfiguration(_)));
    }

    #[test]
    fn validate_accepts_max_grpc_decode_message_size_at_bounds() {
        let mut params = valid_params();

        params.max_grpc_decode_message_size = param(GRPC_MSG_MIN_SIZE_BYTES);
        assert!(params.validate().is_ok());

        params.max_grpc_decode_message_size = param(GRPC_MSG_MAX_SIZE_BYTES);
        assert!(params.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_target_grpc_encode_message_size() {
        let mut params = valid_params();
        params.target_grpc_encode_message_size = param(0);

        let err = params.validate().unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidConfiguration(_)));
    }

    #[test]
    fn validate_rejects_target_grpc_encode_message_size_above_max() {
        let mut params = valid_params();
        params.target_grpc_encode_message_size = param(GRPC_MSG_MAX_SIZE_BYTES + 1);

        let err = params.validate().unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidConfiguration(_)));
    }

    #[test]
    fn validate_accepts_target_grpc_encode_message_size_at_max() {
        let mut params = valid_params();
        params.target_grpc_encode_message_size = param(GRPC_MSG_MAX_SIZE_BYTES);

        assert!(params.validate().is_ok());
    }

    // SAFETY: each test below uses its own dedicated env var name, so concurrent
    // test threads never observe or mutate each other's variables.

    #[test]
    fn optional_or_file_uses_plain_value_when_set() {
        let name = "TEST_OPTIONAL_OR_FILE_PLAIN";
        unsafe { env::set_var(name, "plain-value") };

        let result = Param::<String>::optional_or_file(name, false, None, "default".to_owned());

        unsafe { env::remove_var(name) };

        assert_eq!(result.unwrap().value, "plain-value");
    }

    #[test]
    fn optional_or_file_reads_and_trims_file_when_file_var_set() {
        let name = "TEST_OPTIONAL_OR_FILE_FROM_FILE";
        let file_env = format!("{name}_FILE");

        let path = std::env::temp_dir().join("mosaicod_test_optional_or_file_secret");
        std::fs::write(&path, " secret-from-file\n").unwrap();
        unsafe { env::set_var(&file_env, path.to_str().unwrap()) };

        let result = Param::<String>::optional_or_file(name, false, None, "default".to_owned());

        unsafe { env::remove_var(&file_env) };
        let _ = std::fs::remove_file(&path);

        assert_eq!(result.unwrap().value, "secret-from-file");
    }

    #[test]
    fn optional_or_file_errors_when_both_set() {
        let name = "TEST_OPTIONAL_OR_FILE_BOTH";
        let file_env = format!("{name}_FILE");

        unsafe { env::set_var(name, "plain-value") };
        unsafe { env::set_var(&file_env, "/does/not/matter") };

        let result = Param::<String>::optional_or_file(name, false, None, "default".to_owned());

        unsafe { env::remove_var(name) };
        unsafe { env::remove_var(&file_env) };

        assert!(matches!(
            result.unwrap_err().error().kind(),
            ErrorKind::InvalidConfiguration(_)
        ));
    }

    #[test]
    fn optional_or_file_falls_back_to_default_when_neither_set() {
        let name = "TEST_OPTIONAL_OR_FILE_NEITHER";

        let result = Param::<String>::optional_or_file(name, false, None, "default".to_owned());

        assert_eq!(result.unwrap().value, "default");
    }

    #[test]
    fn required_or_file_uses_plain_value_when_set() {
        let name = "TEST_REQUIRED_OR_FILE_PLAIN";
        unsafe { env::set_var(name, "plain-value") };

        let result = Param::<String>::required_or_file(name, false, None);

        unsafe { env::remove_var(name) };

        assert_eq!(result.unwrap().value, "plain-value");
    }

    #[test]
    fn required_or_file_reads_and_trims_file_when_file_var_set() {
        let name = "TEST_REQUIRED_OR_FILE_FROM_FILE";
        let file_env = format!("{name}_FILE");

        let path = std::env::temp_dir().join("mosaicod_test_required_or_file_secret");
        std::fs::write(&path, "secret-from-file\n").unwrap();
        unsafe { env::set_var(&file_env, path.to_str().unwrap()) };

        let result = Param::<String>::required_or_file(name, false, None);

        unsafe { env::remove_var(&file_env) };
        let _ = std::fs::remove_file(&path);

        assert_eq!(result.unwrap().value, "secret-from-file");
    }

    #[test]
    fn required_or_file_errors_when_both_set() {
        let name = "TEST_REQUIRED_OR_FILE_BOTH";
        let file_env = format!("{name}_FILE");

        unsafe { env::set_var(name, "plain-value") };
        unsafe { env::set_var(&file_env, "/does/not/matter") };

        let result = Param::<String>::required_or_file(name, false, None);

        unsafe { env::remove_var(name) };
        unsafe { env::remove_var(&file_env) };

        assert!(matches!(
            result.unwrap_err().error().kind(),
            ErrorKind::InvalidConfiguration(_)
        ));
    }

    #[test]
    fn required_or_file_errors_when_neither_set() {
        let name = "TEST_REQUIRED_OR_FILE_NEITHER";

        let result = Param::<String>::required_or_file(name, false, None);

        assert!(matches!(
            result.unwrap_err().error().kind(),
            ErrorKind::InvalidConfiguration(_)
        ));
    }

    // --- config-file precedence tier ---

    #[test]
    fn optional_uses_file_value_when_env_unset() {
        let name = "TEST_OPTIONAL_FILE_VALUE";

        let result = Param::<u32>::optional(name, false, Some(42), 0);

        assert_eq!(result.value, 42);
    }

    #[test]
    fn optional_env_wins_over_file_value() {
        let name = "TEST_OPTIONAL_ENV_OVER_FILE";
        unsafe { env::set_var(name, "7") };

        let result = Param::<u32>::optional(name, false, Some(42), 0);

        unsafe { env::remove_var(name) };

        assert_eq!(result.value, 7);
    }

    #[test]
    fn optional_falls_back_to_default_when_env_and_file_unset() {
        let name = "TEST_OPTIONAL_NEITHER_ENV_NOR_FILE";

        let result = Param::<u32>::optional(name, false, None, 0);

        assert_eq!(result.value, 0);
    }

    #[test]
    fn required_uses_file_value_when_env_unset() {
        let name = "TEST_REQUIRED_FILE_VALUE";

        let result = Param::<u32>::required(name, false, Some(42));

        assert_eq!(result.unwrap().value, 42);
    }

    #[test]
    fn required_errors_when_env_and_file_unset() {
        let name = "TEST_REQUIRED_NEITHER_ENV_NOR_FILE";

        let result = Param::<u32>::required(name, false, None);

        assert!(matches!(
            result.unwrap_err().error().kind(),
            ErrorKind::InvalidConfiguration(_)
        ));
    }

    #[test]
    fn optional_or_file_reads_config_file_path_when_env_unset() {
        let name = "TEST_OPTIONAL_OR_FILE_CONFIG_PATH";

        let path = std::env::temp_dir().join("mosaicod_test_optional_or_file_config_secret");
        std::fs::write(&path, "secret-from-config-file\n").unwrap();

        let result = Param::<String>::optional_or_file(
            name,
            false,
            Some(path.to_str().unwrap().to_owned()),
            "default".to_owned(),
        );

        let _ = std::fs::remove_file(&path);

        assert_eq!(result.unwrap().value, "secret-from-config-file");
    }

    #[test]
    fn optional_or_file_env_wins_over_config_file_path() {
        let name = "TEST_OPTIONAL_OR_FILE_ENV_OVER_CONFIG_PATH";
        unsafe { env::set_var(name, "plain-value") };

        let result = Param::<String>::optional_or_file(
            name,
            false,
            Some("/does/not/matter".to_owned()),
            "default".to_owned(),
        );

        unsafe { env::remove_var(name) };

        assert_eq!(result.unwrap().value, "plain-value");
    }

    // --- ignore_env bypass ---

    #[test]
    fn optional_ignores_env_when_ignore_env_true() {
        let name = "TEST_OPTIONAL_IGNORE_ENV";
        unsafe { env::set_var(name, "7") };

        let result = Param::<u32>::optional(name, true, Some(42), 0);

        unsafe { env::remove_var(name) };

        assert_eq!(result.value, 42);
    }

    #[test]
    fn required_or_file_ignores_both_env_forms_when_ignore_env_true() {
        let name = "TEST_REQUIRED_OR_FILE_IGNORE_ENV";
        let file_env = format!("{name}_FILE");

        unsafe { env::set_var(name, "plain-value") };
        unsafe { env::set_var(&file_env, "/does/not/matter") };

        let path = std::env::temp_dir().join("mosaicod_test_required_or_file_ignore_env");
        std::fs::write(&path, "secret-from-config-file\n").unwrap();

        let result =
            Param::<String>::required_or_file(name, true, Some(path.to_str().unwrap().to_owned()));

        unsafe { env::remove_var(name) };
        unsafe { env::remove_var(&file_env) };
        let _ = std::fs::remove_file(&path);

        assert_eq!(result.unwrap().value, "secret-from-config-file");
    }

    // --- load_params field mapping ---

    // `load_params` caches its result in a process-wide `OnceLock`, so a second call
    // anywhere in this binary would be a silent no-op that returns the first call's
    // values instead of its own. This must remain the only test in this file (and the
    // only one in this crate's test binary) that calls `load_params`.
    #[test]
    fn load_params_maps_every_config_file_field_to_the_correct_param() {
        let password_path = std::env::temp_dir().join("mosaicod_test_load_params_db_password");
        std::fs::write(&password_path, "db-password-from-file\n").unwrap();
        let secret_key_path = std::env::temp_dir().join("mosaicod_test_load_params_secret_key");
        std::fs::write(&secret_key_path, "secret-key-from-file\n").unwrap();

        let config_file = config::ConfigFile {
            server: config::ServerConfig {
                host: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
                port: Some(1234),
                gzip_enabled: Some(true),
                api_key_enabled: Some(true),
                max_batch_size: Some(4096),
                default_parallelism: Some(3),
                max_concurrent_writes: Some(7),
                tls: config::ServerTlsConfig {
                    enabled: Some(true),
                    certificate_file: Some("cert.pem".to_owned()),
                    private_key_file: Some("key.pem".to_owned()),
                },
                grpc: config::ServerGrpcConfig {
                    max_decode_message_size: Some(GRPC_MSG_MIN_SIZE_BYTES),
                    target_encode_message_size: Some(GRPC_MSG_MIN_SIZE_BYTES / 2),
                },
            },
            database: config::DatabaseConfig {
                url: Some("postgresql://host:5432/db".to_owned()),
                user: Some("dbuser".to_owned()),
                password_file: Some(password_path.to_str().unwrap().to_owned()),
                max_connections: Some(5),
            },
            store: config::StoreConfig {
                endpoint: Some("https://store.example".to_owned()),
                bucket: Some("bucket-name".to_owned()),
                access_key: Some("access-key".to_owned()),
                secret_key_file: Some(secret_key_path.to_str().unwrap().to_owned()),
            },
            query_engine: config::QueryEngineConfig {
                memory_pool_size: Some(1000),
                max_concurrent_chunk_queries: Some(2),
                max_size_plain_list_eq: Some(512),
            },
            encoding: config::EncodingConfig {
                parquet_in_memory_encoding_buffer_size: Some(2_000_000),
            },
            store_optimizer: config::StoreOptimizerConfig {
                time_interval: Some(60),
                max_chunk_size: Some(1_000_000),
                memory_pool_size: Some(2000),
            },
            cleanup: config::CleanupConfig {
                time_interval: Some(120),
                retention_duration: Some(3600),
            },
            logging: config::LoggingConfig {
                level: Some(LogLevel::Debug),
                format: Some(LogFormat::Json),
            },
        };

        // `ignore_env` makes this deterministic regardless of ambient `MOSAICOD_*`
        // variables set in the developer's shell or CI.
        load_params(ParamsLoadOptions {
            skip_db_config: false,
            ignore_env: true,
            config_file,
        })
        .unwrap();

        let _ = std::fs::remove_file(&password_path);
        let _ = std::fs::remove_file(&secret_key_path);

        let p = params();

        assert_eq!(p.host.value, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(p.port.value, 1234);
        assert!(p.gzip_enabled.value);
        assert!(p.api_key_enabled.value);
        assert_eq!(p.max_batch_size.value, 4096);
        assert_eq!(p.default_parallelism.value, 3);
        assert_eq!(p.max_concurrent_writes.value, 7);
        assert!(p.tls_enabled.value);
        assert_eq!(p.tls_certificate_file.value, "cert.pem");
        assert_eq!(p.tls_private_key_file.value, "key.pem");
        assert_eq!(
            p.max_grpc_decode_message_size.value,
            GRPC_MSG_MIN_SIZE_BYTES
        );
        assert_eq!(
            p.target_grpc_encode_message_size.value,
            GRPC_MSG_MIN_SIZE_BYTES / 2
        );

        assert_eq!(p.db_url.value, "postgresql://host:5432/db");
        assert_eq!(p.db_user.value, "dbuser");
        assert_eq!(p.db_password.value, "db-password-from-file");
        assert_eq!(p.max_db_connections.value, 5);

        assert_eq!(p.store_endpoint.value, "https://store.example");
        assert_eq!(p.store_bucket.value, "bucket-name");
        assert_eq!(p.store_access_key.value, "access-key");
        assert_eq!(p.store_secret_key.value, "secret-key-from-file");

        assert_eq!(p.query_engine_memory_pool_size.value, 1000);
        assert_eq!(p.max_concurrent_chunk_queries.value, 2);
        assert_eq!(p.max_size_plain_list_eq.value, 512);

        assert_eq!(p.parquet_in_memory_encoding_buffer_size.value, 2_000_000);

        assert_eq!(p.store_optimizer_time_interval.value, 60);
        assert_eq!(p.store_optimizer_max_chunk_size.value, 1_000_000);
        assert_eq!(p.store_optimizer_memory_pool_size.value, 2000);

        assert_eq!(p.cleanup_time_interval.value, 120);
        assert_eq!(p.cleanup_retention_duration.value, 3600);

        assert!(matches!(p.log_level.value, LogLevel::Debug));
        assert!(matches!(p.log_format.value, LogFormat::Json));
    }
}
