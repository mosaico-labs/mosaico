//! [`DefaultConfig`], the [`ConfigSource`] of mosaicod's own parameters.

use super::*;
use crate::builder::{ConfigSource, Env};
use crate::param::Param;
use mosaicod_core::error;

/// Source of mosaicod's own parameters: the `[server]`, `[database]`, ... sections of
/// `config.toml` and the `MOSAICOD_*` env vars. Builds [`Params`].
#[derive(Default)]
pub struct DefaultConfig {
    skip_db_config: bool,
}

impl DefaultConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Doesn't require `MOSAICOD_DB_URL`.
    #[cfg(any(test, feature = "testing"))]
    pub fn testing() -> Self {
        Self {
            skip_db_config: true,
        }
    }
}

impl ConfigSource for DefaultConfig {
    type Sections = ConfigFile;
    type Output = Params;

    fn build(self, sections: ConfigFile, env: &Env) -> error::PublicResult<Params> {
        build_params(env, &sections, self.skip_db_config)
    }
}

/// Builds and validates [`Params`], with precedence env var > `config.toml` > hard-coded
/// default for each field.
fn build_params(env: &Env, cfg: &ConfigFile, skip_db_config: bool) -> error::PublicResult<Params> {
    // An explicit value always wins and startup only fails when nothing is set and detection fails too.
    let default_parallelism = Param::required(
        "MOSAICOD_DEFAULT_PARALLELISM",
        env,
        cfg.server
            .default_parallelism
            .or_else(|| std::thread::available_parallelism().ok()),
    )?;

    let grpc_max_decode_message_size = Param::optional(
        "MOSAICOD_GRPC_MAX_DECODE_MESSAGE_SIZE",
        env,
        cfg.server.grpc.max_decode_message_size,
        DEFAULT_GRPC_MAX_DECODE_MESSAGE_SIZE,
    )?;
    let grpc_target_encode_message_size = Param::optional(
        "MOSAICOD_GRPC_TARGET_ENCODE_MESSAGE_SIZE",
        env,
        cfg.server.grpc.target_encode_message_size,
        DEFAULT_GRPC_TARGET_ENCODE_MESSAGE_SIZE,
    )?;

    let params = Params {
        // general
        grpc_max_decode_message_size,
        grpc_target_encode_message_size,
        max_concurrent_chunk_queries: Param::optional(
            "MOSAICOD_MAX_CONCURRENT_CHUNK_QUERIES",
            env,
            cfg.query_engine.max_concurrent_chunk_queries,
            DEFAULT_MAX_CONCURRENT_CHUNK_QUERIES,
        )?,
        max_size_plain_list_eq: Param::optional(
            "MOSAICOD_MAX_SIZE_PLAIN_LIST_EQ",
            env,
            cfg.query_engine.max_size_plain_list_eq,
            DEFAULT_MAX_SIZE_PLAIN_LIST_EQ,
        )?,
        max_db_connections: Param::optional(
            "MOSAICOD_MAX_DB_CONNECTIONS",
            env,
            cfg.database.max_connections,
            DEFAULT_MAX_DB_CONNECTIONS,
        )?,
        max_concurrent_writes: Param::optional(
            "MOSAICOD_MAX_CONCURRENT_WRITES",
            env,
            cfg.server.max_concurrent_writes,
            default_parallelism.value,
        )?,
        default_parallelism,
        parquet_in_memory_encoding_buffer_size: Param::optional(
            "MOSAICOD_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE",
            env,
            cfg.encoding.parquet_in_memory_encoding_buffer_size,
            DEFAULT_PARQUET_IN_MEMORY_ENCODING_BUFFER_SIZE,
        )?,
        max_batch_size: Param::optional(
            "MOSAICOD_MAX_BATCH_SIZE",
            env,
            cfg.server.max_batch_size,
            DEFAULT_MAX_BATCH_SIZE,
        )?,
        query_engine_memory_pool_size: Param::optional(
            "MOSAICOD_QUERY_ENGINE_MEMORY_POOL_SIZE",
            env,
            cfg.query_engine.memory_pool_size,
            DEFAULT_QUERY_ENGINE_MEMORY_POOL_SIZE,
        )?,

        // logging
        log_level: Param::optional(
            "MOSAICOD_LOG_LEVEL",
            env,
            cfg.logging.level,
            DEFAULT_LOG_LEVEL,
        )?,
        log_format: Param::optional(
            "MOSAICOD_LOG_FORMAT",
            env,
            cfg.logging.format,
            DEFAULT_LOG_FORMAT,
        )?,

        // server
        host: Param::optional("MOSAICOD_HOST", env, cfg.server.host, DEFAULT_HOST)?,
        port: Param::optional("MOSAICOD_PORT", env, cfg.server.port, DEFAULT_PORT)?,
        tls_enabled: Param::optional(
            "MOSAICOD_TLS_ENABLED",
            env,
            cfg.server.tls.enabled,
            DEFAULT_TLS_ENABLED,
        )?,
        gzip_enabled: Param::optional(
            "MOSAICOD_GZIP_ENABLED",
            env,
            cfg.server.gzip_enabled,
            DEFAULT_GZIP,
        )?,
        api_key_enabled: Param::optional(
            "MOSAICOD_API_KEY_ENABLED",
            env,
            cfg.server.api_key_enabled,
            DEFAULT_API_KEY_ENABLED,
        )?,

        // tls
        tls_certificate_file: Param::optional(
            "MOSAICOD_TLS_CERT_FILE",
            env,
            cfg.server.tls.certificate_file.clone(),
            DEFAULT_TLS_CERT_FILE.to_owned(),
        )?,
        tls_private_key_file: Param::optional(
            "MOSAICOD_TLS_PRIVATE_KEY_FILE",
            env,
            cfg.server.tls.private_key_file.clone(),
            DEFAULT_TLS_PRIVATE_KEY_FILE.to_owned(),
        )?,

        // database
        db_url: if skip_db_config {
            Param::optional(
                "MOSAICOD_DB_URL",
                env,
                cfg.database.url.clone(),
                String::new(),
            )?
        } else {
            Param::required("MOSAICOD_DB_URL", env, cfg.database.url.clone())?
        },
        // Some databases do not require to specify a user for the connection.
        db_user: Param::optional(
            "MOSAICOD_DB_USER",
            env,
            cfg.database.user.clone(),
            String::new(),
        )?,
        // Some databases do not require to specify a password for the connection.
        db_password: Param::optional_or_file(
            "MOSAICOD_DB_PASSWORD",
            env,
            cfg.database.password_file.clone(),
            String::new(),
        )?,

        // store
        store_endpoint: Param::optional(
            "MOSAICOD_STORE_ENDPOINT",
            env,
            cfg.store.endpoint.clone(),
            DEFAULT_STORE_ENDPOINT.to_owned(),
        )?,
        store_bucket: Param::optional(
            "MOSAICOD_STORE_BUCKET",
            env,
            cfg.store.bucket.clone(),
            DEFAULT_STORE_BUCKET.to_owned(),
        )?,
        store_region: Param::optional(
            "MOSAICOD_STORE_REGION",
            env,
            cfg.store.region.clone(),
            DEFAULT_STORE_REGION.to_owned(),
        )?,
        store_secret_key: Param::optional_or_file(
            "MOSAICOD_STORE_SECRET_KEY",
            env,
            cfg.store.secret_key_file.clone(),
            DEFAULT_STORE_SECRET_KEY.to_owned(),
        )?,
        store_access_key: Param::optional(
            "MOSAICOD_STORE_ACCESS_KEY",
            env,
            cfg.store.access_key.clone(),
            DEFAULT_STORE_ACCESS_KEY.to_owned(),
        )?,
        store_connection_timeout: Param::optional(
            "MOSAICOD_STORE_CONNECTION_TIMEOUT",
            env,
            cfg.store.connection_timeout,
            DEFAULT_STORE_CONNECTION_TIMEOUT,
        )?,

        // store optimizer
        store_optimizer_memory_pool_size: Param::optional(
            "MOSAICOD_STORE_OPTIMIZER_MEMORY_POOL_SIZE",
            env,
            cfg.store_optimizer.memory_pool_size,
            DEFAULT_STORE_OPTIMIZER_MEMORY_POOL_SIZE,
        )?,
        store_optimizer_time_interval: Param::optional(
            "MOSAICOD_STORE_OPTIMIZER_TIME_INTERVAL",
            env,
            cfg.store_optimizer.time_interval,
            DEFAULT_STORE_OPTIMIZER_TIME_INTERVAL,
        )?,
        store_optimizer_max_chunk_size: Param::optional(
            "MOSAICOD_STORE_OPTIMIZER_MAX_CHUNK_SIZE",
            env,
            cfg.store_optimizer.max_chunk_size,
            DEFAULT_STORE_OPTIMIZER_MAX_CHUNK_SIZE,
        )?,

        // cleanup
        cleanup_time_interval: Param::optional(
            "MOSAICOD_CLEANUP_TIME_INTERVAL",
            env,
            cfg.cleanup.time_interval,
            DEFAULT_CLEANUP_TIME_INTERVAL,
        )?,
        cleanup_retention_duration: Param::optional(
            "MOSAICOD_CLEANUP_RETENTION_DURATION",
            env,
            cfg.cleanup.retention_duration,
            DEFAULT_CLEANUP_RETENTION_DURATION,
        )?,
    };

    params.validate()?;

    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_core::types::{LogFormat, LogLevel};
    use std::net::{IpAddr, Ipv4Addr};
    use std::num::NonZero;

    // --- build_params field mapping ---

    // `Env::ignored()` makes these deterministic regardless of ambient `MOSAICOD_*`
    // variables set in the developer's shell or CI.
    fn params_from(config_file: ConfigFile) -> Params {
        build_params(&Env::ignored(), &config_file, true).unwrap()
    }

    #[test]
    fn max_concurrent_writes_defaults_to_default_parallelism() {
        let p = params_from(ConfigFile {
            server: ServerConfig {
                default_parallelism: NonZero::new(3),
                ..Default::default()
            },
            ..Default::default()
        });

        assert_eq!(p.max_concurrent_writes.value.get(), 3);
    }

    #[test]
    fn max_concurrent_writes_overrides_default_parallelism() {
        let p = params_from(ConfigFile {
            server: ServerConfig {
                default_parallelism: NonZero::new(3),
                max_concurrent_writes: NonZero::new(7),
                ..Default::default()
            },
            ..Default::default()
        });

        assert_eq!(p.max_concurrent_writes.value.get(), 7);
    }

    #[test]
    fn default_parallelism_falls_back_to_detected_parallelism() {
        let p = params_from(ConfigFile::default());

        assert_eq!(
            p.default_parallelism.value,
            std::thread::available_parallelism().unwrap()
        );
    }

    #[test]
    fn build_params_maps_every_config_file_field_to_the_correct_param() {
        let password_path = std::env::temp_dir().join("mosaicod_test_load_params_db_password");
        std::fs::write(&password_path, "db-password-from-file\n").unwrap();
        let secret_key_path = std::env::temp_dir().join("mosaicod_test_load_params_secret_key");
        std::fs::write(&secret_key_path, "secret-key-from-file\n").unwrap();

        let config_file = ConfigFile {
            server: ServerConfig {
                host: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
                port: NonZero::new(1234),
                gzip_enabled: Some(true),
                api_key_enabled: Some(true),
                max_batch_size: NonZero::new(4096),
                default_parallelism: NonZero::new(3),
                max_concurrent_writes: NonZero::new(7),
                tls: ServerTlsConfig {
                    enabled: Some(true),
                    certificate_file: Some("cert.pem".to_owned()),
                    private_key_file: Some("key.pem".to_owned()),
                },
                grpc: ServerGrpcConfig {
                    max_decode_message_size: Some(GRPC_MSG_MIN_SIZE_BYTES),
                    target_encode_message_size: Some(GRPC_MSG_MIN_SIZE_BYTES / 2),
                },
            },
            database: DatabaseConfig {
                url: Some("postgresql://host:5432/db".to_owned()),
                user: Some("dbuser".to_owned()),
                password_file: Some(password_path.to_str().unwrap().to_owned()),
                max_connections: NonZero::new(5),
            },
            store: StoreConfig {
                endpoint: Some("https://store.example".to_owned()),
                bucket: Some("bucket-name".to_owned()),
                region: Some("eu-south-1".to_owned()),
                access_key: Some("access-key".to_owned()),
                secret_key_file: Some(secret_key_path.to_str().unwrap().to_owned()),
                connection_timeout: NonZero::new(15),
            },
            query_engine: QueryEngineConfig {
                memory_pool_size: Some(1000),
                max_concurrent_chunk_queries: NonZero::new(2),
                max_size_plain_list_eq: NonZero::new(512),
            },
            encoding: EncodingConfig {
                parquet_in_memory_encoding_buffer_size: Some(2_000_000),
            },
            store_optimizer: StoreOptimizerConfig {
                time_interval: Some(60),
                max_chunk_size: NonZero::new(1_000_000),
                memory_pool_size: Some(2000),
            },
            cleanup: CleanupConfig {
                time_interval: Some(120),
                retention_duration: Some(3600),
            },
            logging: LoggingConfig {
                level: Some(LogLevel::Debug),
                format: Some(LogFormat::Json),
            },
        };

        let p = build_params(&Env::ignored(), &config_file, false);

        let _ = std::fs::remove_file(&password_path);
        let _ = std::fs::remove_file(&secret_key_path);

        let p = p.unwrap();

        assert_eq!(p.host.value, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(p.port.value.get(), 1234);
        assert!(p.gzip_enabled.value);
        assert!(p.api_key_enabled.value);
        assert_eq!(p.max_batch_size.value.get(), 4096);
        assert_eq!(p.default_parallelism.value.get(), 3);
        assert_eq!(p.max_concurrent_writes.value.get(), 7);
        assert!(p.tls_enabled.value);
        assert_eq!(p.tls_certificate_file.value, "cert.pem");
        assert_eq!(p.tls_private_key_file.value, "key.pem");
        assert_eq!(
            p.grpc_max_decode_message_size.value,
            GRPC_MSG_MIN_SIZE_BYTES
        );
        assert_eq!(
            p.grpc_target_encode_message_size.value,
            GRPC_MSG_MIN_SIZE_BYTES / 2
        );

        assert_eq!(p.db_url.value, "postgresql://host:5432/db");
        assert_eq!(p.db_user.value, "dbuser");
        assert_eq!(p.db_password.value, "db-password-from-file");
        assert_eq!(p.max_db_connections.value.get(), 5);

        assert_eq!(p.store_endpoint.value, "https://store.example");
        assert_eq!(p.store_bucket.value, "bucket-name");
        assert_eq!(p.store_region.value, "eu-south-1");
        assert_eq!(p.store_access_key.value, "access-key");
        assert_eq!(p.store_secret_key.value, "secret-key-from-file");
        assert_eq!(p.store_connection_timeout.value.get(), 15);

        assert_eq!(p.query_engine_memory_pool_size.value, 1000);
        assert_eq!(p.max_concurrent_chunk_queries.value.get(), 2);
        assert_eq!(p.max_size_plain_list_eq.value.get(), 512);

        assert_eq!(p.parquet_in_memory_encoding_buffer_size.value, 2_000_000);

        assert_eq!(p.store_optimizer_time_interval.value, 60);
        assert_eq!(p.store_optimizer_max_chunk_size.value.get(), 1_000_000);
        assert_eq!(p.store_optimizer_memory_pool_size.value, 2000);

        assert_eq!(p.cleanup_time_interval.value, 120);
        assert_eq!(p.cleanup_retention_duration.value, 3600);

        assert_eq!(p.log_level.value, LogLevel::Debug);
        assert_eq!(p.log_format.value, LogFormat::Json);
    }
}
