//! The sections of `config.toml` read by [`super::DefaultConfig`], the third tier in
//! mosaicod's configuration precedence (CLI flag > env var > config file > hard-coded
//! default).
//!
//! Every field is optional: an absent section or absent key simply means "not set at
//! this tier", falling through to the next one. Secret values (`database.password`,
//! `store.secret_key`) are intentionally not fields here — only their `_file` path
//! counterparts are.
//!
//! Other [`crate::ConfigSource`]s may declare further sections of the same file.

use std::net::IpAddr;
use std::num::NonZero;

use mosaicod_core::types::{LogFormat, LogLevel};
use serde::Deserialize;

/// The sections of `config.toml` read by [`super::DefaultConfig`].
#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigFile {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub store: StoreConfig,
    pub query_engine: QueryEngineConfig,
    pub encoding: EncodingConfig,
    pub store_optimizer: StoreOptimizerConfig,
    pub cleanup: CleanupConfig,
    pub logging: LoggingConfig,
    pub usage_reporting: UsageReportingConfig,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    pub host: Option<IpAddr>,
    pub port: Option<NonZero<u16>>,
    pub gzip_enabled: Option<bool>,
    pub api_key_enabled: Option<bool>,
    pub max_batch_size: Option<NonZero<usize>>,
    pub default_parallelism: Option<NonZero<usize>>,
    pub max_concurrent_writes: Option<NonZero<usize>>,
    pub tls: ServerTlsConfig,
    pub grpc: ServerGrpcConfig,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct ServerTlsConfig {
    pub enabled: Option<bool>,
    pub certificate_file: Option<String>,
    pub private_key_file: Option<String>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct ServerGrpcConfig {
    pub max_decode_message_size: Option<usize>,
    pub target_encode_message_size: Option<usize>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct DatabaseConfig {
    pub url: Option<String>,
    pub user: Option<String>,
    /// Path to a file containing the password. There is no plain `password` field —
    /// literal secret values are env/CLI only, never read from this file.
    pub password_file: Option<String>,
    pub max_connections: Option<NonZero<u32>>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct StoreConfig {
    pub endpoint: Option<String>,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub access_key: Option<String>,
    /// Path to a file containing the secret key. There is no plain `secret_key` field —
    /// literal secret values are env/CLI only, never read from this file.
    pub secret_key_file: Option<String>,
    pub connection_timeout: Option<NonZero<u32>>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct QueryEngineConfig {
    pub memory_pool_size: Option<usize>,
    pub max_concurrent_chunk_queries: Option<NonZero<usize>>,
    pub max_size_plain_list_eq: Option<NonZero<usize>>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct EncodingConfig {
    pub parquet_in_memory_encoding_buffer_size: Option<usize>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct StoreOptimizerConfig {
    pub time_interval: Option<u32>,
    pub max_chunk_size: Option<NonZero<usize>>,
    pub memory_pool_size: Option<usize>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct CleanupConfig {
    pub time_interval: Option<u32>,
    pub retention_duration: Option<u32>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct LoggingConfig {
    #[serde(deserialize_with = "deserialize_from_str")]
    pub level: Option<LogLevel>,
    #[serde(deserialize_with = "deserialize_from_str")]
    pub format: Option<LogFormat>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct UsageReportingConfig {
    pub enabled: Option<bool>,
}

/// Deserializes a TOML string into any `T: FromStr`, via an intermediate `String` —
/// used for `LogLevel`/`LogFormat`, which live in `mosaicod-core` and so can't implement
/// `Deserialize` themselves without `mosaicod-core` taking on a `serde` dependency.
/// Only called when the field is actually present — `#[serde(default)]` on the
/// struct handles the "absent" case before this ever runs.
fn deserialize_from_str<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let raw = String::deserialize(deserializer)?;
    raw.parse().map(Some).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use mosaicod_core::error::PublicResult as Result;

    use super::*;
    use crate::file::read_config_file;

    /// Reads the file at `path` into a [`ConfigFile`].
    fn load_config_file(path: &Path) -> Result<ConfigFile> {
        Ok(toml::from_str(&read_config_file(path)?).map_err(|e| {
            mosaicod_core::error::Error::invalid_configuration("test".to_owned(), e.to_string())
        })?)
    }

    fn write_temp_file(name: &str, content: &str) -> PathBuf {
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    // --- load_config_file ---

    #[test]
    fn load_config_file_dev_null_loads_empty_config() {
        let cfg = load_config_file(Path::new("/dev/null")).unwrap();

        assert_eq!(cfg, ConfigFile::default());
    }

    #[test]
    fn load_config_file_errors_on_missing_file() {
        let path = std::env::temp_dir().join("mosaicod_test_load_missing.toml");
        let _ = std::fs::remove_file(&path);

        assert!(load_config_file(&path).is_err());
    }

    #[test]
    fn load_config_file_errors_on_invalid_toml_syntax() {
        let path = write_temp_file("mosaicod_test_load_invalid.toml", "not = [valid toml");

        let result = load_config_file(&path);
        let _ = std::fs::remove_file(&path);

        assert!(result.is_err());
    }

    #[test]
    fn load_config_file_errors_on_unknown_field() {
        let path = write_temp_file(
            "mosaicod_test_load_unknown_field.toml",
            "[server]\nhostt = \"127.0.0.1\"\n",
        );

        let result = load_config_file(&path);
        let _ = std::fs::remove_file(&path);

        assert!(result.is_err());
    }

    #[test]
    fn load_config_file_defaults_absent_fields_to_none() {
        let path = write_temp_file("mosaicod_test_load_minimal.toml", "");

        let result = load_config_file(&path);
        let _ = std::fs::remove_file(&path);

        let cfg = result.unwrap();
        assert_eq!(cfg.server.host, None);
        assert_eq!(cfg.database.url, None);
        assert!(cfg.logging.level.is_none());
    }

    #[test]
    fn load_config_file_parses_typed_and_nested_fields() {
        let toml = r#"
            [server]
            host = "0.0.0.0"
            port = 9999
            gzip_enabled = true

            [server.tls]
            enabled = true
            certificate_file = "/tmp/cert.pem"

            [database]
            url = "postgresql://localhost:5432/mosaico"
            max_connections = 25

            [logging]
            level = "debug"
            format = "json"

            [usage_reporting]
            enabled = false
        "#;
        let path = write_temp_file("mosaicod_test_load_full.toml", toml);

        let result = load_config_file(&path);
        let _ = std::fs::remove_file(&path);

        let cfg = result.unwrap();
        assert_eq!(cfg.server.host, Some("0.0.0.0".parse().unwrap()));
        assert_eq!(cfg.server.port, NonZero::new(9999));
        assert_eq!(cfg.server.gzip_enabled, Some(true));
        assert_eq!(cfg.server.tls.enabled, Some(true));
        assert_eq!(
            cfg.server.tls.certificate_file,
            Some("/tmp/cert.pem".to_owned())
        );
        assert_eq!(
            cfg.database.url,
            Some("postgresql://localhost:5432/mosaico".to_owned())
        );
        assert_eq!(cfg.database.max_connections, NonZero::new(25));
        assert_eq!(cfg.logging.level, Some(LogLevel::Debug));
        assert_eq!(cfg.logging.format, Some(LogFormat::Json));
        assert_eq!(cfg.usage_reporting.enabled, Some(false));
    }

    #[test]
    fn load_config_file_errors_on_zero_for_non_zero_field() {
        let path = write_temp_file(
            "mosaicod_test_load_zero_parallelism.toml",
            "[server]\ndefault_parallelism = 0\n",
        );

        let result = load_config_file(&path);
        let _ = std::fs::remove_file(&path);

        assert!(result.is_err());
    }

    #[test]
    fn load_config_file_accepts_max_batch_size_of_one() {
        let path = write_temp_file(
            "mosaicod_test_load_max_batch_size_one.toml",
            "[server]\nmax_batch_size = 1\n",
        );

        let result = load_config_file(&path);
        let _ = std::fs::remove_file(&path);

        assert_eq!(result.unwrap().server.max_batch_size, NonZero::new(1));
    }

    #[test]
    fn load_config_file_errors_on_zero_max_batch_size() {
        let path = write_temp_file(
            "mosaicod_test_load_max_batch_size_zero.toml",
            "[server]\nmax_batch_size = 0\n",
        );

        let result = load_config_file(&path);
        let _ = std::fs::remove_file(&path);

        assert!(result.is_err());
    }

    #[test]
    fn load_config_file_errors_on_invalid_logging_level() {
        let path = write_temp_file(
            "mosaicod_test_load_bad_log_level.toml",
            "[logging]\nlevel = \"bogus\"\n",
        );

        let result = load_config_file(&path);
        let _ = std::fs::remove_file(&path);

        assert!(result.is_err());
    }
}
