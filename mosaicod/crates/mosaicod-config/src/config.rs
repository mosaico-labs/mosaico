//! Optional `config.toml` file, the third tier in mosaicod's configuration precedence
//! (CLI flag > env var > config file > hard-coded default).
//!
//! Every field is optional: an absent section or absent key simply means "not set at
//! this tier", falling through to the next one. Secret values (`database.password`,
//! `store.secret_key`) are intentionally not fields here — only their `_file` path
//! counterparts are.
//!
//! [`ConfigFile`] feeds into [`crate::params::Params`], the runtime configuration
//! registry — the two are inseparable in practice, which is why they live in the same
//! crate rather than forcing every consumer to depend on two crates for one concern.

use mosaicod_core::error::PublicResult as Result;
use mosaicod_core::types::{LogFormat, LogLevel};
use serde::Deserialize;
use std::env;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Deserialize)]
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
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    pub host: Option<IpAddr>,
    pub port: Option<u16>,
    pub gzip_enabled: Option<bool>,
    pub api_key_enabled: Option<bool>,
    pub max_batch_size: Option<usize>,
    pub default_parallelism: Option<usize>,
    pub max_concurrent_writes: Option<usize>,
    pub tls: ServerTlsConfig,
    pub grpc: ServerGrpcConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerTlsConfig {
    pub enabled: Option<bool>,
    pub certificate_file: Option<String>,
    pub private_key_file: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerGrpcConfig {
    pub max_message_size: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DatabaseConfig {
    pub url: Option<String>,
    pub user: Option<String>,
    /// Path to a file containing the password. There is no plain `password` field —
    /// literal secret values are env/CLI only, never read from this file.
    pub password_file: Option<String>,
    pub max_connections: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StoreConfig {
    pub endpoint: Option<String>,
    pub bucket: Option<String>,
    pub access_key: Option<String>,
    /// Path to a file containing the secret key. There is no plain `secret_key` field —
    /// literal secret values are env/CLI only, never read from this file.
    pub secret_key_file: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct QueryEngineConfig {
    pub memory_pool_size: Option<usize>,
    pub max_concurrent_chunk_queries: Option<usize>,
    pub max_size_plain_list_eq: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EncodingConfig {
    pub parquet_in_memory_encoding_buffer_size: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StoreOptimizerConfig {
    pub time_interval: Option<u32>,
    pub max_chunk_size: Option<usize>,
    pub memory_pool_size: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CleanupConfig {
    pub time_interval: Option<u32>,
    pub retention_duration: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LoggingConfig {
    #[serde(deserialize_with = "deserialize_from_str")]
    pub level: Option<LogLevel>,
    #[serde(deserialize_with = "deserialize_from_str")]
    pub format: Option<LogFormat>,
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

/// Name of the `mosaicod`-specific config directory/file under a config root
/// (`$XDG_CONFIG_HOME`, `~/.config`, or `/etc`).
const CONFIG_FILE_RELATIVE_PATH: &str = "mosaicod/config.toml";

/// Env var pointing directly at a config file to load.
const CONFIG_FILE_ENV_VAR: &str = "MOSAICOD_CONFIG_FILE";

/// Resolves which config file (if any) to load, following the search order:
/// explicit `cli_path` > `MOSAICOD_CONFIG_FILE` env var > `$XDG_CONFIG_HOME/mosaicod/config.toml`
/// (or `~/.config/mosaicod/config.toml` if `XDG_CONFIG_HOME` is unset) > `/etc/mosaicod/config.toml`.
///
/// A path named explicitly (via `cli_path` or `MOSAICOD_CONFIG_FILE`) that doesn't exist is
/// an error, since the caller asked for that specific file. The XDG/`/etc` defaults are
/// silently skipped if absent — the config file is entirely optional.
pub fn resolve_config_path(cli_path: Option<&Path>) -> Result<Option<PathBuf>> {
    if let Some(path) = cli_path {
        return require_path(path.to_path_buf(), "--config");
    }

    if let Ok(raw) = env::var(CONFIG_FILE_ENV_VAR) {
        return require_path(PathBuf::from(raw), CONFIG_FILE_ENV_VAR);
    }

    if let Some(path) = xdg_config_path()
        && path.exists()
    {
        return Ok(Some(path));
    }

    let etc_path = PathBuf::from("/etc").join(CONFIG_FILE_RELATIVE_PATH);
    if etc_path.exists() {
        return Ok(Some(etc_path));
    }

    Ok(None)
}

fn require_path(path: PathBuf, source: &str) -> Result<Option<PathBuf>> {
    if path.exists() {
        Ok(Some(path))
    } else {
        Err(mosaicod_core::error::Error::invalid_configuration(
            source.to_owned(),
            format!("file not found: {}", path.display()),
        ))?
    }
}

fn xdg_config_path() -> Option<PathBuf> {
    if let Ok(xdg) = env::var("XDG_CONFIG_HOME")
        && !xdg.is_empty()
    {
        return Some(PathBuf::from(xdg).join(CONFIG_FILE_RELATIVE_PATH));
    }

    env::var("HOME").ok().map(|home| {
        PathBuf::from(home)
            .join(".config")
            .join(CONFIG_FILE_RELATIVE_PATH)
    })
}

/// Reads and parses the config file at `path`.
pub fn load_config_file(path: &Path) -> Result<ConfigFile> {
    let content = std::fs::read_to_string(path).map_err(|e| {
        mosaicod_core::error::Error::invalid_configuration(
            path.display().to_string(),
            e.to_string(),
        )
    })?;

    let parsed = toml::from_str(&content).map_err(|e| {
        mosaicod_core::error::Error::invalid_configuration(
            path.display().to_string(),
            e.to_string(),
        )
    })?;

    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serializes tests that mutate the shared, process-global env vars this module reads
    /// (`MOSAICOD_CONFIG_FILE`, `XDG_CONFIG_HOME`), since Rust runs tests in parallel
    /// threads by default and these var names aren't parameterizable per-test.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn write_temp_file(name: &str, content: &str) -> PathBuf {
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    /// RAII guard that saves an env var's current value on construction and restores
    /// exactly that (set back, or removed if it was unset) on drop — including when a
    /// test panics, since a failed assertion must not leave the var mutated for the rest
    /// of the test process, and a value a developer or CI already had set must never be
    /// permanently clobbered by a plain `remove_var`/`set_var`.
    struct EnvVarGuard {
        key: &'static str,
        original: Option<String>,
    }

    impl EnvVarGuard {
        fn unset(key: &'static str) -> Self {
            let original = env::var(key).ok();
            unsafe { env::remove_var(key) };
            Self { key, original }
        }

        fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let original = env::var(key).ok();
            unsafe { env::set_var(key, value) };
            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => unsafe { env::set_var(self.key, value) },
                None => unsafe { env::remove_var(self.key) },
            }
        }
    }

    // --- resolve_config_path ---

    #[test]
    fn resolve_config_path_prefers_explicit_cli_path() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _config_file = EnvVarGuard::unset(CONFIG_FILE_ENV_VAR);

        let path = write_temp_file("mosaicod_test_resolve_cli.toml", "");

        let result = resolve_config_path(Some(&path));

        let _ = std::fs::remove_file(&path);

        assert_eq!(result.unwrap(), Some(path));
    }

    #[test]
    fn resolve_config_path_errors_when_explicit_cli_path_missing() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _config_file = EnvVarGuard::unset(CONFIG_FILE_ENV_VAR);

        let path = std::env::temp_dir().join("mosaicod_test_resolve_cli_missing.toml");
        let _ = std::fs::remove_file(&path);

        assert!(resolve_config_path(Some(&path)).is_err());
    }

    #[test]
    fn resolve_config_path_uses_env_var_when_no_cli_path() {
        let _lock = ENV_LOCK.lock().unwrap();

        let path = write_temp_file("mosaicod_test_resolve_env.toml", "");
        let _config_file = EnvVarGuard::set(CONFIG_FILE_ENV_VAR, &path);

        let result = resolve_config_path(None);

        let _ = std::fs::remove_file(&path);

        assert_eq!(result.unwrap(), Some(path));
    }

    #[test]
    fn resolve_config_path_errors_when_env_var_path_missing() {
        let _lock = ENV_LOCK.lock().unwrap();

        let path = std::env::temp_dir().join("mosaicod_test_resolve_env_missing.toml");
        let _ = std::fs::remove_file(&path);
        let _config_file = EnvVarGuard::set(CONFIG_FILE_ENV_VAR, &path);

        assert!(resolve_config_path(None).is_err());
    }

    #[test]
    fn resolve_config_path_cli_path_wins_over_env_var() {
        let _lock = ENV_LOCK.lock().unwrap();

        let cli_path = write_temp_file("mosaicod_test_resolve_cli_wins_cli.toml", "");
        let env_path = write_temp_file("mosaicod_test_resolve_cli_wins_env.toml", "");
        let _config_file = EnvVarGuard::set(CONFIG_FILE_ENV_VAR, &env_path);

        let result = resolve_config_path(Some(&cli_path));

        let _ = std::fs::remove_file(&cli_path);
        let _ = std::fs::remove_file(&env_path);

        assert_eq!(result.unwrap(), Some(cli_path));
    }

    #[test]
    fn resolve_config_path_uses_xdg_config_home_when_env_var_unset() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _config_file = EnvVarGuard::unset(CONFIG_FILE_ENV_VAR);

        let xdg_dir = std::env::temp_dir().join("mosaicod_test_xdg_home");
        let config_dir = xdg_dir.join("mosaicod");
        std::fs::create_dir_all(&config_dir).unwrap();
        let config_path = config_dir.join("config.toml");
        std::fs::write(&config_path, "").unwrap();

        let _xdg_home = EnvVarGuard::set("XDG_CONFIG_HOME", &xdg_dir);

        let result = resolve_config_path(None);

        let _ = std::fs::remove_dir_all(&xdg_dir);

        assert_eq!(result.unwrap(), Some(config_path));
    }

    #[test]
    fn resolve_config_path_returns_none_when_nothing_configured() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _config_file = EnvVarGuard::unset(CONFIG_FILE_ENV_VAR);

        // The `/etc` tier has no env var behind it, so nothing here can force it to
        // miss — skip rather than silently assume it's absent on the machine running
        // this test, which would otherwise make `resolve_config_path` correctly return
        // `Some(etc_path)` and fail this assertion.
        let etc_path = std::path::PathBuf::from("/etc/mosaicod/config.toml");
        if etc_path.exists() {
            eprintln!("skipping: {} exists on this machine", etc_path.display());
            return;
        }

        // Points XDG_CONFIG_HOME at an empty temp dir so that lookup misses too.
        let xdg_dir = std::env::temp_dir().join("mosaicod_test_xdg_empty");
        std::fs::create_dir_all(&xdg_dir).unwrap();
        let _xdg_home = EnvVarGuard::set("XDG_CONFIG_HOME", &xdg_dir);

        let result = resolve_config_path(None);

        let _ = std::fs::remove_dir_all(&xdg_dir);

        assert_eq!(result.unwrap(), None);
    }

    // --- load_config_file ---

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
        "#;
        let path = write_temp_file("mosaicod_test_load_full.toml", toml);

        let result = load_config_file(&path);
        let _ = std::fs::remove_file(&path);

        let cfg = result.unwrap();
        assert_eq!(cfg.server.host, Some("0.0.0.0".parse().unwrap()));
        assert_eq!(cfg.server.port, Some(9999));
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
        assert_eq!(cfg.database.max_connections, Some(25));
        assert!(matches!(cfg.logging.level, Some(LogLevel::Debug)));
        assert!(matches!(cfg.logging.format, Some(LogFormat::Json)));
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
