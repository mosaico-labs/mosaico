//! Common functions shared between multiple commands.

use mosaicod_config::{self as config, params};
use mosaicod_core::{self as core, error::PublicResult as Result};
use mosaicod_db as db;
use mosaicod_store as store;
use std::sync::Arc;
use std::sync::OnceLock;

/// Stores startup time.
static STARTUP_TIME: OnceLock<std::time::Instant> = OnceLock::new();

pub fn pin_startup_time() {
    STARTUP_TIME
        .set(std::time::Instant::now())
        .expect("Startup time already set");
}

pub fn startup_time() -> &'static std::time::Instant {
    STARTUP_TIME.get().expect(
        "Startup time not initialized, plase call `common::pin_startup_time()` before accessing.",
    )
}

pub fn init_db(rt: &tokio::runtime::Runtime, config: &db::Config) -> Result<db::Database> {
    let database = rt.block_on(async {
        let database = db::Database::try_new(config).await?;
        Ok::<db::Database, mosaicod_core::error::BoxPublicError>(database)
    })?;

    Ok(database)
}

pub fn init_runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| core::Error::internal(Some("event loop startup failure".to_owned())))?)
}

pub fn init_store() -> Result<store::StoreRef> {
    let params = params::params();

    let endpoint_url: url::Url = params.store_endpoint.value.parse().map_err(|_| {
        core::Error::invalid_configuration(
            params.store_endpoint.env.clone(),
            "not a valid URL".to_owned(),
        )
    })?;

    let mut builder = store::Builder::new(endpoint_url, params.store_bucket.value.clone());

    if !params.store_region.value.is_empty() {
        builder = builder.with_region(params.store_region.value.clone());
    }

    let secret_key = params.store_secret_key.value.clone();
    let access_key = params.store_access_key.value.clone();

    if !access_key.is_empty() && !secret_key.is_empty() {
        builder = builder.with_credentials(access_key, secret_key);
    }

    Ok(Arc::new(builder.build()?))
}

/// Builds the configuration from the sources of `builder` and sets it for the process.
///
/// `config_path` is the explicit `--config` CLI override, if any; otherwise the search
/// order is `MOSAICOD_CONFIG_FILE` env var > `$XDG_CONFIG_HOME/mosaicod/config.toml` (or
/// `~/.config/mosaicod/config.toml`) > `/etc/mosaicod/config.toml` > no config file.
pub fn load_config(
    builder: config::ConfigBuilder,
    config_path: Option<&std::path::Path>,
) -> Result<()> {
    dotenv::dotenv().ok();
    config::set_config(build_config(builder, config_path)?)
}

fn build_config(
    builder: config::ConfigBuilder,
    config_path: Option<&std::path::Path>,
) -> Result<config::Config> {
    let config = builder.build(config::resolve_config_path(config_path)?.as_deref())?;

    if config.get::<params::Params>().is_none() {
        Err(core::Error::internal(Some(
            "no config source builds mosaicod's parameters: add `DefaultConfig::new()` \
             to the `ConfigBuilder`"
                .to_owned(),
        )))?;
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// RAII guard that saves an env var's current value on construction and restores
    /// exactly that (set back, or removed if it was unset) on drop — so these tests
    /// don't clobber whatever a developer or CI already had set for
    /// `MOSAICOD_CONFIG_FILE`/`XDG_CONFIG_HOME`, even if a test panics.
    struct EnvVarGuard {
        key: &'static str,
        original: Option<String>,
    }

    impl EnvVarGuard {
        fn unset(key: &'static str) -> Self {
            let original = std::env::var(key).ok();
            unsafe { std::env::remove_var(key) };
            Self { key, original }
        }

        fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let original = std::env::var(key).ok();
            unsafe { std::env::set_var(key, value) };
            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => unsafe { std::env::set_var(self.key, value) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    fn default_config() -> config::ConfigBuilder {
        config::ConfigBuilder::new().add_conf(params::DefaultConfig::testing())
    }

    #[test]
    fn build_config_loads_explicit_cli_path() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _config_file = EnvVarGuard::unset("MOSAICOD_CONFIG_FILE");

        let path = std::env::temp_dir().join("mosaicod_test_build_params_options_cli.toml");
        std::fs::write(&path, "[server]\nhost = \"10.0.0.5\"\n").unwrap();

        let result = build_config(default_config().ignore_env(true), Some(&path));

        let _ = std::fs::remove_file(&path);

        let config = result.unwrap();
        let params = config.get::<params::Params>().unwrap();
        assert_eq!(
            params.host.value,
            "10.0.0.5".parse::<std::net::IpAddr>().unwrap()
        );
    }

    #[test]
    fn build_config_defaults_to_no_config_file_when_nothing_resolves() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _config_file = EnvVarGuard::unset("MOSAICOD_CONFIG_FILE");

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
        let xdg_dir = std::env::temp_dir().join("mosaicod_test_build_params_options_xdg_empty");
        std::fs::create_dir_all(&xdg_dir).unwrap();
        let _xdg_home = EnvVarGuard::set("XDG_CONFIG_HOME", &xdg_dir);

        let result = build_config(default_config().ignore_env(true), None);

        let _ = std::fs::remove_dir_all(&xdg_dir);

        let config = result.unwrap();
        let params = config.get::<params::Params>().unwrap();
        assert_eq!(params.host.value, params::DEFAULT_HOST);
    }

    #[test]
    fn build_config_without_default_config_is_an_error() {
        let path = std::env::temp_dir().join("mosaicod_test_build_config_no_default.toml");
        std::fs::write(&path, "").unwrap();

        let result = build_config(config::ConfigBuilder::new().ignore_env(true), Some(&path));

        let _ = std::fs::remove_file(&path);

        let err = match result {
            Ok(_) => panic!("expected an error"),
            Err(e) => e.error().kind().clone(),
        };
        assert!(
            matches!(&err, core::error::ErrorKind::Internal(m) if m.contains("DefaultConfig")),
            "{err:?}"
        );
    }
}
