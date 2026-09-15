//! Common functions shared between multiple commands

use mosaicod_config::{self as config, params};
use mosaicod_core::{self as core, error::PublicResult as Result};
use mosaicod_db as db;
use mosaicod_store as store;
use std::sync::Arc;
use std::sync::OnceLock;

/// Stores startup time
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

    let secret_key = params.store_secret_key.value.clone();
    let access_key = params.store_access_key.value.clone();

    if !access_key.is_empty() && !secret_key.is_empty() {
        builder = builder.with_credentials(access_key, secret_key);
    }

    Ok(Arc::new(builder.build()?))
}

/// Resolves and loads the config file (if any), and builds the resulting
/// [`params::ParamsLoadOptions`]. `config_path` is the explicit `--config` CLI override,
/// if any; otherwise the search order is `MOSAICOD_CONFIG_FILE` env var >
/// `$XDG_CONFIG_HOME/mosaicod/config.toml` (or `~/.config/mosaicod/config.toml`) >
/// `/etc/mosaicod/config.toml` > no config file.
fn build_params_options(
    config_path: Option<&std::path::Path>,
) -> Result<params::ParamsLoadOptions> {
    let resolved_path = config::resolve_config_path(config_path)?;

    let config_file = match resolved_path {
        Some(path) => config::load_config_file(&path)?,
        None => Default::default(),
    };

    Ok(params::ParamsLoadOptions {
        config_file,
        skip_db_config: false,
        ignore_env: false,
    })
}

/// Load the defined env variables from the system, merging in a config file if one is found.
pub fn load_env_variables(config_path: Option<&std::path::Path>) -> Result<()> {
    dotenv::dotenv().ok();
    params::load_params(build_params_options(config_path)?)?;
    Ok(())
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

    #[test]
    fn build_params_options_loads_explicit_cli_path() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _config_file = EnvVarGuard::unset("MOSAICOD_CONFIG_FILE");

        let path = std::env::temp_dir().join("mosaicod_test_build_params_options_cli.toml");
        std::fs::write(&path, "[server]\nhost = \"10.0.0.5\"\n").unwrap();

        let result = build_params_options(Some(&path));

        let _ = std::fs::remove_file(&path);

        let options = result.unwrap();
        assert_eq!(
            options.config_file.server.host,
            Some("10.0.0.5".parse().unwrap())
        );
        assert!(!options.skip_db_config);
        assert!(!options.ignore_env);
    }

    #[test]
    fn build_params_options_defaults_to_empty_config_file_when_nothing_resolves() {
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

        let result = build_params_options(None);

        let _ = std::fs::remove_dir_all(&xdg_dir);

        let options = result.unwrap();
        assert!(options.config_file.server.host.is_none());
    }
}
