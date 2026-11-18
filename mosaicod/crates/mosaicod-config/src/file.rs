//! Finds and reads `config.toml`. Its sections are read by the [`crate::ConfigSource`]s
//! that declare them.

use etcetera::base_strategy::BaseStrategy;
use mosaicod_core::error::PublicResult as Result;
use std::env;
use std::path::{Path, PathBuf};

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
///
/// Note: an empty `MOSAICOD_CONFIG_FILE` is treated as unset.
pub fn resolve_config_path(cli_path: Option<&Path>) -> Result<Option<PathBuf>> {
    if let Some(path) = cli_path {
        return require_path(path.to_path_buf(), "--config");
    }

    if let Ok(raw) = env::var(CONFIG_FILE_ENV_VAR)
        && !raw.is_empty()
    {
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

/// Resolves to the XDG strategy on Linux and macOS (`$XDG_CONFIG_HOME`/`~/.config`).
fn xdg_config_path() -> Option<PathBuf> {
    etcetera::choose_base_strategy()
        .ok()
        .map(|strategy| strategy.config_dir().join(CONFIG_FILE_RELATIVE_PATH))
}

/// Reads the config file at `path`. The builder parses it, leaving each section to the
/// source that reads it.
pub(crate) fn read_config_file(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).map_err(|e| {
        mosaicod_core::error::Error::invalid_configuration(
            path.display().to_string(),
            e.to_string(),
        )
        .into()
    })
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
    fn resolve_config_path_treats_empty_env_var_as_unset() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _config_file = EnvVarGuard::set(CONFIG_FILE_ENV_VAR, "");

        let xdg_dir = std::env::temp_dir().join("mosaicod_test_xdg_empty_env_var");
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
}
