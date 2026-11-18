//! A single configuration parameter, [`Param`], resolved from an env var, a value of
//! `config.toml` and a default, in that precedence order. Reusable by any
//! [`crate::ConfigSource`].

use crate::builder::Env;
use mosaicod_core::error;
use std::marker::PhantomData;
use std::str::FromStr;

/// Marker trait for parameters visibility.
pub trait ParamVisibility {}

/// Marker used to specify that a parameers needs to be hidden from prints.
#[derive(Default)]
pub struct Hidden;
impl ParamVisibility for Hidden {}

/// Marker for default parameter visibility.
#[derive(Default)]
pub struct Plain;
impl ParamVisibility for Plain {}

#[derive(Default)]
pub struct Param<T, V = Plain>
where
    V: ParamVisibility,
{
    /// Name of the environment variable.
    pub env: String,

    /// Value.
    pub value: T,

    _visibility: PhantomData<V>,
}

impl<T, V> Param<T, V>
where
    V: ParamVisibility,
{
    /// Precedence: `<NAME>` env var, then `file_value` (config file), then `default`.
    pub fn optional(
        env_name: &str,
        env: &Env,
        file_value: Option<T>,
        default: T,
    ) -> error::PublicResult<Param<T, V>>
    where
        T: std::str::FromStr,
        <T as FromStr>::Err: std::fmt::Display,
    {
        let value = match env.var(env_name) {
            Some(raw) => parse_value(env_name, &raw)?,
            None => file_value.unwrap_or(default),
        };

        Ok(Self {
            value,
            env: env_name.to_owned(),
            _visibility: PhantomData,
        })
    }

    /// Precedence: `<NAME>` env var, then `file_value` (config file); errors if neither is set.
    pub fn required(
        env_name: &str,
        env: &Env,
        file_value: Option<T>,
    ) -> error::PublicResult<Param<T, V>>
    where
        T: std::str::FromStr,
        <T as FromStr>::Err: std::fmt::Display,
    {
        let value = match env.var(env_name) {
            Some(raw) => parse_value(env_name, &raw)?,
            None => file_value.ok_or_else(|| {
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
    pub fn optional_or_file(
        env_name: &str,
        env: &Env,
        file_path: Option<String>,
        default: T,
    ) -> error::PublicResult<Param<T, V>>
    where
        T: std::str::FromStr,
        <T as FromStr>::Err: std::fmt::Display,
    {
        let raw = match resolve_plain_or_file(env_name, env)? {
            Some(raw) => Some(raw),
            None => file_path.map(|path| read_file_trimmed(&path)).transpose()?,
        };

        let value = match raw {
            Some(raw) => parse_value(env_name, &raw)?,
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
    pub fn required_or_file(
        env_name: &str,
        env: &Env,
        file_path: Option<String>,
    ) -> error::PublicResult<Param<T, V>>
    where
        T: std::str::FromStr,
        <T as FromStr>::Err: std::fmt::Display,
    {
        let raw = match resolve_plain_or_file(env_name, env)? {
            Some(raw) => raw,
            None => match file_path {
                Some(path) => read_file_trimmed(&path)?,
                None => Err(error::Error::invalid_configuration(
                    env_name.to_owned(),
                    format!("missing: set `{env_name}` or `{env_name}_FILE`"),
                ))?,
            },
        };

        let value = parse_value(env_name, &raw)?;

        Ok(Self {
            value,
            env: env_name.to_owned(),
            _visibility: PhantomData,
        })
    }
}

/// Parses a raw value read for `env_name`, mapping a failure to a configuration error.
/// The error carries the parser's message (e.g. "number would be zero for non-zero
/// type") but never the raw value, which may be a secret.
fn parse_value<T>(env_name: &str, raw: &str) -> error::PublicResult<T>
where
    T: std::str::FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    raw.parse().map_err(|e| {
        error::Error::invalid_configuration(env_name.to_owned(), format!("unable to parse: {e}"))
            .into()
    })
}

/// Reads a file's content and strips trailing newlines (`\n` or `\r\n`).
/// Whitespace is kept, since it may be part of a password.
fn read_file_trimmed(path: &str) -> error::PublicResult<String> {
    Ok(std::fs::read_to_string(path)
        .map_err(|e| error::Error::invalid_configuration(path.to_owned(), e.to_string()))?
        .trim_end_matches(['\n', '\r'])
        .to_owned())
}

/// Resolves a value that may be set either directly via `<NAME>` or indirectly via
/// `<NAME>_FILE` (a path to a file containing it, read and trailing-newline-trimmed).
/// Returns `Ok(None)` if neither is set. Setting both is an error.
fn resolve_plain_or_file(name: &str, env: &Env) -> error::PublicResult<Option<String>> {
    let file_env = format!("{name}_FILE");

    let plain = env.var(name);
    let file_path = env.var(&file_env);

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

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_core::error::ErrorKind;
    use std::env;
    use std::num::NonZero;

    // SAFETY: each test below uses its own dedicated env var name, so concurrent
    // test threads never observe or mutate each other's variables.

    #[test]
    fn optional_or_file_uses_plain_value_when_set() {
        let name = "TEST_OPTIONAL_OR_FILE_PLAIN";
        unsafe { env::set_var(name, "plain-value") };

        let result =
            Param::<String>::optional_or_file(name, &Env::process(), None, "default".to_owned());

        unsafe { env::remove_var(name) };

        assert_eq!(result.unwrap().value, "plain-value");
    }

    #[test]
    fn optional_or_file_reads_file_when_file_var_set() {
        let name = "TEST_OPTIONAL_OR_FILE_FROM_FILE";
        let file_env = format!("{name}_FILE");

        let path = std::env::temp_dir().join("mosaicod_test_optional_or_file_secret");
        std::fs::write(&path, " secret-from-file\n").unwrap();
        unsafe { env::set_var(&file_env, path.to_str().unwrap()) };

        let result =
            Param::<String>::optional_or_file(name, &Env::process(), None, "default".to_owned());

        unsafe { env::remove_var(&file_env) };
        let _ = std::fs::remove_file(&path);

        assert_eq!(result.unwrap().value, " secret-from-file");
    }

    #[test]
    fn read_file_trimmed_strips_only_trailing_newlines() {
        let path = std::env::temp_dir().join("mosaicod_test_read_file_trimmed");
        std::fs::write(&path, "  pass word \r\n\n").unwrap();

        let result = read_file_trimmed(path.to_str().unwrap());
        let _ = std::fs::remove_file(&path);

        assert_eq!(result.unwrap(), "  pass word ");
    }

    #[test]
    fn optional_or_file_errors_when_both_set() {
        let name = "TEST_OPTIONAL_OR_FILE_BOTH";
        let file_env = format!("{name}_FILE");

        unsafe { env::set_var(name, "plain-value") };
        unsafe { env::set_var(&file_env, "/does/not/matter") };

        let result =
            Param::<String>::optional_or_file(name, &Env::process(), None, "default".to_owned());

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

        let result =
            Param::<String>::optional_or_file(name, &Env::process(), None, "default".to_owned());

        assert_eq!(result.unwrap().value, "default");
    }

    #[test]
    fn required_or_file_uses_plain_value_when_set() {
        let name = "TEST_REQUIRED_OR_FILE_PLAIN";
        unsafe { env::set_var(name, "plain-value") };

        let result = Param::<String>::required_or_file(name, &Env::process(), None);

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

        let result = Param::<String>::required_or_file(name, &Env::process(), None);

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

        let result = Param::<String>::required_or_file(name, &Env::process(), None);

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

        let result = Param::<String>::required_or_file(name, &Env::process(), None);

        assert!(matches!(
            result.unwrap_err().error().kind(),
            ErrorKind::InvalidConfiguration(_)
        ));
    }

    // --- config-file precedence tier ---

    #[test]
    fn optional_uses_file_value_when_env_unset() {
        let name = "TEST_OPTIONAL_FILE_VALUE";

        let result = Param::<u32>::optional(name, &Env::process(), Some(42), 0).unwrap();

        assert_eq!(result.value, 42);
    }

    #[test]
    fn optional_env_wins_over_file_value() {
        let name = "TEST_OPTIONAL_ENV_OVER_FILE";
        unsafe { env::set_var(name, "7") };

        let result = Param::<u32>::optional(name, &Env::process(), Some(42), 0).unwrap();

        unsafe { env::remove_var(name) };

        assert_eq!(result.value, 7);
    }

    #[test]
    fn optional_errors_on_unparsable_env() {
        // e.g. `MOSAICOD_PORT=abc` is a configuration error, not a panic.
        let name = "TEST_OPTIONAL_UNPARSABLE_ENV";
        unsafe { env::set_var(name, "abc") };

        let result = Param::<u32>::optional(name, &Env::process(), Some(42), 0);

        unsafe { env::remove_var(name) };

        let ErrorKind::InvalidConfiguration(msg) = result.unwrap_err().error().kind().clone()
        else {
            panic!("expected an invalid configuration error");
        };
        assert!(msg.contains("invalid digit"), "unexpected message: {msg}");
        assert!(
            !msg.contains("abc"),
            "the raw value must not be part of the message"
        );
    }

    #[test]
    fn optional_non_zero_errors_on_zero_env() {
        let name = "TEST_OPTIONAL_NON_ZERO_ZERO_ENV";
        unsafe { env::set_var(name, "0") };

        let result =
            Param::<NonZero<usize>>::optional(name, &Env::process(), None, NonZero::<usize>::MIN);

        unsafe { env::remove_var(name) };

        assert!(matches!(
            result.unwrap_err().error().kind(),
            ErrorKind::InvalidConfiguration(_)
        ));
    }

    #[test]
    fn optional_empty_env_falls_back_to_config_file_value() {
        // e.g. `MOSAICOD_*=` is treated as unset, so the config file value
        // is used instead of being overridden by an empty string.
        let name = "TEST_OPTIONAL_EMPTY_ENV_OVER_CONFIG_FILE";
        unsafe { env::set_var(name, "") };

        let result = Param::<String>::optional(
            name,
            &Env::process(),
            Some("value".to_owned()),
            String::new(),
        )
        .unwrap();

        unsafe { env::remove_var(name) };

        assert_eq!(result.value, "value");
    }

    #[test]
    fn optional_falls_back_to_default_when_env_and_file_unset() {
        let name = "TEST_OPTIONAL_NEITHER_ENV_NOR_FILE";

        let result = Param::<u32>::optional(name, &Env::process(), None, 0).unwrap();

        assert_eq!(result.value, 0);
    }

    #[test]
    fn required_uses_file_value_when_env_unset() {
        let name = "TEST_REQUIRED_FILE_VALUE";

        let result = Param::<u32>::required(name, &Env::process(), Some(42));

        assert_eq!(result.unwrap().value, 42);
    }

    #[test]
    fn required_errors_when_env_and_file_unset() {
        let name = "TEST_REQUIRED_NEITHER_ENV_NOR_FILE";

        let result = Param::<u32>::required(name, &Env::process(), None);

        assert!(matches!(
            result.unwrap_err().error().kind(),
            ErrorKind::InvalidConfiguration(_)
        ));
    }

    #[test]
    fn required_non_zero_errors_on_zero_env() {
        // e.g. `MOSAICOD_DEFAULT_PARALLELISM=0` is a configuration error, not a panic.
        let name = "TEST_REQUIRED_NON_ZERO_ZERO_ENV";
        unsafe { env::set_var(name, "0") };

        let result = Param::<NonZero<usize>>::required(name, &Env::process(), NonZero::new(4));

        unsafe { env::remove_var(name) };

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
            &Env::process(),
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
            &Env::process(),
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

        let result = Param::<u32>::optional(name, &Env::ignored(), Some(42), 0).unwrap();

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

        let result = Param::<String>::required_or_file(
            name,
            &Env::ignored(),
            Some(path.to_str().unwrap().to_owned()),
        );

        unsafe { env::remove_var(name) };
        unsafe { env::remove_var(&file_env) };
        let _ = std::fs::remove_file(&path);

        assert_eq!(result.unwrap().value, "secret-from-config-file");
    }
}
