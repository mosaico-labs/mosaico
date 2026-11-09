//! Builds the process configuration from a list of [`ConfigSource`].
//!
//! Each source declares the part of `config.toml` it reads as a struct, its
//! [`ConfigSource::Sections`]: every field of that struct is a top level section. Before building
//! anything, [`ConfigBuilder::build`] checks that no two sources declare the same section,
//! and that every section of `config.toml` is declared by some source.
//!
//! Env vars are read through [`Env`], which records them, so that two sources reading the
//! same variable are caught too.
//!
//! The result is set once per process with [`set_config`] and read with [`get`].

use crate::config;
use mosaicod_core::error::{self, PublicResult as Result};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_aux::serde_introspection::serde_introspect;
use std::any::{Any, TypeId, type_name};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, btree_map::Entry};
use std::path::Path;
use std::sync::OnceLock;

/// A set of configuration parameters, added to a [`ConfigBuilder`] with
/// [`ConfigBuilder::add_conf`].
pub trait ConfigSource: 'static {
    /// The part of `config.toml` this source reads. Each field is a top level section,
    /// named as serde names it (so `#[serde(rename)]`).
    ///
    /// It must be a plain struct: `#[serde(flatten)]` hides the field names and is
    /// rejected, and `#[serde(alias)]` is ignored, so an aliased name is an unknown
    /// section. Mark it `#[serde(default)]` unless its sections are meant to be required:
    /// a section absent from `config.toml` is otherwise a "missing field" error.
    type Sections: DeserializeOwned;

    /// The typed configuration this source builds, read back with [`get`].
    type Output: Send + Sync + 'static;

    /// Builds the configuration from the sections it declared, and from env vars read
    /// through `env`.
    fn build(self, sections: Self::Sections, env: &Env) -> Result<Self::Output>;
}

/// The environment tier of the configuration, which records every variable read from it.
pub struct Env {
    ignore: bool,
    read: RefCell<BTreeSet<String>>,
}

impl Env {
    /// Reads the process environment.
    #[cfg(any(test, feature = "testing"))]
    pub fn process() -> Self {
        Self::new(false)
    }

    /// Behaves as if no variable were set, e.g. for deterministic tests.
    #[cfg(any(test, feature = "testing"))]
    pub fn ignored() -> Self {
        Self::new(true)
    }

    fn new(ignore: bool) -> Self {
        Self {
            ignore,
            read: RefCell::default(),
        }
    }

    /// Returns the value of `name`. An empty value counts as unset, so that `NAME=` falls
    /// through to the next tier instead of overriding it with an empty string.
    ///
    /// The name is recorded even when the environment is ignored, so that a clash between
    /// sources doesn't depend on which variables happen to be set.
    pub fn var(&self, name: &str) -> Option<String> {
        self.read.borrow_mut().insert(name.to_owned());

        if self.ignore {
            return None;
        }

        std::env::var(name).ok().filter(|value| !value.is_empty())
    }
}

/// Builds a [`Config`] from the sources added to it.
///
/// ```ignore
/// let config = ConfigBuilder::new()
///     .add_conf(DefaultConfig::new())
///     .build(config_path)?;
///
/// set_config(config)?;
/// ```
#[derive(Default)]
pub struct ConfigBuilder {
    ignore_env: bool,
    sources: Vec<Box<dyn DynConfigSource>>,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Skips the environment tier for every source, as if no variable were set.
    #[cfg(any(test, feature = "testing"))]
    pub fn ignore_env(mut self, ignore_env: bool) -> Self {
        self.ignore_env = ignore_env;
        self
    }

    /// Adds a source. Sources are built in the order they are added.
    pub fn add_conf<S: ConfigSource>(mut self, source: S) -> Self {
        self.sources.push(Box::new(source));
        self
    }

    /// Checks the sources against each other and against `config.toml`, then builds them.
    /// `file` is the `config.toml` to read, if any.
    ///
    /// Before building anything, it fails with:
    /// - an internal error if two sources declare the same section, build the same type,
    ///   or declare their sections with `#[serde(flatten)]`;
    /// - a configuration error if `config.toml` has a section no source declares.
    ///
    /// While building, it fails with an internal error if two sources read the same env var.
    pub fn build(self, file: Option<&Path>) -> Result<Config> {
        let (content, origin) = match file {
            Some(path) => (config::read_config_file(path)?, path.display().to_string()),
            None => (String::new(), "config file".to_owned()),
        };

        // Keeps the position of every key and value, so that a deserialization error can
        // point at the line of `config.toml` it comes from.
        let mut file = toml::de::DeTable::parse(&content)
            .map_err(|e| error::Error::invalid_configuration(origin.clone(), e.to_string()))?;

        // 1. What is known from the types alone, checked before reading any value.
        let mut sections = BTreeMap::new();
        let mut outputs = HashMap::new();

        for source in &self.sources {
            let declared = source.sections();

            if declared.is_empty() {
                Err(error::Error::internal(Some(format!(
                    "the `Sections` of the `{}` config source must be a plain struct",
                    source.name(),
                ))))?;
            }

            set_owner(
                &mut sections,
                declared.iter().copied(),
                source.name(),
                "section",
            )?;

            if let Some(other) = outputs.insert(source.output_type(), source.name()) {
                Err(error::Error::internal(Some(format!(
                    "`{}` is built by both the `{other}` and `{}` config sources",
                    source.output_type_name(),
                    source.name(),
                ))))?;
            }
        }

        if let Some(unknown) = file
            .get_ref()
            .keys()
            .find(|key| !sections.contains_key(&**key.get_ref()))
        {
            Err(error::Error::invalid_configuration(
                origin.clone(),
                format!("unknown section `{}`", unknown.get_ref()),
            ))?;
        }

        // 2. Building, with each source given only its own sections.
        let mut values = HashMap::new();
        let mut env_vars = BTreeMap::new();

        for source in self.sources {
            let name = source.name();
            let output = source.output_type();

            // From the table of all the sections of `config.toml`, takes the ones this source
            // declared (the serde field names of its `Sections`), absent ones skipped.
            let own: toml::de::DeTable = source
                .sections()
                .iter()
                .filter_map(|section| file.get_mut().remove_entry(*section))
                .collect();

            let env = Env::new(self.ignore_env);
            let own = toml::Spanned::new(file.span(), own);
            let value = source.build(own, &content, &origin, &env)?;

            set_owner(&mut env_vars, env.read.into_inner(), name, "env var")?;
            values.insert(output, value);
        }

        Ok(Config { values })
    }
}

/// Records that `source` owns each of `keys`, sections or env vars (`kind` names which,
/// for the error message). `owners` maps every key assigned so far to its source.
///
/// Fails if another source already owns a key: the same setting would otherwise be read
/// by two sources, each giving it its own meaning.
fn set_owner<K: Ord + std::fmt::Display>(
    owners: &mut BTreeMap<K, &'static str>,
    keys: impl IntoIterator<Item = K>,
    source: &'static str,
    kind: &str,
) -> Result<()> {
    for key in keys {
        match owners.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(source);
            }
            Entry::Occupied(entry) => Err(error::Error::internal(Some(format!(
                "{kind} `{}` is read by both the `{}` and `{source}` config sources",
                entry.key(),
                entry.get(),
            ))))?,
        }
    }

    Ok(())
}

/// The configuration built by a [`ConfigBuilder`]: one value per source.
pub struct Config {
    values: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl Config {
    /// The value built by the source whose output is `T`, if any.
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.values.get(&TypeId::of::<T>())?.downcast_ref()
    }
}

pub(crate) static CONFIG: OnceLock<Config> = OnceLock::new();

/// Sets the process configuration. It can be set only once, and never changes afterwards.
pub fn set_config(config: Config) -> Result<()> {
    CONFIG
        .set(config)
        .map_err(|_| error::Error::internal(Some("configuration already set".to_owned())))?;

    Ok(())
}

/// The value built by the source whose output is `T`.
///
/// # Panics
///
/// If the configuration hasn't been set with [`set_config`], or no source builds `T`.
pub fn get<T: 'static>() -> &'static T {
    CONFIG
        .get()
        .expect("configuration not set, call `set_config` first")
        .get()
        .unwrap_or_else(|| panic!("no config source builds `{}`", type_name::<T>()))
}

/// A [`ConfigSource`] seen without its types, so that sources of different types can be
/// kept in the same list.
///
/// `ConfigSource` can't be used as `dyn ConfigSource`: its `Sections` and `Output` types differ
/// from source to source. This trait exposes the same source through types that are the
/// same for all of them (names, a [`TypeId`], a TOML table, a boxed value). It is
/// implemented for every `ConfigSource` by the impl below, and [`ConfigBuilder::add_conf`]
/// boxes a source as `dyn DynConfigSource`: who writes a source never sees it.
trait DynConfigSource {
    /// The type name of the source, used in error messages and to record which source
    /// owns a section or an env var.
    fn name(&self) -> &'static str;

    /// The top level sections of `config.toml` the source reads: the field names of its
    /// [`ConfigSource::Sections`], as serde names them. Empty if `Sections` isn't a plain struct.
    fn sections(&self) -> &'static [&'static str];

    /// The type the source builds, the key its value is stored under in [`Config`]. Two
    /// sources can't build the same type, or [`get`] couldn't tell which value to return.
    fn output_type(&self) -> TypeId;

    /// The name of the type the source builds, for error messages.
    fn output_type_name(&self) -> &'static str;

    /// Builds the source: deserializes `own`, the sections of `config.toml` it declared,
    /// into its `Sections`, calls [`ConfigSource::build`] and boxes the result.
    ///
    /// `content` is the text of the config file, and `origin` names it in the errors of
    /// the deserialization. `env` records the env vars the source reads.
    fn build<'i>(
        self: Box<Self>,
        own: toml::Spanned<toml::de::DeTable<'i>>,
        content: &'i str,
        origin: &str,
        env: &Env,
    ) -> Result<Box<dyn Any + Send + Sync>>;
}

impl<S: ConfigSource> DynConfigSource for S {
    fn name(&self) -> &'static str {
        type_name::<S>()
    }

    fn sections(&self) -> &'static [&'static str] {
        serde_introspect::<S::Sections>()
    }

    fn output_type(&self) -> TypeId {
        TypeId::of::<S::Output>()
    }

    fn output_type_name(&self) -> &'static str {
        type_name::<S::Output>()
    }

    fn build<'i>(
        self: Box<Self>,
        own: toml::Spanned<toml::de::DeTable<'i>>,
        content: &'i str,
        origin: &str,
        env: &Env,
    ) -> Result<Box<dyn Any + Send + Sync>> {
        let sections = S::Sections::deserialize(toml::de::Deserializer::from(own)).map_err(
            |mut e: toml::de::Error| {
                // The error knows where it is in the file but not the file's text: with
                // it, it shows the line and column, and the line itself.
                e.set_input(Some(content));
                error::Error::invalid_configuration(origin.to_owned(), e.to_string())
            },
        )?;

        let value = ConfigSource::build(*self, sections, env)?;

        Ok(Box::new(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::{DefaultConfig, Params};
    use mosaicod_core::error::ErrorKind;
    use serde::Deserialize;

    /// A source with its own `[extra]` section and `MOSAICOD_EXTRA_LEVEL` env var.
    struct ExtraSource;

    #[derive(Debug, Default, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields, default)]
    struct ExtraFile {
        extra: ExtraConfig,
    }

    #[derive(Debug, Default, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields, default)]
    struct ExtraConfig {
        level: Option<u32>,
    }

    impl ConfigSource for ExtraSource {
        type Sections = ExtraFile;
        type Output = ExtraConfig;

        fn build(self, file: ExtraFile, env: &Env) -> Result<ExtraConfig> {
            let mut config = file.extra;
            if let Some(level) = env.var("MOSAICOD_EXTRA_LEVEL") {
                config.level = level.parse().ok();
            }
            Ok(config)
        }
    }

    /// Declares `[server]`, which [`DefaultConfig`] declares too.
    struct SameSection;

    #[derive(Default, Deserialize)]
    #[serde(default)]
    struct SameSectionFile {
        #[allow(dead_code)]
        server: toml::Table,
    }

    impl ConfigSource for SameSection {
        type Sections = SameSectionFile;
        type Output = u8;

        fn build(self, _file: SameSectionFile, _env: &Env) -> Result<u8> {
            Ok(0)
        }
    }

    /// Declares only `[other]`.
    #[derive(Default, Deserialize)]
    #[serde(default)]
    struct OtherFile {
        #[allow(dead_code)]
        other: toml::Table,
    }

    /// Reads `MOSAICOD_PORT`, which [`DefaultConfig`] reads too.
    struct SameEnvVar;

    impl ConfigSource for SameEnvVar {
        type Sections = OtherFile;
        type Output = u16;

        fn build(self, _file: OtherFile, env: &Env) -> Result<u16> {
            env.var("MOSAICOD_PORT");
            Ok(0)
        }
    }

    /// Reads `MOSAICOD_DB_URL`, which [`DefaultConfig`] reads even when testing.
    struct SameDbEnvVar;

    impl ConfigSource for SameDbEnvVar {
        type Sections = OtherFile;
        type Output = u16;

        fn build(self, _file: OtherFile, env: &Env) -> Result<u16> {
            env.var("MOSAICOD_DB_URL");
            Ok(0)
        }
    }

    /// Builds the same type as [`ExtraSource`].
    struct SameOutput;

    impl ConfigSource for SameOutput {
        type Sections = OtherFile;
        type Output = ExtraConfig;

        fn build(self, _file: OtherFile, _env: &Env) -> Result<ExtraConfig> {
            Ok(ExtraConfig::default())
        }
    }

    /// Declares its sections through `#[serde(flatten)]`, which hides them.
    struct Flattened;

    #[derive(Default, Deserialize)]
    struct FlattenedFile {
        #[serde(flatten)]
        #[allow(dead_code)]
        inner: ExtraFile,
    }

    impl ConfigSource for Flattened {
        type Sections = FlattenedFile;
        type Output = i64;

        fn build(self, _file: FlattenedFile, _env: &Env) -> Result<i64> {
            Ok(0)
        }
    }

    fn build(name: &str, content: &str, builder: ConfigBuilder) -> Result<Config> {
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, content).unwrap();

        let result = builder.ignore_env(true).build(Some(&path));

        let _ = std::fs::remove_file(&path);
        result
    }

    fn error_kind(result: Result<Config>) -> ErrorKind {
        match result {
            Ok(_) => panic!("expected an error"),
            Err(e) => e.error().kind().clone(),
        }
    }

    #[test]
    fn serde_introspect_reads_serde_field_names() {
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct Renamed {
            #[serde(rename = "iam")]
            identity: u8,
            other: u8,
        }

        assert_eq!(
            serde_introspect::<config::ConfigFile>(),
            [
                "server",
                "database",
                "store",
                "query_engine",
                "encoding",
                "store_optimizer",
                "cleanup",
                "logging",
            ]
        );
        assert_eq!(serde_introspect::<Renamed>(), ["iam", "other"]);
        assert!(serde_introspect::<FlattenedFile>().is_empty());
    }

    #[test]
    fn default_config_alone_reads_its_sections() {
        let config = build(
            "mosaicod_test_builder_default.toml",
            "[server]\nport = 9999\n",
            ConfigBuilder::new().add_conf(DefaultConfig::testing()),
        )
        .unwrap();

        assert_eq!(config.get::<Params>().unwrap().port.value.get(), 9999);
    }

    #[test]
    fn another_source_reads_its_own_section_from_the_same_file() {
        let config = build(
            "mosaicod_test_builder_extra.toml",
            "[server]\nport = 9999\n\n[extra]\nlevel = 3\n",
            ConfigBuilder::new()
                .add_conf(DefaultConfig::testing())
                .add_conf(ExtraSource),
        )
        .unwrap();

        assert_eq!(config.get::<Params>().unwrap().port.value.get(), 9999);
        assert_eq!(
            config.get::<ExtraConfig>(),
            Some(&ExtraConfig { level: Some(3) })
        );
    }

    #[test]
    fn unknown_section_is_a_configuration_error() {
        let kind = error_kind(build(
            "mosaicod_test_builder_unknown.toml",
            "[extra]\nlevel = 3\n",
            ConfigBuilder::new().add_conf(DefaultConfig::testing()),
        ));

        assert!(
            matches!(&kind, ErrorKind::InvalidConfiguration(m) if m.contains("`extra`")),
            "{kind:?}"
        );
    }

    #[test]
    fn unknown_section_is_reported_before_missing_values() {
        // `DefaultConfig::new()` requires `MOSAICOD_DB_URL`, which is missing here: the
        // unknown section must still be the error reported, as it's found first.
        let kind = error_kind(build(
            "mosaicod_test_builder_unknown_first.toml",
            "[extra]\nlevel = 3\n",
            ConfigBuilder::new().add_conf(DefaultConfig::new()),
        ));

        assert!(
            matches!(&kind, ErrorKind::InvalidConfiguration(m) if m.contains("`extra`")),
            "{kind:?}"
        );
    }

    #[test]
    fn unknown_field_in_a_section_is_a_configuration_error() {
        let kind = error_kind(build(
            "mosaicod_test_builder_unknown_field.toml",
            "[server]\nhostt = \"127.0.0.1\"\n",
            ConfigBuilder::new().add_conf(DefaultConfig::testing()),
        ));

        assert!(
            matches!(&kind, ErrorKind::InvalidConfiguration(m) if m.contains("hostt")),
            "{kind:?}"
        );
    }

    #[test]
    fn invalid_value_error_points_at_its_line() {
        let kind = error_kind(build(
            "mosaicod_test_builder_invalid_value_line.toml",
            "# comment\n\n[server]\nport = \"abc\"\n",
            ConfigBuilder::new().add_conf(DefaultConfig::testing()),
        ));

        assert!(
            matches!(&kind, ErrorKind::InvalidConfiguration(m) if m.contains("line 4, column 8")),
            "{kind:?}"
        );
    }

    #[test]
    fn two_sources_declaring_the_same_section_is_an_internal_error() {
        let kind = error_kind(build(
            "mosaicod_test_builder_same_section.toml",
            "",
            ConfigBuilder::new()
                .add_conf(DefaultConfig::testing())
                .add_conf(SameSection),
        ));

        assert!(
            matches!(&kind, ErrorKind::Internal(m) if m.contains("section `server`")),
            "{kind:?}"
        );
    }

    #[test]
    fn two_sources_reading_the_same_env_var_is_an_internal_error() {
        let kind = error_kind(build(
            "mosaicod_test_builder_same_env.toml",
            "",
            ConfigBuilder::new()
                .add_conf(DefaultConfig::testing())
                .add_conf(SameEnvVar),
        ));

        assert!(
            matches!(&kind, ErrorKind::Internal(m) if m.contains("env var `MOSAICOD_PORT`")),
            "{kind:?}"
        );
    }

    #[test]
    fn db_env_var_of_testing_config_is_still_owned() {
        let kind = error_kind(build(
            "mosaicod_test_builder_same_db_env.toml",
            "",
            ConfigBuilder::new()
                .add_conf(DefaultConfig::testing())
                .add_conf(SameDbEnvVar),
        ));

        assert!(
            matches!(&kind, ErrorKind::Internal(m) if m.contains("env var `MOSAICOD_DB_URL`")),
            "{kind:?}"
        );
    }

    #[test]
    fn two_sources_building_the_same_type_is_an_internal_error() {
        let kind = error_kind(
            ConfigBuilder::new()
                .add_conf(ExtraSource)
                .add_conf(SameOutput)
                .build(None),
        );

        assert!(
            matches!(&kind, ErrorKind::Internal(m) if m.contains("ExtraConfig")),
            "{kind:?}"
        );
    }

    #[test]
    fn flattened_file_is_an_internal_error() {
        let kind = error_kind(ConfigBuilder::new().add_conf(Flattened).build(None));

        assert!(
            matches!(&kind, ErrorKind::Internal(m) if m.contains("plain struct")),
            "{kind:?}"
        );
    }

    #[test]
    fn set_config_can_be_called_only_once() {
        // The only test of this binary that sets the process configuration.
        let config = || {
            ConfigBuilder::new()
                .add_conf(ExtraSource)
                .build(None)
                .unwrap()
        };

        set_config(config()).unwrap();
        assert!(set_config(config()).is_err());

        assert_eq!(get::<ExtraConfig>(), &ExtraConfig::default());
    }
}
