//! Helpers for tests, here and in the crates depending on this one, enabled by the
//! `testing` feature.

use crate::{ConfigBuilder, builder::CONFIG, params::DefaultConfig};

/// Sets a configuration made of [`DefaultConfig::testing`], unless one is already set.
pub fn load() {
    set_config(ConfigBuilder::new().add_conf(DefaultConfig::testing()));
}

/// Sets a configuration built from `builder`, unless one is already set: the first
/// call in a test binary wins, the later ones do nothing.
///
/// # Panics
///
/// If `builder` fails to build.
pub fn set_config(builder: ConfigBuilder) {
    if CONFIG.get().is_none() {
        let _ = crate::set_config(builder.build(None).expect("invalid testing configuration"));
    }
}
