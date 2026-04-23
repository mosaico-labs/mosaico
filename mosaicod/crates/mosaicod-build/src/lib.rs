use std::sync::OnceLock;

pub const BUILD_PROFILE: &str = env!("MOSAICOD_BUILD_PROFILE");
pub const BUILD_OPT_LEVEL: &str = env!("MOSAICOD_BUILD_OPT_LEVEL");
pub const BUILD_DEBUG: &str = env!("MOSAICOD_BUILD_DEBUG");
pub const BUILD_ARCH: &str = env!("MOSAICOD_BUILD_ARCH");

pub const SEMVER: &str = env!("CARGO_PKG_VERSION");
pub const GIT_HASH: &str = env!("MOSAICOD_GIT_HASH");
pub const BUILD_TIME: &str = env!("MOSAICOD_BUILD_TIME");

static VERSION: OnceLock<String> = OnceLock::new();

pub fn version_description() -> &'static str {
    VERSION.get_or_init(|| {
        format!(
            r#"version {semver} ({hash})

Profile:  {profile}-opt{opt_level}
Arch:     {arch} 
Built:    {time}"#,
            semver = SEMVER,
            profile = BUILD_PROFILE,
            hash = &GIT_HASH[..8],
            time = BUILD_TIME,
            arch = BUILD_ARCH,
            opt_level = BUILD_OPT_LEVEL,
        )
    })
}
