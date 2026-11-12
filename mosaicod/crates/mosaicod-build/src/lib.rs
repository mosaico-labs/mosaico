use std::sync::OnceLock;

pub const BUILD_PROFILE: &str = env!("MOSAICOD_BUILD_PROFILE");
pub const BUILD_OPT_LEVEL: &str = env!("MOSAICOD_BUILD_OPT_LEVEL");
pub const BUILD_DEBUG: &str = env!("MOSAICOD_BUILD_DEBUG");
pub const BUILD_ARCH: &str = env!("MOSAICOD_BUILD_ARCH");

pub const SEMVER: &str = env!("CARGO_PKG_VERSION");
pub const GIT_HASH: &str = env!("MOSAICOD_GIT_HASH");
pub const BUILD_TIME: &str = env!("MOSAICOD_BUILD_TIME");

/// `true` when built from the release tag of this version (`v<version>`), `false` otherwise.
pub const IS_RELEASE: bool = cfg!(mosaicod_release);

static VERSION: OnceLock<String> = OnceLock::new();

pub fn version_description() -> &'static str {
    let hash = GIT_HASH.get(..8).unwrap_or("undefined");

    VERSION.get_or_init(|| {
        // Builds not made from the release tag (e.g. the nightly or a local build of main) get
        // `-dev`, so they can't be mistaken for the release with the same version.
        let version = if IS_RELEASE {
            format!("version {SEMVER} ({hash})")
        } else {
            format!("version {SEMVER}-dev ({hash})")
        };

        format!(
            r#"{version}

Profile:  {profile}-opt{opt_level}
Arch:     {arch} 
Built:    {time}"#,
            profile = BUILD_PROFILE,
            time = BUILD_TIME,
            arch = BUILD_ARCH,
            opt_level = BUILD_OPT_LEVEL,
        )
    })
}
