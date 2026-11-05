use std::sync::OnceLock;

pub const BUILD_PROFILE: &str = env!("MOSAICOD_BUILD_PROFILE");
pub const BUILD_OPT_LEVEL: &str = env!("MOSAICOD_BUILD_OPT_LEVEL");
pub const BUILD_DEBUG: &str = env!("MOSAICOD_BUILD_DEBUG");
pub const BUILD_ARCH: &str = env!("MOSAICOD_BUILD_ARCH");

pub const SEMVER: &str = env!("CARGO_PKG_VERSION");
pub const GIT_HASH: &str = env!("MOSAICOD_GIT_HASH");
pub const BUILD_TIME: &str = env!("MOSAICOD_BUILD_TIME");

/// `"true"` when built from the release tag of this version, `"false"` otherwise.
pub const BUILD_RELEASE: &str = env!("MOSAICOD_BUILD_RELEASE");

/// Fingerprint of the key that signed the release tag, if the signature was verified at build
/// time. Empty for development builds and for releases whose signature could not be verified.
pub const RELEASE_KEY: &str = env!("MOSAICOD_RELEASE_KEY");

/// Returns `true` if this is a release build, i.e. built from the release tag of this version.
pub fn is_release() -> bool {
    BUILD_RELEASE == "true"
}

static VERSION: OnceLock<String> = OnceLock::new();

pub fn version_description() -> &'static str {
    let hash = GIT_HASH.get(..8).unwrap_or("undefined");

    VERSION.get_or_init(|| {
        let version = match (is_release(), RELEASE_KEY) {
            (true, "") => format!("version {SEMVER} ({hash})"),
            (true, key) => format!("version {SEMVER} ({hash}) signed {key}"),
            (false, _) => format!("version {SEMVER}-dev ({hash})"),
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
