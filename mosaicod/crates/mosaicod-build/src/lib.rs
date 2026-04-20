use std::sync::OnceLock;

pub const BUILD_PROFILE: &str = env!("BUILD_PROFILE");
pub const SEMVER: &str = env!("VERSION");
pub const GIT_HASH: &str = env!("GIT_HASH");
pub const BUILD_TIME: &str = env!("BUILD_TIME");
pub const GIT_TAG: &str = env!("GIT_TAG");

static VERSION: OnceLock<String> = OnceLock::new();

pub fn version_description() -> &'static str {
    VERSION.get_or_init(|| {
        format!(
            r#"version {semver}, build {hash} ({profile})
Built: {time}"#,
            semver = SEMVER,
            profile = BUILD_PROFILE,
            hash = &GIT_HASH[..8],
            time = BUILD_TIME,
        )
    })
}
