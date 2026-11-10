use chrono::Utc;
use std::{env, process::Command};

fn main() {
    let opt_level = env::var("OPT_LEVEL").unwrap();
    println!("cargo:rustc-env=MOSAICOD_BUILD_OPT_LEVEL={}", opt_level);

    let debug = env::var("DEBUG").unwrap();
    println!("cargo:rustc-env=MOSAICOD_BUILD_DEBUG={}", debug);

    let arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let target_env = env::var("CARGO_CFG_TARGET_ENV").unwrap_or_else(|_| "unknown".to_string());
    println!(
        "cargo:rustc-env=MOSAICOD_BUILD_ARCH={}-{}-{}",
        os, arch, target_env
    );

    let profile = env::var("PROFILE").unwrap();
    println!("cargo:rustc-env=MOSAICOD_BUILD_PROFILE={}", profile);

    let build_time = Utc::now().to_rfc2822();
    println!("cargo:rustc-env=MOSAICOD_BUILD_TIME={}", build_time);

    // Builds without the git history (e.g. containers) get the commit and the tags from the
    // environment.
    let git_hash = env_or_git("GIT_HASH", &["rev-parse", "HEAD"]).unwrap_or("undefined".into());
    println!("cargo:rustc-env=MOSAICOD_GIT_HASH={}", git_hash);

    // A release is a commit with the `v<version>` tag.
    let release_tag = format!("v{}", env::var("CARGO_PKG_VERSION").unwrap());
    let tags = env_or_git("GIT_TAG", &["tag", "--points-at", "HEAD"]).unwrap_or_default();
    let release = tags.lines().any(|tag| tag.trim() == release_tag);
    println!("cargo:rustc-env=MOSAICOD_BUILD_RELEASE={}", release);
}

/// Returns the environment variable `var` if set, otherwise the output of `git <args>`.
fn env_or_git(var: &str, args: &[&str]) -> Option<String> {
    if let Ok(value) = env::var(var)
        && !value.trim().is_empty()
    {
        return Some(value.trim().to_owned());
    }

    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_owned())
}
