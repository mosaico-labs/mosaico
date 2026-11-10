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
    println!("cargo::rerun-if-env-changed=GIT_HASH");
    println!("cargo::rerun-if-env-changed=GIT_TAG");
    let git_hash = env_or_git("GIT_HASH", &["rev-parse", "HEAD"]).unwrap_or("undefined".into());
    println!("cargo:rustc-env=MOSAICOD_GIT_HASH={}", git_hash);

    // A release is a commit with the `v<version>` tag: it sets the `mosaicod_release` cfg.
    let release_tag = format!("v{}", env::var("CARGO_PKG_VERSION").unwrap());
    let tags = env_or_git("GIT_TAG", &["tag", "--points-at", "HEAD"]).unwrap_or_default();
    println!("cargo::rustc-check-cfg=cfg(mosaicod_release)");
    if tags.lines().any(|tag| tag.trim() == release_tag) {
        println!("cargo::rustc-cfg=mosaicod_release");
    }
}

/// Returns the environment variable `var` if set (even if empty), otherwise the output of
/// `git <args>`. Git errors are printed as cargo warnings, since builds without the git history
/// (e.g. from a source tarball) are still valid.
fn env_or_git(var: &str, args: &[&str]) -> Option<String> {
    if let Ok(value) = env::var(var) {
        return Some(value.trim().to_owned());
    }

    let result = Command::new("git")
        .args(args)
        .output()
        .map_err(|e| e.to_string())
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).map_err(|e| e.to_string())
            } else {
                Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
            }
        });

    match result {
        Ok(stdout) => Some(stdout.trim().to_owned()),
        Err(e) => {
            println!("cargo:warning=`git {}` failed: {e}", args.join(" "));
            None
        }
    }
}
