use chrono::Utc;
use std::process::Command;

fn main() {
    let opt_level = std::env::var("OPT_LEVEL").unwrap();
    println!("cargo:rustc-env=MOSAICOD_BUILD_OPT_LEVEL={}", opt_level);

    let debug = std::env::var("DEBUG").unwrap();
    println!("cargo:rustc-env=MOSAICOD_BUILD_DEBUG={}", debug);

    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
    let env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_else(|_| "unknown".to_string());
    println!(
        "cargo:rustc-env=MOSAICOD_BUILD_ARCH={}-{}-{}",
        os, arch, env
    );

    let profile = std::env::var("PROFILE").unwrap();
    println!("cargo:rustc-env=MOSAICOD_BUILD_PROFILE={}", profile);

    let build_time = Utc::now().to_rfc2822();
    println!("cargo:rustc-env=MOSAICOD_BUILD_TIME={}", build_time);

    let git_hash = std::env::var("GIT_HASH").unwrap_or_else(|_| {
        String::from_utf8(
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .expect("Failed to execute git command")
                .stdout,
        )
        .unwrap()
    });
    println!("cargo:rustc-env=MOSAICOD_GIT_HASH={}", git_hash);
}
