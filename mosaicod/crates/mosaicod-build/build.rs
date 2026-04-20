use chrono::Utc;
use std::process::Command;

fn main() {
    let profile = std::env::var("PROFILE").expect("Missing PROFILE during build.");
    println!("cargo:rustc-env=BUILD_PROFILE={}", profile);

    let sha = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("Failed to execute git command");
    let git_hash = String::from_utf8(sha.stdout).unwrap();
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);

    let output = Command::new("git")
        .args(["tag", "--points-at", "HEAD"])
        .output()
        .expect("Failed to execute git command");
    let git_tag = String::from_utf8(output.stdout)
        .unwrap_or_else(|_| "".to_string())
        .trim()
        .to_string();
    let git_tag = if git_tag.starts_with("mosaicod/v") {
        git_tag
    } else {
        "".to_owned()
    };
    println!("cargo:rustc-env=GIT_TAG={}", git_tag);

    let version = if !git_tag.is_empty() {
        git_tag
            .strip_prefix("mosaicod/v")
            .expect("Non conforming tag")
            .to_owned()
    } else {
        format!("{}-devel", env!("CARGO_PKG_VERSION"))
    };
    println!("cargo:rustc-env=VERSION={}", version);

    let build_time = Utc::now().to_rfc2822();
    println!("cargo:rustc-env=BUILD_TIME={}", build_time);
}
