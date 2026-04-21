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

    let build_time = Utc::now().to_rfc2822();
    println!("cargo:rustc-env=BUILD_TIME={}", build_time);
}
