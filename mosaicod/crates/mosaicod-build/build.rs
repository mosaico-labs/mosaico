use base64::Engine;
use chrono::Utc;
use std::{
    env, fs,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

/// Public release key (binary keyring). Release tags signed with it show the key in
/// `mosaicod --version`. It's also published on keyserver.ubuntu.com.
const RELEASE_KEYRING: &str = "release-key.gpg";

/// Base64 encoded release tag object (`git cat-file tag vX.Y.Z | base64 -w0`), for builds without
/// the git history (e.g. containers).
const RELEASE_TAG_ENV: &str = "MOSAICOD_RELEASE_TAG";

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

    let git_hash = env::var("GIT_HASH")
        .ok()
        .filter(|hash| !hash.trim().is_empty())
        .or_else(|| git(&["rev-parse", "HEAD"]))
        .unwrap_or_else(|| "undefined".to_owned());
    let git_hash = git_hash.trim();
    println!("cargo:rustc-env=MOSAICOD_GIT_HASH={}", git_hash);

    // A release is a commit with the `vX.Y.Z` tag of this version. If the tag is signed with the
    // release key, the key is shown too; if it can't be verified, the release has no signature.
    let tag = format!("v{}", env::var("CARGO_PKG_VERSION").unwrap());
    let tag_object = release_tag_object(&tag, git_hash);
    let release_key = tag_object.as_deref().and_then(verify_signature);

    println!(
        "cargo:rustc-env=MOSAICOD_BUILD_RELEASE={}",
        tag_object.is_some()
    );
    println!(
        "cargo:rustc-env=MOSAICOD_RELEASE_KEY={}",
        release_key.unwrap_or_default()
    );
}

/// Returns the object of the annotated `tag` if it points at the commit being built.
fn release_tag_object(tag: &str, git_hash: &str) -> Option<String> {
    let object = match env::var(RELEASE_TAG_ENV) {
        Ok(encoded) if !encoded.trim().is_empty() => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded.trim())
                .ok()?;
            String::from_utf8(bytes).ok()?
        }
        _ => git(&["cat-file", "tag", tag])?,
    };

    let headers: Vec<_> = object.lines().take_while(|line| !line.is_empty()).collect();
    let is_release = headers.contains(&format!("object {git_hash}").as_str())
        && headers.contains(&format!("tag {tag}").as_str());
    is_release.then_some(object)
}

/// Verifies the signature of a tag object against the release keyring and returns the fingerprint
/// of the key that made it.
fn verify_signature(tag_object: &str) -> Option<String> {
    let Some(start) = tag_object.find("-----BEGIN PGP SIGNATURE-----") else {
        println!("cargo:warning=release tag is not signed");
        return None;
    };
    let (payload, signature) = tag_object.split_at(start);

    let out_dir = env::var("OUT_DIR").unwrap();
    let signature_path = Path::new(&out_dir).join("release-tag.asc");
    fs::write(&signature_path, signature).ok()?;
    let keyring = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap()).join(RELEASE_KEYRING);

    // `gpgv` only trusts the given keyring; `--homedir` keeps the user's GnuPG files out of it.
    let mut gpgv = Command::new("gpgv")
        .arg("--homedir")
        .arg(&out_dir)
        .arg("--keyring")
        .arg(&keyring)
        .args(["--status-fd", "1"])
        .arg(&signature_path)
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .inspect_err(|e| println!("cargo:warning=unable to verify the release tag: {e}"))
        .ok()?;
    gpgv.stdin.take()?.write_all(payload.as_bytes()).ok()?;
    let output = gpgv.wait_with_output().ok()?;

    // `[GNUPG:] VALIDSIG <key fpr> ... <primary key fpr>` is only printed for a good signature.
    let fingerprint = String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("[GNUPG:] VALIDSIG "))
        .and_then(|fields| fields.split_whitespace().nth(9))
        .filter(|_| output.status.success())
        .map(str::to_owned);

    if fingerprint.is_none() {
        let error = String::from_utf8_lossy(&output.stderr).replace('\n', " ");
        println!("cargo:warning=invalid release tag signature: {error}");
    }
    fingerprint
}

/// Runs a git command and returns its trimmed output, or `None` if git is not available or fails.
fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    Some(stdout.trim().to_owned())
}
