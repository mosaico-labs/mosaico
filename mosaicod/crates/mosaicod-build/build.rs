use base64::Engine;
use chrono::Utc;
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

/// Fingerprint of the GPG key that signs release tags, published on [`KEYSERVER`].
///
/// A build of a commit tagged as a release (`vX.Y.Z`) verifies the tag signature against this key:
/// if it matches, the key is shown in `mosaicod --version`, otherwise the release has no signature.
// TODO: set the fingerprint of the release key once it is published.
const RELEASE_KEY_FINGERPRINT: &str = "";

const KEYSERVER: &str = "hkps://keyserver.ubuntu.com";

/// Base64 encoded signed tag object (`git cat-file tag vX.Y.Z | base64 -w0`), used to build a
/// release where the git history is not available (e.g. in a container).
const RELEASE_TAG_ENV: &str = "MOSAICOD_RELEASE_TAG";

const PGP_SIGNATURE_HEADER: &str = "-----BEGIN PGP SIGNATURE-----";

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
        .map(|hash| hash.trim().to_owned())
        .filter(|hash| !hash.is_empty())
        .or_else(|| git(&["rev-parse", "HEAD"]))
        .unwrap_or_else(|| "undefined".to_owned());
    println!("cargo:rustc-env=MOSAICOD_GIT_HASH={}", git_hash);

    let (release, release_key) = release(&git_hash);
    println!("cargo:rustc-env=MOSAICOD_BUILD_RELEASE={}", release);
    println!(
        "cargo:rustc-env=MOSAICOD_RELEASE_KEY={}",
        release_key.unwrap_or_default()
    );

    rerun_if_changed();
}

/// A `vX.Y.Z` tag pointing at the commit being built.
struct ReleaseTag {
    name: String,
    /// The tag object (`git cat-file tag`), `None` for a lightweight tag.
    object: Option<String>,
}

/// Returns whether this is a release build and, if the release tag signature is verified, the
/// fingerprint of the key that signed it.
///
/// Never fails the build: when something doesn't add up it prints a cargo warning, and builds a
/// development version (tag not matching this version or commit) or a release without signature
/// (signature missing or not verifiable).
fn release(git_hash: &str) -> (bool, Option<String>) {
    let tag = match release_tag(git_hash) {
        Ok(Some(tag)) => tag,
        Ok(None) => return (false, None),
        Err(e) => {
            println!("cargo:warning=building a development version: {e}");
            return (false, None);
        }
    };

    let expected = format!("v{}", env::var("CARGO_PKG_VERSION").unwrap());
    if tag.name != expected {
        println!(
            "cargo:warning=building a development version: tag {} doesn't match the crate version ({expected})",
            tag.name
        );
        return (false, None);
    }

    let key = tag
        .object
        .ok_or_else(|| "it is a lightweight tag".to_owned())
        .and_then(|object| verify_signature(&object));
    match key {
        Ok(key) => (true, Some(key)),
        Err(e) => {
            println!(
                "cargo:warning=building release {} without signature: {e}",
                tag.name
            );
            (true, None)
        }
    }
}

/// Returns the release tag of the commit being built, if any.
///
/// The tag object is read from [`RELEASE_TAG_ENV`] when set, otherwise from the `vX.Y.Z` tag
/// pointing at the commit being built.
fn release_tag(git_hash: &str) -> Result<Option<ReleaseTag>, String> {
    if let Ok(encoded) = env::var(RELEASE_TAG_ENV)
        && !encoded.trim().is_empty()
    {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded.trim())
            .map_err(|e| format!("{RELEASE_TAG_ENV} is not valid base64: {e}"))?;
        let object = String::from_utf8(bytes)
            .map_err(|e| format!("{RELEASE_TAG_ENV} is not valid UTF-8: {e}"))?;

        let name = tag_header(&object, "tag")?;
        let target = tag_header(&object, "object")?;
        if target != git_hash {
            return Err(format!(
                "tag {name} points at {target}, but the commit being built is {git_hash}"
            ));
        }

        return Ok(Some(ReleaseTag {
            name: name.to_owned(),
            object: Some(object),
        }));
    }

    let Some(tags) = git(&["tag", "--points-at", git_hash, "--list", "v*"]) else {
        return Ok(None);
    };
    let Some(name) = tags.lines().find(|tag| is_release_tag(tag)) else {
        return Ok(None);
    };

    Ok(Some(ReleaseTag {
        name: name.to_owned(),
        object: git(&["cat-file", "tag", name]),
    }))
}

/// Returns the value of a header (`object`, `type`, `tag`, `tagger`) of a tag object.
fn tag_header<'a>(object: &'a str, name: &str) -> Result<&'a str, String> {
    object
        .lines()
        .take_while(|line| !line.is_empty())
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
        .ok_or_else(|| format!("tag object has no `{name}` header"))
}

/// Verifies the GPG signature of a tag object against the release key. Returns the fingerprint of
/// the release key.
fn verify_signature(object: &str) -> Result<String, String> {
    let signature_start = object
        .find(PGP_SIGNATURE_HEADER)
        .ok_or_else(|| "the tag is not signed with a GPG key".to_owned())?;
    let (payload, signature) = object.split_at(signature_start);

    if RELEASE_KEY_FINGERPRINT.is_empty() {
        return Err("the release key fingerprint is not configured".to_owned());
    }

    let fingerprint = gpg_verify(payload, signature)?;
    if !fingerprint.eq_ignore_ascii_case(RELEASE_KEY_FINGERPRINT) {
        return Err(format!(
            "the tag is signed with key {fingerprint}, not with the release key {RELEASE_KEY_FINGERPRINT}"
        ));
    }

    Ok(RELEASE_KEY_FINGERPRINT.to_uppercase())
}

/// Verifies `signature` over `payload` with the release key fetched from [`KEYSERVER`], using a
/// temporary GPG home so the user keyring is never involved. Returns the fingerprint of the
/// primary key that made the signature.
fn gpg_verify(payload: &str, signature: &str) -> Result<String, String> {
    // GPG sockets live in its home directory and have a short path limit, so `OUT_DIR` can't be used.
    let home = env::temp_dir().join(format!("mosaicod-build-gpg-{}", std::process::id()));
    let result = gpg_verify_in(&home, payload, signature);
    cleanup_gpg_home(&home);
    result
}

fn gpg_verify_in(home: &Path, payload: &str, signature: &str) -> Result<String, String> {
    let _ = fs::remove_dir_all(home);
    fs::create_dir_all(home).map_err(|e| format!("unable to create a GPG home: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(home, fs::Permissions::from_mode(0o700))
            .map_err(|e| format!("unable to set permissions on the GPG home: {e}"))?;
    }

    let payload_path = home.join("tag");
    let signature_path = home.join("tag.asc");
    fs::write(&payload_path, payload).map_err(|e| format!("unable to write the tag: {e}"))?;
    fs::write(&signature_path, signature)
        .map_err(|e| format!("unable to write the tag signature: {e}"))?;

    let gpg = |args: &[&str]| {
        Command::new("gpg")
            .arg("--batch")
            .arg("--homedir")
            .arg(home)
            .args(args)
            .output()
            .map_err(|e| format!("gpg is not available: {e}"))
    };

    let recv = gpg(&[
        "--keyserver",
        KEYSERVER,
        "--recv-keys",
        RELEASE_KEY_FINGERPRINT,
    ])?;
    if !recv.status.success() {
        return Err(format!(
            "unable to fetch the release key from {KEYSERVER}: {}",
            String::from_utf8_lossy(&recv.stderr).trim()
        ));
    }

    let verify = gpg(&[
        "--status-fd",
        "1",
        "--verify",
        &signature_path.to_string_lossy(),
        &payload_path.to_string_lossy(),
    ])?;

    // `[GNUPG:] VALIDSIG <key fpr> <date> <timestamp> <expiry> <version> <reserved> <pubkey algo>
    // <hash algo> <class> <primary key fpr>` is only printed for a good signature.
    let status = String::from_utf8_lossy(&verify.stdout);
    let primary_fingerprint = status
        .lines()
        .find_map(|line| line.strip_prefix("[GNUPG:] VALIDSIG "))
        .and_then(|fields| fields.split_whitespace().nth(9));

    match primary_fingerprint {
        Some(fingerprint) if verify.status.success() => Ok(fingerprint.to_owned()),
        _ => Err(format!(
            "invalid signature: {}",
            String::from_utf8_lossy(&verify.stderr).trim()
        )),
    }
}

fn cleanup_gpg_home(home: &Path) {
    let _ = Command::new("gpgconf")
        .arg("--homedir")
        .arg(home)
        .args(["--kill", "all"])
        .output();
    let _ = fs::remove_dir_all(home);
}

/// A release tag is `vX.Y.Z`, with no suffix.
fn is_release_tag(tag: &str) -> bool {
    let Some(version) = tag.strip_prefix('v') else {
        return false;
    };
    let parts: Vec<_> = version.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
}

/// Reruns the build script when the inputs that decide the version change: the environment, the
/// current commit and the tags.
fn rerun_if_changed() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=GIT_HASH");
    println!("cargo:rerun-if-env-changed={RELEASE_TAG_ENV}");

    let mut paths = vec![
        "HEAD".to_owned(),
        "packed-refs".to_owned(),
        "refs/tags".to_owned(),
    ];
    if let Some(head_ref) = git(&["symbolic-ref", "-q", "HEAD"]) {
        paths.push(head_ref);
    }

    for path in paths {
        if let Some(git_path) = git(&["rev-parse", "--git-path", &path]) {
            let git_path = PathBuf::from(git_path);
            if git_path.exists() {
                println!("cargo:rerun-if-changed={}", git_path.display());
            }
        }
    }
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
