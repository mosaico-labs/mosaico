//! Tiny helper to read the local hostname, with a sane fallback. Kept as its own crate since it
//! has no relation to any particular domain (instance registry, logging, ...) and shouldn't force
//! its callers to pull in a heavier crate just for this.

use tracing::warn;

/// Reads the local hostname, falling back to `"unknown"` if it can't be determined.
pub fn local_hostname() -> String {
    hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_else(|e| {
            warn!("unable to determine local hostname: {}", e);
            "unknown".to_owned()
        })
}
