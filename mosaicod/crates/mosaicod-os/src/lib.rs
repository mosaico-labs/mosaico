//! Tiny helper to read the local hostname, with a sane fallback. Kept as its own crate since it
//! has no relation to any particular domain (instance registry, logging, ...) and shouldn't force
//! its callers to pull in a heavier crate just for this.

mod host;
pub use host::*;
