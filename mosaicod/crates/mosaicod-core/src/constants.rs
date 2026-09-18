//! Module containing general-purpose constants used across the codebase.
//!
//! Runtime *configuration* (the `Params` registry, populated from CLI/env/config-file/
//! default) lives in the `mosaicod-config` crate instead — it can't live here, since some
//! of these constants (e.g. [`ext`]) are used by `mosaicod-core` itself, and
//! `mosaicod-config` depends on `mosaicod-core` (for `error`/`types`), so the dependency
//! can't run the other way without a cycle.

/// Defines the name of the index timestamp column in the arrow schema
pub const ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP: &str = "timestamp_ns";

/// Internal resolution for floating point comparisons
pub const EPSILON: f64 = 1.0e-06;

pub const MAX_BUFFERED_FUTURES: usize = 8;

/// Module containing several file extensions
pub mod ext {
    /// Json file extension
    pub const JSON: &str = "json";
    pub const PARQUET: &str = "parquet";
}
