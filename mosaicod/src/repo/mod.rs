pub mod core;
pub use core::{AsExec, Config, Cx, Database, Repository, Tx, UNREGISTERED};

mod facades;
pub use facades::*;

// Exported queries
//
// We expose a minimal set of queries to ensure that database logic and
// operations remain encapsulated within the facade layer.
pub use sql_models::{get_resource_locator_from_name, layer_bootstrap};

mod error;
pub use error::Error;

#[cfg(test)]
pub use core::testing;

// Private to module exports

mod sql_models;
use sql_models::*;
