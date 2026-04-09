//! Module responsible for marshaling and unmarshaling data structures
//! including metadata, actions, queries, and error handling.
mod metadata;
pub use metadata::*;

mod format;
pub use format::*;

mod actions;
pub use actions::*;

mod query;
pub use query::*;

mod errors;
pub use errors::*;

mod cli;
pub use cli::*;

pub mod flight;
