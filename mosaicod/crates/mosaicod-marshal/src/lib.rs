//! Module responsible for marshaling and unmarshaling data structures
//! including metadata, actions, queries, and error handling.
mod metadata;
pub use metadata::*;

mod metadata_file;
pub use metadata_file::*;

mod format;
pub use format::*;

mod actions;
pub use actions::*;

mod query;
pub use query::*;

mod error;
pub use error::*;

mod cli;
pub use cli::*;

pub mod flight;

mod time;
pub use time::*;
