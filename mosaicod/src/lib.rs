#![deny(clippy::semicolon_if_nothing_returned)]
#![deny(clippy::unnecessary_semicolon)]
#![deny(clippy::explicit_iter_loop)]
#![deny(clippy::manual_string_new)]
#![deny(clippy::unwrap_or_default)]

pub mod arrow;
pub mod marshal;
pub mod params;
pub mod query;
pub mod repo;
pub mod rw;
pub mod server;
pub mod store;
pub mod traits;
pub mod types;
pub mod utils;
