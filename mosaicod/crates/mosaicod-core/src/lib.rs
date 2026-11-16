pub mod error;
pub use error::{Error, ErrorKind};
#![allow(clippy::module_name_repetitions)] // Domain names intentionally mirror the wire/DB vocabulary.
#![allow(clippy::must_use_candidate)] // Applied selectively where ignoring the result is meaningful.

pub mod constants;
pub mod random;
pub mod traits;
pub mod types;
