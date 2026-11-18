#![allow(clippy::module_name_repetitions)] // Type names intentionally mirror their module path.
#![allow(clippy::must_use_candidate)] // Accessors are called for their side effects in this crate.
#![allow(clippy::missing_errors_doc)] // Error conditions are documented on the error type itself.
#![allow(clippy::missing_panics_doc)] // Panics are documented where they can actually occur.
pub mod error;
pub use error::{Error, ErrorKind};
#![allow(clippy::module_name_repetitions)] // Domain names intentionally mirror the wire/DB vocabulary.
#![allow(clippy::must_use_candidate)] // Applied selectively where ignoring the result is meaningful.

pub mod constants;
pub mod random;
pub mod traits;
pub mod types;
