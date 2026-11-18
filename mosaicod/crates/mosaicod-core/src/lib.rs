#![allow(clippy::module_name_repetitions)] // Type names intentionally mirror their module path.
#![warn(clippy::pedantic)]
// Pedantic lints that do not fit this crate's public API or binary-encoding
// code, allowed crate-wide with the reason for each.
#![allow(clippy::module_name_repetitions)] // Public names mirror the domain vocabulary.
#![allow(clippy::must_use_candidate)] // Not every pure accessor benefits from `#[must_use]`.
#![allow(clippy::missing_errors_doc)] // Error conditions are documented on the error type.
#![allow(clippy::missing_panics_doc)] // Panics are not part of the public contract.
#![allow(clippy::cast_possible_truncation)] // Explicit casts are intentional in binary codecs.
#![allow(clippy::cast_sign_loss)] // Explicit casts are intentional in binary codecs.
#![allow(clippy::cast_precision_loss)] // Explicit casts are intentional in binary codecs.
#![allow(clippy::doc_markdown)] // Identifiers are intentionally left unquoted in docs.
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
