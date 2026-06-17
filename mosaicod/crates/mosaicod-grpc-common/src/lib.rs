//! Shared primitives for all mosaicod gRPC services.
//!
//! Provides the building blocks that every service crate depends on:
//! - [`middleware`] — Tower [`AuthLayer`](middleware::AuthLayer) that validates
//!   `mosaico-api-key-token` headers and injects [`AuthContext`](middleware::AuthContext)
//!   into request extensions.
//! - [`Error`] / [`Result`] — server-level error type with mapping to
//!   [`tonic::Status`] codes via [`PublicErrorGrpcExt`].
//! - [`ShutdownNotifier`] — thin wrapper around a [`CancellationToken`](tokio_util::sync::CancellationToken)
//!   used to coordinate graceful shutdown across tasks.
//!
pub mod middleware;

mod error;
pub use error::*;

mod shutdown;
pub use shutdown::*;
