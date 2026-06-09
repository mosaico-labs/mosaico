//! Entry point for the mosaicod gRPC server.
//!
//! This crate wires together the Arrow Flight service ([`mosaicod-grpc-flight`])
//! and the event broker service ([`mosaicod-grpc-broker`]) into a single runnable
//! server process. It owns the [`Server`] struct that binds both services to their
//! configured addresses, spawns the background cleanup task, and coordinates
//! graceful shutdown through a shared [`ShutdownNotifier`](mosaicod_grpc_common::ShutdownNotifier).
mod server;
pub use server::*;
