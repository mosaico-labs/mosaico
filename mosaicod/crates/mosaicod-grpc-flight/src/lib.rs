//! Apache Arrow Flight gRPC service for mosaicod.
//!
//! Implements the [`FlightService`](arrow_flight::flight_service_server::FlightService)
//! trait on top of the mosaicod data layer, exposing:
//! - `do_get` / `do_put` — streaming reads and writes for topic data.
//! - `get_flight_info` / `list_flights` — metadata and discovery.
//! - `do_action` — dispatches named actions (sequence, topic, session,
//!   query, API-key management) defined in `endpoint::actions`.
//!
//! The server itself, with the auth middleware from [`mosaicod_grpc_common`] and optional
//! TLS and gzip compression, is built by the `mosaicod-grpc` crate.

pub mod endpoint;

mod flight;
pub use flight::*;
