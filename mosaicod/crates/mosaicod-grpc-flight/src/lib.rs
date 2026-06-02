//! Apache Arrow Flight gRPC service for mosaicod.
//!
//! Implements the [`FlightService`](arrow_flight::flight_service_server::FlightService)
//! trait on top of the mosaicod data layer, exposing:
//! - `do_get` / `do_put` — streaming reads and writes for topic data.
//! - `get_flight_info` / `list_flights` — metadata and discovery.
//! - `do_action` — dispatches named actions (sequence, topic, session,
//!   query, API-key management) defined in [`endpoint::actions`].
//!
//! The [`start`] function builds the Tonic server with the auth middleware from
//! [`mosaicod-grpc-common`] and optional TLS and gzip compression.

pub mod endpoint;

mod flight;
pub use flight::*;
