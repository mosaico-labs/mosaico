//! OpenTelemetry support for `mosaicod`.
//!
//! This crate exports metrics and traces over OTLP (HTTP/protobuf) to an OpenTelemetry
//! Collector. By default logs are not exported here: they go to stdout and are collected by
//! the platform (e.g. the Collector `filelog` receiver on Kubernetes). With
//! `OTEL_LOGS_EXPORTER=otlp` they are also exported over OTLP, next to metrics and traces,
//! and the Collector should then stop reading them from stdout.
//!
//! It provides two pieces:
//!
//! * [`Telemetry`], which sets up the meter, tracer and logger providers from the standard
//!   `OTEL_*` environment variables, installs them globally and flushes them on shutdown.
//!   Telemetry is disabled when no OTLP endpoint is configured, so `mosaicod` behaves as
//!   before unless explicitly opted in.
//! * [`RpcLayer`], a tower layer that records a duration histogram and an in-flight counter
//!   for every gRPC call and opens a server span for it.
//!
//! Instruments are created from the global meter provider, so any crate can record its own
//! metrics with [`opentelemetry::global::meter`] using [`METER_NAME`].

mod provider;
mod rpc;

pub use provider::{Error, Telemetry};
pub use rpc::{RpcBody, RpcLayer, RpcResponseFuture, RpcService};

/// Name of the meter and tracer used by `mosaicod`.
pub const METER_NAME: &str = "mosaicod";

/// Target of the spans created by [`RpcLayer`].
///
/// It intentionally does not start with `mosaico`, so the default log filter
/// (`mosaico=<level>`) does not print these spans as a prefix on every log line. They are
/// only consumed by the OpenTelemetry layer.
pub const RPC_SPAN_TARGET: &str = "otel::rpc";
