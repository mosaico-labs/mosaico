use crate::{METER_NAME, RPC_SPAN_TARGET};
use mosaicod_core::types::ExtraLogLayer;
use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_otlp::{ExporterBuildError, MetricExporter, SpanExporter, WithExportConfig as _};
use opentelemetry_sdk::{
    Resource, metrics::SdkMeterProvider, propagation::TraceContextPropagator,
    trace::SdkTracerProvider,
};
use tracing_subscriber::{Layer as _, filter::Targets};

const OTEL_SDK_DISABLED: &str = "OTEL_SDK_DISABLED";
const OTEL_EXPORTER_OTLP_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
const OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";
const OTEL_EXPORTER_OTLP_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";
const OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL";
const OTEL_EXPORTER_OTLP_METRICS_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL";

/// The only OTLP protocol supported by `mosaicod`.
const SUPPORTED_PROTOCOL: &str = "http/protobuf";

/// Default `service.name`, used when neither `OTEL_SERVICE_NAME` nor
/// `OTEL_RESOURCE_ATTRIBUTES` provide one.
const DEFAULT_SERVICE_NAME: &str = "mosaicod";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unable to build the OTLP exporter: {0}")]
    Exporter(#[from] ExporterBuildError),

    #[error(
        "unsupported OTLP protocol `{value}` in {var}, only `{SUPPORTED_PROTOCOL}` is supported"
    )]
    UnsupportedProtocol { var: &'static str, value: String },
}

/// Owns the OpenTelemetry providers of the process.
///
/// Dropping it (or calling [`Telemetry::shutdown`]) flushes pending metrics and spans. It
/// must be created and dropped outside of any tokio runtime, since the OTLP HTTP client is
/// blocking and runs its exports on dedicated background threads.
#[derive(Default)]
pub struct Telemetry {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl Telemetry {
    /// Returns a disabled instance: nothing is exported and the global providers stay no-op.
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Configures telemetry from the standard `OTEL_*` environment variables.
    ///
    /// Each signal is enabled only when its endpoint is configured, either through
    /// `OTEL_EXPORTER_OTLP_ENDPOINT` or through the signal specific
    /// `OTEL_EXPORTER_OTLP_{TRACES,METRICS}_ENDPOINT`. `OTEL_SDK_DISABLED=true` disables
    /// everything. The resource is built from `OTEL_SERVICE_NAME` and
    /// `OTEL_RESOURCE_ATTRIBUTES`, plus `service.version`.
    pub fn from_env(service_version: &str) -> Result<Self, Error> {
        if env_flag(OTEL_SDK_DISABLED) {
            return Ok(Self::disabled());
        }

        let traces_enabled =
            env_set(OTEL_EXPORTER_OTLP_ENDPOINT) || env_set(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT);
        let metrics_enabled =
            env_set(OTEL_EXPORTER_OTLP_ENDPOINT) || env_set(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT);

        if !traces_enabled && !metrics_enabled {
            return Ok(Self::disabled());
        }

        check_protocol(OTEL_EXPORTER_OTLP_PROTOCOL)?;

        let resource = resource(service_version);
        let mut telemetry = Self::disabled();

        if traces_enabled {
            check_protocol(OTEL_EXPORTER_OTLP_TRACES_PROTOCOL)?;

            let exporter = SpanExporter::builder()
                .with_http()
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .build()?;

            let provider = SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource.clone())
                .build();

            global::set_text_map_propagator(TraceContextPropagator::new());
            global::set_tracer_provider(provider.clone());
            telemetry.tracer_provider = Some(provider);
        }

        if metrics_enabled {
            check_protocol(OTEL_EXPORTER_OTLP_METRICS_PROTOCOL)?;

            let exporter = MetricExporter::builder()
                .with_http()
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .build()?;

            // The periodic reader honours `OTEL_METRIC_EXPORT_INTERVAL` (default 60s).
            let provider = SdkMeterProvider::builder()
                .with_periodic_exporter(exporter)
                .with_resource(resource)
                .build();

            global::set_meter_provider(provider.clone());
            telemetry.meter_provider = Some(provider);
        }

        Ok(telemetry)
    }

    /// Returns `true` if at least one signal is exported.
    pub fn is_enabled(&self) -> bool {
        self.tracer_provider.is_some() || self.meter_provider.is_some()
    }

    /// Returns the `tracing` layer that turns spans into OpenTelemetry spans, if traces are
    /// enabled.
    ///
    /// The layer has its own filter, independent from the log level: it records `mosaico*`
    /// spans and events at `INFO` and above, plus the spans created by
    /// [`RpcLayer`](crate::RpcLayer).
    pub fn tracing_layer(&self) -> Option<ExtraLogLayer> {
        let provider = self.tracer_provider.as_ref()?;

        let filter = Targets::new()
            .with_target("mosaico", tracing::Level::INFO)
            .with_target(RPC_SPAN_TARGET, tracing::Level::INFO);

        let layer = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer(METER_NAME))
            .with_filter(filter);

        Some(Box::new(layer))
    }

    /// Flushes and shuts down the providers. Calling it more than once is a no-op.
    pub fn shutdown(&mut self) {
        if let Some(provider) = self.meter_provider.take()
            && let Err(e) = provider.shutdown()
        {
            tracing::warn!("failed to shut down the OpenTelemetry meter provider: {e}");
        }

        if let Some(provider) = self.tracer_provider.take()
            && let Err(e) = provider.shutdown()
        {
            tracing::warn!("failed to shut down the OpenTelemetry tracer provider: {e}");
        }
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn resource(service_version: &str) -> Resource {
    // `Resource::builder` already reads `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`,
    // and falls back to `unknown_service[:<executable>]` for the service name.
    let detected = Resource::builder().build();

    let has_service_name = detected
        .get(&opentelemetry::Key::from_static_str("service.name"))
        .is_some_and(|name| !name.as_str().starts_with("unknown_service"));

    let mut builder = Resource::builder()
        .with_attribute(KeyValue::new("service.version", service_version.to_owned()));

    if !has_service_name {
        builder = builder.with_service_name(DEFAULT_SERVICE_NAME);
    }

    builder.build()
}

fn check_protocol(var: &'static str) -> Result<(), Error> {
    match std::env::var(var) {
        Ok(value) if !value.is_empty() && value != SUPPORTED_PROTOCOL => {
            Err(Error::UnsupportedProtocol { var, value })
        }
        _ => Ok(()),
    }
}

fn env_set(var: &str) -> bool {
    std::env::var(var).is_ok_and(|v| !v.trim().is_empty())
}

fn env_flag(var: &str) -> bool {
    std::env::var(var).is_ok_and(|v| v.trim().eq_ignore_ascii_case("true"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_has_no_layer() {
        let telemetry = Telemetry::disabled();
        assert!(!telemetry.is_enabled());
        assert!(telemetry.tracing_layer().is_none());
    }
}
