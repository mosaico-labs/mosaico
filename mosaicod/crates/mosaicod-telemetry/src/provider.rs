use mosaicod_core::types::ExtraLogLayer;
use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{
    ExporterBuildError, LogExporter, MetricExporter, SpanExporter, WithExportConfig as _,
};
use opentelemetry_sdk::{
    Resource, logs::SdkLoggerProvider, metrics::SdkMeterProvider,
    propagation::TraceContextPropagator, trace::SdkTracerProvider,
};
use tracing_subscriber::{
    Layer as _,
    filter::{LevelFilter, Targets},
};

use crate::{METER_NAME, RPC_SPAN_TARGET};

const OTEL_SDK_DISABLED: &str = "OTEL_SDK_DISABLED";
const OTEL_EXPORTER_OTLP_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
const OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";
const OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT";
const OTEL_EXPORTER_OTLP_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";
const OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL";
const OTEL_EXPORTER_OTLP_METRICS_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL";
const OTEL_EXPORTER_OTLP_LOGS_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL";
const OTEL_LOGS_EXPORTER: &str = "OTEL_LOGS_EXPORTER";

/// Value of `OTEL_LOGS_EXPORTER` that enables the export of logs over OTLP.
const OTLP_LOGS_EXPORTER: &str = "otlp";

/// Targets whose events are never exported as logs. They belong to the OTLP exporter and
/// its HTTP client: exporting them would emit new events on every export, in a loop.
const LOGS_EXCLUDED_TARGETS: [&str; 4] = ["opentelemetry", "hyper", "reqwest", "h2"];

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
/// Dropping it flushes pending metrics, spans and logs. It must be created and dropped
/// outside of any tokio runtime, since the OTLP HTTP client is blocking and runs its exports
/// on dedicated background threads.
#[derive(Default)]
pub struct Telemetry {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
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
    /// `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_ENDPOINT`. Logs also need
    /// `OTEL_LOGS_EXPORTER=otlp`, since by default they only go to stdout.
    /// `OTEL_SDK_DISABLED=true` disables everything. The resource is built from
    /// `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`, plus `service.version`.
    pub fn from_env(service_version: &str) -> Result<Self, Error> {
        if env_is(OTEL_SDK_DISABLED, "true") {
            return Ok(Self::disabled());
        }

        let traces_enabled = endpoint_set(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT);
        let metrics_enabled = endpoint_set(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT);
        let logs_enabled = env_is(OTEL_LOGS_EXPORTER, OTLP_LOGS_EXPORTER)
            && endpoint_set(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT);

        if !traces_enabled && !metrics_enabled && !logs_enabled {
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
                .with_resource(resource.clone())
                .build();

            global::set_meter_provider(provider.clone());
            telemetry.meter_provider = Some(provider);
        }

        if logs_enabled {
            check_protocol(OTEL_EXPORTER_OTLP_LOGS_PROTOCOL)?;

            let exporter = LogExporter::builder()
                .with_http()
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .build()?;

            // The batch processor honours the `OTEL_BLRP_*` variables (default delay 1s).
            let provider = SdkLoggerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource)
                .build();

            telemetry.logger_provider = Some(provider);
        }

        Ok(telemetry)
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

        // Thread attributes mean little for async tasks that move between worker threads,
        // and `busy_ns`/`idle_ns` read the clock on every poll. Both cost allocations on
        // every span, so they are left out.
        let layer = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer(METER_NAME))
            .with_threads(false)
            .with_tracked_inactivity(false)
            .with_filter(filter);

        Some(Box::new(layer))
    }

    /// Returns the `tracing` layer that exports log events over OTLP, if logs are enabled.
    ///
    /// Unlike [`Telemetry::tracing_layer`], the layer is meant to receive the same events as
    /// the log output, so the caller applies the log filter to it (see
    /// [`init_logger`](mosaicod_core::types::init_logger)). The layer only drops the events of
    /// the OTLP exporter itself. When traces are enabled too, each record carries the trace
    /// and span IDs of the span it was emitted in.
    pub fn logs_layer(&self) -> Option<ExtraLogLayer> {
        let provider = self.logger_provider.as_ref()?;

        let filter = Targets::new()
            .with_default(LevelFilter::TRACE)
            .with_targets(LOGS_EXCLUDED_TARGETS.map(|target| (target, LevelFilter::OFF)));

        let layer = OpenTelemetryTracingBridge::new(provider).with_filter(filter);

        Some(Box::new(layer))
    }
}

impl Drop for Telemetry {
    /// Flushes and shuts down the providers.
    fn drop(&mut self) {
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

        // Last, so the warnings above can still be exported.
        if let Some(provider) = self.logger_provider.take()
            && let Err(e) = provider.shutdown()
        {
            tracing::warn!("failed to shut down the OpenTelemetry logger provider: {e}");
        }
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

/// Returns `true` if the OTLP endpoint is set, either for every signal or through the
/// given signal specific variable.
fn endpoint_set(signal_var: &str) -> bool {
    env_set(OTEL_EXPORTER_OTLP_ENDPOINT) || env_set(signal_var)
}

fn env_set(var: &str) -> bool {
    std::env::var(var).is_ok_and(|v| !v.trim().is_empty())
}

fn env_is(var: &str, value: &str) -> bool {
    std::env::var(var).is_ok_and(|v| v.trim().eq_ignore_ascii_case(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_has_no_layer() {
        let telemetry = Telemetry::disabled();
        assert!(telemetry.tracing_layer().is_none());
        assert!(telemetry.logs_layer().is_none());
    }

    /// Returns a telemetry instance that exports logs and spans to the in-memory exporters.
    fn in_memory_telemetry() -> (
        Telemetry,
        opentelemetry_sdk::logs::InMemoryLogExporter,
        opentelemetry_sdk::trace::InMemorySpanExporter,
    ) {
        let logs = opentelemetry_sdk::logs::InMemoryLogExporter::default();
        let spans = opentelemetry_sdk::trace::InMemorySpanExporter::default();

        let telemetry = Telemetry {
            logger_provider: Some(
                SdkLoggerProvider::builder()
                    .with_simple_exporter(logs.clone())
                    .build(),
            ),
            tracer_provider: Some(
                SdkTracerProvider::builder()
                    .with_simple_exporter(spans.clone())
                    .build(),
            ),
            meter_provider: None,
        };

        (telemetry, logs, spans)
    }

    fn exported_targets(logs: &opentelemetry_sdk::logs::InMemoryLogExporter) -> Vec<String> {
        logs.get_emitted_logs()
            .unwrap()
            .iter()
            .filter_map(|log| log.record.target().map(|t| t.to_string()))
            .collect()
    }

    #[test]
    fn logs_layer_drops_exporter_events() {
        use tracing_subscriber::prelude::*;

        let (telemetry, logs, _) = in_memory_telemetry();
        let subscriber = tracing_subscriber::registry().with(telemetry.logs_layer());

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "mosaicod::server", "kept");
            tracing::info!(target: "opentelemetry_sdk", "dropped");
            tracing::info!(target: "hyper_util::client", "dropped");
            tracing::info!(target: "reqwest::connect", "dropped");
            tracing::info!(target: "h2::codec", "dropped");
        });

        assert_eq!(exported_targets(&logs), vec!["mosaicod::server"]);
    }

    #[test]
    fn logs_carry_the_trace_context() {
        use tracing_subscriber::prelude::*;

        let (telemetry, logs, _) = in_memory_telemetry();
        // Both layers are `Layer<Registry>`, so they are installed together, as in
        // `init_logger`.
        let layers: Vec<ExtraLogLayer> = telemetry
            .tracing_layer()
            .into_iter()
            .chain(telemetry.logs_layer())
            .collect();
        let subscriber = tracing_subscriber::registry().with(layers);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(target: "mosaicod::server", "request");
            let _guard = span.enter();
            tracing::info!(target: "mosaicod::server", "inside the request");
        });

        let emitted = logs.get_emitted_logs().unwrap();
        assert_eq!(emitted.len(), 1);
        assert!(emitted[0].record.trace_context().is_some());
    }
}
