//! Usage reporting: events that tell Mosaico how mosaicod is used, sent as OTLP log records
//! to Mosaico's endpoint. Users turn it off with `MOSAICOD_USAGE_REPORTING_ENABLED=false`.
//!
//! Every event is one log record, whose event name says its kind (`mosaicod.identity`, …).
//! Every record carries the installation ID and the instance ID as resource attributes.
//!
//! It is kept apart from any OpenTelemetry setup the customer runs, in both directions:
//! - its logger provider is never installed as the global one, and there is no `tracing`
//!   bridge: events are only emitted through [`UsageReporter`], so no log line of the
//!   customer's can become a usage event;
//! - its resource reads no `OTEL_*` variable, and its exporter overrides those that would
//!   change where and how it sends, and drops the headers they add (see [`client`]), so
//!   none of the customer's endpoints, headers or resource attributes reach us.
//!
//! Records are exported in batches by a task on mosaicod's Tokio runtime, through a
//! bounded queue that drops records when full: an unreachable endpoint never slows a
//! request down.

// The integration tests' dependencies, unused by the unit tests.
#![cfg_attr(test, allow(unused_crate_dependencies))]

mod client;

use std::time::{Duration, SystemTime};

use mosaicod_config::default;
use mosaicod_core::types;
use opentelemetry::KeyValue;
use opentelemetry::logs::{AnyValue, LogRecord, Logger, LoggerProvider, Severity};
use opentelemetry_otlp::{LogExporter, Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::log_processor_with_async_runtime::BatchLogProcessor;
use opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider};
use opentelemetry_sdk::runtime;

/// The OTLP/HTTP URL usage reports are posted to.
///
/// Debug builds report to a local collector instead (see `docker/devel`), so that
/// development runs never mix with the data of real installations.
const ENDPOINT: &str = if cfg!(debug_assertions) {
    "http://localhost:4318/v1/logs"
} else {
    "https://telemetry.mosaico.dev/v1/logs"
};

/// How long a single export may take, connection included.
const EXPORT_TIMEOUT: Duration = Duration::from_secs(10);

/// The instrumentation scope of every usage record.
const SCOPE: &str = "mosaicod.usage";

/// Event name of the record sent once at startup.
const IDENTITY_EVENT: &str = "mosaicod.identity";

/// The installation ID sent when the real one couldn't be read: the nil UUID, so the
/// attribute is still a valid UUID.
const UNKNOWN_INSTALLATION_ID: &str = "00000000-0000-0000-0000-000000000000";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unable to build the HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("unable to build the exporter: {0}")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
}

/// What [`UsageReporter::new`] needs.
pub struct Config {
    /// The OTLP/HTTP URL records are posted to.
    pub endpoint: String,

    /// Shared by every process connected to the same database. `None` when it couldn't be
    /// read: records then carry [`UNKNOWN_INSTALLATION_ID`].
    pub installation_id: Option<types::Uuid>,

    /// This process's entry in the instance registry, as `mosaicod ps` shows it. Unique only
    /// within its installation.
    pub instance_id: i32,
}

impl Config {
    /// Reports to Mosaico's endpoint, or to the local collector in debug builds.
    pub fn new(installation_id: Option<types::Uuid>, instance_id: i32) -> Self {
        Self {
            endpoint: ENDPOINT.to_owned(),
            installation_id,
            instance_id,
        }
    }
}

/// Sends usage events. Cheap to clone: clones share the same queue and exporter.
///
/// Dropping the last clone sends what is still queued, then stops the export task. It
/// blocks until that last export ends, at most 10 seconds when the endpoint doesn't
/// answer. Drop it before the runtime, or what is still queued is lost, and never from a
/// current-thread runtime, which the wait would block forever.
#[derive(Clone)]
pub struct UsageReporter {
    /// Holds the only references to the provider, which shuts down when the last one
    /// drops.
    logger: SdkLogger,
}

impl UsageReporter {
    /// Starts the export task on `rt`.
    pub fn new(rt: &tokio::runtime::Handle, config: Config) -> Result<Self, Error> {
        let installation_id = config
            .installation_id
            .map_or_else(|| UNKNOWN_INSTALLATION_ID.to_owned(), |id| id.to_string());

        // Values set here take precedence over the `OTEL_EXPORTER_OTLP_*` variables. Headers
        // are the exception, which the client drops instead (see the `client` module).
        let exporter = LogExporter::builder()
            .with_http()
            .with_endpoint(config.endpoint)
            .with_protocol(Protocol::HttpBinary)
            .with_timeout(EXPORT_TIMEOUT)
            .with_http_client(client::OwnHeadersOnly::new(
                &default::user_agent(),
                EXPORT_TIMEOUT,
            )?)
            .build()?;

        // Built from scratch rather than with `Resource::builder()`, which would add
        // `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` from the environment.
        let resource = Resource::builder_empty()
            .with_attributes([
                KeyValue::new("service.name", default::NAME),
                // OpenTelemetry requires `service.instance.id` to be unique within its
                // namespace, and registry IDs are only unique within an installation.
                KeyValue::new("service.namespace", installation_id.clone()),
                KeyValue::new("service.version", default::version()),
                KeyValue::new("service.instance.id", config.instance_id.to_string()),
                KeyValue::new("mosaicod.installation_id", installation_id),
            ])
            .build();

        let processor = {
            // Building the processor spawns its export task with `tokio::spawn`, which only
            // works on a thread that has entered a runtime: this one enters `rt` until the
            // block ends.
            let _rt = rt.enter();
            BatchLogProcessor::builder(exporter, runtime::Tokio).build()
        };

        let provider = SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_log_processor(processor)
            .build();

        Ok(Self {
            logger: provider.logger(SCOPE),
        })
    }

    /// Emits the identity event, which records that this process started.
    pub fn emit_identity_event(&self) {
        self.emit(IDENTITY_EVENT, []);
    }

    fn emit(
        &self,
        event_name: &'static str,
        attributes: impl IntoIterator<Item = (&'static str, AnyValue)>,
    ) {
        let mut record = self.logger.create_log_record();
        record.set_event_name(event_name);
        record.set_timestamp(SystemTime::now());
        record.set_severity_number(Severity::Info);
        record.add_attributes(attributes);

        self.logger.emit(record);
    }
}
