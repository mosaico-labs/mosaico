use clap::ValueEnum;

#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum LogLevel {
    Warning,
    Info,
    Debug,
}

impl LogLevel {
    pub fn as_filter(&self) -> String {
        let level = match self {
            Self::Warning => "warn",
            Self::Info => "info",
            Self::Debug => "trace",
        };

        format!("mosaico={}", level)
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Warning => write!(f, "warning"),
            Self::Info => write!(f, "info"),
            Self::Debug => write!(f, "debug"),
        }
    }
}

impl std::str::FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        <Self as ValueEnum>::from_str(s, true)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    Json,
    Pretty,
    Plain,
}

impl std::fmt::Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json => write!(f, "json"),
            Self::Plain => write!(f, "plain"),
            Self::Pretty => write!(f, "pretty"),
        }
    }
}

impl std::str::FromStr for LogFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        <Self as ValueEnum>::from_str(s, true)
    }
}

/// An additional `tracing` layer installed next to the log output, e.g. the OpenTelemetry
/// layer from `mosaicod-telemetry`. It carries its own filter, independent from the log level.
pub type ExtraLogLayer =
    Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync>;

/// Installs the global `tracing` subscriber.
///
/// `extra` is installed with its own filter, independent from the log level. `export`
/// sends the log lines somewhere else too, e.g. over OTLP, so it gets the same filter as
/// the log output and receives exactly the lines that are printed. The output on stdout
/// always stays, so the lines survive a crash or an unreachable destination.
pub fn init_logger(
    format: LogFormat,
    level: LogLevel,
    extra: Option<ExtraLogLayer>,
    export: Option<ExtraLogLayer>,
) {
    use tracing_subscriber::{fmt, prelude::*};

    // The filter only applies to the log output, so extra layers can see spans and events
    // that are not printed.
    let output = match format {
        LogFormat::Json => fmt::layer().json().boxed(),
        LogFormat::Pretty => fmt::layer().with_target(false).with_ansi(true).boxed(),
        LogFormat::Plain => fmt::layer().with_target(false).with_ansi(false).boxed(),
    };

    let mut layers: Vec<ExtraLogLayer> = Vec::new();
    layers.extend(extra);
    layers.extend(export.map(|layer| layer.with_filter(log_filter(level)).boxed()));

    tracing_subscriber::registry()
        .with(layers)
        .with(output.with_filter(log_filter(level)))
        .init();
}

/// Returns the filter of the log output: `RUST_LOG` if set, the given level otherwise.
fn log_filter(level: LogLevel) -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level.as_filter()))
}
