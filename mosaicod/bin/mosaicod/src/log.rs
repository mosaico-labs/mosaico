use clap::ValueEnum;

#[derive(Debug, Copy, Clone, ValueEnum)]
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

#[derive(Debug, Copy, Clone, ValueEnum)]
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

pub fn init_logger(format: LogFormat, level: LogLevel) {
    use tracing_subscriber::prelude::*;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level.as_filter()));

    match format {
        LogFormat::Json => tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::TRACE)
            .json()
            .finish()
            .with(filter)
            .init(),
        LogFormat::Pretty => tracing_subscriber::FmtSubscriber::builder()
            .with_target(false)
            .with_ansi(true)
            .with_max_level(tracing::Level::TRACE)
            .finish()
            .with(filter)
            .init(),
        LogFormat::Plain => tracing_subscriber::FmtSubscriber::builder()
            .with_target(false)
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .finish()
            .with(filter)
            .init(),
    }
}
