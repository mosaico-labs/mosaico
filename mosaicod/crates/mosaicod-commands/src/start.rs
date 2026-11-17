//! The pieces of the command line shared by every mosaicod binary.
//!
//! Each binary defines its own CLI with clap: it flattens [`GlobalArgs`] and the command
//! groups of [`crate::command`] it wants ([`crate::command::ServiceCommands`],
//! [`crate::command::UtilityCommands`]) into it, adds its own commands, and hands the
//! parsed command line to [`start`] through [`CommandLine`]. Every command implements
//! [`Execute`].

use std::process::ExitCode;

use clap::Args;
use mosaicod_config::{self as config, default};
use mosaicod_core::error::PublicResult as Result;
use mosaicod_core::types;
use tracing::debug;

use crate::{common, print};

/// The flags every mosaicod binary accepts, to add to its CLI with `#[command(flatten)]`.
#[derive(Args, Debug)]
pub struct GlobalArgs {
    /// Path to a config.toml file. Defaults to MOSAICOD_CONFIG_FILE, then
    /// $XDG_CONFIG_HOME/mosaicod/config.toml (or ~/.config/mosaicod/config.toml),
    /// then /etc/mosaicod/config.toml, then no config file.
    #[arg(long, global = true)]
    pub config: Option<std::path::PathBuf>,

    /// Set the log output format. Defaults to MOSAICOD_LOG_FORMAT, then `pretty`.
    #[arg(long, global = true)]
    pub log_format: Option<types::LogFormat>,

    /// Set the log level. Defaults to MOSAICOD_LOG_LEVEL, then `warning`.
    #[arg(long, global = true)]
    pub log_level: Option<types::LogLevel>,
}

/// What the global flags resolved to, for the command being run.
pub struct Context {
    pub is_json_output: bool,
}

/// A command of a mosaicod binary, run by [`start`] once the process is set up.
///
/// A binary implements it for its own enum of commands, delegating each variant to the
/// command it holds.
pub trait Execute {
    fn execute(self, ctx: &Context) -> Result<()>;

    /// Whether the command exports telemetry. Only the long-running commands do, so CLI
    /// commands such as `ps` stay quiet.
    fn exports_telemetry(&self) -> bool {
        false
    }
}

/// The parsed command line of a mosaicod binary, handed to [`start`]: its global flags
/// and the command to run.
pub trait CommandLine {
    type Command: Execute;

    fn into_parts(self) -> (GlobalArgs, Self::Command);
}

/// Runs the command parsed from the command line: loads the configuration from the
/// sources of `config`, sets up the logger, then executes it. On error, prints it and
/// returns a failure exit code.
pub fn start(config: config::ConfigBuilder, cli: impl CommandLine) -> ExitCode {
    let (args, command) = cli.into_parts();

    // The telemetry guard lives until the command returns, then flushes pending metrics,
    // spans and logs when it goes out of scope, outside the tokio runtime of the command.
    let res = init(config, &args, command.exports_telemetry())
        .and_then(|(ctx, _telemetry)| command.execute(&ctx));

    match res {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            print::error(e);
            ExitCode::FAILURE
        }
    }
}

/// Keeps the OpenTelemetry providers alive while the command runs.
#[cfg(feature = "telemetry")]
type TelemetryGuard = mosaicod_telemetry::Telemetry;
#[cfg(not(feature = "telemetry"))]
type TelemetryGuard = ();

/// Prepares the process to run a command: pins the startup time, loads the configuration
/// from the sources of `config`, sets up telemetry when `telemetry` is true, and sets up
/// the logger.
fn init(
    config: config::ConfigBuilder,
    args: &GlobalArgs,
    telemetry: bool,
) -> Result<(Context, TelemetryGuard)> {
    common::pin_startup_time();

    common::load_config(config, args.config.as_deref())?;

    let params = default::params();
    let log_format = args.log_format.unwrap_or(params.log_format.value);
    let log_level = args.log_level.unwrap_or(params.log_level.value);

    #[cfg(feature = "telemetry")]
    let guard = if telemetry {
        mosaicod_telemetry::Telemetry::from_env(env!("CARGO_PKG_VERSION"))
            .map_err(|e| mosaicod_core::Error::internal(Some(e.to_string())))?
    } else {
        mosaicod_telemetry::Telemetry::disabled()
    };
    #[cfg(feature = "telemetry")]
    let (extra_log_layer, export_log_layer) = (guard.tracing_layer(), guard.logs_layer());
    #[cfg(not(feature = "telemetry"))]
    let (guard, extra_log_layer, export_log_layer) = {
        let _ = telemetry;
        ((), None, None)
    };

    print::set_colors(log_format);
    types::init_logger(log_format, log_level, extra_log_layer, export_log_layer);

    debug!("{:#?}", params);

    let ctx = Context {
        is_json_output: matches!(log_format, types::LogFormat::Json),
    };

    Ok((ctx, guard))
}
