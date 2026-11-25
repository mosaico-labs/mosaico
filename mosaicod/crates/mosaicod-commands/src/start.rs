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

    match init(config, &args).and_then(|ctx| command.execute(&ctx)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            print::error(e);
            ExitCode::FAILURE
        }
    }
}

/// Prepares the process to run a command: pins the startup time, loads the configuration
/// from the sources of `config` and sets up the logger.
fn init(config: config::ConfigBuilder, args: &GlobalArgs) -> Result<Context> {
    common::pin_startup_time();

    common::load_config(config, args.config.as_deref())?;

    let params = default::params();
    let log_format = args.log_format.unwrap_or(params.log_format.value);
    let log_level = args.log_level.unwrap_or(params.log_level.value);

    print::set_colors(log_format);
    types::init_logger(log_format, log_level);

    debug!("{:#?}", params);

    Ok(Context {
        is_json_output: matches!(log_format, types::LogFormat::Json),
    })
}
