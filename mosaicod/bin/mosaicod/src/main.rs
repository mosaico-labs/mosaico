// Setup default global allocator
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod command;
mod common;
mod log;
mod print;

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use mosaicod_core::error::PublicResult as Result;

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
/// mosaicod - Mosaico high-performance daemon
struct Cli {
    /// Set the log output format
    #[arg(long, global = true, default_value_t = log::LogFormat::Pretty)]
    log_format: log::LogFormat,

    /// Set the log level
    #[arg(long, global = true, default_value_t = log::LogLevel::Warning)]
    log_level: log::LogLevel,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start mosaicod daemon
    Run(command::Run),

    /// Manage mosaico API keys
    #[command(subcommand, name = "api-key")]
    Auth(command::ApiKey),
}

fn start() -> Result<Option<String>> {
    let mut cmd = Cli::command();

    cmd = cmd.long_version(mosaicod_build::version_description());

    // Avoid to show error message when parsing cli commands
    let matches = match cmd.try_get_matches() {
        Ok(args) => args,
        Err(err) => {
            return Ok(Some(err.render().ansi().to_string()));
        }
    };

    let args = match Cli::from_arg_matches(&matches) {
        Ok(args) => args,
        Err(err) => {
            return Ok(Some(err.render().ansi().to_string()));
        }
    };

    print::set_colors(args.log_format);
    log::init_logger(args.log_format, args.log_level);

    common::load_env_variables()?;

    let is_json_output = matches!(args.log_format, log::LogFormat::Json);

    match args.cmd {
        Commands::Run(sub_args) => command::run(sub_args, is_json_output)?,
        Commands::Auth(sub_args) => command::auth(sub_args)?,
    }

    Ok(None)
}

fn main() {
    common::pin_startup_time();

    let res = start();

    match res {
        Ok(opt_msg) => {
            if let Some(msg) = opt_msg {
                print!("{msg}");
            }
        }
        Err(e) => {
            print::error(e);
            std::process::exit(-1);
        }
    }
}
