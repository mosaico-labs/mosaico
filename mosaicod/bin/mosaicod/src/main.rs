// Setup default global allocator
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use mosaicod_commands::{command, common, print};
use mosaicod_core::error::PublicResult as Result;
use mosaicod_core::{params, types};
use tracing::debug;

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
/// mosaicod - Mosaico high-performance daemon
struct Cli {
    /// Set the log output format. Defaults to MOSAICOD_LOG_FORMAT, then `pretty`.
    #[arg(long, global = true)]
    log_format: Option<types::LogFormat>,

    /// Set the log level. Defaults to MOSAICOD_LOG_LEVEL, then `warning`.
    #[arg(long, global = true)]
    log_level: Option<types::LogLevel>,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the mosaicod server
    Server(command::Server),

    /// Run the store cleanup routine
    Cleanup(command::Cleanup),

    /// Run the store optimization routine
    #[command(name = "store-optimizer")]
    StoreOptimizer(command::StoreOptimizer),

    /// Manage mosaico API keys
    #[command(subcommand, name = "api-key")]
    Auth(command::ApiKey),

    /// List registered mosaicod instances (server, cleanup) and cleanup routine status
    Ps(command::Ps),
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

    common::load_env_variables()?;

    let params = params::params();
    let log_format = args.log_format.unwrap_or(params.log_format.value);
    let log_level = args.log_level.unwrap_or(params.log_level.value);

    print::set_colors(log_format);
    types::init_logger(log_format, log_level);

    debug!("{:#?}", params);

    let is_json_output = matches!(log_format, types::LogFormat::Json);

    match args.cmd {
        Commands::Server(sub_args) => command::server(sub_args, is_json_output)?,
        Commands::Cleanup(sub_args) => command::cleanup(sub_args)?,
        Commands::StoreOptimizer(sub_args) => command::store_optimization(sub_args)?,
        Commands::Auth(sub_args) => command::auth(sub_args)?,
        Commands::Ps(sub_args) => command::ps(sub_args)?,
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
