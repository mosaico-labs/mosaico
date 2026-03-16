// Setup default global allocator
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod command;
mod common;
mod log;
mod print;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
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
    /// Start the mosaico server
    Run(command::Run),

    /// Manage mosaico API keys
    #[command(subcommand, name = "api-key")]
    Auth(command::ApiKey),
}

fn start() -> Result<Option<String>, common::Error> {
    let cli_parse_res = Cli::try_parse().map_err(|e| e.to_string());

    // Avoid to show error message when parsing cli commands
    let args = match cli_parse_res {
        Ok(args) => args,
        Err(err) => {
            return Ok(Some(err.to_string()));
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

use colored::Colorize;

fn main() {
    common::pin_startup_time();

    let res = start();

    match res {
        Ok(opt_msg) => {
            if let Some(msg) = opt_msg {
                println!("{msg}");
            }
        }
        Err(e) => {
            print::error(&e.to_string());
            println!(
                "Please refer to {} for more informations.",
                "https://docs.mosaico.dev/daemon".cyan()
            )
        }
    }
}
