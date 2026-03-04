// Setup default global allocator
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod command;
mod common;
mod print;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
/// mosaicod - Mosaico high-performance daemon
struct Cli {
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

    // Avoid to show error message when parsing cli
    let args = match cli_parse_res {
        Ok(args) => args,
        Err(err) => {
            return Ok(Some(err.to_string()));
        }
    };

    common::init_logger();
    common::load_env_variables()?;

    match args.cmd {
        Commands::Run(args) => command::run(args)?,
        Commands::Auth(args) => command::auth(args)?,
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
