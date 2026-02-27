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
    #[command(subcommand)]
    Auth(command::Auth),
}

fn start() -> Result<(), common::Error> {
    let args = Cli::try_parse()?;

    common::init_logger();
    common::load_env_variables()?;

    match args.cmd {
        Commands::Run(args) => command::run(args)?,
        Commands::Auth(args) => command::auth(args)?,
    }

    Ok(())
}

use colored::Colorize;

fn main() {
    common::pin_startup_time();

    let res = start();

    match res {
        Ok(_) => {}
        Err(e) => {
            print::error(&e.to_string());
            println!(
                "Please refer to {} for more informations.",
                "https://docs.mosaico.dev/daemon".cyan()
            )
        }
    }
}
