// Setup default global allocator
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod cli;

use std::process::ExitCode;

use clap::Parser;
use cli::Cli;
use mosaicod_config::{ConfigBuilder, default::DefaultConfig};

fn main() -> ExitCode {
    let config = ConfigBuilder::new().add_conf(DefaultConfig::new());

    mosaicod_commands::start(config, Cli::parse())
}
