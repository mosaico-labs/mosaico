use super::{Cleanup, Server, StoreOptimizer};
use crate::{Context, Execute};
use clap::Subcommand;
use mosaicod_core::error::PublicResult as Result;

/// The commands that run a mosaicod service: the server and its background routines.
/// A binary adds them to its own commands with `#[command(flatten)]`.
#[derive(Subcommand, Debug)]
pub enum ServiceCommands {
    Server(Server),
    Cleanup(Cleanup),
    StoreOptimizer(StoreOptimizer),
}

impl Execute for ServiceCommands {
    fn execute(self, ctx: &Context) -> Result<()> {
        match self {
            Self::Server(cmd) => cmd.execute(ctx),
            Self::Cleanup(cmd) => cmd.execute(ctx),
            Self::StoreOptimizer(cmd) => cmd.execute(ctx),
        }
    }
}
