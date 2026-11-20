use clap::Subcommand;
use mosaicod_core::error::PublicResult as Result;

use super::Ps;
use crate::{Context, Execute};

/// The commands that support the services without running one, e.g. inspecting the
/// running instances. A binary adds them to its own commands with `#[command(flatten)]`.
#[derive(Subcommand, Debug)]
pub enum UtilityCommands {
    Ps(Ps),
}

impl Execute for UtilityCommands {
    fn execute(self, ctx: &Context) -> Result<()> {
        match self {
            Self::Ps(cmd) => cmd.execute(ctx),
        }
    }
}
