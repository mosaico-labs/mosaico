use crate::{Context, Execute, common, print};
use clap::Args;
use mosaicod_config::default;
use mosaicod_core::{self as core, error::PublicResult as Result};
use mosaicod_db as db;

/// List registered mosaicod instances (server, cleanup) and cleanup routine status.
#[derive(Args, Debug)]
pub struct Ps {
    /// Also show instances with a "dead" status. By default these are hidden.
    #[arg(short, long, default_value_t = false)]
    all: bool,

    /// Also show the STARTED and LAST HEARTBEAT columns. By default these are hidden.
    #[arg(short, long, default_value_t = false)]
    verbose: bool,
}

impl Execute for Ps {
    fn execute(self, _ctx: &Context) -> Result<()> {
        ps(self)
    }
}

/// Lists registered `mosaicod` instances (servers, cleanup routines, and store optimizers) and
/// summarizes the cleanup routine's current status.
fn ps(args: Ps) -> Result<()> {
    let rt = common::init_runtime()?;

    let params = default::params();

    let db_config = db::Config {
        db_url: params.db_url.value.parse().map_err(|_| {
            core::Error::invalid_configuration(
                params.db_url.env.clone(),
                "unable to parse".to_owned(),
            )
        })?,
        user: params.db_user.value.clone(),
        password: params.db_password.value.clone(),
        // Here we are using only one connection since it's a CLI command
        max_connections: 1,
    };

    let db = common::init_db(&rt, &db_config)?;

    let res: Result<()> = rt.block_on(async {
        let mut cx = db.connection();

        let instances = db::instance_registry_list(&mut cx).await?;
        let latest_cleanup = db::cleanup_log_latest(&mut cx).await?;

        print::print_instance_list(&instances, args.all, args.verbose);
        println!();
        print::print_cleanup_status(latest_cleanup.as_ref(), &instances);

        Ok(())
    });

    res?;

    Ok(())
}
