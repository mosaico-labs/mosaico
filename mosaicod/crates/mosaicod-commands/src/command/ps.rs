use crate::{common, print};
use clap::Args;
use mosaicod_core::{self as core, error::PublicResult as Result, params};
use mosaicod_db as db;

#[derive(Args, Debug)]
pub struct Ps {
    /// Also show instances with a "dead" status. By default these are hidden.
    #[arg(short, long, default_value_t = false)]
    all: bool,

    /// Also show the STARTED and LAST HEARTBEAT columns. By default these are hidden.
    #[arg(short, long, default_value_t = false)]
    verbose: bool,
}

/// Lists registered `mosaicod` instances (servers, cleanup routines, and store optimizers) and
/// summarizes the cleanup routine's current status.
pub fn ps(args: Ps) -> Result<()> {
    let rt = common::init_runtime()?;

    let params = params::params();

    let db_config = db::Config {
        db_url: params.db_url.value.parse().map_err(|_| {
            core::Error::invalid_configuration(
                params::params().db_url.env.clone(),
                "unable to parse".to_owned(),
            )
        })?,
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
