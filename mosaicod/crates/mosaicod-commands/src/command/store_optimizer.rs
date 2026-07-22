use crate::common;
use clap::Args;
use mosaicod_core::{self as core, error::PublicResult as Result, params, types};
use mosaicod_db as db;
use mosaicod_task as task;
use signal_hook::{consts::SIGINT, iterator::Signals};
use std::thread;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

#[derive(Args, Debug)]
pub struct StoreOptimizer {
    /// Minimum interval, in seconds, that must pass between an optimization run and the next one.
    ///
    /// When set to `0` (the default) a single run is performed and then the process
    /// terminates. Any value greater than `0` runs the routine in a loop, sleeping
    /// `time_interval` seconds between each.
    #[arg(long, default_value_t = 0)]
    pub time_interval: u32,

    /// Maximum size (in bytes) for an output chunk after optimization.
    /// This is a soft limit, actual files on store may exceed this value slightly.
    #[arg(long, default_value_t = 256_000_000)]
    pub max_chunk_size: usize,
}

/// Run the store optimization routine.
pub fn store_optimization(args: StoreOptimizer) -> Result<()> {
    info!("startup store");
    let store = common::init_store()?;

    info!("startup multi-threaded runtime");
    let rt = common::init_runtime()?;

    let params = params::params();

    let db_config = db::Config {
        db_url: params.db_url.value.parse().map_err(|_| {
            core::Error::invalid_configuration(
                params::params().db_url.env.clone(),
                "unable to parse".to_owned(),
            )
        })?,
        max_connections: params.max_db_connections.value,
    };

    info!("startup database connection");
    let db = common::init_db(&rt, &db_config)?;

    let shutdown = CancellationToken::new();

    // Forward SIGINT to the cancellation token so a running loop can exit gracefully.
    let mut signals = Signals::new([SIGINT]).map_err(|_| {
        core::Error::internal(Some("unable to create termination signal".to_owned()))
    })?;

    let shutdown_signal = shutdown.clone();
    thread::spawn(move || {
        for sig in signals.forever() {
            debug!("received signal {:?}", sig);
            shutdown_signal.cancel();
        }
    });

    let time_interval = types::Duration::seconds(args.time_interval);

    rt.block_on(async {
        let store_optimizer = task::StoreOptimizer::new(db, store)
            .with_time_interval(time_interval)
            .with_max_file_size(args.max_chunk_size);

        store_optimizer.run(shutdown).await;
    });

    Ok(())
}
