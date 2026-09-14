use crate::common;
use clap::Args;
use mosaicod_core::{self as core, error::PublicResult as Result, params, types};
use mosaicod_db as db;
use mosaicod_task as task;
use signal_hook::{consts::SIGINT, consts::SIGTERM, iterator::Signals};
use std::thread;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

#[derive(Args, Debug)]
pub struct Cleanup {
    /// Minimum interval, in seconds, that must pass between a cleanup run and the next one.
    ///
    /// When set to `0` a single cleanup is performed and then the process terminates. Any
    /// value greater than `0` runs the cleanup routine in a loop, sleeping `time_interval`
    /// seconds between runs. Defaults to MOSAICOD_CLEANUP_TIME_INTERVAL, then `0`.
    #[arg(long)]
    pub time_interval: Option<u32>,

    /// Maximum period, in seconds, an obsolete file is kept in the store before being
    /// permanently deleted. Defaults to MOSAICOD_CLEANUP_RETENTION_DURATION, then `86400`.
    #[arg(long)]
    pub retention_duration: Option<u32>,
}

/// Run the store cleanup routine.
pub fn cleanup(args: Cleanup) -> Result<()> {
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
        user: params.db_user.value.clone(),
        password: params.db_password.value.clone(),
        max_connections: params.max_db_connections.value,
    };

    info!("startup database connection");
    let db = common::init_db(&rt, &db_config)?;

    let shutdown = CancellationToken::new();

    // Forward SIGINT to the cancellation token so a running loop can exit gracefully.
    let mut signals = Signals::new([SIGINT, SIGTERM]).map_err(|_| {
        core::Error::internal(Some("unable to create termination signal".to_owned()))
    })?;

    let shutdown_signal = shutdown.clone();
    thread::spawn(move || {
        for sig in signals.forever() {
            debug!("received signal {:?}", sig);
            shutdown_signal.cancel();
        }
    });

    let time_interval = types::Duration::seconds(
        args.time_interval
            .unwrap_or(params.cleanup_time_interval.value),
    );
    let retention_duration = types::Duration::seconds(
        args.retention_duration
            .unwrap_or(params.cleanup_retention_duration.value),
    );

    rt.block_on(async {
        let cleanup = task::Cleanup::new(db, store)
            .with_time_interval(time_interval)
            .with_retention_duration(retention_duration);

        cleanup.run(shutdown).await
    })?;

    Ok(())
}
