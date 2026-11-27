use std::net::IpAddr;
use std::num::NonZero;
use std::thread;

use clap::Args;
use mosaicod_config::default;
use mosaicod_core::{self as core, error::PublicResult as Result, types};
use mosaicod_db as db;
use mosaicod_grpc as grpc;
use mosaicod_os::local_hostname;
use signal_hook::{consts::SIGINT, consts::SIGTERM, iterator::Signals};
use tracing::{debug, info, warn};

use crate::{Context, Execute, common, print};

/// Start the mosaicod server.
#[derive(Args, Debug)]
pub struct Server {
    /// Specify a host address. Defaults to MOSAICOD_HOST, then the loopback address
    /// `127.0.0.1`.
    #[arg(long)]
    pub host: Option<IpAddr>,

    /// Defines the port to listen on. Defaults to MOSAICOD_PORT, then `6726`.
    #[arg(long)]
    pub port: Option<NonZero<u16>>,
}

fn tls_config() -> grpc::TlsConfig {
    grpc::TlsConfig {
        certificate_file: default::params().tls_certificate_file.value.clone().into(),
        private_key_file: default::params().tls_private_key_file.value.clone().into(),
    }
}

impl Execute for Server {
    fn execute(self, ctx: &Context) -> Result<()> {
        server(self, ctx.is_json_output)
    }
}

/// Start mosaicod server.
///
/// If `json_log` is enabled logs will be formatted in JSON format and startup info
/// are hidden.
fn server(args: Server, json_format: bool) -> Result<()> {
    info!("startup multi-threaded runtime");
    let rt = common::init_runtime()?;

    info!("startup store");
    let store = common::init_store()?;
    common::check_store_connectivity(&rt, &store)?;
    let store_display_name = print::store_display_name(&store);

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
        max_connections: params.max_db_connections.value.get(),
    };

    info!("startup database connection");
    let db = common::init_db(&rt, &db_config)?;

    let host = args.host.unwrap_or(params.host.value);
    let port = args.port.unwrap_or(params.port.value).get();

    let mut server = grpc::Server::new(host, port, store, db.clone());

    if params.api_key_enabled.value {
        server.options.enable_api_key_management();
    }

    if params.tls_enabled.value {
        server.options.tls(tls_config());
    }

    if params.gzip_enabled.value {
        server.options.gzip(true);
    }

    // (cabba) NOTE: maybe we need to return a more specific error ?
    let mut signals = Signals::new([SIGINT, SIGTERM]).map_err(|_| {
        core::Error::internal(Some("unable to create termination signal".to_owned()))
    })?;

    let shutdown = server.shutdown.clone();
    thread::spawn(move || {
        for sig in signals.forever() {
            debug!("received signal {:?}", sig);
            shutdown.shutdown();
        }
    });

    // Registered before usage reporting starts, since every report carries the instance ID.
    let instance_id = rt
        .block_on(db::instance_registry_create(
            &mut db.connection(),
            types::InstanceKind::Server,
            &local_hostname(),
            std::process::id() as i32,
            chrono::Utc::now().timestamp(),
            // The server has no one-shot mode: it always runs until shut down.
            false,
        ))?
        .instance_id;

    // Kept until the server stops: dropping it sends what is still queued, on `rt`, so `rt`
    // must outlive it (don't move it into `start_and_wait`).
    let _usage = common::init_usage_reporting(&rt, &db, instance_id);

    let result = server.start_and_wait(&rt, instance_id, || {
        if !json_format {
            print::startup_info(
                &host,
                port,
                &store_display_name,
                &db_config,
                &default::version(),
                common::startup_time(),
            );
        }
    });

    // Deregistered even when the server failed, rather than left to be reaped later as stale.
    if let Err(e) = rt.block_on(db::instance_registry_delete(
        &mut db.connection(),
        instance_id,
    )) {
        warn!("failed to deregister instance {instance_id}: {e}");
    }

    result
}
