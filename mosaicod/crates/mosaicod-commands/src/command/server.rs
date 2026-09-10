use crate::{common, print};
use clap::Args;
use mosaicod_core::{self as core, error::PublicResult as Result, params};
use mosaicod_db as db;
use mosaicod_grpc as grpc;
use signal_hook::{consts::SIGINT, consts::SIGTERM, iterator::Signals};
use std::net::IpAddr;
use std::thread;
use tracing::{debug, info};

#[derive(Args, Debug)]
pub struct Server {
    /// Specify a host address. Defaults to MOSAICOD_HOST, then the loopback address
    /// `127.0.0.1`.
    #[arg(long)]
    pub host: Option<IpAddr>,

    /// Defines the port to listen on. Defaults to MOSAICOD_PORT, then `6726`.
    #[arg(long)]
    pub port: Option<u16>,
}

fn tls_config() -> grpc::TlsConfig {
    grpc::TlsConfig {
        certificate_file: params::params().tls_certificate_file.value.clone().into(),
        private_key_file: params::params().tls_private_key_file.value.clone().into(),
    }
}

/// Start mosaicod server.
///
/// If `json_log` is enabled logs will be formatted in JSON format and startup info
/// are hidden.
pub fn server(args: Server, json_format: bool) -> Result<()> {
    info!("startup store");
    let store = common::init_store()?;
    let store_display_name = print::store_display_name(&store);

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

    let host = args.host.unwrap_or(params.host.value);
    let port = args.port.unwrap_or(params.port.value);

    let mut server = grpc::Server::new(host, port, store, db);

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

    server.start_and_wait(rt, || {
        if !json_format {
            print::startup_info(
                &host,
                port,
                &store_display_name,
                &db_config,
                &params::version(),
                common::startup_time(),
            );
        }
    })?;

    Ok(())
}
