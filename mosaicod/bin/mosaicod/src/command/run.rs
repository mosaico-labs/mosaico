use crate::{common, print};
use clap::Args;
use mosaicod_core::params;
use mosaicod_db as db;
use mosaicod_server as server;
use mosaicod_store as store;
use signal_hook::{consts::SIGINT, iterator::Signals};
use std::thread;
use tracing::{debug, info};

#[derive(Args, Debug)]
pub struct Run {
    /// Specify a host address. It defaults to the loopback address `127.0.0.1`.
    #[arg(long)]
    pub host: Option<String>,

    /// Defines the default port value.
    #[arg(long, default_value_t = 6726)]
    pub port: u16,

    /// Enable to store objects on the local filesystem at the specified directory path
    #[arg(long)]
    pub local_store: Option<std::path::PathBuf>,

    /// Enable TLS. When enabled, the following envirnoment variables needs to be set
    /// MOSAICOD_TLS_CERT_FILE: certificate file path, MOSAICOD_TLS_PRIVATE_KEY_FILE:
    /// private key file path
    #[arg(long, default_value_t = false)]
    pub tls: bool,

    /// Require API keys to operate. When enabled the system will require API keys to
    /// perform any actions. See command `mosaicod api-key` for more info.
    #[arg(long, default_value_t = false)]
    pub api_key: bool,
}

fn get_store(cmds: &Run) -> Result<store::StoreRef, common::Error> {
    if let Some(path) = &cmds.local_store {
        info!("initializing filesystem store");
        Ok(common::init_local_store(path)?)
    } else {
        info!("initializing s3-compatible store");
        Ok(common::init_s3_store()?)
    }
}

fn tls_config() -> server::flight::TlsConfig {
    server::flight::TlsConfig {
        certificate_file: params::params().tls_certificate_file.clone().into(),
        private_key_file: params::params().tls_private_key_file.clone().into(),
    }
}

/// Start mosaicod server.
///
/// If `json_log` is enabled logs will be formatted in JSON format and startup info
/// are hidden.
pub fn run(args: Run, json_format: bool) -> Result<(), common::Error> {
    info!("startup store");
    let store = get_store(&args)?;
    let store_display_name = print::store_display_name(&store);

    info!("startup multi-threaded runtime");
    let rt = common::init_runtime()?;

    let db_config = db::Config {
        db_url: params::params().db_url.parse()?,
    };

    info!("startup database connection");
    let db = common::init_db(&rt, &db_config)?;

    // If not specified by the user use the default loopback address
    let host_is_specified = args.host.is_some();
    let host = args.host.unwrap_or("127.0.0.1".to_owned());

    let mut server = server::Server::new(host, args.port, store, db);

    if args.api_key {
        server.flight_config.enable_api_key_management();
    }

    if args.tls {
        server.flight_config.tls(tls_config());
    }

    let mut signals = Signals::new([SIGINT]).map_err(|e| e.to_string())?;
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
                host_is_specified,
                args.port,
                &store_display_name,
                &db_config,
                &params::version(),
                common::startup_time(),
            );
        }
    })?;

    Ok(())
}
