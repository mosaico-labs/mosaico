use crate::{common, print};
use clap::Args;
use log::{info, trace};
use mosaicod_core::params;
use mosaicod_db as db;
use mosaicod_server as server;
use mosaicod_store as store;
use signal_hook::{consts::SIGINT, iterator::Signals};
use std::thread;

#[derive(Args, Debug)]
pub struct Run {
    /// Listen on all addresses, including LAN and public addresses
    #[arg(long, default_value_t = false)]
    pub host: bool,

    /// Port
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

fn tls_config(tls: bool) -> Option<server::flight::TlsConfig> {
    if tls {
        return Some(server::flight::TlsConfig {
            certificate_file: params::params().tls_certificate_file.clone().into(),
            private_key_file: params::params().tls_private_key_file.clone().into(),
        });
    }

    None
}

pub fn run(args: Run) -> Result<(), common::Error> {
    let tls = tls_config(args.tls);

    info!("startup store");
    let store = get_store(&args)?;
    let store_display_name = print::store_display_name(&store);

    info!("startup multi-threaded runtime");
    let rt = common::init_runtime()?;

    info!("startup database connection");
    let db = common::init_db(
        &rt,
        db::Config {
            db_url: params::params().db_url.parse()?,
        },
    )?;

    let server = server::Server::new(args.host, args.port, store, db);

    let mut signals = Signals::new([SIGINT]).map_err(|e| e.to_string())?;
    let shutdown = server.shutdown.clone();
    thread::spawn(move || {
        for sig in signals.forever() {
            trace!("received signal {:?}", sig);
            shutdown.shutdown();
        }
    });

    server.start_and_wait(
        rt,
        || {
            print::startup_info(
                args.host,
                args.port,
                &store_display_name,
                &params::version(),
                common::startup_time(),
            );
        },
        tls,
    )?;

    Ok(())
}
