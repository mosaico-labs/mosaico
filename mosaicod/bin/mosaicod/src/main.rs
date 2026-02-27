// Setup default global allocator
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod print;
use clap::{Args, Parser, Subcommand};
use dotenv::dotenv;
use log::{debug, info, trace};
use mosaicod_core::params;
use mosaicod_db as db;
use mosaicod_server as server;
use mosaicod_store as store;
use std::{sync::Arc, thread, time::Instant};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
/// mosaicod - Mosaico high-performance daemon
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Args, Debug)]
struct CommandRun {
    /// Listen on all addresses, including LAN and public addresses
    #[arg(long, default_value_t = false)]
    host: bool,

    /// Port
    #[arg(long, default_value_t = 6726)]
    port: u16,

    /// Enable to store objects on the local filesystem at the specified directory path
    #[arg(long)]
    local_store: Option<std::path::PathBuf>,

    /// Enable TLS. When enabled, the following envirnoment variables needs to be set
    /// MOSAICOD_TLS_CERT_FILE: certificate file path, MOSAICOD_TLS_PRIVATE_KEY_FILE:
    /// private key file path
    #[arg(long, default_value_t = false)]
    tls: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the mosaico server
    Run(CommandRun),
}

#[derive(Debug)]
struct Variables {
    database_url: url::Url,
}

fn init_logger() {
    env_logger::builder().format_target(true).init();
}

/// Load the defined env variables from the system.
fn load_env_variables() -> Result<Variables, Box<dyn std::error::Error>> {
    info!("Loading .env file");
    dotenv().ok();

    params::load_params_from_env()?;

    if params::params().max_chunk_size_in_bytes == 0 {
        print::warning(
            "Max chunk size in bytes is 0: automatic chunk splitting is disabled. Large uploads may cause high memory usage.",
        );
    }

    let database_url: url::Url = params::params().db_url.parse()?;

    let vars = Variables { database_url };

    debug!("{:#?}", params::params());
    debug!("{:#?}", vars);

    Ok(vars)
}

fn load_remote_store_vars() -> Result<store::S3Config, Box<dyn std::error::Error>> {
    let params = params::params();

    let config = store::S3Config {
        endpoint: params.store_endpoint.clone(),
        bucket: params.store_bucket.clone(),
        secret_key: params.store_secret_key.clone(),
        access_key: params.store_access_key.clone(),
    };

    // This will return and error if the s3 confuration has some problems
    config.validate()?;

    debug!("{:#?}", config);

    Ok(config)
}

fn check_tls(tls: bool) -> Option<server::flight::TlsConfig> {
    if tls {
        return Some(server::flight::TlsConfig {
            certificate_file: params::params().tls_certificate_file.clone().into(),
            private_key_file: params::params().tls_private_key_file.clone().into(),
        });
    }

    None
}

fn run(startup_time: &Instant) -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();

    init_logger();

    let vars = load_env_variables()?;

    match args.cmd {
        Commands::Run(args) => {
            let store = get_store(&args)?;
            let store_display_name = get_store_display_name(&store);

            let tls = check_tls(args.tls);

            let server = server::Server::new(
                args.host,
                args.port,
                store,
                db::Config {
                    db_url: vars.database_url.clone(),
                },
            );

            let mut signals = Signals::new([SIGINT]).map_err(|e| e.to_string())?;
            let shutdown = server.shutdown.clone();
            thread::spawn(move || {
                for sig in signals.forever() {
                    trace!("received signal {:?}", sig);
                    shutdown.shutdown();
                }
            });

            server.start_and_wait(
                || {
                    print::print_startup_info(
                        args.host,
                        args.port,
                        &store_display_name,
                        &params::get_version(),
                        startup_time,
                    );
                },
                tls,
            )?;
        }
    }

    Ok(())
}

fn get_store(cmds: &CommandRun) -> Result<store::StoreRef, Box<dyn std::error::Error>> {
    if let Some(path) = &cmds.local_store {
        info!("initializing filesystem store");
        Ok(Arc::new(store::Store::try_from_filesystem(path)?))
    } else {
        info!("initializing s3-compatible store");

        let s3_config = load_remote_store_vars()?;

        let store = Arc::new(store::Store::try_from_s3_store(s3_config)?);

        Ok(store)
    }
}

/// Returns the name to display on the console for the current in use store
fn get_store_display_name(store: &store::StoreRef) -> String {
    match store.target() {
        store::StoreTarget::Filesystem(path) => {
            format!(
                "{} {}{}{}",
                path.yellow().bold(),
                "[".dimmed(),
                "local".cyan(),
                "]".dimmed()
            )
        }
        store::StoreTarget::S3Compatible(bucket) => {
            format!(
                "{}{} {}{}{}",
                "s3://".yellow(),
                bucket.yellow(),
                "[".dimmed(),
                "remote".cyan(),
                "]".dimmed(),
            )
        }
    }
}

use colored::Colorize;
use signal_hook::{consts::SIGINT, iterator::Signals};

fn main() {
    let startup_time = Instant::now();

    let res = run(&startup_time);

    match res {
        Ok(_) => println!("\n{}\n", "All done. Bye!".dimmed()),
        Err(e) => {
            print::error(&e.to_string());
            println!(
                "\nPlease refer to {} for more informations.",
                "https://docs.mosaico.dev/daemon".cyan()
            )
        }
    }
}
