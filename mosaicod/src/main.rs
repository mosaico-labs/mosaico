#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::{env, sync::Arc, thread, time::Instant};

use clap::{Args, Parser, Subcommand};

use dotenv::dotenv;

use log::{debug, error, info, trace, warn};
use mosaicod::{params, repo, server, store, utils::print};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
/// Mosaico command-line-interface
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
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the mosaico server
    Run(CommandRun),
}

#[derive(Debug)]
struct Variables {
    repository_db_url: url::Url,
}

fn init_logger() {
    env_logger::builder().format_target(true).init();
}

/// Load the defined env variables from the system.
fn load_env_variables() -> Result<Variables, Box<dyn std::error::Error>> {
    info!("Loading .env file");
    dotenv().ok();

    params::load_configurables_from_env();

    if params::configurables().max_chunk_size_in_bytes == 0 {
        warn!(
            "MOSAICO_MAX_CHUNK_SIZE_IN_BYTES=0: automatic chunk splitting is disabled. \
             Large uploads may cause high memory usage."
        );
    }

    let repository_db_url: String = params::require_env_var("MOSAICO_REPOSITORY_DB_URL")?;
    let repository_db_url: url::Url = repository_db_url.parse()?;

    let vars = Variables { repository_db_url };

    debug!("{:#?}", params::configurables());
    debug!("{:#?}", vars);

    Ok(vars)
}

fn load_remote_store_vars() -> Result<store::S3Config, Box<dyn std::error::Error>> {
    let store_endpoint: String = params::require_env_var("MOSAICO_STORE_ENDPOINT")?;
    let store_bucket: String = params::require_env_var("MOSAICO_STORE_BUCKET")?;
    let secret_key: String = params::require_env_var("MOSAICO_STORE_SECRET_KEY")?;
    let store_secret_key = params::Hidden::from(secret_key);
    let store_access_key: String = params::require_env_var("MOSAICO_STORE_ACCESS_KEY")?;

    let vars = store::S3Config {
        endpoint: store_endpoint,
        bucket: store_bucket,
        secret_key: store_secret_key,
        access_key: store_access_key,
    };

    debug!("{:#?}", vars);

    Ok(vars)
}

fn run(startup_time: &Instant) -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();

    init_logger();

    let vars = load_env_variables()?;

    match args.cmd {
        Commands::Run(args) => {
            let store = get_store(&args)?;
            let store_display_name = get_store_display_name(&store);

            let server = server::Server::new(
                args.host,
                args.port,
                store,
                repo::Config {
                    db_url: vars.repository_db_url.clone(),
                },
            );

            let mut signals = Signals::new([SIGINT]).map_err(|e| e.to_string())?;
            let shutdown = server.shutdown.clone();
            thread::spawn(move || {
                for sig in signals.forever() {
                    trace!("received signal {:?}", sig);
                    shutdown.notify_waiters();
                }
            });

            server.start_and_wait(|| {
                print::print_startup_info(
                    args.host,
                    args.port,
                    &store_display_name,
                    get_version(),
                    startup_time,
                );
            })?;
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

fn get_version() -> &'static str {
    if cfg!(debug_assertions) {
        "devel"
    } else {
        env!("CARGO_PKG_VERSION")
    }
}

use colored::Colorize;
use signal_hook::{consts::SIGINT, iterator::Signals};

fn main() {
    let startup_time = Instant::now();

    let res = run(&startup_time);

    match res {
        Ok(_) => println!("\n{}\n", "All done. Bye!".dimmed()),
        Err(e) => error!("{}", e),
    }
}
