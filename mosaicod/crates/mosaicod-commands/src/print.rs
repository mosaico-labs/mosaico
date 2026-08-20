use super::log;
use colored::Colorize;
use mosaicod_core::error::PublicError;
use mosaicod_db as db;
use mosaicod_store as store;
use std::{net::IpAddr, time::Instant};
use tracing::error;

fn format_addr(is_loopback: bool, msg: String) {
    println!(
        " {} {:10} {}",
        "=".bold().purple(),
        if is_loopback { "Local" } else { "Network" },
        msg,
    );
}

/// Enable or disable colors based on the log format
pub fn set_colors(format: log::LogFormat) {
    if matches!(format, log::LogFormat::Plain) {
        colored::control::set_override(false);
    }
}

pub fn startup_info(
    host: &IpAddr,
    port: u16,
    store: &str,
    db_config: &db::Config,
    version: &str,
    startup_time: &Instant,
) {
    println!(
        "\n{:^12} {} {} {} {}\n {}",
        "mosaicod".on_purple().black().bold(),
        version.purple(),
        "ready in".dimmed(),
        startup_time.elapsed().as_millis().to_string().bold(),
        "ms".dimmed(),
        "|".bold().purple()
    );

    if host.is_loopback() {
        format_addr(true, format!("{}:{}", host, port).cyan().to_string());
        format_addr(false, "use --host to expose".dimmed().to_string());
    } else if host.is_unspecified() {
        // List all machine's network interfaces.
        let addrs = if_addrs::get_if_addrs().unwrap_or_default();
        for interface in addrs {
            if let IpAddr::V4(ip_v4) = interface.ip() {
                format_addr(
                    ip_v4.is_loopback(),
                    format!("{}:{}", ip_v4, port).cyan().to_string(),
                );
            }
        }
    } else {
        format_addr(false, format!("{}:{}", host, port).cyan().to_string());
    }

    println!(" {}", "|".bold().purple());
    println!(" {} {:10} {}", "=".bold().purple(), "Store", store);
    println!(
        " {} {:10} {}",
        "=".bold().purple(),
        "Database",
        format_db_host(db_config).yellow()
    );
    println!();
    println!("{}", "Press Ctrl+C to stop.".dimmed());
    println!();
}

pub fn error(err: impl AsRef<dyn PublicError + Send + Sync>) {
    eprintln!("{} {msg}.", "error:".red(), msg = err.as_ref().error());
    error!("{:?}", err.as_ref());
    if let Some(link) = err.as_ref().documentation_link() {
        eprintln!(
            "\nFor more information visit {doc}",
            doc = link.to_string().cyan()
        );
    }
}

fn format_db_host(db_config: &db::Config) -> String {
    // let schema = db_config.db_url.scheme();
    // let domain = db_config.db_url.domain().unwrap_or("???");
    // let port = db_config.db_url.port();
    let mut url = db_config.db_url.clone();

    url.set_username("").unwrap();
    url.set_password(None).unwrap();

    url.to_string()
}
/// Returns the name to display on the console for the current in use store
pub fn store_display_name(store: &store::StoreRef) -> String {
    match store.target() {
        store::Target::Filesystem(path) => {
            format!(
                "{} {}{}{}",
                path.to_string_lossy().yellow(),
                "[".dimmed(),
                "local".cyan(),
                "]".dimmed()
            )
        }
        store::Target::S3Compatible(bucket) => {
            format!(
                "{}{} {}{}{}",
                "s3://".yellow(),
                bucket.to_string().yellow(),
                "[".dimmed(),
                "remote".cyan(),
                "]".dimmed(),
            )
        }
    }
}
