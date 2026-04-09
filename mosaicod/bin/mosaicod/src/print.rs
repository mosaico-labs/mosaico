use super::log;
use colored::Colorize;
use mosaicod_db as db;
use mosaicod_store as store;
use std::{net::IpAddr, time::Instant};

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
    host: bool,
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

    let addrs = if_addrs::get_if_addrs().unwrap_or_default();

    if !host {
        // List only loopback addresses
        for iface in addrs {
            match iface.ip() {
                IpAddr::V4(ipv4) if ipv4.is_loopback() => {
                    format_addr(true, format!("{}:{}", ipv4, port).cyan().to_string());
                }
                _ => {}
            }
        }
        format_addr(false, "use --host to expose".dimmed().to_string());
    } else {
        // List all of the machine's network interfaces
        for iface in addrs {
            if let IpAddr::V4(ipv4) = iface.ip() {
                format_addr(
                    ipv4.is_loopback(),
                    format!("{}:{}", iface.ip(), port).cyan().to_string(),
                );
            }
        }
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

#[allow(unused)]
pub fn warning(msg: &str) {
    println!("{:^12} {}", "warning: ".on_yellow().black(), msg);
}

pub fn error(msg: &str) {
    eprintln!("{} {}", "error: ".on_red().black(), msg);
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
        store::StoreTarget::Filesystem(path) => {
            format!(
                "{} {}{}{}",
                path.yellow(),
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
