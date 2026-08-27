use super::log;
use colored::Colorize;
use mosaicod_core::{error::PublicError, params, types};
use mosaicod_db as db;
use mosaicod_store as store;
use std::{net::IpAddr, time::Instant};
use tracing::error;

const DATETIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

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

/// Prints the table of registered `mosaicod` instances (see `mosaicod ps`).
pub fn print_instance_list(instances: &[db::InstanceRegistryRecord]) {
    const W_PROCESS: usize = 9;
    const W_MODE: usize = 11;
    const W_ID: usize = 5;
    const W_HOST: usize = 15;
    const W_PID: usize = 8;
    const W_STARTED: usize = 22;
    const W_HEARTBEAT: usize = 22;

    println!(
        "{} {} {} {} {} {} {} {}",
        format!("{:<W_PROCESS$}", "PROCESS").bold(),
        format!("{:<W_MODE$}", "MODE").bold(),
        format!("{:<W_ID$}", "ID").bold(),
        format!("{:<W_HOST$}", "HOST").bold(),
        format!("{:<W_PID$}", "PID").bold(),
        format!("{:<W_STARTED$}", "STARTED").bold(),
        format!("{:<W_HEARTBEAT$}", "LAST HEARTBEAT").bold(),
        "STATUS".bold(),
    );

    if instances.is_empty() {
        println!("{}", "No registered instances found.".dimmed());
        return;
    }

    let now = chrono::Utc::now();

    for instance in instances {
        let alive = instance.is_alive(
            now,
            types::Duration::seconds(params::INSTANCE_STALE_THRESHOLD_SECS),
        );

        let status = if alive {
            "alive".green()
        } else {
            "stale".yellow()
        };

        let kind = instance
            .kind()
            .map(|k| k.to_string())
            .unwrap_or_else(|| "unknown".to_owned());

        let mode = if instance.one_shot {
            "one-shot"
        } else {
            "continuous"
        };

        println!(
            "{:<W_PROCESS$} {:<W_MODE$} {:<W_ID$} {:<W_HOST$} {:<W_PID$} {:<W_STARTED$} {:<W_HEARTBEAT$} {}",
            kind,
            mode,
            instance.instance_id,
            instance.hostname,
            instance.pid,
            instance
                .started_datetime()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                .to_string(),
            instance
                .last_heartbeat_datetime()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                .to_string(),
            status,
        );
    }
}

/// Prints a one-line summary of the cleanup routine's current status (idle, running, or never
/// run), and, when known, which instance last ran (or is currently running) it (see
/// `mosaicod ps`).
pub fn print_cleanup_status(
    latest: Option<&db::CleanupLogRecord>,
    instances: &[db::InstanceRegistryRecord],
) {
    print!("{} ", "Cleanup:".bold());

    let Some(log) = latest else {
        println!("{}", "no run recorded yet".dimmed());
        return;
    };

    let by_instance = log
        .instance_id
        .and_then(|id| instances.iter().find(|i| i.instance_id == id))
        .map(|i| format!(" by instance {} ({})", i.instance_id, i.hostname))
        .unwrap_or_default();

    match log.end_datetime() {
        None => {
            println!(
                "{} — started {}{}",
                "RUNNING".yellow(),
                log.start_datetime().format(DATETIME_FMT),
                by_instance,
            );
        }
        Some(end) => {
            println!(
                "{} — last run {} .. {}{}",
                "IDLE".green(),
                log.start_datetime().format(DATETIME_FMT),
                end.format(DATETIME_FMT),
                by_instance,
            );
        }
    }
}
