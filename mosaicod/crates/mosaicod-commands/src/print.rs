use super::log;
use colored::Colorize;
use mosaicod_core::{error::PublicError, types};
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

fn format_duration_short(delta: chrono::TimeDelta) -> Option<String> {
    let total_secs = delta.num_seconds().max(0);

    let days = delta.num_days();
    let hours = (total_secs % 86400) / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    match (days, hours, mins, secs) {
        (d, h, m, _) if d > 0 => Some(format!("{d}d {h}h {m}m")),
        (_, h, m, _) if h > 0 => Some(format!("{h}h {m}m")),
        (_, _, m, s) if m > 0 => Some(format!("{m}m {s}s")),
        (_, _, _, s) if s > 0 => Some(format!("{s}s")),
        _ => None,
    }
}

fn format_uptime(delta: chrono::TimeDelta) -> String {
    format_duration_short(delta).unwrap_or_else(|| "-".to_owned())
}

/// Formats a duration elapsed since a past event, e.g. "3m 20s ago", "14h 12m ago".
fn format_elapsed_since(delta: chrono::TimeDelta) -> String {
    match format_duration_short(delta) {
        Some(s) => format!("{s} ago"),
        None => "just now".to_owned(),
    }
}

/// Prints the table of registered `mosaicod` instances (see `mosaicod ps`). Instances with a
/// "dead" status are hidden unless `show_dead` is set. The STARTED column is hidden unless
/// `verbose` is set. The LAST HEARTBEAT column is always shown, as an exact timestamp when
/// `verbose` is set, or otherwise as an elapsed time (e.g. "3m 20s ago"), except for dead
/// instances, which show "-".
pub fn print_instance_list(
    instances: &[db::InstanceRegistryRecord],
    show_dead: bool,
    verbose: bool,
) {
    const W_PROCESS: usize = 16;
    const W_MODE: usize = 11;
    const W_ID: usize = 5;
    const W_HOST: usize = 15;
    const W_PID: usize = 8;
    const W_STARTED: usize = 22;
    const W_UPTIME: usize = 11;
    const W_HEARTBEAT: usize = 22;

    let mut header = vec![
        format!("{:<W_PROCESS$}", "PROCESS").bold().to_string(),
        format!("{:<W_MODE$}", "MODE").bold().to_string(),
        format!("{:<W_ID$}", "ID").bold().to_string(),
        format!("{:<W_HOST$}", "HOST").bold().to_string(),
        format!("{:<W_PID$}", "PID").bold().to_string(),
    ];
    if verbose {
        header.push(
            format!("{:<W_STARTED$}", "STARTED (UTC)")
                .bold()
                .to_string(),
        );
    }
    header.push(format!("{:<W_UPTIME$}", "UPTIME").bold().to_string());
    let heartbeat_header = if verbose {
        "LAST HEARTBEAT (UTC)"
    } else {
        "LAST HEARTBEAT"
    };
    header.push(
        format!("{:<W_HEARTBEAT$}", heartbeat_header)
            .bold()
            .to_string(),
    );
    header.push("STATUS".bold().to_string());

    println!("{}", header.join(" "));

    let instances: Vec<&db::InstanceRegistryRecord> = instances
        .iter()
        .filter(|i| show_dead || !matches!(i.status(), types::InstanceStatus::Dead))
        .collect();

    if instances.is_empty() {
        if show_dead {
            println!("{}", "No instances.".dimmed());
        } else {
            println!("{}", "No instances. Try with --all to see more.".dimmed());
        }

        return;
    }

    for instance in instances {
        let start_datetime = instance.started_datetime();
        let last_heartbeat_datetime = instance.last_heartbeat_datetime();

        let status = instance.status();

        let kind = instance
            .kind()
            .map(|k| k.to_string())
            .unwrap_or_else(|| "unknown".to_owned());

        let mode = if instance.one_shot {
            "one-shot"
        } else {
            "continuous"
        };

        let uptime = match status {
            types::InstanceStatus::Alive | types::InstanceStatus::Stale => {
                chrono::Utc::now() - start_datetime
            }
            types::InstanceStatus::Dead => chrono::Duration::seconds(0),
        };

        let status_str = match status {
            types::InstanceStatus::Alive => "alive".green(),
            types::InstanceStatus::Stale => "stale".yellow(),
            types::InstanceStatus::Dead => "dead".red(),
        };

        let mut row = vec![
            format!("{:<W_PROCESS$}", kind),
            format!("{:<W_MODE$}", mode),
            format!("{:<W_ID$}", instance.instance_id),
            format!("{:<W_HOST$}", instance.hostname),
            format!("{:<W_PID$}", instance.pid),
        ];
        if verbose {
            row.push(format!(
                "{:<W_STARTED$}",
                start_datetime.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            ));
        }
        row.push(format!("{:<W_UPTIME$}", format_uptime(uptime)));
        let heartbeat_str = if verbose {
            last_heartbeat_datetime.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        } else if matches!(status, types::InstanceStatus::Dead) {
            "-".to_owned()
        } else {
            format_elapsed_since(chrono::Utc::now() - last_heartbeat_datetime)
        };
        row.push(format!("{:<W_HEARTBEAT$}", heartbeat_str));
        row.push(status_str.to_string());

        println!("{}", row.join(" "));
    }
}

/// Prints a one-line summary of the cleanup routine's current status (idle, running,
/// interrupted, or never run), and, when known, which instance last ran (or is currently
/// running) it (see `mosaicod ps`).
pub fn print_cleanup_status(
    latest: Option<&db::CleanupLogRecord>,
    instances: &[db::InstanceRegistryRecord],
) {
    print!("{} ", "Cleanup:".bold());

    let Some(log) = latest else {
        println!("{}", "no run recorded yet".dimmed());
        return;
    };

    let owning_instance = log
        .instance_id
        .and_then(|id| instances.iter().find(|i| i.instance_id == id));

    let by_instance = owning_instance
        .map(|i| format!(" by instance {} ({})", i.instance_id, i.hostname))
        .unwrap_or_default();

    match log.end_datetime() {
        None => {
            // An open log row (no end timestamp) normally just means the run is still in
            // progress, but if the owning instance died mid-run the row is left open forever.
            // In this case it is reported as INTERRUPTED.

            let interrupted = log.instance_id.is_some_and(|_| {
                owning_instance.is_none_or(|i| matches!(i.status(), types::InstanceStatus::Dead))
            });

            if interrupted {
                println!(
                    "{} — started {}{}, run never completed (instance no longer alive)",
                    "INTERRUPTED".red(),
                    log.start_datetime()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    by_instance,
                );
            } else {
                println!(
                    "{} — started {}{}",
                    "RUNNING".yellow(),
                    log.start_datetime()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    by_instance,
                );
            }
        }
        Some(end) => {
            println!(
                "{} — last run {} .. {}{}",
                "IDLE".green(),
                log.start_datetime()
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                end.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                by_instance,
            );
        }
    }
}
