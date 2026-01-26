use colored::Colorize;
use std::{net::IpAddr, time::Instant};

pub fn print_startup_info(
    host: bool,
    port: u16,
    store: &str,
    version: &str,
    startup_time: &Instant,
) {
    println!(
        "\n{} {} {} {} {}\n {}",
        "  mosaicod  ".on_purple().black(),
        version.purple(),
        "ready in".dimmed(),
        startup_time.elapsed().as_millis().to_string().bold(),
        "ms".dimmed(),
        "⎪".purple()
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
    println!(" {}", "⎪".purple());
    println!(" {} {:10} {}", "⎬".purple(), "Store", store);
    println!();
    println!("{}", "Press Ctrl+C to stop.".dimmed());
    println!();
}

fn format_addr(is_loopback: bool, msg: String) {
    println!(
        " {} {:10} {}",
        "⎬".purple(),
        if is_loopback { "Local" } else { "Network" },
        msg,
    );
}
