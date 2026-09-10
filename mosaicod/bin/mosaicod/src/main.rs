// Setup default global allocator
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use mosaicod_commands::{command, common, print};
use mosaicod_config::params;
use mosaicod_core::error::PublicResult as Result;
use mosaicod_core::types;
use tracing::debug;

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
/// mosaicod - Mosaico high-performance daemon
struct Cli {
    /// Path to a config.toml file. Defaults to MOSAICOD_CONFIG_FILE, then
    /// $XDG_CONFIG_HOME/mosaicod/config.toml (or ~/.config/mosaicod/config.toml),
    /// then /etc/mosaicod/config.toml, then no config file.
    #[arg(long, global = true)]
    config: Option<std::path::PathBuf>,

    /// Set the log output format. Defaults to MOSAICOD_LOG_FORMAT, then `pretty`.
    #[arg(long, global = true)]
    log_format: Option<types::LogFormat>,

    /// Set the log level. Defaults to MOSAICOD_LOG_LEVEL, then `warning`.
    #[arg(long, global = true)]
    log_level: Option<types::LogLevel>,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the mosaicod server
    Server(command::Server),

    /// Run the store cleanup routine
    Cleanup(command::Cleanup),

    /// Run the store optimization routine
    #[command(name = "store-optimizer")]
    StoreOptimizer(command::StoreOptimizer),

    /// Manage mosaico API keys
    #[command(subcommand, name = "api-key")]
    Auth(command::ApiKey),

    /// List registered mosaicod instances (server, cleanup) and cleanup routine status
    Ps(command::Ps),
}

fn start() -> Result<Option<String>> {
    let mut cmd = Cli::command();

    cmd = cmd.long_version(mosaicod_build::version_description());

    // Avoid to show error message when parsing cli commands
    let matches = match cmd.try_get_matches() {
        Ok(args) => args,
        Err(err) => {
            return Ok(Some(err.render().ansi().to_string()));
        }
    };

    let args = match Cli::from_arg_matches(&matches) {
        Ok(args) => args,
        Err(err) => {
            return Ok(Some(err.render().ansi().to_string()));
        }
    };

    common::load_env_variables(args.config.as_deref())?;

    let params = params::params();
    let log_format = args.log_format.unwrap_or(params.log_format.value);
    let log_level = args.log_level.unwrap_or(params.log_level.value);

    print::set_colors(log_format);
    types::init_logger(log_format, log_level);

    debug!("{:#?}", params);

    let is_json_output = matches!(log_format, types::LogFormat::Json);

    match args.cmd {
        Commands::Server(sub_args) => command::server(sub_args, is_json_output)?,
        Commands::Cleanup(sub_args) => command::cleanup(sub_args)?,
        Commands::StoreOptimizer(sub_args) => command::store_optimization(sub_args)?,
        Commands::Auth(sub_args) => command::auth(sub_args)?,
        Commands::Ps(sub_args) => command::ps(sub_args)?,
    }

    Ok(None)
}

fn main() {
    common::pin_startup_time();

    let res = start();

    match res {
        Ok(opt_msg) => {
            if let Some(msg) = opt_msg {
                print!("{msg}");
            }
        }
        Err(e) => {
            print::error(e);
            std::process::exit(-1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // No CLI flag below has a `default_value`/`default_value_t` — every one of them must
    // parse to `None` when unset, so `args.x.unwrap_or(params.x.value)` (in `start()` and
    // in each `command::*` function) falls through to the env var/config file/hard-coded
    // default tiers underneath instead of always being clobbered by a CLI default.

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("mosaicod").chain(args.iter().copied())).unwrap()
    }

    #[test]
    fn global_flags_default_to_none_when_unset() {
        let cli = parse(&["server"]);

        assert!(cli.config.is_none());
        assert!(cli.log_format.is_none());
        assert!(cli.log_level.is_none());
    }

    #[test]
    fn global_flags_are_some_when_set() {
        let cli = parse(&[
            "--config",
            "/tmp/mosaicod.toml",
            "--log-format",
            "json",
            "--log-level",
            "debug",
            "server",
        ]);

        assert_eq!(
            cli.config,
            Some(std::path::PathBuf::from("/tmp/mosaicod.toml"))
        );
        assert!(matches!(cli.log_format, Some(types::LogFormat::Json)));
        assert!(matches!(cli.log_level, Some(types::LogLevel::Debug)));
    }

    #[test]
    fn server_args_default_to_none_when_unset() {
        let cli = parse(&["server"]);
        let Commands::Server(args) = cli.cmd else {
            panic!("expected Server subcommand")
        };

        assert!(args.host.is_none());
        assert!(args.port.is_none());
    }

    #[test]
    fn server_args_are_some_when_set() {
        let cli = parse(&["server", "--host", "10.0.0.1", "--port", "9000"]);
        let Commands::Server(args) = cli.cmd else {
            panic!("expected Server subcommand")
        };

        assert_eq!(args.host, Some("10.0.0.1".parse().unwrap()));
        assert_eq!(args.port, Some(9000));
    }

    #[test]
    fn cleanup_args_default_to_none_when_unset() {
        let cli = parse(&["cleanup"]);
        let Commands::Cleanup(args) = cli.cmd else {
            panic!("expected Cleanup subcommand")
        };

        assert!(args.time_interval.is_none());
        assert!(args.retention_duration.is_none());
    }

    #[test]
    fn cleanup_args_are_some_when_set() {
        let cli = parse(&[
            "cleanup",
            "--time-interval",
            "30",
            "--retention-duration",
            "100",
        ]);
        let Commands::Cleanup(args) = cli.cmd else {
            panic!("expected Cleanup subcommand")
        };

        assert_eq!(args.time_interval, Some(30));
        assert_eq!(args.retention_duration, Some(100));
    }

    #[test]
    fn store_optimizer_args_default_to_none_when_unset() {
        let cli = parse(&["store-optimizer"]);
        let Commands::StoreOptimizer(args) = cli.cmd else {
            panic!("expected StoreOptimizer subcommand")
        };

        assert!(args.time_interval.is_none());
        assert!(args.max_chunk_size.is_none());
    }

    #[test]
    fn store_optimizer_args_are_some_when_set() {
        let cli = parse(&[
            "store-optimizer",
            "--time-interval",
            "5",
            "--max-chunk-size",
            "123",
        ]);
        let Commands::StoreOptimizer(args) = cli.cmd else {
            panic!("expected StoreOptimizer subcommand")
        };

        assert_eq!(args.time_interval, Some(5));
        assert_eq!(args.max_chunk_size, Some(123));
    }
}
