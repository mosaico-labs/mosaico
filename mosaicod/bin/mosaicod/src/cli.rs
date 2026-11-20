//! The command line of mosaicod: the global flags of [`GlobalArgs`], the service and
//! utility commands ([`ServiceCommands`], [`UtilityCommands`]) and API key management.

use clap::{Parser, Subcommand};
use mosaicod_commands::command::{self, ServiceCommands, UtilityCommands};
use mosaicod_commands::{CommandLine, Context, Execute, GlobalArgs};
use mosaicod_core::error::PublicResult as Result;

/// mosaicod - Mosaico high-performance daemon.
#[derive(Parser, Debug)]
#[command(name = "mosaicod", about, long_about = None, long_version = mosaicod_build::version_description())]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalArgs,

    #[command(subcommand)]
    pub cmd: Commands,
}

impl CommandLine for Cli {
    type Command = Commands;

    fn into_parts(self) -> (GlobalArgs, Commands) {
        (self.global, self.cmd)
    }
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    #[command(flatten)]
    Service(ServiceCommands),

    #[command(flatten)]
    Utility(UtilityCommands),

    #[command(subcommand)]
    ApiKey(command::ApiKey),

    #[command(subcommand)]
    Hook(command::Hook),
}

impl Execute for Commands {
    fn execute(self, ctx: &Context) -> Result<()> {
        match self {
            Self::Service(cmd) => cmd.execute(ctx),
            Self::Utility(cmd) => cmd.execute(ctx),
            Self::ApiKey(cmd) => cmd.execute(ctx),
            Self::Hook(cmd) => cmd.execute(ctx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_core::types;
    use std::num::NonZero;

    // No CLI flag below has a `default_value`/`default_value_t` — every one of them must
    // parse to `None` when unset, so `args.x.unwrap_or(params.x.value)` (in
    // `mosaicod_commands::start` and in each command) falls through to the env var/config
    // file/hard-coded default tiers underneath instead of always being clobbered by a CLI
    // default.

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("mosaicod").chain(args.iter().copied())).unwrap()
    }

    #[test]
    fn global_flags_default_to_none_when_unset() {
        let cli = parse(&["server"]);

        assert!(cli.global.config.is_none());
        assert!(cli.global.log_format.is_none());
        assert!(cli.global.log_level.is_none());
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
            cli.global.config,
            Some(std::path::PathBuf::from("/tmp/mosaicod.toml"))
        );
        assert!(matches!(
            cli.global.log_format,
            Some(types::LogFormat::Json)
        ));
        assert!(matches!(cli.global.log_level, Some(types::LogLevel::Debug)));
    }

    #[test]
    fn server_args_default_to_none_when_unset() {
        let cli = parse(&["server"]);
        let Commands::Service(ServiceCommands::Server(args)) = cli.cmd else {
            panic!("expected Server subcommand")
        };

        assert!(args.host.is_none());
        assert!(args.port.is_none());
    }

    #[test]
    fn server_args_are_some_when_set() {
        let cli = parse(&["server", "--host", "10.0.0.1", "--port", "9000"]);
        let Commands::Service(ServiceCommands::Server(args)) = cli.cmd else {
            panic!("expected Server subcommand")
        };

        assert_eq!(args.host, Some("10.0.0.1".parse().unwrap()));
        assert_eq!(args.port, std::num::NonZero::new(9000));
    }

    #[test]
    fn server_rejects_zero_port() {
        let res = Cli::try_parse_from(["mosaicod", "server", "--port", "0"]);

        assert!(res.is_err());
    }

    #[test]
    fn cleanup_args_default_to_none_when_unset() {
        let cli = parse(&["cleanup"]);
        let Commands::Service(ServiceCommands::Cleanup(args)) = cli.cmd else {
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
        let Commands::Service(ServiceCommands::Cleanup(args)) = cli.cmd else {
            panic!("expected Cleanup subcommand")
        };

        assert_eq!(args.time_interval, Some(30));
        assert_eq!(args.retention_duration, Some(100));
    }

    #[test]
    fn store_optimizer_args_default_to_none_when_unset() {
        let cli = parse(&["store-optimizer"]);
        let Commands::Service(ServiceCommands::StoreOptimizer(args)) = cli.cmd else {
            panic!("expected StoreOptimizer subcommand")
        };

        assert!(args.time_interval.is_none());
        assert!(args.max_chunk_size.is_none());
    }

    #[test]
    fn store_optimizer_rejects_zero_max_chunk_size() {
        let res = Cli::try_parse_from(["mosaicod", "store-optimizer", "--max-chunk-size", "0"]);

        assert!(res.is_err());
    }

    #[test]
    fn hook_create_requires_url() {
        let res = Cli::try_parse_from(["mosaicod", "hook", "create"]);

        assert!(res.is_err());
    }

    #[test]
    fn hook_rm_takes_several_ids() {
        let cli = parse(&["hook", "rm", "3", "1", "2"]);
        let Commands::Hook(command::Hook::Rm { ids }) = cli.cmd else {
            panic!("expected Hook Rm subcommand")
        };

        assert_eq!(ids, vec![3, 1, 2]);
    }

    #[test]
    fn hook_rm_requires_an_id() {
        let res = Cli::try_parse_from(["mosaicod", "hook", "rm"]);

        assert!(res.is_err());
    }

    #[test]
    fn hook_aliases() {
        let cli = parse(&["hook", "list"]);
        assert!(matches!(cli.cmd, Commands::Hook(command::Hook::Ls)));

        let cli = parse(&["hook", "remove", "1"]);
        assert!(matches!(cli.cmd, Commands::Hook(command::Hook::Rm { .. })));
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
        let Commands::Service(ServiceCommands::StoreOptimizer(args)) = cli.cmd else {
            panic!("expected StoreOptimizer subcommand")
        };

        assert_eq!(args.time_interval, Some(5));
        assert_eq!(args.max_chunk_size, NonZero::new(123));
    }

    #[test]
    fn api_key_is_offered_next_to_the_service_commands() {
        let cli = parse(&["api-key", "list"]);

        assert!(matches!(cli.cmd, Commands::ApiKey(command::ApiKey::List)));
    }

    #[test]
    fn ps_is_offered_next_to_the_service_commands() {
        let cli = parse(&["ps"]);

        assert!(matches!(cli.cmd, Commands::Utility(UtilityCommands::Ps(_))));
    }
}
