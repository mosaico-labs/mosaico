from typing import List, Optional

import typer
from typing_extensions import Annotated

from mosaicolabs_cli.utils.bridge import (
    PATTERNS_HELP,
    DryRunOpt,
    InputFileArg,
    LogLevel,
    LogLevelOpt,
    MetadataOpt,
    TopicMetadataOpt,
    connection_kwargs,
    parse_json_option,
    require_profile,
    run_injector,
)


def inject(
    ctx: typer.Context,
    dataset_path: InputFileArg,
    sequence_name: Annotated[
        str,
        typer.Option(
            "--name",
            "-n",
            help=(
                "Prefix of the sequence names: each episode is ingested as "
                "'<name>_episode_<episode index>'. Defaults to the dataset folder name."
            ),
        ),
    ] = "",
    column_names: Annotated[
        Optional[List[str]],
        typer.Option(
            "--column-names",
            "-c",
            help=(
                "Dataset column pattern to filter (supports glob wildcards like "
                f"'observation.images.*'). {PATTERNS_HELP}"
            ),
        ),
    ] = None,
    metadata: MetadataOpt = None,
    topic_metadata: TopicMetadataOpt = None,
    dry_run: DryRunOpt = False,
    log_level: LogLevelOpt = LogLevel.INFO,
) -> None:
    """
    Inject a LeRobotDataset into Mosaico, one sequence per episode.
    """
    # A dry run never connects, so it can run without a profile
    profile = ctx.obj if dry_run else require_profile(ctx)

    user_metadata = parse_json_option(metadata, "--metadata")
    user_topic_metadata = parse_json_option(topic_metadata, "--topic-metadata")

    from mosaicolabs.bridges.lerobot import LeRobotInjectionConfig, LeRobotInjector

    config = LeRobotInjectionConfig(
        file_path=dataset_path,
        sequence_name=sequence_name,
        column_names=column_names or None,
        metadata=user_metadata,
        topic_metadata=user_topic_metadata or None,
        dry_run=dry_run,
        log_level=log_level.value,
        **connection_kwargs(profile),
    )
    run_injector(LeRobotInjector(config))
