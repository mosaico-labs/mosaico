from pathlib import Path
from typing import List, Optional

import typer
from typing_extensions import Annotated

from mosaicolabs_cli.utils.bridge import (
    PATTERNS_HELP,
    DryRunOpt,
    InputDirArg,
    LogLevel,
    LogLevelOpt,
    MetadataOpt,
    OverwriteOpt,
    SequenceNamesArg,
    TopicMetadataOpt,
    connection_kwargs,
    parse_json_option,
    require_profile,
    run_extractor,
    run_injector,
)


def inject(
    ctx: typer.Context,
    dataset_path: InputDirArg,
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

    `dataset_path` is the root folder of a LeRobotDataset v3.0:

    - meta/: schema, stats, tasks and episode index.

    - data/: Parquet frame partitions.

    Video features (videos/) are not supported yet and are skipped.
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


def extract(
    ctx: typer.Context,
    sequence_names: SequenceNamesArg,
    rosbag_path: Annotated[
        Path,
        typer.Option(
            "--saving-path",
            "--saving_path",
            help="Directory where to save the LeRobot dataset.",
        ),
    ] = Path("."),
    overwrite: OverwriteOpt = False,
    log_level: LogLevelOpt = LogLevel.INFO,
) -> None:
    """
    Extract a sequence from Mosaico and encode it as a rosbag.

    The bag is written under <ROSBAG_PATH>/<SEQUENCE_NAME>.
    """
    profile = require_profile(ctx)

    from mosaicolabs.bridges.lerobot import (
        LeRobotExtractorConfig,
        LeRobotSequencesExtractor,
    )

    config = LeRobotExtractorConfig(
        saving_path=rosbag_path,
        sequence_names=sequence_names,
        overwrite=overwrite,
        log_level=log_level.value,
        **connection_kwargs(profile),
    )
    run_extractor(LeRobotSequencesExtractor(config))
