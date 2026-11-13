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
    injector = LeRobotInjector(config)
    run_injector(injector)

    # Some episodes failed (already logged by the injector)
    if injector.failed_episodes:
        raise typer.Exit(code=1)


def extract(
    ctx: typer.Context,
    sequence_names: SequenceNamesArg,
    saving_path: Annotated[
        Path,
        typer.Option(
            "--saving-path",
            "--saving_path",
            help="Directory where to save the LeRobot dataset.",
        ),
    ] = Path("."),
    dataset_name: Annotated[
        str,
        typer.Option(
            "--name",
            "-n",
            help="Name of the dataset folder created under the saving path.",
        ),
    ] = "LeRobotDataset",
    overwrite: Annotated[
        bool,
        typer.Option(
            "--overwrite",
            help=(
                "Replace the dataset folder if it already exists. It must be empty or a "
                "LeRobot dataset, and is replaced only once the new dataset is complete."
            ),
        ),
    ] = False,
    log_level: LogLevelOpt = LogLevel.INFO,
) -> None:
    """
    Extract sequences from Mosaico and write them as a LeRobot v3.0 dataset.

    Each sequence becomes one episode, in the order given. The dataset is written
    under <SAVING_PATH>/<NAME>. A sequence that cannot be exported is skipped with a
    warning, and the command then exits with code 1.
    """
    profile = require_profile(ctx)

    from mosaicolabs.bridges.lerobot import (
        ExtractionStatus,
        LeRobotExtractorConfig,
        LeRobotSequencesExtractor,
    )

    config = LeRobotExtractorConfig(
        saving_path=saving_path,
        sequence_names=sequence_names,
        dataset_name=dataset_name,
        overwrite=overwrite,
        log_level=log_level.value,
        **connection_kwargs(profile),
    )
    status = run_extractor(LeRobotSequencesExtractor(config))

    # Some sequences were dropped (already logged by the extractor): exit non-zero, so that
    # scripts and CI detect that the dataset holds fewer episodes than requested
    if status is ExtractionStatus.PARTIAL:
        raise typer.Exit(code=1)
