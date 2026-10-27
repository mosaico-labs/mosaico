from pathlib import Path
from typing import List, Optional

import typer
from typing_extensions import Annotated

from mosaicolabs_cli.utils.bridge import (
    PATTERNS_HELP,
    DryRunOpt,
    EndTimestampOpt,
    InputFileArg,
    LogLevel,
    LogLevelOpt,
    MetadataOpt,
    OverwriteOpt,
    SequenceNameArg,
    SequenceNameOpt,
    StartTimestampOpt,
    TopicMetadataOpt,
    TopicsOpt,
    UpdateIfExistsOpt,
    connection_kwargs,
    parse_json_option,
    require_profile,
    run_extractor,
    run_injector,
)

app = typer.Typer(no_args_is_help=True)


@app.command(name="inject")
def inject(
    ctx: typer.Context,
    mcap_path: InputFileArg,
    sequence_name: SequenceNameOpt,
    channels: Annotated[
        Optional[List[str]],
        typer.Option(
            "--channels",
            "-c",
            help=(
                "MCAP channel pattern to filter (supports glob wildcards like '/cam/*' "
                f"or '*camera_info'). {PATTERNS_HELP}"
            ),
        ),
    ] = None,
    metadata: MetadataOpt = None,
    topic_metadata: TopicMetadataOpt = None,
    update_if_exists: UpdateIfExistsOpt = False,
    dry_run: DryRunOpt = False,
    log_level: LogLevelOpt = LogLevel.INFO,
) -> None:
    """
    Inject an MCAP file into Mosaico.
    """
    # A dry run never connects, so it can run without a profile
    profile = ctx.obj if dry_run else require_profile(ctx)

    user_metadata = parse_json_option(metadata, "--metadata")
    # Traceability metadata, as in the `mosaicolabs.mcap_injector` script
    user_metadata["mcap_injection"] = mcap_path.name
    user_topic_metadata = parse_json_option(topic_metadata, "--topic-metadata")

    from mosaicolabs.bridges.mcap import MCAPInjectionConfig, MCAPInjector

    config = MCAPInjectionConfig(
        file_path=mcap_path,
        sequence_name=sequence_name,
        metadata=user_metadata,
        topic_metadata=user_topic_metadata or None,
        update_if_exists=update_if_exists,
        dry_run=dry_run,
        channels=channels or None,
        log_level=log_level.value,
        **connection_kwargs(profile),
    )
    run_injector(MCAPInjector(config))


@app.command(name="extract")
def extract(
    ctx: typer.Context,
    sequence_name: SequenceNameArg,
    mcap_path: Annotated[
        Path,
        typer.Option(
            "--mcap-path",
            "--mcap_path",
            help="Directory where to save the mcap file.",
        ),
    ] = Path("."),
    topics: TopicsOpt = None,
    start_timestamp_ns: StartTimestampOpt = None,
    end_timestamp_ns: EndTimestampOpt = None,
    overwrite: OverwriteOpt = False,
    dry_run: DryRunOpt = False,
    log_level: LogLevelOpt = LogLevel.INFO,
) -> None:
    """
    Extract a sequence from Mosaico and encode it as an mcap file.

    The file is written to <MCAP_PATH>/<SEQUENCE_NAME>/<SEQUENCE_NAME>.mcap.
    """
    profile = require_profile(ctx)

    from mosaicolabs.bridges.mcap import MCAPExtractorConfig, MCAPSequenceExtractor

    config = MCAPExtractorConfig(
        saving_path=mcap_path,
        sequence_name=sequence_name,
        topics=topics or None,
        start_timestamp_ns=start_timestamp_ns,
        end_timestamp_ns=end_timestamp_ns,
        overwrite=overwrite,
        dry_run=dry_run,
        log_level=log_level.value,
        **connection_kwargs(profile),
    )
    run_extractor(MCAPSequenceExtractor(config))
