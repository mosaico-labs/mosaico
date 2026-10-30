from enum import Enum
from pathlib import Path
from typing import Optional

import typer
from rosbags.rosbag2 import StoragePlugin
from rosbags.typesys import Stores
from typing_extensions import Annotated

from mosaicolabs_cli.utils.bridge import (
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

# Name-valued mirrors of the rosbags enums, so typer can list them as choices
RosDistro = Enum("RosDistro", {s.name: s.name for s in Stores}, type=str)
RosStoragePlugin = Enum(
    "RosStoragePlugin", {sp.name: sp.name for sp in StoragePlugin}, type=str
)


def inject(
    ctx: typer.Context,
    bag_path: InputFileArg,
    sequence_name: SequenceNameOpt,
    ros_distro: Annotated[
        Optional[RosDistro],
        typer.Option(
            "--ros-distro",
            "--ros_distro",
            case_sensitive=False,
            help=(
                "ROS distribution used to parse the messages. If omitted, the types "
                "are read from the bag itself."
            ),
        ),
    ] = None,
    topics: TopicsOpt = None,
    metadata: MetadataOpt = None,
    topic_metadata: TopicMetadataOpt = None,
    update_if_exists: UpdateIfExistsOpt = False,
    dry_run: DryRunOpt = False,
    log_level: LogLevelOpt = LogLevel.INFO,
) -> None:
    """
    Inject a ROS 1 (.bag) or ROS 2 (.mcap / .db3) bag into Mosaico.
    """
    # A dry run never connects, so it can run without a profile
    profile = ctx.obj if dry_run else require_profile(ctx)

    user_metadata = parse_json_option(metadata, "--metadata")
    # Traceability metadata, as in the `mosaicolabs.ros_injector` script
    user_metadata["rosbag_injection"] = bag_path.name
    user_topic_metadata = parse_json_option(topic_metadata, "--topic-metadata")

    from mosaicolabs.bridges.ros import RosbagInjector, ROSInjectionConfig

    config = ROSInjectionConfig(
        file_path=bag_path,
        sequence_name=sequence_name,
        metadata=user_metadata,
        topic_metadata=user_topic_metadata or None,
        update_if_exists=update_if_exists,
        dry_run=dry_run,
        topics=topics or None,
        ros_distro=Stores[ros_distro.value] if ros_distro else Stores.EMPTY,
        log_level=log_level.value,
        **connection_kwargs(profile),
    )
    run_injector(RosbagInjector(config))


def extract(
    ctx: typer.Context,
    sequence_name: SequenceNameArg,
    rosbag_path: Annotated[
        Path,
        typer.Option(
            "--rosbag-path",
            "--rosbag_path",
            help="Directory where to save the rosbag.",
        ),
    ] = Path("."),
    ros_distro: Annotated[
        RosDistro,
        typer.Option(
            "--ros-distro",
            "--ros_distro",
            case_sensitive=False,
            help="Target ROS distribution for the messages.",
        ),
    ] = RosDistro[Stores.ROS2_JAZZY.name],
    storage_plugin: Annotated[
        RosStoragePlugin,
        typer.Option(
            "--storage-plugin",
            "--storage_plugin",
            case_sensitive=False,
            help="Storage plugin used to save ROS 2 bags.",
        ),
    ] = RosStoragePlugin[StoragePlugin.MCAP.name],
    topics: TopicsOpt = None,
    start_timestamp_ns: StartTimestampOpt = None,
    end_timestamp_ns: EndTimestampOpt = None,
    overwrite: OverwriteOpt = False,
    dry_run: DryRunOpt = False,
    log_level: LogLevelOpt = LogLevel.INFO,
) -> None:
    """
    Extract a sequence from Mosaico and encode it as a rosbag.

    The bag is written under <ROSBAG_PATH>/<SEQUENCE_NAME>.
    """
    profile = require_profile(ctx)

    from mosaicolabs.bridges.ros import ROSExtractorConfig, ROSSequenceExtractor

    config = ROSExtractorConfig(
        saving_path=rosbag_path,
        sequence_name=sequence_name,
        topics=topics or None,
        start_timestamp_ns=start_timestamp_ns,
        end_timestamp_ns=end_timestamp_ns,
        overwrite=overwrite,
        dry_run=dry_run,
        log_level=log_level.value,
        ros_distro=Stores[ros_distro.value],
        storage_plugin=StoragePlugin[storage_plugin.value],
        **connection_kwargs(profile),
    )
    run_extractor(ROSSequenceExtractor(config))
