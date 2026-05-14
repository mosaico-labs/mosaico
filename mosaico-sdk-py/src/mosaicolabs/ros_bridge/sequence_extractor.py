"""
TODO
"""

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from rosbags.interfaces import Connection
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

from mosaicolabs.comm.mosaico_client import MosaicoClient

# from mosaicolabs.enum import (
#     SessionLevelErrorPolicy,
#     TopicLevelErrorPolicy,
# )
from mosaicolabs.logging_config import get_logger, setup_sdk_logging
from mosaicolabs.ros_bridge import ROSBridge
from mosaicolabs.ros_bridge.helpers import _filter_topics_from_list
from mosaicolabs.ros_bridge.qos import get_qos_for_topic

# Set the hierarchical logger
logger = get_logger(__name__)

# _DEFAULT_TOPIC_ON_ERROR = TopicLevelErrorPolicy.Raise
# _DEFAULT_SESSION_ON_ERROR = SessionLevelErrorPolicy.Report


# --- Configuration ---
@dataclass
class ROSExtractorConfig:
    """
    TODO
    """

    rosbag_path: Path
    """
    The path where to extract the ROS bag file.
    """

    sequence_name: str
    """
    The name of the sequence to extract.
    """

    host: str = "localhost"
    """
    The hostname of the Mosaico server.
    """

    port: int = 6726
    """
    The port of the Mosaico server.
    """

    ros_distro: Optional[Stores] = None
    """
    The specific ROS distribution to use for message parsing (e.g., Stores.ROS2_HUMBLE). If None, defaults to Empty/Auto.

    See [`rosbags.typesys.Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores).
    """

    # on_error: SessionLevelErrorPolicy = _DEFAULT_SESSION_ON_ERROR
    # """the `SequenceWriter` `on_error` behavior when a sequence write fails (Report vs Delete)"""

    # topics_on_error: Union[TopicLevelErrorPolicy, Dict[str, TopicLevelErrorPolicy]] = (
    #     _DEFAULT_TOPIC_ON_ERROR
    # )
    # """
    # The TopicWriter `on_error` behavior ([`TopicLevelErrorPolicy`][mosaicolabs.enum.TopicLevelErrorPolicy]) when a topic write fails.
    # Default is `TopicLevelErrorPolicy.Raise` for all topics.
    # Set to a `TopicLevelErrorPolicy` to apply the same policy to all topics.
    # Set to a `Dict[str, TopicLevelErrorPolicy]` to apply different policies to different topics.
    # """

    storage_plugin: StoragePlugin = StoragePlugin.SQLITE3
    """
    Storage plugin to use. Available: StoragePlugin.SQLITE3 or StoragePlugin.MCAP
    """

    custom_msgs: Optional[list[tuple[str, Path, Optional[Stores]]]] = None
    """
    A list of tuples (package_name, path, store) to register custom .msg definitions before loading.

    For example, for "my_robot_msgs/msg/Location" pass:

    package_name = "my_robot_msgs"; path = path/to/Location.msg; store = Stores.ROS2_HUMBLE (e.g.) or None

    See [`rosbags.typesys.Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores).
    """

    topics: Optional[list[str]] = None
    """List of topic patterns used to filter available topics.

    Supports shell-style glob patterns (e.g., "/cam/*", "*camera_info").
    Patterns starting with '!' are treated as exclusions (e.g., "!/cam/debug*").
    
    **Pattern order matters**:
        - Each non-'!' pattern adds matching topics to the selection.
        - Each '!' pattern removes matching topics from the selection.
        - Later patterns override earlier ones.
        - If no inclusion pattern is provided, selection starts from ALL topics,
          and only exclusion patterns reduce the set.

    If None, all topics are loaded.
    """

    log_level: str = "INFO"
    """The Log Level"""

    mosaico_api_key: Optional[str] = None
    """
    The API key for authentication on the mosaico server. Defaults to None.
    
    If provided it must be have at least [`APIKeyPermissionEnum.Write`][mosaicolabs.enum.APIKeyPermissionEnum.Write]
    permission.
    """

    tls_cert_path: Optional[str] = None
    """Path to the TLS certificate file for secure connection on the mosaico server. Defaults to None."""

    enable_tls: bool = False
    """Enable the TLS commmunication protocol. Defaults to False"""

    start_timestamp_ns: Optional[int] = None
    """Timestamp (in nanoseconds) from where to start extracting data of specified sequence"""

    end_timestamp_ns: Optional[int] = None
    """Timestamp (in nanoseconds) to finish extracting data of specified sequence"""


# --- Main Deinjector Class ---


class ROSSequenceExtractor:
    """
    TODO
    """

    def __init__(self, config: ROSExtractorConfig):
        self.cfg = config

        from rich.console import Console

        self.console = Console(stderr=True)
        setup_sdk_logging(
            level=self.cfg.log_level.upper(), pretty=True, console=self.console
        )

        self.ignored_topics: set[str] = set()
        self.accepted_connections: dict[str, Connection] = {}
        self.typestore: Typestore = get_typestore(self.cfg.ros_distro)
        self.bagwriter = None

    def open_or_get_bagwriter(self) -> Writer:
        if self.bagwriter is None:
            # TODO: understand how to infeer correct version
            self.bagwriter = Writer(
                self.cfg.rosbag_path, storage_plugin=self.cfg.storage_plugin, version=8
            )
        return self.bagwriter

    def run(self):
        """
        TODO
        """

        # TODO: validate rosbag path!

        # self.register_custom_types() # TODO: is this useful?

        try:
            with MosaicoClient.connect(
                host=self.cfg.host,
                port=self.cfg.port,
                api_key=self.cfg.mosaico_api_key,
                enable_tls=self.cfg.enable_tls,
                tls_cert_path=self.cfg.tls_cert_path,
            ) as mclient:
                logger.info(f"Writing bag '{self.cfg.rosbag_path}'")

                # Creating the ROSUnloader
                with self.open_or_get_bagwriter() as bag_writer:
                    # ui = ProgressManager(ros_sequence_writer)
                    # ui.setup()

                    # Getting requested Sequence from Mosaico backend
                    seq_handler = mclient.sequence_handler(
                        sequence_name=self.cfg.sequence_name
                    )

                    if seq_handler is None:
                        all_seq = mclient.list_sequences()
                        raise (
                            ValueError(
                                f"Your requested sequence '{self.cfg.sequence_name}' could not be found. The available Sequences are: {all_seq}"
                            )
                        )

                    # TODO: you should check that if a time window is provided by the user,
                    # TODO: it is not outside the min and max sequence timestamps

                    # Filtering topics
                    filtered_topics = _filter_topics_from_list(
                        seq_handler.topics, self.cfg.topics
                    )

                    streamer = seq_handler.get_data_streamer(
                        topics=filtered_topics,
                        start_timestamp_ns=self.cfg.start_timestamp_ns,
                        end_timestamp_ns=self.cfg.end_timestamp_ns,
                    )

                    for ms_topic, ms_msg in streamer:
                        if ms_topic in self.ignored_topics:
                            continue

                        # For each Message find its adapter based on Mosaico type. If fails, add to self.ignored_topics
                        # --- Adapter Resolution ---
                        mosaico_type = ms_msg.ontology_tag()
                        adapter = ROSBridge.get_default_mosaico_adapter(mosaico_type)

                        if adapter is None:
                            self.ignored_topics.add(ms_topic)
                            logger.warning(
                                f"Could not find Adapter for topic '{ms_topic}' of type '{mosaico_type}'. Skipping the topic associated to this message"
                            )
                            continue

                        # Call from the adapter the to_ros()
                        ros_msg = adapter.to_ros(ms_msg, self.typestore)
                        ros_msgtype = ros_msg.__msgtype__
                        ros_timestamp = ms_msg.timestamp_ns  # TODO: this should be offsetted by start_timestamp_ns if present!

                        # Resolve connection
                        if (
                            ms_topic not in self.accepted_connections
                        ):  # New connection available
                            new_connection = bag_writer.add_connection(
                                ms_topic,
                                ros_msgtype,
                                typestore=self.typestore,
                                offered_qos_profiles=get_qos_for_topic(ms_topic),
                            )
                            self.accepted_connections.update({ms_topic: new_connection})

                        connection = self.accepted_connections.get(ms_topic)

                        # Write the encoded ros_msg to rosbag through Writer
                        if self.cfg.ros_distro == Stores.ROS1_NOETIC:  # ROS1
                            bag_writer.write(
                                connection,
                                ros_timestamp,
                                self.typestore.serialize_ros1(ros_msg, ros_msgtype),
                            )
                        else:  # ROS2
                            bag_writer.write(
                                connection,
                                ros_timestamp,
                                self.typestore.serialize_cdr(ros_msg, ros_msgtype),
                            )

                    # # Finalize the reading channel to release server resources
                    seq_handler.close()

        except KeyboardInterrupt:
            logger.warning("Operation cancelled by user. Shutting down...")
            return
        except Exception as e:
            logger.exception(f"Fatal error during sequence extraction: '{e}'")
            return


# --- CLI Entry Point ---


def ros_sequence_extractor():
    """
    Console script entrypointy.
    Parses arguments. sets up configuration, and initiates the sequence extractor
    """

    parser = argparse.ArgumentParser(
        description="Extracts sequences from Mosaico and encode them as rosbags"
    )

    # Required Arguments
    parser.add_argument(
        "sequence_name",
        type=str,
        help="Name of the Mosaico sequence to extract",
    )

    # ROS arguments
    parser.add_argument(
        "--ros_distro",
        default="ROS2_JAZZY",
        choices=[s.name for s in Stores],
        help="Target ROS distribution for messages. If not set defaults to ROS2_HUMBLE",
    )
    parser.add_argument(
        "--storage_plugin",
        default="MCAP",
        choices=[sp.name for sp in StoragePlugin],
        help="Storage plugin to save rosbag. If not set defaults to MCAP",
    )

    # Filter Arguments
    parser.add_argument(
        "--topics",
        nargs="+",
        help=(
            "Topic patterns to filter (supports glob wildcards like '/cam/*' or '*camera_info'). "
            "Prefix a pattern with '!' to exclude it (e.g., '/cam/*' '!/cam/debug*'). "
            "If only exclusions are provided, all topics are included except those excluded. "
            "Patterns are evaluated in ORDER. "
            "Note: in some shells (e.g., zsh), '!' triggers history expansion, so patterns "
            'should be quoted or escaped (e.g., "!/cam/debug*" or \\\\!/cam/debug*). '
        ),
    )

    parser.add_argument(
        "--rosbag_path",
        default=Path("./rosbag"),
        type=Path,
        help="Path where to save the rosbag file where to save Mosaico Sequence",
    )

    parser.add_argument(
        "--start_timestamp_ns",
        default=None,
        help="Timestamp from where to start extractiong from sequence and create rosbag. None by default",
    )
    parser.add_argument(
        "--end_timestamp_ns",
        default=None,
        help="Timestamp from where to stop extractiong from sequence and create rosbag. None by default",
    )

    # Connection Arguments
    parser.add_argument("--host", default="localhost", help="Mosaico Server Host")
    parser.add_argument(
        "--port", type=int, default=6726, help="Mosaico Server Port (Default: 6726)"
    )

    # Advanced Arguments
    parser.add_argument(
        "--mosaico_api_key", default=None, help="The API key for authentification"
    )
    parser.add_argument(
        "--tls_cert_path", default=None, help="Path to the TLS certificate file"
    )
    parser.add_argument(
        "--enable_tls", default=False, help="Whether Mosaico Server requires tls"
    )

    parser.add_argument(
        "--log",
        "-l",
        type=str.upper,  # Automatically converts input (e.g., 'debug') to uppercase
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging verbosity level. Default is INFO",
    )

    args = parser.parse_args()

    selected_distro = Stores.__members__.get(args.ros_distro)
    selected_storage_plugin = StoragePlugin.__members__.get(args.storage_plugin)

    configs = ROSExtractorConfig(
        rosbag_path=args.rosbag_path,
        sequence_name=args.sequence_name,
        host=args.host,
        port=args.port,
        ros_distro=selected_distro,
        storage_plugin=selected_storage_plugin,
        # custom_msgs=,
        topics=args.topics,
        log_level=args.log,
        mosaico_api_key=args.mosaico_api_key,
        tls_cert_path=args.tls_cert_path,
        enable_tls=args.enable_tls,
        start_timestamp_ns=args.start_timestamp_ns,
        end_timestamp_ns=args.end_timestamp_ns,
    )

    # --- Execution ---
    extractor = ROSSequenceExtractor(configs)
    extractor.run()


if __name__ == "__main__":
    ros_sequence_extractor()
