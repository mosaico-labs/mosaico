"""
ROSSequenceExtractor — extracts a Mosaico sequence and writes it as a ROS bag file.

Provides :class:`ROSSequenceExtractor` and the :class:`ROSExtractorConfig` dataclass
that drive the extraction pipeline:

1. Connect to the Mosaico server.
2. Stream every message in the requested sequence (optionally filtered by topic or
   time window).
3. Convert each message to its ROS equivalent via the registered :class:`ROSBridge` adapters.
4. Write the result into a new ROS 1 (``.bag``) or ROS 2 (``.mcap`` / ``.db3``) bag file.

The module also exposes ``ros_sequence_extractor()``, the console-script entry point
installed by the package.
"""

import argparse
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from rich.live import Live
from rosbags.interfaces import Connection
from rosbags.rosbag1 import Writer as Ros1Writer
from rosbags.rosbag2 import StoragePlugin, Writer as Ros2Writer
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

from mosaicolabs import Message, MosaicoClient
from mosaicolabs.logging_config import get_logger, setup_sdk_logging
from mosaicolabs.ros_bridge.loader import MosaicoLoader, ProgressManager
from mosaicolabs.ros_bridge.qos import get_qos_for_topic
from mosaicolabs.ros_bridge.registry import ROSTypeRegistry

# Set the hierarchical logger
logger = get_logger(__name__)


# --- Configuration ---
@dataclass
class ROSExtractorConfig:
    """
    Configuration for :class:`ROSSequenceExtractor`.

    Collects all parameters needed to connect to the Mosaico server, select the
    target sequence, control topic and time-window filtering, and control the
    ROS bag output format and storage location.
    """

    rosbag_path: Path
    """
    The path where to save the ROS bag file.
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

    storage_plugin: StoragePlugin = StoragePlugin.MCAP
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
    
    If provided it must be have the `write` permission.
    """

    tls_cert_path: Optional[str] = None
    """
    Path to the TLS certificate file for secure connection on the mosaico server. Defaults to None. 
    If tls_cert_path=None and enable_tls=True, a standard one-way TLS (server authenticated only) connection is established
    """

    enable_tls: bool = False
    """
    Enable the TLS standard one-way TLS (server authenticated only) communication protocol. Defaults to False. 
    If tls_cert_path is provided (not None), this flag does not have any effect.
    """

    start_timestamp_ns: Optional[int] = None
    """Timestamp (in nanoseconds) from where to start extracting data of specified sequence"""

    end_timestamp_ns: Optional[int] = None
    """Timestamp (in nanoseconds) to finish extracting data of specified sequence"""

    overwrite: bool = False
    """If True, delete and recreate the rosbag path if it already exists. Defaults to False."""


# --- Main Deinjector Class ---


class ROSSequenceExtractor:
    """
    Orchestrates the extraction of a Mosaico sequence into a ROS bag file.

    On each call to :meth:`run`, the extractor:

    1. Prepares (and optionally clears) the output directory.
    2. Connects to the Mosaico server via :class:`MosaicoClient`.
    3. Opens a :class:`MosaicoLoader` to stream messages for the configured sequence.
    4. For every message, looks up the appropriate :class:`ROSAdapterBase` via
       :class:`ROSBridge`, converts the payload to a native ROS type, and writes it
       to the bag.

    Topics that have no registered adapter, or whose ``to_ros()`` call fails, are
    silently skipped after logging a warning.
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
        self.typestore: Typestore = get_typestore(self.cfg.ros_distro or Stores.EMPTY)
        self.mosaico_loader: Optional[MosaicoLoader] = None

    def _register_custom_types(self):
        """
        Loads custom ROS message definitions into the global `ROSTypeRegistry`.

        This enables the extractor to correctly deserialize proprietary message types
        found within the bag file.
        """
        if not self.cfg.custom_msgs:
            return

        # Register Global Types (Registry Pattern)
        logger.info("Registering custom message definitions...")
        for package, path, store in self.cfg.custom_msgs:
            try:
                ROSTypeRegistry.register_directory(
                    package_name=package, dir_path=path, store=store
                )
                logger.debug(f"Registered package '{package}' from '{path}'")
            except Exception as e:
                logger.error(f"Failed to register custom msgs at '{path}': '{e}'")

    def _register_definitions(self, types_map: dict[str, str]):
        """Safe registration wrapper."""
        from rosbags.typesys import get_types_from_msg

        for msg_type, msg_def in types_map.items():
            try:
                add_types = get_types_from_msg(msg_def, msg_type)
                self.typestore.register(add_types)
            except Exception as e:
                logger.warning(f"Failed to register type '{msg_type}': '{e}'")

    def _open_or_get_mosaicoloader(self, mclient: MosaicoClient) -> MosaicoLoader:
        """
        Returns the MosaicoLoader.

        Returns:
            An instance of MosaicoLoader.
        """

        if self.mosaico_loader:
            return self.mosaico_loader

        self.mosaico_loader = MosaicoLoader(
            mclient,
            self.typestore,
            self.cfg.sequence_name,
            self.cfg.topics,
            self.cfg.start_timestamp_ns,
            self.cfg.end_timestamp_ns,
        )

        return self.mosaico_loader

    def _open_bagwriter(self, path: Path) -> Union[Ros1Writer, Ros2Writer]:
        """
        Returns the bag writer for the given path.

        On first invocation this method:

        1. Registers any custom ROS message types from the ``ROSTypeRegistry``.
        2. For **ROS 1** (``Stores.ROS1_NOETIC``): creates the output directory and
           opens a ``rosbags.rosbag1.Writer`` targeting ``<path>/<path.name>.bag``.
        3. For **ROS 2**: opens a ``rosbags.rosbag2.Writer`` with the requested
           ``storage_plugin``. The bag format version is inferred from the distro
           (version 9 for Jazzy / Kilted / LATEST, version 8 for all others).

        Args:
            path: The output directory for the bag file.

        Returns:
            The open bag writer instance.
        """
        bagwriter = None

        # Importing correct writer
        if self.cfg.ros_distro is Stores.ROS1_NOETIC:
            path.mkdir(parents=True, exist_ok=True)
            full_path = path / (path.name + ".bag")
            bagwriter = Ros1Writer(full_path)

        else:
            # Deducing rosbag2 version from ROS_DISTRO
            if (
                self.cfg.ros_distro is Stores.ROS2_JAZZY
                or self.cfg.ros_distro is Stores.ROS2_KILTED
                or self.cfg.ros_distro is Stores.LATEST
            ):
                bagversion = 9
            else:
                bagversion = 8

            bagwriter = Ros2Writer(
                path, storage_plugin=self.cfg.storage_plugin, version=bagversion
            )

        return bagwriter

    def _prepare_output_path(self):
        """
        Resolves the final rosbag output directory and enforces the overwrite policy.

        The output path is ``cfg.rosbag_path / cfg.sequence_name``. If that path
        already exists:

        - ``overwrite=False`` (default): raises :class:`FileExistsError`.
        - ``overwrite=True``: the existing directory is deleted recursively before
          the path is returned.

        Returns:
            Path: The prepared (non-existent) output directory ready for the bag writer.

        Raises:
            FileExistsError: If the path exists and ``cfg.overwrite`` is ``False``.
        """
        rosbag_path = self.cfg.rosbag_path / self.cfg.sequence_name

        if rosbag_path.exists():
            if not self.cfg.overwrite:
                raise FileExistsError(
                    f"Rosbag path '{rosbag_path}' already exists. "
                    "Pass overwrite=True (or --overwrite on CLI) to replace it."
                )
            shutil.rmtree(rosbag_path)

        return rosbag_path

    def _process_message(
        self,
        bagwriter: Union[Ros1Writer, Ros2Writer],
        t_name: str,
        ms_msg: Message,
        ui: ProgressManager,
    ):
        """
        Internal business logic for processing a single Mosaico message.

        Steps:
        1. **Resolve Adapter**: Locates the appropriate Mosaico Adapter for the message type.
        2. **Translate**: Obtains or creates a `RosMsg` for the specific topic (looking for message type within Mosaico topic metadata).
        3. **Resolve Connection**: Obtains or creates a `Connection` for the specific topic.
        4. **Write**: Writes the RosMsg into the rosbag.
        """

        if self.mosaico_loader is None:
            raise RuntimeError(
                "Impossible to process messages if Mosaico Loader is not instanciated first"
            )

        if t_name in self.ignored_topics:
            ui.advance_global()
            return

        # --- Resolve Adapter Check ---
        # For each Mosaico type Message find its adapter
        adapter = self.mosaico_loader.resolve_adapter(t_name)

        if adapter is None:
            return  # This should not happen since MosaicoLoader should filter unsupported message types

        # --- Translate Check ---
        ros_msg_type = self.mosaico_loader.resolve_rosmsg_type(t_name)

        try:
            ros_msg = adapter.to_ros(ms_msg, self.typestore, ros_msg_type)
        except TypeError as e:
            self.ignored_topics.add(t_name)
            logger.warning(
                f"Could not encode to ros '{ms_msg.ontology_tag()}' type because: {e}. Skipping the topic associated to this message"
            )
            ui.update_status(t_name, "Failed encoding", style="yellow")
            ui.advance_global()
            return

        # --- Resolve Check ---
        ros_msgtype = ros_msg.__msgtype__
        ros_recording_timestamp_ns = ms_msg.timestamp_ns

        # --- Resolve Connection check ---
        if t_name not in self.accepted_connections:  # New connection available
            if self.cfg.ros_distro is Stores.ROS1_NOETIC and isinstance(
                bagwriter, Ros1Writer
            ):
                new_connection = bagwriter.add_connection(
                    t_name,
                    ros_msgtype,
                    typestore=self.typestore,
                )
            elif self.cfg.ros_distro in [
                Stores.LATEST,
                Stores.ROS2_DASHING,
                Stores.ROS2_ELOQUENT,
                Stores.ROS2_FOXY,
                Stores.ROS2_GALACTIC,
                Stores.ROS2_HUMBLE,
                Stores.ROS2_IRON,
                Stores.ROS2_JAZZY,
                Stores.ROS2_KILTED,
            ] and isinstance(bagwriter, Ros2Writer):
                new_connection = bagwriter.add_connection(
                    t_name,
                    ros_msgtype,
                    typestore=self.typestore,
                    offered_qos_profiles=get_qos_for_topic(t_name),
                )
            else:
                raise ValueError(f"Unsupported ros distro: {self.cfg.ros_distro}")

            self.accepted_connections.update({t_name: new_connection})

        connection = self.accepted_connections[t_name]

        # --- Write check ---
        try:
            if self.cfg.ros_distro is Stores.ROS1_NOETIC:  # ROS1
                bagwriter.write(
                    connection,
                    ros_recording_timestamp_ns,
                    self.typestore.serialize_ros1(ros_msg, ros_msgtype),
                )
            else:  # ROS2
                bagwriter.write(
                    connection,
                    ros_recording_timestamp_ns,
                    self.typestore.serialize_cdr(ros_msg, ros_msgtype),
                )
        except Exception:
            self.ignored_topics.add(t_name)
            ui.update_status(t_name, "Failed writing to bag", style="yellow")
            ui.advance_global()
            return

        ui.advance_all(t_name)

    def run(self):
        """
        Executes the full extraction pipeline.

        Steps:

        1. Calls :meth:`_prepare_output_path` to validate / clear the output location.
        2. Opens a :class:`MosaicoClient` connection and a bag writer.
        3. Instantiates a :class:`MosaicoLoader` to stream the sequence messages.
        4. For each ``(topic, message)`` pair, delegates to
           :meth:`_process_message` which translates and writes the ROS message.

        Progress is displayed in real-time via a ``rich`` live progress bar.
        A ``KeyboardInterrupt`` exits cleanly with a warning log.
        """

        # Create rosbag path and check whether it already exists
        rosbag_path = self._prepare_output_path()

        try:
            with MosaicoClient.connect(
                host=self.cfg.host,
                port=self.cfg.port,
                api_key=self.cfg.mosaico_api_key,
                enable_tls=self.cfg.enable_tls,
                tls_cert_path=self.cfg.tls_cert_path,
            ) as mclient:
                logger.info(f"Writing bag '{self.cfg.rosbag_path}'")

                # Register custom ROS messages to ROSTypeRegistry
                self._register_custom_types()

                # Register all ROS messages to typestore
                custom_types = ROSTypeRegistry.get_types(self.cfg.ros_distro)
                if custom_types:
                    self._register_definitions(custom_types)

                # Creating the bagwriter
                with self._open_or_get_mosaicoloader(mclient) as ms_loader:
                    with self._open_bagwriter(rosbag_path) as bagwriter:
                        ui = ProgressManager(ms_loader)
                        ui.setup()

                        with Live(ui.progress, console=self.console):
                            for t_name, ms_msg in ms_loader:
                                self._process_message(bagwriter, t_name, ms_msg, ui)

        except KeyboardInterrupt:
            logger.warning("Operation cancelled by user. Shutting down...")
            return
        # except Exception as e:
        #     logger.exception(f"Fatal error during sequence extraction: '{e}'")
        #     return


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
        default=Path("."),
        type=Path,
        help="Path where to save the rosbag file",
    )

    parser.add_argument(
        "--start_timestamp_ns",
        default=None,
        help="Timestamp from where to start extractiong from sequence and create rosbag. None by default",
    )
    parser.add_argument(
        "--end_timestamp_ns",
        type=int,
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
        "--overwrite",
        action="store_true",
        default=False,
        help="Delete and recreate the rosbag path if it already exists",
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

    selected_distro = Stores.__members__[args.ros_distro]
    selected_storage_plugin = StoragePlugin.__members__[args.storage_plugin]

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
        overwrite=args.overwrite,
    )

    # --- Execution ---
    extractor = ROSSequenceExtractor(configs)
    extractor.run()


if __name__ == "__main__":
    ros_sequence_extractor()
