"""
ROS Bag Injection Tool.

This module provides a command-line interface (CLI) and a Python API for injecting
data from ROS 1/2 bag files (MCAP, DB3, BAG) into the Mosaico data platform.

It handles the complex orchestration of:
1.  **Ingestion:** Reading raw messages from bag files using `ROSLoader`.
2.  **Adaptation:** converting ROS-specific types (e.g., `sensor_msgs/Image`) into
    Mosaico Ontology types (e.g., `Image`) via `ROSAdapter`.
3.  **Transmission:** streaming the converted data to the Mosaico server using
    `MosaicoClient` with efficient batching and parallelism.
4.  **Configuration:** Managing custom message definitions via `ROSTypeRegistry`.

Typical usage as a script:
    $ mosaico.ros_injector ./data.mcap --name "Test_Run_01"

Typical usage as a library:
    config = ROSInjectionConfig(file_path=Path("data.mcap"), ...)
    injector = RosbagInjector(config)
    injector.run()
"""

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Type
from rich.live import Live
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rosbags.typesys import Stores

from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.enum import OnErrorPolicy, SequenceStatus
from mosaicolabs.handlers import SequenceWriter
from mosaicolabs.logging_config import get_logger, setup_sdk_logging

from .ros_bridge import ROSAdapterBase, ROSBridge
from .loader import LoaderErrorPolicy, ROSLoader
from .registry import ROSTypeRegistry
from .ros_message import ROSMessage

# Set the hierarchical logger
logger = get_logger(__name__)


# --- Configuration ---
@dataclass
class ROSInjectionConfig:
    """
    The central configuration object for the ROS Bag injection process.

    This data class serves as the single source of truth for all injection settings,
    decoupling the orchestration logic from CLI arguments or configuration files.
    It encapsulates network parameters, file paths, and advanced filtering logic required
    to drive a successful ingestion session.

    Attributes:
        file_path (Path): Absolute or relative path to the input ROS bag file (.mcap, .db3, or .bag).
        sequence_name (str): The name for the new sequence to be created on the Mosaico server.
        metadata (dict): User-defined metadata to attach to the sequence (e.g., driver, weather, location).
        host (str): Hostname or IP of the Mosaico server. Defaults to "localhost".
        port (int): Port of the Mosaico server. Defaults to 6726.
        ros_distro (Optional[Stores]): The target ROS distribution for message parsing (e.g., Stores.ROS2_HUMBLE).
            See [`rosbags.typesys.Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores).
        on_error (OnErrorPolicy): Behavior when an ingestion error occurs (Delete the partial sequence or Report the error).
        custom_msgs (Optional[List[Tuple]]): List of custom .msg definitions to register before loading.
        topics (Optional[List[str]]): List of topics to filter, supporting glob patterns (e.g., ["/cam/*"]).
        log_level (str): Logging verbosity level ("DEBUG", "INFO", "WARNING", "ERROR").

    Example:
        ```python
        from pathlib import Path
        from rosbags.typesys import Stores
        from mosaicolabs.enum import OnErrorPolicy
        from mosaicolabs.ros_bridge import ROSInjectionConfig

        config = ROSInjectionConfig(
            file_path=Path("recording.mcap"),
            sequence_name="test_drive_01",
            metadata={"environment": "urban", "vehicle": "robot_alpha"},
            ros_distro=Stores.ROS2_FOXY,
            on_error=OnErrorPolicy.Delete
        )
        ```
    """

    file_path: Path
    sequence_name: str
    metadata: dict
    host: str = "localhost"
    port: int = 6726

    ros_distro: Optional[Stores] = None
    """
    The specific ROS distribution to use for message parsing (e.g., Stores.ROS2_HUMBLE). If None, defaults to Empty/Auto.

    See [`rosbags.typesys.Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores).
    """

    on_error: OnErrorPolicy = OnErrorPolicy.Delete
    """the `SequenceWriter` `on_error` behavior when a sequence write fails (Report vs Delete)"""

    custom_msgs: Optional[List[Tuple[str, Path, Optional[Stores]]]] = None
    """
    A list of tuples (package_name, path, store) to register custom .msg definitions before loading.

    For example, for "my_robot_msgs/msg/Location" pass: 

    package_name = "my_robot_msgs"; path = path/to/Location.msg; store = Stores.ROS2_HUMBLE (e.g.) or None

    See [`rosbags.typesys.Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores).
    """

    topics: Optional[List[str]] = None
    """A list of specific topics to filter (supports glob patterns). If None, all compatible topics are loaded."""

    log_level: str = "INFO"


# --- UI / Progress Helper ---


class ProgressManager:
    """
    Visual management system for ROS injection tracking.

    This class decouples the UI presentation logic from the data processing pipeline.
    It utilizes the `rich` library to provide real-time feedback through multi-lane progress bars,
    tracking individual topic throughput and aggregate global progress.


    Methods:
        setup(): Initializes the progress tracking tasks by querying message counts from the loader.
        update_status(topic, status, style): Modifies the label of a specific topic bar (e.g., to show "No Adapter").
        advance_global(): Increments the master progress bar without affecting individual topic bars.
        advance_all(topic): Increments both the specific topic task and the global master task.
    """

    def __init__(self, loader: ROSLoader):
        """
        Initialize the progress manager.

        Args:
            loader (ROSLoader): The initialized data loader. Used to query total
                                message counts for setting up progress bars.
        """
        self.loader = loader
        self.progress = Progress(
            TextColumn("[bold cyan]{task.fields[name]}"),
            BarColumn(),
            MofNCompleteColumn(),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            TimeRemainingColumn(),
            "•",
            TimeElapsedColumn(),
            expand=True,
        )
        self.tasks: Dict[str, TaskID] = {}
        self.global_task: Optional[TaskID] = None

    def setup(self):
        """
        Calculates totals and creates the visual progress tasks.
        Must be called before the main processing loop starts.
        """
        # Create individual progress bars for each topic
        for topic in self.loader.topics:
            count = self.loader.msg_count(topic)
            self.tasks[topic] = self.progress.add_task("", total=count, name=topic)

        # Create a master progress bar for the aggregate total
        total_msgs = sum(self.loader.msg_count(t) for t in self.loader.topics)
        self.global_task = self.progress.add_task(
            "Total", total=total_msgs, name="Total Upload"
        )

    def update_status(self, topic: str, status: str, style: str = "white"):
        """
        Updates the text description of a specific topic's progress bar.
        Useful for indicating errors or skipped topics (e.g. "[red]No Adapter").

        Args:
            topic: The topic name.
            status: The status message to display.
            style: The rich style string (e.g., 'red', 'bold yellow').
        """
        if topic in self.tasks:
            self.progress.update(
                self.tasks[topic],
                name=f"[{style}]{topic}: {status}",
            )

    def advance_global(self):
        """Advances only the global progress bar (used when skipping messages)."""
        if self.global_task is not None:
            self.progress.advance(self.global_task)

    def advance_all(self, topic: str):
        """Advances both the specific topic's bar and the global bar (successful process)."""
        if topic in self.tasks:
            self.progress.advance(self.tasks[topic])
        if self.global_task is not None:
            self.progress.advance(self.global_task)


# --- Main Injector Class ---


class RosbagInjector:
    """
    Main controller for the ROS Bag ingestion workflow.

    The `RosbagInjector` orchestrates the entire data pipeline from the physical storage
    to the remote Mosaico server. It manages the initialization of the registry,
    establishes network connections, and drives the main adaptation loop.

    **Core Workflow Architecture:**

    1.  **Registry Initialization**: Pre-loads custom message definitions via the `ROSTypeRegistry`.
    2.  **Resource Management**: Opens the `ROSLoader` for file access and the `MosaicoClient` for networking.
    3.  **Stream Negotiation**: Creates a `SequenceWriter` on the server and opens individual `TopicWriter` streams.
    4.  **Adaptation Loop**: Iterates through ROS records, translates them via the `ROSBridge`, and pushes them to the server.

    Example:
        ```python
        from mosaicolabs.ros_bridge import RosbagInjector, ROSInjectionConfig

        # Define configuration
        config = ROSInjectionConfig(file_path=Path("data.db3"), sequence_name="auto_ingest")

        # Initialize and run
        injector = RosbagInjector(config)
        injector.run() # This handles the full lifecycle including cleanup on failure
        ```

    Attributes:
        cfg (ROSInjectionConfig): The active configuration settings.
        console (Console): The rich console instance for logging and UI output.
        _ignored_topics (Set[str]): Cache of topics that lack a compatible adapter, used for fast-fail filtering.
    """

    def __init__(self, config: ROSInjectionConfig):
        """
        Args:
            config: The fully resolved configuration object.
        """
        self.cfg = config
        # Create the single "source of truth" for the terminal
        from rich.console import Console

        self.console = Console(stderr=True)
        setup_sdk_logging(
            level=self.cfg.log_level.upper(), pretty=True, console=self.console
        )

        # Set of topics to skip (e.g., no adapter found), allowing O(1) fast-fail in the loop.
        self._ignored_topics: Set[str] = set()

    def _register_custom_types(self):
        """
        Loads custom ROS message definitions into the global `ROSTypeRegistry`.

        This enables the loader to correctly deserialize proprietary message types
        found within the bag file.
        """
        if not self.cfg.custom_msgs:
            return

        logger.info("Registering custom message definitions...")
        for package, path, store in self.cfg.custom_msgs:
            try:
                ROSTypeRegistry.register_directory(
                    package_name=package, dir_path=path, store=store
                )
                logger.debug(f"Registered package '{package}' from '{path}'")
            except Exception as e:
                logger.error(f"Failed to register custom msgs at '{path}': '{e}'")

    def _get_adapter(self, msg_type: str) -> Optional[Type[ROSAdapterBase]]:
        """
        Memoized lookup for Mosaico ROS Adapters.

        Args:
            msg_type: The ROS message type string (e.g., "sensor_msgs/msg/Image").

        Returns:
            The adapter class if found, otherwise None.
        """

        return ROSBridge.get_adapter(msg_type)

    def run(self):
        """
        Main execution entry point for the injection pipeline.

        This method establishes the necessary contexts (Network Client, File Loader, Server Writer)
        and executes the processing loop. It handles graceful shutdowns in case of
        user interrupts and provides a summary report upon completion.

        Raises:
            KeyboardInterrupt: If the user cancels the operation via Ctrl+C.
            Exception: Any fatal error encountered during networking or file access.
        """
        # 1. Prepare Registry
        self._register_custom_types()

        logger.info(f"Connecting to Mosaico at '{self.cfg.host}:{self.cfg.port}'...")

        try:
            # Context: Mosaico Client (Network Connection)
            with MosaicoClient.connect(
                host=self.cfg.host, port=self.cfg.port
            ) as mclient:
                # Context: ROS Loader (File Access)
                logger.info(f"Opening bag: '{self.cfg.file_path}'")
                with ROSLoader(
                    file_path=self.cfg.file_path,
                    topics=self.cfg.topics,
                    typestore_name=self.cfg.ros_distro or Stores.EMPTY,
                    error_policy=LoaderErrorPolicy.IGNORE,
                ) as ros_loader:
                    # Setup Progress UI
                    ui = ProgressManager(ros_loader)
                    ui.setup()

                    # Context: Sequence Writer (Server Transaction)
                    seq_writer = mclient.sequence_create(
                        sequence_name=self.cfg.sequence_name,
                        metadata=self.cfg.metadata,
                        on_error=self.cfg.on_error,
                    )
                    with seq_writer:
                        logger.info("Starting upload...")

                        # Main Processing Loop
                        # By passing self.console, any 'logger.info' calls inside
                        # this loop will print cleanly ABOVE the progress bars.
                        with Live(ui.progress, console=self.console):
                            for ros_msg, exc in ros_loader:
                                self._process_message(ros_msg, exc, seq_writer, ui)

                if seq_writer.sequence_status == SequenceStatus.Error:
                    logger.error(
                        "`SequenceWriter` returned a `SequenceStatus.Error` status. Upload might have failed!"
                    )
                    return

                logger.info("Sequence upload completed successfully.")

                # Retrieve the sequence info
                seq_handler = mclient.sequence_handler(self.cfg.sequence_name)
                if seq_handler is not None:
                    # --- Final Statistics Report ---
                    self._print_summary(
                        original_size=self.cfg.file_path.stat().st_size,
                        remote_size=seq_handler.total_size_bytes,
                    )
                else:
                    logger.error(
                        f"Oops, Something bad happened: Sequence '{self.cfg.sequence_name}' not found on remote server. This should not happen..."
                    )

        except KeyboardInterrupt:
            logger.warning("Operation cancelled by user. Shutting down...")
            return
        except Exception as e:
            logger.exception(f"Fatal error during injection: '{e}'")
            return

    def _print_summary(self, original_size: int, remote_size: int):
        """
        Calculates and displays the ingestion performance summary.

        Outputs the original file size, the remote sequence size, the compression ratio,
        and the percentage of disk space saved.
        """
        if remote_size == 0:
            logger.warning("No data was written; cannot calculate compression ratio.")
            return

        # Calculate ratio: (Original / Remote)
        # A ratio > 1 means the remote sequence is smaller (better compression)
        ratio = original_size / remote_size
        savings = max(0, (1 - (remote_size / original_size)) * 100)

        from rich.panel import Panel

        summary_text = (
            f"Original Size:  [bold]{original_size / (1024 * 1024):.2f}[/bold]\n"
            f"Remote Size:    [bold]{remote_size / (1024 * 1024):.2f}[/bold]\n"
            f"Ratio:          [bold cyan]{ratio:.2f}x[/bold cyan]\n"
            f"Space Saved:    [bold green]{savings:.1f}%[/bold green]"
        )

        self.console.print(
            Panel(
                summary_text,
                title="[bold]Injection Summary[/bold]",
                expand=False,
                border_style="green",
                padding=1,
                highlight=True,
            )
        )

    def _process_message(
        self,
        ros_msg: ROSMessage,
        exc: Optional[Exception],
        seq_writer: SequenceWriter,
        ui: ProgressManager,
    ):
        """
        Internal business logic for processing a single ROS message.

        Steps:
        1. **Filter**: Checks if the topic is blacklisted (e.g., no adapter found).
        2. **Validate**: Checks for deserialization errors or empty payloads.
        3. **Resolve**: Locates the appropriate Mosaico Adapter for the message type.
        4. **Stream**: Obtains or creates a `TopicWriter` for the specific topic.
        5. **Adapt & Push**: Translates the ROS dictionary into a Mosaico object and pushes it to the server buffer.
        """

        # --- Filter Check ---
        if ros_msg.topic in self._ignored_topics:
            ui.advance_global()
            return

        # --- Integrity Check ---
        # If the loader yielded an exception or empty data, mark as error
        if exc or not ros_msg.data:
            ui.update_status(ros_msg.topic, "Deserialization Error.", "red")
            ui.advance_global()
            return

        # --- Adapter Resolution ---
        adapter = self._get_adapter(ros_msg.msg_type)

        if adapter is None:
            # If no adapter exists, blacklist this topic to prevent future lookups
            self._ignored_topics.add(ros_msg.topic)
            ui.update_status(ros_msg.topic, "No Adapter", "yellow")
            ui.advance_global()
            return

        # Retrieve the writer from SequenceWriter local cache or create new one on server
        twriter = seq_writer.get_topic_writer(ros_msg.topic)
        # Should theoretically not be None if exists returned True
        if twriter is None:
            # Register new topic on server
            twriter = seq_writer.topic_create(
                topic_name=ros_msg.topic,
                metadata={},  # TODO: how-to push metadata per topic?
                ontology_type=adapter.ontology_data_type(),
            )
            if twriter is None:
                ui.update_status(ros_msg.topic, "Write Error", "red")
                # We assume transient error and continue; strict policies are handled by Client
                ui.advance_all(ros_msg.topic)
                return

        # --- Adapt & Push ---
        try:
            # Convert ROS dict -> Mosaico Object -> Arrow Batch
            twriter.push(adapter.translate(ros_msg))
            ui.advance_all(ros_msg.topic)
        except Exception:
            # If writing fails (e.g. network error, validation error), update UI
            ui.update_status(ros_msg.topic, "Write Error", "red")
            # We assume transient error and continue; strict policies are handled by Client
            ui.advance_all(ros_msg.topic)


# --- CLI Entry Point ---


def _parse_metadata_arg(metadata_input: Optional[str]) -> dict:
    """
    Parses the CLI metadata argument.

    Supports two formats:
    1. A raw JSON string: '{"driver": "John"}'
    2. A path to a JSON file: './configs/meta.json'

    Returns:
        dict: The parsed metadata, or empty dict on failure.
    """
    if not metadata_input:
        return {}

    # Attempt JSON Parse
    try:
        data = json.loads(metadata_input)
        logger.info("Metadata parsed successfully from JSON string.")
        return data
    except json.JSONDecodeError:
        pass  # Not a valid JSON string, proceed to check file

    # Attempt File Read
    file_path = Path(metadata_input)
    if file_path.is_file():
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"Metadata loaded successfully from file: '{file_path}'")
            return data
        except json.JSONDecodeError as e:
            logger.error(
                f"File found at '{file_path}' but contained invalid JSON: '{e}'"
            )
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error reading metadata file '{file_path}': '{e}'")
            sys.exit(1)

    # Failure
    logger.error(
        f"Metadata argument is neither a valid JSON string nor a valid file path: '{metadata_input}'"
    )
    sys.exit(1)


def ros_injector():
    """
    Console script entry point.
    Parses arguments, sets up configuration, and initiates the injector.
    """
    parser = argparse.ArgumentParser(description="Inject ROS Bag data into Mosaico.")

    # Required Arguments
    parser.add_argument("bag_path", type=Path, help="Path to .mcap or .db3 file")
    parser.add_argument("--name", "-n", required=True, help="Target Sequence Name")

    # Connection Arguments
    parser.add_argument("--host", default="localhost", help="Mosaico Server Host")
    parser.add_argument(
        "--port", type=int, default=6726, help="Mosaico Server Port (Default: 6726)"
    )

    # Filter Arguments
    parser.add_argument(
        "--topics",
        nargs="+",
        help="Specific topics to filter (supports glob patterns like /cam/*)",
    )

    # Metadata Arguments
    parser.add_argument(
        "--metadata",
        help="JSON string or path to JSON file containing sequence metadata",
    )

    # Advanced Arguments
    parser.add_argument(
        "--ros-distro",
        default=None,
        choices=[s.name.lower() for s in Stores],
        help="Target ROS Distribution for message parsing (e.g., ros2_humble). "
        "If not set, defaults to ROS2_HUMBLE.",
    )

    parser.add_argument(
        "--log",
        "-l",
        help="Set the logging verbosity level",
        default="INFO",  # Optional: defaults to INFO
        type=str.upper,  # Automatically converts input (e.g., 'debug') to uppercase
        choices=[
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
        ],  # Restricts input to these specific strings
    )
    args = parser.parse_args()

    # --- Configuration Construction ---

    # Resolve Enum from string input
    selected_distro = (
        Stores(args.ros_distro.lower()) if args.ros_distro else Stores.EMPTY
    )

    # Parse metadata
    user_metadata = _parse_metadata_arg(args.metadata)
    # Inject traceability metadata
    user_metadata.update({"rosbag_injection": args.bag_path.name})

    config = ROSInjectionConfig(
        file_path=args.bag_path,
        sequence_name=args.name,
        metadata=user_metadata,
        host=args.host,
        port=args.port,
        topics=args.topics,
        ros_distro=selected_distro,
        log_level=args.log,
    )

    # --- Execution ---
    injector = RosbagInjector(config)
    injector.run()


if __name__ == "__main__":
    ros_injector()
