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
    $ mosaicolabs.ros_injector ./data.mcap --name "Test_Run_01"

Typical usage as a library:
    config = ROSInjectionConfig(file_path=Path("data.mcap"), ...)
    injector = RosbagInjector(config)
    injector.run()
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Type, Union

from rich.live import Live
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.enum import (
    SerializationFormat,
    SessionLevelErrorPolicy,
    TopicLevelErrorPolicy,
    TopicWriterStatus,
)
from mosaicolabs.enum.session_status import SessionStatus
from mosaicolabs.handlers.base_session_writer import AnySessionWriter
from mosaicolabs.logging_config import get_logger, setup_sdk_logging

from .adapter_base import RosSchemaMetadata
from .loader import ROSLoader
from .registry import ROSTypeRegistry
from .ros_bridge import ROSAdapterBase, ROSBridge
from .ros_message import ROSMessage
from .ui import ProgressManager

# Set the hierarchical logger
logger = get_logger(__name__)

_DEFAULT_TOPIC_ON_ERROR = TopicLevelErrorPolicy.Raise
_DEFAULT_SESSION_ON_ERROR = SessionLevelErrorPolicy.Report


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
        on_error (SessionLevelErrorPolicy): Behavior when an ingestion error occurs (Delete the partial sequence or Report the error).
            Default: [`SessionLevelErrorPolicy.Report`][mosaicolabs.enum.SessionLevelErrorPolicy.Report]
        topics_on_error (Union[TopicLevelErrorPolicy, Dict[str, TopicLevelErrorPolicy]]): Behavior when a topic write fails.
            Default: [`TopicLevelErrorPolicy.Raise`][mosaicolabs.enum.TopicLevelErrorPolicy.Raise]
            Set to a [`TopicLevelErrorPolicy`][mosaicolabs.enum.TopicLevelErrorPolicy] to apply the same policy to all topics.
            Set to a `Dict[str, TopicLevelErrorPolicy]` to apply different policies to different (subset of) topics.
        custom_msgs (Optional[List[Tuple]]): List of custom .msg definitions to register before loading.
        registry (Optional[ROSTypeRegistry]): Registry to register `custom_msgs` into; a private
            one is created if `None`. Pass a shared instance to reuse definitions across runs.
        topics (Optional[List[str]]): List of topic patterns used to filter available topics.
            Supports shell-style glob patterns (e.g., ["/cam/\\*", "\\*camera_info"]).
            Patterns starting with "!" are treated as exclusions (e.g., ["\\!/cam/debug\\*"]).
            Patterns are evaluated in ORDER (gitignore-like semantics). If None, all available topics are loaded.
        adapter_overrides (Optional[Dict[str, Type[ROSAdapterBase]]]): Mapping of topics to adapter overrides,
            allowing the use of specific adapters instead of the default for designated topics.
            Deafult: None
        serialization_formats (Optional[Dict[str, SerializationFormat]]): Mapping of ROS message type strings
            (e.g. "sensor_msgs/msg/PointCloud2") to the `SerializationFormat` used when synthesizing an
            `Unmodeled` ontology for topics that have no registered Mosaico adapter. Message types not
            present in this mapping default to `SerializationFormat.Default`.
            Default: None
        log_level (str): Logging verbosity level ("DEBUG", "INFO", "WARNING", "ERROR").
        mosaico_api_key (Optional[str]): The API key for authentication on the mosaico server.
            If provided it must be have the `write` permission.
            Default: None
        tls_cert_path (Optional[str]): Path to the TLS certificate file for secure connection on the mosaico server.
            Default: None

    Example:
        ```python
        from pathlib import Path
        from rosbags.typesys import Stores
        from mosaicolabs.enum import SessionLevelErrorPolicy
        from mosaicolabs.ros_bridge import ROSInjectionConfig

        config = ROSInjectionConfig(
            file_path=Path("recording.mcap"),
            sequence_name="test_drive_01",
            metadata={"environment": "urban", "vehicle": "robot_alpha"},
            ros_distro=Stores.ROS2_FOXY,
            on_error=SessionLevelErrorPolicy.Delete,
            topics_on_error=TopicLevelErrorPolicy.Finalize,
        )
        ```
    """

    file_path: Path
    """
    The path to the ROS bag file to ingest.
    """

    sequence_name: str
    """
    The name of the sequence to create.
    """

    metadata: dict = field(default_factory=dict)
    """
    Metadata to associate with the sequence.
    """

    topic_metadata: Optional[Dict[str, dict]] = None
    """
    A mapping of exact topic name to metadata to associate with that topic, merged into the
    metadata computed from the message schema and the source bag file (see `_process_message`).
    User-supplied values take precedence over the auto-computed ones on key conflicts.

    Only applied to topics that end up being ingested; entries for topics excluded by `topics`
    filtering are simply unused. Default: None.
    """

    update_if_exists: bool = False
    """
    Controls what happens when a sequence named `sequence_name` already exists on the server.

    If `True`, the injector appends this bag's topics to the existing sequence instead of
    creating a new one. Use this both when a ROS recording is split across multiple bag files
    that should all land in the same sequence, and when re-ingesting a derived/reprocessed bag
    (e.g. offline estimation results) whose topics should be merged into a sequence that was
    already ingested from the original recording.

    If `False` (default), the injector creates a new sequence and raises an error if a sequence
    with the same name already exists.

    Each topic's metadata records the source bag file it was ingested from (see
    `schema_metadata` handling in `_process_message`), so which bag file contributed which
    topics remains traceable even after multiple updates to the same sequence.

    Caveat: existence is checked and then acted upon in two separate steps (not atomically),
    so running concurrent injections against the same `sequence_name` can race. Avoid
    concurrent ingestion into the same sequence name.

    Caveat: resuming after a crash is NOT idempotent. `session_writer.get_topic_writer()`
    (see `_process_message`) only consults an in-memory cache scoped to the current process's
    session (`_BaseSessionWriter._topic_writers`); it has no knowledge of topics created by a
    previous, crashed run. So re-running the same bag with `update_if_exists=True` after a
    crash will call `topic_create` again for topics that were already fully ingested before
    the crash, which the server is expected to reject as duplicates (behavior not covered by
    SDK-level tests as of this writing). There is currently no dedup against the topics already
    present in the target sequence (available server-side via `MosaicoClient.sequence_handler(
    sequence_name).topics`, the same mechanism `MosaicoLoader` already uses) before calling
    `topic_create`. A safe resume would need to check that list first and skip topics already
    present, rather than only checking the local per-process cache.
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

    on_error: SessionLevelErrorPolicy = _DEFAULT_SESSION_ON_ERROR
    """the `SequenceWriter` `on_error` behavior when a sequence write fails (Report vs Delete)"""

    topics_on_error: Union[TopicLevelErrorPolicy, Dict[str, TopicLevelErrorPolicy]] = (
        _DEFAULT_TOPIC_ON_ERROR
    )
    """
    The TopicWriter `on_error` behavior ([`TopicLevelErrorPolicy`][mosaicolabs.enum.TopicLevelErrorPolicy]) when a topic write fails.
    Default is `TopicLevelErrorPolicy.Raise` for all topics.
    Set to a `TopicLevelErrorPolicy` to apply the same policy to all topics.
    Set to a `Dict[str, TopicLevelErrorPolicy]` to apply different policies to different topics.
    """

    custom_msgs: Optional[List[Tuple[str, Path, Optional[Stores]]]] = None
    """
    A list of tuples (package_name, path, store) to register custom .msg definitions before loading.

    For example, for "my_robot_msgs/msg/Location" pass:

    package_name = "my_robot_msgs"; path = path/to/Location.msg; store = Stores.ROS2_HUMBLE (e.g.) or None

    See [`rosbags.typesys.Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores).

    Registered into `registry` (or a fresh, private `ROSTypeRegistry` if `registry` is
    `None`) before the loader's `Typestore` is built.
    """

    registry: Optional[ROSTypeRegistry] = None
    """
    The `ROSTypeRegistry` instance to register `custom_msgs` into and to pull existing
    definitions from. If `None` (default), a fresh, private instance is created for this
    injector alone — so its custom types can never leak into another injector/extractor
    run in the same process. Pass the *same* `ROSTypeRegistry` instance across multiple
    configs to deliberately share a centrally pre-registered set of definitions between them.
    """

    topics: Optional[List[str]] = None
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

    adapter_overrides: Optional[Dict[str, Type[ROSAdapterBase]]] = None
    """A mapping of topics to adapter overrides, allowing the use of specific adapters instead of the default for designated topics."""

    serialization_formats: Optional[Dict[str, SerializationFormat]] = None
    """A mapping of ROS message type strings (e.g. "sensor_msgs/msg/PointCloud2") to the
    [`SerializationFormat`][mosaicolabs.enum.SerializationFormat] used when synthesizing an
    `Unmodeled` ontology for topics that have no registered Mosaico adapter.

    Only applies to non-adapted (unmodeled) message types. Types not present in this mapping
    default to `SerializationFormat.Default`.
    """

    log_level: str = "INFO"
    """The Log Level"""

    mosaico_api_key: Optional[str] = None
    """
    The API key for authentication on the mosaico server. Defaults to None.
    
    If provided it must be have the `write` permission.
    """

    tls_cert_path: Optional[str] = None
    """Path to the TLS certificate file for secure connection on the mosaico server. Defaults to None."""

    enable_tls: bool = False
    """Enable the TLS commmunication protocol. Defaults to False"""

    dry_run: bool = False
    """
    If `True`, resolves and reports which topics would be ingested (and with which adapter),
    which topics would be rejected (and why), and which `topic_metadata` entries would be
    unused, without connecting to the Mosaico server or writing any data. Default: False.
    """


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
            config (ROSInjectionConfig): The fully resolved configuration object.
        """
        self._cfg = config
        # Create the single "source of truth" for the terminal
        from rich.console import Console

        self._console = Console(stderr=True)
        setup_sdk_logging(
            level=self._cfg.log_level.upper(), pretty=True, console=self._console
        )

        # Set of topics to skip (e.g., no adapter found), allowing O(1) fast-fail in the loop.
        self._ignored_topics: Set[str] = set()
        self._malformed_message_counts: Dict[str, int] = (
            dict()
        )  # Tracks malformed message counts per topic
        self._typestore: Typestore = get_typestore(self._cfg.ros_distro or Stores.EMPTY)
        self._loader: Optional[ROSLoader] = None

        # Own a private registry by default, so this injector's custom types can never
        # leak into another injector/extractor run in the same process. Pass the same
        # `ROSTypeRegistry` instance via `cfg.registry` to deliberately share definitions
        # across multiple runs (e.g. a centralized setup routine).
        self._registry: ROSTypeRegistry = self._cfg.registry or ROSTypeRegistry()

        # Register custom ROS messages to the local typestore
        self._typestore_custom_msgtypes()

    def _typestore_custom_msgtypes(self):
        """
        Registers any custom ROS message definitions provided in ``cfg.custom_msgs``
        into ``self._registry``, then pulls every definition currently registered there
        (including ones registered elsewhere on a *shared* `cfg.registry` instance) into
        the local typestore. Safe to always run: `self._registry` is either private to
        this injector, or an instance the caller explicitly chose to share.
        """
        if self._cfg.custom_msgs:
            logger.info("Registering custom message definitions...")
            for package, path, store in self._cfg.custom_msgs:
                try:
                    self._registry.register_directory(
                        package_name=package, dir_path=path, store=store
                    )
                    logger.debug(f"Registered package '{package}' from '{path}'")
                except Exception as e:
                    logger.error(f"Failed to register custom msgs at '{path}': '{e}'")

        self._register_definitions()

    def _register_definitions(self):
        """Safe registration wrapper."""
        from rosbags.typesys import get_types_from_msg

        custom_types = self._registry.get_types(self._cfg.ros_distro)
        if not custom_types:
            return

        logger.info(
            f"Registering {list(custom_types.keys())} definitions to typestore..."
        )
        for msg_type, msg_def in custom_types.items():
            try:
                add_types = get_types_from_msg(msg_def, msg_type)
                self._typestore.register(add_types)
            except Exception as e:
                logger.warning(f"Failed to register type '{msg_type}': '{e}'")

    def _get_default_adapter(self, msg_type: str) -> Optional[Type[ROSAdapterBase]]:
        """
        Memoized lookup for Mosaico ROS Adapters.

        Args:
            msg_type (str): The ROS message type string (e.g., "sensor_msgs/msg/Image").

        Returns:
            Optional[Type[ROSAdapterBase]]:The adapter class if found, otherwise None.
        """

        return ROSBridge.get_default_adapter(msg_type)

    def _open_or_get_loader(self) -> ROSLoader:
        if self._loader is None:
            self._loader = ROSLoader(
                file_path=self._cfg.file_path,
                topics=self._cfg.topics,
                typestore_or_distro=self._typestore,
                serialization_formats=self._cfg.serialization_formats,
            )

        return self._loader

    def _dry_run_report(self):
        """
        Resolves the bag's topics against the current configuration and prints a report
        of what would be ingested, without connecting to the Mosaico server or writing data.

        Reports, per topic: acceptance status, resolved adapter (or rejection reason), and
        message count. Also flags any `topic_metadata` entry that doesn't match an accepted
        topic (e.g. because it was excluded by `topics` filtering or misspelled).
        """
        from rich.table import Table

        logger.info(f"[DRY RUN] Opening bag: '{self._cfg.file_path}'")

        with self._open_or_get_loader() as ros_loader:
            table = Table(
                title=f"Dry Run: '{self._cfg.file_path.name}' -> sequence '{self._cfg.sequence_name}'"
            )
            table.add_column("Topic")
            table.add_column("Status")
            table.add_column("Adapter / Reason")
            table.add_column("Messages", justify="right")

            for topic in ros_loader.topics:
                adapter = (self._cfg.adapter_overrides or {}).get(
                    topic
                ) or ros_loader.resolve_adapter(topic)
                table.add_row(
                    topic,
                    "[bright_green]Accepted",
                    adapter.__name__ if adapter else "?",
                    str(ros_loader.msg_count(topic)),
                )

            for topic, status in ros_loader.rejected_topics:
                table.add_row(
                    topic,
                    f"[{status.display_color()}]{status.value}",
                    "-",
                    "-",
                )

            self._console.print(table)

            accepted = set(ros_loader.topics)
            unused_topic_metadata = set(self._cfg.topic_metadata or {}) - accepted
            if unused_topic_metadata:
                logger.warning(
                    f"'topic_metadata' entries for topics that would NOT be ingested "
                    f"(filtered out or unresolved): {sorted(unused_topic_metadata)}"
                )

            self._console.print(
                f"[bold]{len(accepted)}[/bold] topic(s) would be ingested, "
                f"[bold]{len(ros_loader.rejected_topics)}[/bold] rejected. "
                "No connection to the Mosaico server was made."
            )

    def run(self):
        """
        Main execution entry point for the injection pipeline.

        This method establishes the necessary contexts (Network Client, File Loader, Server Writer)
        and executes the processing loop. It handles graceful shutdowns in case of
        user interrupts and provides a summary report upon completion.

        If `self.cfg.dry_run` is `True`, delegates to `_dry_run_report()` and returns
        without connecting to the server.

        Raises:
            Exception: Any fatal error encountered during connection, loading, or upload is
                logged and then re-raised, so callers can detect failure (e.g. `try`/`except`
                around `run()`, or a non-zero process exit code from the CLI entry point).
                `KeyboardInterrupt` is the only exception handled silently, to allow a clean
                shutdown on user interrupt.
        """
        if self._cfg.dry_run:
            self._dry_run_report()
            return

        logger.info(f"Connecting to Mosaico at '{self._cfg.host}:{self._cfg.port}'...")

        try:
            # Context: Mosaico Client (Network Connection)
            with MosaicoClient.connect(
                host=self._cfg.host,
                port=self._cfg.port,
                api_key=self._cfg.mosaico_api_key,
                enable_tls=self._cfg.enable_tls,
                tls_cert_path=self._cfg.tls_cert_path,
            ) as mclient:
                # Context: ROS Loader (File Access)
                logger.info(f"Opening bag: '{self._cfg.file_path}'")

                with self._open_or_get_loader() as ros_loader:
                    # Setup Progress UI
                    ui = ProgressManager(ros_loader)
                    ui.setup()
                    # Handle sequence creation or update based on existence and user preference
                    # NOTE: `update_if_exists` covers two scenarios: a ROS recording split across
                    # multiple bags that should all land in the same sequence, and a derived/
                    # reprocessed bag whose topics should be merged into an already-ingested
                    # sequence. Should the sequence not exist yet, a new one is created regardless.
                    if (
                        mclient.sequence_exists(self._cfg.sequence_name)
                        and self._cfg.update_if_exists
                    ):
                        logger.info(
                            f"Sequence '{self._cfg.sequence_name}' already exists. Updating instead of creating a new one."
                        )
                        # Context: Sequence Updadeter (Server Transaction)
                        seq_writer = mclient.sequence_update(
                            sequence_name=self._cfg.sequence_name,
                            on_error=self._cfg.on_error,
                        )
                    else:
                        # NOTE: this will raise an error if the sequence already
                        # exists and `update_sequence` is False
                        # Context: Sequence Writer (Server Transaction)
                        seq_writer = mclient.sequence_create(
                            sequence_name=self._cfg.sequence_name,
                            metadata=self._cfg.metadata,
                            on_error=self._cfg.on_error,
                        )

                    with seq_writer:
                        logger.info("Starting upload...")

                        # Main Processing Loop
                        # By passing self.console, any 'logger.info' calls inside
                        # this loop will print cleanly ABOVE the progress bars.
                        with Live(ui.progress, console=self._console):
                            for ros_msg, exc in ros_loader:
                                self._process_message(ros_msg, exc, seq_writer, ui)

                if seq_writer.session_status == SessionStatus.Error:
                    raise RuntimeError(
                        f"`SequenceWriter` returned a `SequenceStatus.Error` status for "
                        f"sequence '{self._cfg.sequence_name}'. Upload might have failed!"
                    )

                logger.info("Sequence upload completed successfully.")

                # Retrieve the sequence info
                seq_handler = mclient.sequence_handler(self._cfg.sequence_name)
                if seq_handler is None:
                    raise RuntimeError(
                        f"Oops, Something bad happened: Sequence '{self._cfg.sequence_name}' "
                        "not found on remote server. This should not happen..."
                    )

                # --- Final Statistics Report ---
                self._print_summary(
                    original_size=self._cfg.file_path.stat().st_size,
                    remote_size=seq_handler.total_size_bytes,
                )

        except KeyboardInterrupt:
            logger.warning("Operation cancelled by user. Shutting down...")
            return
        except Exception as e:
            logger.exception(f"Fatal error during ingestion: '{e}'")
            raise

    def _print_summary(self, original_size: int, remote_size: int):
        """
        Calculates and displays the ingestion performance summary.

        Outputs the original file size, the remote sequence size, the compression ratio,
        and the percentage of disk space saved.
        """
        if self._malformed_message_counts:
            from rich.table import Table

            table = Table(
                title="[bold yellow]Malformed Messages (Skipped)[/bold yellow]"
            )
            table.add_column("Topic")
            table.add_column("Skipped Messages", justify="right")
            for topic, count in sorted(
                self._malformed_message_counts.items(), key=lambda kv: -kv[1]
            ):
                table.add_row(topic, str(count))

            self._console.print(table)

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

        self._console.print(
            Panel(
                summary_text,
                title="[bold]Injection Summary[/bold]",
                expand=False,
                border_style="green",
                padding=1,
                highlight=True,
            )
        )

    def _get_topic_on_error(self, topic: str) -> TopicLevelErrorPolicy:
        if isinstance(self._cfg.topics_on_error, dict):
            return self._cfg.topics_on_error.get(topic, _DEFAULT_TOPIC_ON_ERROR)
        elif isinstance(self._cfg.topics_on_error, TopicLevelErrorPolicy):
            return self._cfg.topics_on_error

        return _DEFAULT_TOPIC_ON_ERROR

    def _process_message(
        self,
        ros_msg: ROSMessage,
        exc: Optional[Exception],
        session_writer: AnySessionWriter,
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

        Args:
            ros_msg (ROSMessage): The ROS message to process.
            exc (Optional[Exception]): Any exception raised during deserialization.
            session_writer (AnySessionWriter): The active session writer for the sequence.
            ui (ProgressManager): The progress manager for updating the UI.
        """

        if self._loader is None:
            raise RuntimeError(
                "Impossible to process messages if ROSLoader is not instantiated first"
            )

        # --- Filter Check ---
        if ros_msg.topic in self._ignored_topics:
            ui.advance_global()
            return

        # --- Integrity Check ---
        # If the loader yielded an exception or empty data, mark as error
        if exc or not ros_msg.data_field:
            logger.warning(
                f"Skipping message on topic '{ros_msg.topic}' due to error: '{exc}'"
            )
            ui.update_status(
                ros_msg.topic, "Message-related Error. Check the logs.", "red"
            )
            ui.advance_global()
            # Update the malformed message count for this topic
            self._malformed_message_counts[ros_msg.topic] = (
                self._malformed_message_counts.get(ros_msg.topic, 0) + 1
            )
            return

        # --- Adapter Resolution ---
        adapter = (self._cfg.adapter_overrides or {}).get(
            ros_msg.topic
        ) or self._loader.resolve_adapter(ros_msg.topic)

        if adapter is None:
            # This should never happen, but we handle it gracefully
            # Blacklist this topic to prevent future lookups
            self._ignored_topics.add(ros_msg.topic)
            ui.update_status(ros_msg.topic, "Unable to adapt.", "red")
            ui.advance_global()
            return

        # Retrieve the writer from SequenceWriter local cache or create new one on server
        twriter = session_writer.get_topic_writer(ros_msg.topic)

        # Should theoretically not be None if exists returned True
        if twriter is None:
            # --- Schema metadata Resolution ---
            ros_version = 1 if self._cfg.ros_distro is Stores.ROS1_NOETIC else 2
            ros_meta = RosSchemaMetadata.from_dict(
                adapter.schema_metadata(
                    self._loader._typestore, ros_msg.msg_type, ros_version
                )
            )
            # Record which bag file introduced this topic, inside the reserved `_ros_`
            # namespace. This lets the source of each topic remain traceable even after
            # later updates to the same sequence (e.g. multi-part recordings or merged
            # reprocessing results), since sequence metadata cannot be changed once the
            # sequence has been ingested.
            ros_meta.update(source_file=self._cfg.file_path.name)

            # Start from the user-supplied per-topic metadata, then layer the bridge-computed
            # `_ros_` block on top: `_ros_` is reserved and always wins on conflict, every
            # other key is fully user-owned.
            metadata = dict((self._cfg.topic_metadata or {}).get(ros_msg.topic, {}))
            metadata.update(ros_meta.to_dict())

            # Register new topic on server
            twriter = session_writer.topic_create(
                topic_name=ros_msg.topic,
                metadata=metadata,
                ontology_type=adapter.ontology_data_type(),
                on_error=self._get_topic_on_error(ros_msg.topic),
            )
            if twriter is None:
                ui.update_status(ros_msg.topic, "Write Error", "red")
                # We assume transient error and continue; strict policies are handled by Client
                ui.advance_all(ros_msg.topic)
                return

        # --- Adapt & Push ---
        if (
            twriter.is_active
        ):  # Avoid computations if prematurely closed (TopicLevelErrorPolicy.Finalize)
            with twriter:
                # Convert ROS dict -> Mosaico Object -> Arrow Batch
                twriter.push(adapter.translate(ros_msg))
            if twriter.status == TopicWriterStatus.IgnoredLastError:
                # If writing fails (e.g. network error, validation error), update UI
                ui.update_status(ros_msg.topic, "Write Error (Ignored)", "yellow")
            elif twriter.status == TopicWriterStatus.FinalizedWithError:
                ui.update_status(
                    ros_msg.topic, "Fatal Error: Prematurely finalized", "red"
                )

        ui.advance_all(ros_msg.topic)


# --- CLI Entry Point ---


def _parse_json_arg(arg_input: Optional[str], arg_name: str = "Metadata") -> dict:
    """
    Parses a CLI argument that may be a raw JSON string or a path to a JSON file.

    Supports two formats:
    1. A raw JSON string: '{"driver": "John"}'
    2. A path to a JSON file: './configs/meta.json'

    Args:
        arg_input (Optional[str]): The raw CLI argument value.
        arg_name (str): Human-readable name of the argument, used in log/error messages.

    Returns:
        dict: The parsed JSON object, or empty dict if `arg_input` is falsy.
    """
    if not arg_input:
        return {}

    # Attempt JSON Parse
    try:
        data = json.loads(arg_input)
        logger.info(f"{arg_name} parsed successfully from JSON string.")
        return data
    except json.JSONDecodeError:
        pass  # Not a valid JSON string, proceed to check file

    # Attempt File Read
    file_path = Path(arg_input)
    if file_path.is_file():
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"{arg_name} loaded successfully from file: '{file_path}'")
            return data
        except json.JSONDecodeError as e:
            logger.error(
                f"File found at '{file_path}' but contained invalid JSON: '{e}'"
            )
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error reading {arg_name.lower()} file '{file_path}': '{e}'")
            sys.exit(1)

    # Failure
    logger.error(
        f"{arg_name} argument is neither a valid JSON string nor a valid file path: '{arg_input}'"
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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Resolve topics/adapters/rejections and print a report, without connecting "
            "to the Mosaico server or writing any data."
        ),
    )
    parser.add_argument(
        "--update-if-exists",
        action="store_true",
        help=(
            "If a sequence named --name already exists, append this bag's topics to it "
            "instead of raising an error (e.g. for multi-part bags or merging reprocessed "
            "results into an already-ingested sequence)."
        ),
    )

    # Connection Arguments
    parser.add_argument("--host", default="localhost", help="Mosaico Server Host")
    parser.add_argument(
        "--port", type=int, default=6726, help="Mosaico Server Port (Default: 6726)"
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

    # Metadata Arguments
    parser.add_argument(
        "--metadata",
        help="JSON string or path to JSON file containing sequence metadata",
    )
    parser.add_argument(
        "--topic-metadata",
        help=(
            "JSON string or path to JSON file containing a mapping of exact topic name to "
            'metadata, e.g. \'{"/imu": {"unit": "rad/s"}}\'. Only applied to topics that are '
            "actually ingested (see --topics)."
        ),
    )

    # Advanced Arguments
    parser.add_argument(
        "--ros-distro",
        default=None,
        choices=[s.name.lower() for s in Stores],
        help="Target ROS Distribution for message parsing (e.g., ros2_humble). "
        "If not set, defaults to an empty/auto-detected typestore.",
    )

    # Advanced Arguments
    parser.add_argument(
        "--api-key",
        default=None,
        help=(
            "Mosaico API-Key. Prefer setting the MOSAICO_API_KEY environment variable "
            "instead, to avoid leaking the key via shell history or the process list "
            "(e.g. `ps aux`); --api-key takes precedence if both are set."
        ),
    )

    # Advanced Arguments
    parser.add_argument(
        "--tls-cert",
        default=None,
        help="Path of the .cert file for secure connection",
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
    user_metadata = _parse_json_arg(args.metadata, arg_name="Metadata")
    # Inject traceability metadata
    user_metadata.update({"rosbag_injection": args.bag_path.name})
    user_topic_metadata = _parse_json_arg(
        args.topic_metadata, arg_name="Topic metadata"
    )

    config = ROSInjectionConfig(
        file_path=args.bag_path,
        sequence_name=args.name,
        metadata=user_metadata,
        topic_metadata=user_topic_metadata or None,
        update_if_exists=args.update_if_exists,
        dry_run=args.dry_run,
        host=args.host,
        port=args.port,
        topics=args.topics,
        ros_distro=selected_distro,
        log_level=args.log,
        tls_cert_path=args.tls_cert,
        mosaico_api_key=args.api_key or os.environ.get("MOSAICO_API_KEY"),
    )

    # --- Execution ---
    injector = RosbagInjector(config)
    try:
        injector.run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        # Already logged with a full traceback inside run(); exit non-zero so
        # calling scripts/CI can detect the failure.
        sys.exit(1)


if __name__ == "__main__":
    ros_injector()
