"""
FIXME
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
    $ mosaicolabs.lerobot_injector ./data.mcap --name "Test_Run_01"

Typical usage as a library:
    config = ROSInjectionConfig(file_path=Path("data.mcap"), ...)
    injector = RosbagInjector(config)
    injector.run()
"""

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from rich.live import Live

from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.enum import (
    SessionLevelErrorPolicy,
    TopicLevelErrorPolicy,
    TopicWriterStatus,
)
from mosaicolabs.enum.session_status import SessionStatus
from mosaicolabs.handlers.base_session_writer import AnySessionWriter
from mosaicolabs.logging_config import get_logger, setup_sdk_logging

from ..base_injector import (
    _DEFAULT_TOPIC_ON_ERROR,
    InjectionConfigBase,
    InjectionStatus,
)
from ..helpers import _parse_json_arg
from .lerobot_message import LeRobotMessage
from .loader import LeRobotEpisodeLoader, LeRobotLoader
from .metadata import LeRobotSchemaMetadata
from .ui_lerobot import LeRobotProgressManager

# Set the hierarchical logger
logger = get_logger(__name__)


# --- Configuration ---
@dataclass
class LeRobotInjectionConfig(InjectionConfigBase):
    """
    FIXME
    The central configuration object for the ROS Bag injection process.

    This data class serves as the single source of truth for all injection settings,
    decoupling the orchestration logic from CLI arguments or configuration files.
    It extends `InjectionConfigBase`, which holds the settings shared by all bridge
    injectors (sequence, server connection, error policies, metadata, logging, dry-run),
    with the ROS-specific options: `ros_distro`, custom message definitions, `topics`
    filtering and `adapter_overrides`.

    Attributes:
        file_path (Path): Absolute or relative path to the input ROS bag file (.mcap, .db3, or .bag).
        sequence_name (str): The name of the sequence to create on the Mosaico server
            (or to update, see `update_if_exists`).
        metadata (dict): User-defined metadata to attach to the sequence (e.g., driver, weather, location).
            Ignored when an existing sequence is updated.
        topic_metadata (Optional[Dict[str, dict]]): Mapping of exact topic name to metadata to
            attach to that topic, alongside the `_lerobot_` metadata computed from the message schema and
            the source bag file. Default: None
        update_if_exists (bool): If `True`, append this bag's topics to an existing sequence with
            the same name instead of raising an error. Default: False.
        host (str): Hostname or IP of the Mosaico server. Defaults to "localhost".
        port (int): Port of the Mosaico server. Defaults to 6726.
        on_error (SessionLevelErrorPolicy): Behavior when an episode fails to be uploaded (Delete the partial sequence or Report the error).
            Default: [`SessionLevelErrorPolicy.Delete`][mosaicolabs.enum.SessionLevelErrorPolicy.Delete]
        topics_on_error (Union[TopicLevelErrorPolicy, Dict[str, TopicLevelErrorPolicy]]): Behavior when a topic write fails.
            Default: [`TopicLevelErrorPolicy.Raise`][mosaicolabs.enum.TopicLevelErrorPolicy.Raise]
            Set to a [`TopicLevelErrorPolicy`][mosaicolabs.enum.TopicLevelErrorPolicy] to apply the same policy to all topics.
            Set to a `Dict[str, TopicLevelErrorPolicy]` to apply different policies to different (subset of) topics.
        serialization_formats (Optional[Dict[str, SerializationFormat]]): Mapping of ROS message type strings
            (e.g. "sensor_msgs/msg/PointCloud2") to the `SerializationFormat` used when synthesizing an
            `Unmodeled` ontology for topics that have no registered Mosaico adapter. Message types not
            present in this mapping default to `SerializationFormat.Default`.
            Default: None
        log_level (str): Logging verbosity level ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL").
            Default: "INFO"
        mosaico_api_key (Optional[str]): The API key for authentication on the mosaico server.
            If provided it must have the `write` permission.
            Default: None
        tls_cert_path (Optional[str]): Path to the TLS certificate file for secure connection on the mosaico server.
            Default: None
        enable_tls (bool): Enable the TLS communication protocol. Defaults to False.
        dry_run (bool): If `True`, resolves and reports which topics would be ingested without
            connecting to the Mosaico server or writing any data. Default: False.
        ros_distro (Optional[Stores]): The target ROS distribution for message parsing (e.g., Stores.ROS2_HUMBLE).
            See [`rosbags.typesys.Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores).
        custom_msgs (Optional[List[Tuple]]): List of custom .msg definitions to register before loading.
        registry (Optional[ROSTypeRegistry]): Registry to register `custom_msgs` into; a private
            one is created if `None`. Pass a shared instance to reuse definitions across runs.
        topics (Optional[List[str]]): List of topic patterns used to filter available topics.
            Supports shell-style glob patterns (e.g., ["/cam/\\*", "\\*camera_info"]).
            Patterns starting with "!" are treated as exclusions (e.g., ["\\!/cam/debug\\*"]).
            Patterns are evaluated in ORDER (gitignore-like semantics). If None, all available topics are loaded.
        adapter_overrides (Optional[Dict[str, Type[ROSAdapterBase]]]): Mapping of topics to adapter overrides,
            allowing the use of specific adapters instead of the default for designated topics.
            Default: None

    Example:
        ```python
        from pathlib import Path
        from rosbags.typesys import Stores
        from mosaicolabs.enum import SessionLevelErrorPolicy, TopicLevelErrorPolicy
        from mosaicolabs.bridges.ros import ROSInjectionConfig

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

    on_error: SessionLevelErrorPolicy = SessionLevelErrorPolicy.Delete
    """
    The `SequenceWriter` `on_error` behavior when an episode fails to be uploaded (Report vs
    Delete). Defaults to `Delete`: an episode that is not fully uploaded is not kept.
    """


# --- Main Injector Class ---


class LeRobotInjector:
    """
    FIXME
    Main controller for the ROS Bag ingestion workflow.

    The `LeRobotInjector` orchestrates the entire data pipeline from the physical storage
    to the remote Mosaico server. It manages the initialization of the registry,
    establishes network connections, and drives the main adaptation loop.

    **Core Workflow Architecture:**

    1.  **Registry Initialization**: Pre-loads custom message definitions via the `ROSTypeRegistry`.
    2.  **Resource Management**: Opens the `ROSLoader` for file access and the `MosaicoClient` for networking.
    3.  **Stream Negotiation**: Creates a `SequenceWriter` on the server and opens individual `TopicWriter` streams.
    4.  **Adaptation Loop**: Iterates through ROS records, translates them via the `ROSBridge`, and pushes them to the server.

    Example:
        ```python
        from mosaicolabs.bridges.ros import LeRobotInjector, ROSInjectionConfig

        # Define configuration
        config = ROSInjectionConfig(file_path=Path("data.db3"), sequence_name="auto_ingest")

        # Initialize and run
        injector = RosbagInjector(config)
        injector.run() # This handles the full lifecycle including cleanup on failure
        ```

    Attributes:
        cfg (ROSInjectionConfig): The active configuration settings.
        console (Console): The rich console instance for logging and UI output.
    """

    def __init__(self, config: LeRobotInjectionConfig):
        """
        Args:
            config (LeRobotInjectionConfig): The fully resolved configuration object.
        """
        self._cfg = config
        # Create the single "source of truth" for the terminal
        from rich.console import Console

        self._console = Console(stderr=True)
        setup_sdk_logging(
            level=self._cfg.log_level.upper(), pretty=True, console=self._console
        )

        self._loader: Optional[LeRobotLoader] = None
        """TODO"""

    def _open_or_get_loader(self, max_batch_size_bytes: int) -> LeRobotLoader:
        if self._loader is None:
            self._loader = LeRobotLoader(
                file_path=self._cfg.file_path,
                max_batch_size_bytes=max_batch_size_bytes,
                # columns=self._cfg.to # TODO: fix this
            )

        return self._loader

    def run(self) -> InjectionStatus:
        """
        FIXME
        Main execution entry point for the injection pipeline.

        This method establishes the necessary contexts (Network Client, File Loader, Server Writer)
        and executes the processing loop. It handles graceful shutdowns in case of
        user interrupts and provides a summary report upon completion.

        If `self.cfg.dry_run` is `True`, delegates to `_dry_run_report()` and returns
        without connecting to the server.

        Returns:
            InjectionStatus: `COMPLETED` if the file was injected, `CANCELLED` if the user
                interrupted the injection (`KeyboardInterrupt`), `DRY_RUN` in dry-run mode.

        Raises:
            Exception: Any fatal error encountered during connection, loading, or upload is
                logged and then re-raised, so callers can detect failure (e.g. `try`/`except`
                around `run()`, or a non-zero process exit code from the CLI entry point).
        """
        if self._cfg.dry_run:
            # self._dry_run_report()
            logger.error("dry_run() is not yet available")
            return InjectionStatus.DRY_RUN

        logger.info(f"Connecting to Mosaico at '{self._cfg.host}:{self._cfg.port}'...")

        failed_sequences: List[str] = []

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
                logger.info(f"Opening LeRobotdataset: '{self._cfg.file_path}'")

                lerobot_loader = self._open_or_get_loader(mclient.max_batch_size_bytes)

                # Setup Progress UI: a single UI for every episode of the dataset
                ui = LeRobotProgressManager(lerobot_loader)
                ui.setup()

                # Main Processing Loop
                # By passing self.console, any 'logger.info' calls inside
                # this loop will print cleanly ABOVE the progress bars.
                with Live(ui.progress, console=self._console):
                    # Each episode is ingested as its own sequence
                    for episode_loader in lerobot_loader:
                        sequence_name = f"{self._cfg.sequence_name}_{episode_loader.episode_index}"  # TODO: change this so that sequence name is deduced from episode_loader
                        ui.start_episode(episode_loader.episode_index)

                        try:
                            self._upload_episode(
                                mclient, episode_loader, sequence_name, ui
                            )
                        except Exception as e:
                            # A failed episode does not stop the ingestion: its sequence, if
                            # created, has applied its `on_error` policy
                            logger.error(
                                f"Episode {episode_loader.episode_index} (sequence "
                                f"'{sequence_name}') failed and was skipped "
                                f"('{self._cfg.on_error.value}' error policy): '{e}'"
                            )
                            failed_sequences.append(sequence_name)
                            ui.finish_episode(succeeded=False)
                        else:
                            logger.info(
                                f"Sequence '{sequence_name}' upload completed successfully."
                            )
                            ui.finish_episode(succeeded=True)

        except KeyboardInterrupt:
            logger.warning("Operation cancelled by user. Shutting down...")
            return InjectionStatus.CANCELLED
        except Exception as e:
            logger.exception(f"Fatal error during ingestion: '{e}'")
            raise

        # Raised once every episode is processed, outside of the fatal errors handling above:
        # each failed episode has already been logged with its error
        if failed_sequences:
            summary = (
                f"{len(failed_sequences)} episode(s) failed to be ingested (see the "
                f"errors above): {failed_sequences}"
            )
            logger.error(summary)
            raise RuntimeError(summary)

        return InjectionStatus.COMPLETED

    def _get_topic_on_error(self, topic: str) -> TopicLevelErrorPolicy:
        if isinstance(self._cfg.topics_on_error, dict):
            return self._cfg.topics_on_error.get(topic, _DEFAULT_TOPIC_ON_ERROR)
        elif isinstance(self._cfg.topics_on_error, TopicLevelErrorPolicy):
            return self._cfg.topics_on_error

        return _DEFAULT_TOPIC_ON_ERROR

    def _upload_episode(
        self,
        mclient: MosaicoClient,
        episode_loader: LeRobotEpisodeLoader,
        sequence_name: str,
        ui: LeRobotProgressManager,
    ):
        """
        Uploads an episode as its own sequence.

        Any error while uploading the episode is raised within the sequence, so that the sequence
        applies its `on_error` policy (by default `Delete`: a partially uploaded episode is not
        kept), and then propagated to the caller.

        Args:
            mclient (MosaicoClient): The connected Mosaico client.
            episode_loader (LeRobotEpisodeLoader): The episode to upload.
            sequence_name (str): The name of the sequence to create.
            ui (LeRobotProgressManager): The progress manager for updating the UI.

        Raises:
            Exception: If the episode could not be fully uploaded.
        """
        seq_writer = mclient.sequence_create(
            sequence_name=sequence_name,
            metadata=self._cfg.metadata,
            on_error=self._cfg.on_error,
        )

        with seq_writer:
            logger.info(f"Starting upload of sequence '{sequence_name}'...")

            for msg, ex in episode_loader:
                if ex is not None:
                    # The loader could not read the rest of the topic
                    raise RuntimeError(
                        f"Could not read Mosaico topic '{msg.msco_topic_name}': {ex}"
                    ) from ex

                self._upload_batch(msg, seq_writer)
                ui.advance_all_topics(msg.column_names, msg.n_rows)

        if seq_writer.session_status == SessionStatus.Error:
            raise RuntimeError(
                f"`SequenceWriter` returned a `SequenceStatus.Error` status for "
                f"sequence '{sequence_name}'. Upload might have failed!"
            )

    def _upload_batch(
        self,
        lerobot_msg: LeRobotMessage,
        session_writer: AnySessionWriter,
    ):
        """
        Uploads a batch of an episode to its topic, creating the topic on its first batch, and
        advances the bars of the columns of the batch.

        Args:
            lerobot_msg (LeRobotMessage): The batch to upload.
            session_writer (AnySessionWriter): The active session writer for the sequence.

        Raises:
            RuntimeError: If the topic cannot be created, or the batch is not written (e.g. a
                write error ignored or finalized by the topic error policy): a partially uploaded
                episode is a failed episode. The write errors raised by the topic error policy
                are propagated as they are.
        """
        # Retrieve the writer from SequenceWriter local cache or create new one on server
        twriter = session_writer.get_topic_writer(lerobot_msg.msco_topic_name)

        # Should theoretically not be None if exists returned True
        if twriter is None:
            # --- Schema metadata Resolution ---
            lerobot_meta = LeRobotSchemaMetadata.from_dict({})

            # Record which bag file introduced this topic, inside the reserved `_lerobot_`
            # namespace. This lets the source of each topic remain traceable even after
            # later updates to the same sequence (e.g. multi-part recordings or merged
            # reprocessing results), since sequence metadata cannot be changed once the
            # sequence has been ingested.
            lerobot_meta.update(source_file=self._cfg.file_path.name)

            # Start from the user-supplied per-topic metadata, then layer the bridge-computed
            # `_lerobot_` block on top: `_lerobot_` is reserved and always wins on conflict, every
            # other key is fully user-owned.
            metadata = dict(
                (self._cfg.topic_metadata or {}).get(lerobot_msg.msco_topic_name, {})
            )
            metadata.update(lerobot_meta.to_dict())

            # Register new topic on server
            twriter = session_writer.topic_create(
                topic_name=lerobot_msg.msco_topic_name,
                metadata=metadata,
                ontology_type=lerobot_msg.msco_ontology,
                on_error=self._get_topic_on_error(lerobot_msg.msco_topic_name),
            )
            if twriter is None:
                raise RuntimeError(
                    f"Could not create Mosaico topic '{lerobot_msg.msco_topic_name}'."
                )

        # --- Push ---
        if (
            twriter.is_active
        ):  # Avoid computations if prematurely closed (TopicLevelErrorPolicy.Finalize)
            with twriter:
                twriter.push_batch(lerobot_msg.timestamp_ns, lerobot_msg.data)

        # A batch not written (e.g. a write error ignored or finalized by the topic error
        # policy, or a record too large) leaves the episode partially uploaded
        if twriter.status != TopicWriterStatus.Active:
            raise RuntimeError(
                f"Could not write a batch to Mosaico topic '{lerobot_msg.msco_topic_name}' "
                f"('{twriter.status.value}'): {twriter.last_error}"
            )


# --- CLI Entry Point ---


def lerobot_injector():
    """
    Console script entry point.
    Parses arguments, sets up configuration, and initiates the injector.
    """
    parser = argparse.ArgumentParser(description="Inject LeRobot data into Mosaico.")

    # Required Arguments
    parser.add_argument(
        "dataset_path", type=Path, help="Path to Hugging Face Project Folder"
    )
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
        default="WARNING",  # Optional: defaults to INFO
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

    # Parse metadata
    user_metadata = _parse_json_arg(args.metadata, arg_name="Metadata")
    # Inject traceability metadata
    user_metadata.update({"lerobot_dataset": args.dataset_path.name})
    user_topic_metadata = _parse_json_arg(
        args.topic_metadata, arg_name="Topic metadata"
    )

    config = LeRobotInjectionConfig(
        file_path=args.dataset_path,
        sequence_name=args.name,
        metadata=user_metadata,
        topic_metadata=user_topic_metadata or None,
        update_if_exists=args.update_if_exists,
        dry_run=args.dry_run,
        host=args.host,
        port=args.port,
        log_level=args.log,
        tls_cert_path=args.tls_cert,
        mosaico_api_key=args.api_key or os.environ.get("MOSAICO_API_KEY"),
    )

    # --- Execution ---
    injector = LeRobotInjector(config)
    try:
        status = injector.run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        # Already logged with a full traceback inside run(); exit non-zero so
        # calling scripts/CI can detect the failure.
        sys.exit(1)

    # Interrupted by the user while uploading: same exit code as an interrupt outside `run()`
    if status is InjectionStatus.CANCELLED:
        sys.exit(130)


if __name__ == "__main__":
    lerobot_injector()
