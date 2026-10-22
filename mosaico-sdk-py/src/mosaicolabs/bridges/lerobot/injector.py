"""
LeRobot Dataset Injection Tool.

This module provides a command-line interface (CLI) and a Python API for injecting a LeRobot
v3.0 dataset folder into the Mosaico data platform, each episode as its own sequence, named
`<name>_episode_<episode index>` (e.g. `pusht_episode_000042`).

It handles the orchestration of:
1.  **Loading:** reading the `meta/` and `data/` folders with `pyarrow` (without the `lerobot`
    package) through `LeRobotLoader`, one episode at a time.
2.  **Topics:** pushing the timeseries columns together to the `/timeseries` topic, with an
    `Unmodeled` ontology, and each image column to its own `CompressedImage` topic.
3.  **Transmission:** streaming the rows as Arrow record batches with `TopicWriter.push_batch`,
    each as large as possible within `MosaicoClient.max_batch_size_bytes`.
4.  **Failures:** an episode that fails to be uploaded applies the `on_error` policy to its
    sequence (by default `Delete`), without stopping the ingestion of the next episodes.

Typical usage as a script:
    $ mosaicolabs.lerobot_injector ./datasets/pusht --name pusht

Typical usage as a library:
    config = LeRobotInjectionConfig(file_path=Path("datasets/pusht"), sequence_name="pusht")
    injector = LeRobotInjector(config)
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
from ..topic_status import to_color
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
    The configuration of the LeRobot dataset injection.

    It extends `InjectionConfigBase`, which holds the settings shared by all bridge injectors,
    with the LeRobot defaults: `sequence_name` is the prefix of the sequence names (one sequence
    per episode), and a failed episode deletes its sequence by default (`on_error`).

    Attributes:
        file_path (Path): Path to the root folder of the LeRobot v3.0 dataset.
        sequence_name (str): The prefix of the sequence names: each episode is ingested as the
            sequence `<sequence_name>_episode_<episode index>`, the index being zero-padded to
            6 digits (e.g. `pusht_episode_000042`).
            If empty, the name of the dataset folder is used. Default: ""
        column_names (Optional[List[str]]): List of column patterns used to filter the dataset
            columns (the image and timeseries features of `meta/info.json`).
            Supports shell-style glob patterns (e.g., ["observation.images.\\*"]).
            Patterns starting with "!" are treated as exclusions (e.g., ["\\!observation.images.\\*"]).
            Patterns are evaluated in ORDER (gitignore-like semantics). If None, all columns are loaded.
        metadata (dict): User-defined metadata to attach to every sequence (e.g., operator, location).
        topic_metadata (Optional[Dict[str, dict]]): Mapping of exact Mosaico topic name (e.g.
            `/timeseries`, `/observation/images/front`) to metadata to attach to that topic,
            alongside the `_lerobot_` metadata computed by the bridge. Default: None
        update_if_exists (bool): Not used by the LeRobot bridge: ignored. Default: False.
        host (str): Hostname or IP of the Mosaico server. Defaults to "localhost".
        port (int): Port of the Mosaico server. Defaults to 6726.
        on_error (SessionLevelErrorPolicy): Behavior when an episode fails to be uploaded (Delete
            its partial sequence or Report the error). The next episodes are uploaded in any case.
            Default: [`SessionLevelErrorPolicy.Delete`][mosaicolabs.enum.SessionLevelErrorPolicy.Delete]
        topics_on_error (Union[TopicLevelErrorPolicy, Dict[str, TopicLevelErrorPolicy]]): Behavior
            of the topic writers when a write fails. Whatever the policy, a batch that is not
            written fails its episode.
            Default: [`TopicLevelErrorPolicy.Raise`][mosaicolabs.enum.TopicLevelErrorPolicy.Raise]
            Set to a [`TopicLevelErrorPolicy`][mosaicolabs.enum.TopicLevelErrorPolicy] to apply the same policy to all topics.
            Set to a `Dict[str, TopicLevelErrorPolicy]` to apply different policies to different (subset of) topics.
        serialization_formats (Optional[Dict[str, SerializationFormat]]): Not used by the LeRobot
            bridge: ignored. Default: None
        log_level (str): Logging verbosity level ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL").
            Default: "INFO"
        mosaico_api_key (Optional[str]): The API key for authentication on the mosaico server.
            If provided it must have the `write` permission.
            Default: None
        tls_cert_path (Optional[str]): Path to the TLS certificate file for secure connection on the mosaico server.
            Default: None
        enable_tls (bool): Enable the TLS communication protocol. Defaults to False.
        dry_run (bool): If `True`, resolves and reports which columns, topics and sequences would
            be ingested without connecting to the Mosaico server or writing any data. Default: False.

    Example:
        ```python
        from pathlib import Path
        from mosaicolabs.enum import SessionLevelErrorPolicy
        from mosaicolabs.bridges.lerobot.injector import LeRobotInjectionConfig

        config = LeRobotInjectionConfig(
            file_path=Path("datasets/pusht"),
            sequence_name="pusht",
            metadata={"operator": "John"},
            on_error=SessionLevelErrorPolicy.Report,
        )
        ```
    """

    sequence_name: str = ""
    """
    The prefix of the sequence names: each episode is ingested as the sequence
    `<sequence_name>_episode_<episode index>`. If empty, the name of the dataset folder is used.
    """

    column_names: Optional[List[str]] = None
    """List of column patterns used to filter the dataset columns: the image and timeseries
    features of `meta/info.json` (e.g. "observation.state", "observation.images.front").

    Supports shell-style glob patterns (e.g., "observation.images.*").
    Patterns starting with '!' are treated as exclusions (e.g., "!observation.images.*").

    **Pattern order matters**:
        - Each non-'!' pattern adds matching columns to the selection.
        - Each '!' pattern removes matching columns from the selection.
        - Later patterns override earlier ones.
        - If no inclusion pattern is provided, selection starts from ALL columns,
          and only exclusion patterns reduce the set.

    If None, all columns are loaded.
    """

    on_error: SessionLevelErrorPolicy = SessionLevelErrorPolicy.Delete
    """
    The `SequenceWriter` `on_error` behavior when an episode fails to be uploaded (Report vs
    Delete). Defaults to `Delete`: an episode that is not fully uploaded is not kept.
    """


# --- Main Injector Class ---


class LeRobotInjector:
    """
    Main controller of the LeRobot dataset ingestion workflow.

    For each episode of the dataset (see `LeRobotLoader`), it creates a sequence, creates the
    `/timeseries` topic and one topic per camera on their first batch, and pushes the batches of
    the episode. A single `LeRobotProgressManager` shows the progress of the whole dataset.

    An episode that fails to be uploaded, for any reason, exits its sequence with an error, so
    that the sequence applies the `on_error` policy (by default `Delete`); the failure is logged
    and counted in the UI, and the ingestion continues with the next episode. A summary of the
    failed episodes is logged at the end. A `KeyboardInterrupt` stops the whole ingestion.

    Example:
        ```python
        from pathlib import Path
        from mosaicolabs.bridges.lerobot.injector import (
            LeRobotInjectionConfig,
            LeRobotInjector,
        )

        config = LeRobotInjectionConfig(file_path=Path("datasets/pusht"), sequence_name="pusht")
        injector = LeRobotInjector(config)
        injector.run()
        ```

    Attributes:
        _cfg (LeRobotInjectionConfig): The active configuration settings.
        _console (Console): The rich console instance for logging and UI output.
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
        """The loader of the dataset, lazily created by `_open_or_get_loader`."""

    def _open_or_get_loader(self, max_batch_size_bytes: int) -> LeRobotLoader:
        """Returns the loader of the dataset, created on the first call."""
        if self._loader is None:
            self._loader = LeRobotLoader(
                file_path=self._cfg.file_path,
                max_batch_size_bytes=max_batch_size_bytes,
                columns=self._cfg.column_names,
            )

        return self._loader

    def _dry_run_report(self):
        """
        Resolves the dataset columns against the current configuration and prints a report of
        what would be ingested, without connecting to the Mosaico server or writing data.

        Reports, per column: acceptance status (or rejection reason), the Mosaico topic it would
        be pushed to, and its message count over every episode. Also reports the sequences the
        episodes would be ingested as.
        """
        from rich.table import Table

        logger.info(f"[DRY RUN] Opening LeRobot dataset: '{self._cfg.file_path}'")

        # No batch is read in a dry run: the maximum batch size, given by the server, is unused
        lerobot_loader = self._open_or_get_loader(max_batch_size_bytes=0)

        sequence_names = [
            episode_loader.msco_seq_name(self._cfg.sequence_name or None)
            for episode_loader in lerobot_loader
        ]

        table = Table(
            title=(
                f"Dry Run: '{self._cfg.file_path.resolve().name}' -> "
                f"{len(sequence_names)} sequence(s)"
            )
        )
        table.add_column("Columns")
        table.add_column("Status")
        table.add_column("Mosaico topic")
        table.add_column("Messages", justify="right")

        for column in lerobot_loader.topics:
            table.add_row(
                column,
                "[bright_green]Accepted",
                lerobot_loader.msco_topic_name(column) or "?",
                str(lerobot_loader.msg_count(column)),
            )

        for column, status in lerobot_loader.rejected_topics:
            table.add_row(
                column,
                f"[{to_color(status)}]{status.value}",
                "-",
                "-",
            )

        self._console.print(table)

        if sequence_names:
            self._console.print(
                f"Episodes would be ingested as the sequences '{sequence_names[0]}' "
                f"to '{sequence_names[-1]}'."
            )

        msco_topics = {
            lerobot_loader.msco_topic_name(column) for column in lerobot_loader.topics
        }

        self._console.print(
            f"[bold]{len(lerobot_loader.topics)}[/bold] column(s) would be ingested in "
            f"[bold]{len(msco_topics)}[/bold] topic(s) per sequence, "
            f"[bold]{len(lerobot_loader.rejected_topics)}[/bold] rejected. "
            "No connection to the Mosaico server was made."
        )

    def _get_topic_on_error(self, topic: str) -> TopicLevelErrorPolicy:
        """Returns the `TopicWriter` error policy of `topic` (see `topics_on_error`)."""
        if isinstance(self._cfg.topics_on_error, dict):
            return self._cfg.topics_on_error.get(topic, _DEFAULT_TOPIC_ON_ERROR)
        elif isinstance(self._cfg.topics_on_error, TopicLevelErrorPolicy):
            return self._cfg.topics_on_error

        return _DEFAULT_TOPIC_ON_ERROR

    def _upload_episode(
        self,
        mclient: MosaicoClient,
        episode_loader: LeRobotEpisodeLoader,
        ui: LeRobotProgressManager,
    ) -> bool:
        """
        Uploads an episode as its own sequence.

        Any error while uploading the episode is raised within the sequence, so that the sequence
        applies its `on_error` policy (by default `Delete`: a partially uploaded episode is not
        kept), and then logged: the episode is reported as failed, without stopping the ingestion.

        Args:
            mclient (MosaicoClient): The connected Mosaico client.
            episode_loader (LeRobotEpisodeLoader): The episode to upload.
            ui (LeRobotProgressManager): The progress manager for updating the UI.

        Returns:
            bool: Whether the episode was fully uploaded.
        """
        sequence_name = episode_loader.msco_seq_name(self._cfg.sequence_name or None)

        seq_writer = mclient.sequence_create(
            sequence_name=sequence_name,
            metadata=self._cfg.metadata,
            on_error=self._cfg.on_error,
        )

        successful_ingestion = True
        ui.start_episode(episode_loader.episode_index, episode_loader.n_rows)

        try:
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

        except Exception as e:
            # A failed episode does not stop the ingestion: its sequence, if
            # created, has applied its `on_error` policy
            logger.error(
                f"Episode {episode_loader.episode_index} (sequence "
                f"'{sequence_name}') failed and was skipped "
                f"('{self._cfg.on_error.value}' error policy): '{e}'"
            )

            successful_ingestion = False

        if successful_ingestion:
            logger.info(f"Sequence '{sequence_name}' upload completed.")

        ui.finish_episode(succeeded=successful_ingestion)
        return successful_ingestion

    def _report_failed_episodes(self, failed_episodes: List[int]):
        """
        Logs a summary of the episodes that failed to be uploaded, once every episode is
        processed. Each failed episode has already been logged with its error and sequence name.

        Args:
            failed_episodes (List[int]): The indexes of the failed episodes.
        """
        if not failed_episodes:
            return

        logger.error(
            f"{len(failed_episodes)} episode(s) failed to be ingested, with the "
            f"'{self._cfg.on_error.value}' error policy (see the errors above): "
            f"episodes {failed_episodes}"
        )

    def _upload_batch(
        self,
        lerobot_msg: LeRobotMessage,
        session_writer: AnySessionWriter,
    ):
        """
        Uploads a batch of an episode to its topic, creating the topic on its first batch.

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

        # The topic is created on its first batch
        if twriter is None:
            # --- Schema metadata Resolution ---
            lerobot_meta = LeRobotSchemaMetadata.from_dict({})

            # Record which dataset introduced this topic, inside the reserved `_lerobot_`
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

    # Public API
    def run(self) -> InjectionStatus:
        """
        Main execution entry point for the injection pipeline.

        Connects to the Mosaico server, opens the dataset and uploads every episode as its own
        sequence (see `_upload_episode`), with a single progress UI for the whole dataset. A
        failed episode does not stop the ingestion: each one is logged with its error, then a
        summary is logged once every episode is processed (see `_report_failed_episodes`).

        If `dry_run` is set, delegates to `_dry_run_report()` and returns without connecting to
        the server.

        Returns:
            InjectionStatus: `COMPLETED` once every episode is processed, even if some of them
                failed, `CANCELLED` if the user interrupted the injection (`KeyboardInterrupt`),
                `DRY_RUN` in dry-run mode.

        Raises:
            Exception: Any fatal error outside of an episode (e.g. while connecting or opening
                the dataset) is logged and then re-raised, so callers can detect failure (e.g.
                `try`/`except` around `run()`, or a non-zero process exit code from the CLI
                entry point).
        """
        if self._cfg.dry_run:
            self._dry_run_report()
            return InjectionStatus.DRY_RUN

        logger.info(f"Connecting to Mosaico at '{self._cfg.host}:{self._cfg.port}'...")

        failed_episodes: List[int] = []

        try:
            # Context: Mosaico Client (Network Connection)
            with MosaicoClient.connect(
                host=self._cfg.host,
                port=self._cfg.port,
                api_key=self._cfg.mosaico_api_key,
                enable_tls=self._cfg.enable_tls,
                tls_cert_path=self._cfg.tls_cert_path,
            ) as mclient:
                # Context: LeRobot Loader (Dataset Access)
                logger.info(f"Opening LeRobot dataset: '{self._cfg.file_path}'")

                lerobot_loader = self._open_or_get_loader(mclient.max_batch_size_bytes)

                # Setup Progress UI: a single UI for every episode of the dataset
                ui = LeRobotProgressManager(lerobot_loader, lerobot_loader.n_episodes)
                ui.setup()

                # Main Processing Loop
                # By passing self.console, any 'logger.info' calls inside
                # this loop will print cleanly ABOVE the progress bars.
                with Live(ui.progress, console=self._console):
                    # Each episode is ingested as its own sequence
                    for episode_loader in lerobot_loader:
                        if not self._upload_episode(mclient, episode_loader, ui):
                            failed_episodes.append(episode_loader.episode_index)

        except KeyboardInterrupt:
            logger.warning("Operation cancelled by user. Shutting down...")
            return InjectionStatus.CANCELLED
        except Exception as e:
            logger.exception(f"Fatal error during ingestion: '{e}'")
            raise

        self._report_failed_episodes(failed_episodes)

        return InjectionStatus.COMPLETED


# --- CLI Entry Point ---


def lerobot_injector():
    """
    Console script entry point.
    Parses arguments, sets up configuration, and initiates the injector.
    """
    parser = argparse.ArgumentParser(description="Inject LeRobot data into Mosaico.")

    # Required Arguments
    parser.add_argument(
        "dataset_path", type=Path, help="Path to the LeRobot v3.0 dataset folder"
    )
    parser.add_argument(
        "--name",
        "-n",
        default="",
        help=(
            "Prefix of the sequence names: each episode is ingested as the sequence "
            "'<name>_episode_<episode index>' (e.g. 'pusht_episode_000042'). Defaults to the "
            "name of the dataset folder."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Resolve columns/topics/sequences/rejections and print a report, without "
            "connecting to the Mosaico server or writing any data."
        ),
    )
    parser.add_argument(
        "--update-if-exists",
        action="store_true",
        help="Not used by the LeRobot bridge: ignored.",
    )

    # Connection Arguments
    parser.add_argument("--host", default="localhost", help="Mosaico Server Host")
    parser.add_argument(
        "--port", type=int, default=6726, help="Mosaico Server Port (Default: 6726)"
    )

    # Filter Arguments
    parser.add_argument(
        "--column-names",
        nargs="+",
        help=(
            "Column patterns to filter (supports glob wildcards like 'observation.images.*'). "
            "Prefix a pattern with '!' to exclude it (e.g., 'observation.*' '!observation.images.*'). "
            "If only exclusions are provided, all columns are included except those excluded. "
            "Patterns are evaluated in ORDER. "
            "Note: in some shells (e.g., zsh), '!' triggers history expansion, so patterns "
            'should be quoted or escaped (e.g., "!observation.images.*").'
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
            "JSON string or path to JSON file containing a mapping of exact Mosaico topic name "
            'to metadata, e.g. \'{"/timeseries": {"robot": "so100"}}\'. Only applied to '
            "topics that are actually ingested."
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
        default="WARNING",  # Optional: defaults to WARNING
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
        column_names=args.column_names,
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
