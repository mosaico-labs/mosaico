"""
SequenceExtractor: streams a Mosaico sequence back out and writes it to a file on disk.

Provides [`SequenceExtractor`][mosaicolabs.bridges.base_sequence_extractor.SequenceExtractor]
and the [`ExtractorConfig`][mosaicolabs.bridges.base_sequence_extractor.ExtractorConfig]
dataclass, which together own the target-agnostic half of the extraction pipeline:

1. Prepare (and optionally clear) the output location.
2. Connect to the Mosaico server.
3. Stream every message in the requested sequence (optionally filtered by topic or
   time window) through a [`MosaicoLoader`][mosaicolabs.bridges.loader_base.MosaicoLoader].
4. Hand each message to the specialization, which encodes it and writes it out.

Everything that depends on the *target* format (which loader to open, which writer to
open, and how a single message is encoded and written) lives in a subclass. See
[`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor] and
[`MCAPSequenceExtractor`][mosaicolabs.bridges.mcap.MCAPSequenceExtractor].

The module also provides the command-line arguments shared by the extractors' entry points.
"""

import argparse
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from rich.live import Live

from mosaicolabs import Message, MosaicoClient
from mosaicolabs.bridges.ui import ProgressManager
from mosaicolabs.logging_config import get_logger, setup_sdk_logging

from .loader_base import MosaicoLoader
from .topic_status import to_color

# Set the hierarchical logger
logger = get_logger(__name__)


# --- Configuration ---
@dataclass
class ExtractorConfig:
    """
    Shared configuration for every
    [`SequenceExtractor`][mosaicolabs.bridges.base_sequence_extractor.SequenceExtractor].

    Collects the parameters that do not depend on the output format: how to reach the
    Mosaico server, which sequence to extract, how to filter it by topic and time window,
    and where to write the result.

    Attributes:
        saving_path (Path): The path where to save the final file.
        sequence_name (str): The name of the sequence to extract.
        host (str): The hostname of the Mosaico server. Defaults to `"localhost"`.
        port (int): The port of the Mosaico server. Defaults to `6726`.
        topics (Optional[list[str]]): List of topic patterns (shell-style globs, `!` for
            exclusions) used to filter the topics to extract. If None, all topics are loaded.
        log_level (str): The log level. Defaults to `"INFO"`.
        mosaico_api_key (Optional[str]): The API key for authentication on the Mosaico
            server; it must have (at least) the `read` permission. Defaults to None.
        tls_cert_path (Optional[str]): Path to the TLS certificate file for a secure
            connection to the Mosaico server. Defaults to None.
        enable_tls (bool): Enable standard one-way TLS (server authenticated only).
            Ignored if `tls_cert_path` is provided. Defaults to False.
        start_timestamp_ns (Optional[int]): Inclusive lower-bound timestamp (in nanoseconds)
            from which to start extracting data. Defaults to None.
        end_timestamp_ns (Optional[int]): Exclusive upper-bound timestamp (in nanoseconds)
            at which to stop extracting data. Defaults to None.
        overwrite (bool): If True, delete and recreate the output path if it already
            exists. Defaults to False.
        dry_run (bool): If True, report which topics would be extracted or rejected and
            whether the output path already exists, without writing or deleting any file.
            Defaults to False.
    """

    saving_path: Path
    """
    The path where to save the final file.
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

    If provided it must have (at least) the `read` permission.
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
    """Inclusive timestamp upper-bound (in nanoseconds) from where to start extracting data of specified sequence"""

    end_timestamp_ns: Optional[int] = None
    """Exclusive timestamp lower-bound (in nanoseconds) to finish extracting data of specified sequence"""

    overwrite: bool = False
    """If True, delete and recreate the output path if it already exists. Defaults to False."""

    dry_run: bool = False
    """
    If `True`, connects to the Mosaico server and reports which topics would be extracted
    (and with which adapter/native message type), which topics would be rejected (and why),
    and whether the output path already exists, without writing any file and without
    deleting an existing output path even if `overwrite=True`. Default: False.
    """

    def __post_init__(self):
        """
        Rejects direct instantiation of the base configuration.

        Raises:
            TypeError: when `ExtractorConfig` is instantiated directly instead of one of
                its bridge-specific subclasses.
        """
        if type(self) is ExtractorConfig:
            raise TypeError(
                "ExtractorConfig is a base configuration and cannot be instantiated "
                "directly; use a bridge-specific subclass such as ROSExtractorConfig or "
                "MCAPExtractorConfig."
            )


# --- Main Extractor Class ---


class SequenceExtractor(ABC):
    """
    Orchestrates the extraction of a Mosaico sequence into a file on disk.

    This is the target-agnostic half of the extraction pipeline: preparing the output
    location and enforcing the overwrite policy, connecting to the Mosaico server, driving
    the loader that streams the sequence, reporting live progress and, in dry-run mode,
    printing what *would* be extracted. Everything that depends on the output format lives
    in a subclass; see
    [`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor] and
    [`MCAPSequenceExtractor`][mosaicolabs.bridges.mcap.MCAPSequenceExtractor].

    A subclass supplies exactly three things:

    * `_open_mosaicoloader`: the
      [`MosaicoLoader`][mosaicolabs.bridges.loader_base.MosaicoLoader] specialization that
      adapts the sequence into the target format.
    * `_open_writer`: the writer that turns encoded messages into a file.
    * `_process_message`: the per-message encode-and-write step.

    `run`, `_dry_run_report`, `_prepare_output_path` and `_handle_encoding_error` are
    inherited unchanged.

    Attributes:
        cfg (ExtractorConfig): The configuration this extractor was built with.
        console (Console): The `rich` console, bound to stderr so that progress bars and
            reports never pollute a piped stdout.
        ignored_topics (set[str]): Topics dropped mid-run after a failure. Specializations
            add to it from `_process_message` so a broken topic is reported once and then
            skipped.
    """

    def __init__(self, config: ExtractorConfig):
        """
        Initializes the shared state, console and SDK logging.

        Args:
            config (ExtractorConfig): A bridge-specific configuration subclass.
        """
        self.cfg = config

        from rich.console import Console

        self.console = Console(stderr=True)
        setup_sdk_logging(
            level=self.cfg.log_level.upper(), pretty=True, console=self.console
        )

        self.ignored_topics: set[str] = set()

    @abstractmethod
    def _open_mosaicoloader(self, mclient: MosaicoClient) -> MosaicoLoader:
        """
        Opens a fresh loader for the configured sequence.

        Implemented by each specialization to build the
        [`MosaicoLoader`][mosaicolabs.bridges.loader_base.MosaicoLoader] subclass matching
        its output format (`MosaicoToROSLoader` for ROS bags, `MosaicoToMCAPLoader` for
        MCAP files), forwarding `cfg.sequence_name`, `cfg.topics` and the timestamp
        window, plus whatever else that loader needs.

        Args:
            mclient (MosaicoClient): The open connection the loader should read through.

        Returns:
            MosaicoLoader: A new loader for the configured sequence.
        """

    @abstractmethod
    def _open_writer(self, path: Path) -> Any:
        """
        Opens the writer that will receive the encoded messages.

        Implemented by each specialization to open its output-format writer: a `rosbags`
        bag writer, an MCAP writer, and so on. The returned object must be usable as a
        context manager, since `run` consumes it with a `with` statement, and must accept
        whatever `_process_message` hands it.

        Args:
            path (Path): The output location, already validated and cleared. Whether it
                becomes a directory or a single file is up to the implementation.

        Returns:
            Any: The open writer, usable as a context manager.
        """

    @abstractmethod
    def _process_message(
        self,
        writer: Any,
        ms_loader: MosaicoLoader,
        t_name: str,
        ms_msg: Message,
        ui: ProgressManager,
    ):
        """
        Encodes a single Mosaico message and writes it to the output file.

        Implemented by each specialization. The expected shape is:

        1. **Skip**: ignore the message if `t_name` is already in `ignored_topics`.
        2. **Resolve Adapter**: ask `ms_loader` for the adapter registered for this topic.
        3. **Encode**: translate the Mosaico message into the target format's native message.
        4. **Resolve Connection**: reuse, or lazily open, the writer handle for this topic.
        5. **Write**: serialize the native message and hand it to `writer`.

        Implementations must not raise: a topic that cannot be encoded or written is added
        to `ignored_topics` and reported through `ui` (`_handle_encoding_error` does both),
        so one bad topic never aborts the whole extraction. They must also advance the
        progress UI on *every* path, with `ui.advance_all` on success and
        `ui.advance_global` on a skip or a failure, or the bars will never complete.

        Args:
            writer (Any): The writer returned by `_open_writer`.
            ms_loader (MosaicoLoader): The loader `run` is streaming from, used to resolve
                the topic's adapter and native message type.
            t_name (str): The topic the message belongs to.
            ms_msg (Message): The Mosaico message to encode and write.
            ui (ProgressManager): The live progress reporter for this run.
        """
        ...

    def _prepare_output_path(self):
        """
        Resolves the final output location and enforces the overwrite policy.

        The output path is `cfg.saving_path / cfg.sequence_name`. If that path already
        exists:

        - `overwrite=False` (default): raises `FileExistsError`.
        - `overwrite=True`: the existing directory is deleted recursively before the path
          is returned.

        The `saving_path / sequence_name` layout is part of the public contract, since
        callers recompute it to locate the written file, so specializations derive their
        file names from the returned path instead of redefining it.

        Returns:
            Path: The prepared (non-existent) output location, ready for `_open_writer`.

        Raises:
            FileExistsError: If the path exists and `cfg.overwrite` is `False`.
        """
        final_path = self.cfg.saving_path / self.cfg.sequence_name

        if final_path.exists():
            if not self.cfg.overwrite:
                raise FileExistsError(
                    f"Impossible to save to '{final_path}' since it already exists. "
                    "Pass overwrite=True (or --overwrite on CLI) to replace it."
                )
            shutil.rmtree(final_path)

        return final_path

    def _handle_encoding_error(
        self,
        t_name: str,
        ui: ProgressManager,
        log_message: str,
        status: str,
        style: str = "red",
    ):
        """
        Drops a topic that failed to encode or write, and reports it.

        Args:
            t_name (str): The topic to drop.
            ui (ProgressManager): The live progress reporter for this run.
            log_message (str): The full explanation, logged at `error` level.
            status (str): The short label shown next to the topic in the UI.
            style (str): The `rich` style of that label. Defaults to `"red"`.

        Returns:
            None: Always `None`, so callers can `return self._handle_encoding_error(...)`.
        """
        self.ignored_topics.add(t_name)
        logger.error(log_message)
        ui.update_status(t_name, status, style=style)
        ui.advance_global()
        return None

    def _dry_run_report(self):
        """
        Resolves the sequence's topics against the current configuration and prints a
        report of what would be extracted, without writing a file or touching the output
        path.

        Reports, per topic: acceptance status, resolved adapter and native message type (or
        rejection reason), and message count. Also reports whether the target output path
        already exists and what `run` would do about it (fail, or delete+recreate under
        `overwrite=True`), without actually deleting anything.
        """
        from rich.table import Table

        logger.info(
            f"[DRY RUN] Connecting to Mosaico at '{self.cfg.host}:{self.cfg.port}'..."
        )

        with MosaicoClient.connect(
            host=self.cfg.host,
            port=self.cfg.port,
            api_key=self.cfg.mosaico_api_key,
            enable_tls=self.cfg.enable_tls,
            tls_cert_path=self.cfg.tls_cert_path,
        ) as mclient:
            with self._open_mosaicoloader(mclient) as ms_loader:
                table = Table(
                    title=f"Dry Run: sequence '{self.cfg.sequence_name}' -> '{self.cfg.saving_path}'"
                )
                table.add_column("Topic")
                table.add_column("Status")
                table.add_column("Adapter / Native Type / Reason")
                table.add_column("Messages", justify="right")

                for topic in ms_loader.topics:
                    adapter = ms_loader.resolve_adapter(topic)
                    native_msg_type = ms_loader.resolve_native_msg_type(topic)
                    table.add_row(
                        topic,
                        "[bright_green]Accepted",
                        f"{adapter.__name__ if adapter else '?'} -> {native_msg_type or '?'}",
                        str(ms_loader.msg_count(topic)),
                    )

                for topic, status in ms_loader.rejected_topics:
                    table.add_row(
                        topic,
                        f"[{to_color(status)}]{status.value}",
                        "-",
                        "-",
                    )

                self.console.print(table)

                output_path = self.cfg.saving_path / self.cfg.sequence_name
                if output_path.exists():
                    if self.cfg.overwrite:
                        self.console.print(
                            f"[yellow]Output path '{output_path}' already exists and would "
                            "be deleted and recreated (overwrite=True).[/yellow]"
                        )
                    else:
                        self.console.print(
                            f"[red]Output path '{output_path}' already exists and "
                            "overwrite=False: run() would raise FileExistsError.[/red]"
                        )
                else:
                    self.console.print(f"Output path '{output_path}' would be created.")

                self.console.print(
                    f"[bold]{len(ms_loader.topics)}[/bold] topic(s) would be extracted, "
                    f"[bold]{len(ms_loader.rejected_topics)}[/bold] rejected. "
                    "No file was written."
                )

    def run(self):
        """
        Executes the full extraction pipeline.

        Steps:

        1. If `cfg.dry_run` is `True`, delegates to `_dry_run_report` and returns without
           writing a file or touching the output path.
        2. Calls `_prepare_output_path` to validate / clear the output location.
        3. Opens a [`MosaicoClient`][mosaicolabs.MosaicoClient] connection.
        4. Opens the loader via `_open_mosaicoloader` and the writer via
           `_open_writer`, both as context managers, so both are released even on failure.
        5. For each `(topic, message)` pair streamed by the loader, delegates to
           `_process_message`, which encodes and writes it.

        Progress is displayed in real-time via a `rich` live progress bar. A
        `KeyboardInterrupt` exits cleanly with a warning log; any other exception
        propagates to the caller.

        Raises:
            FileExistsError: If the output path exists and `cfg.overwrite` is `False`.
            ValueError: If the configured sequence does not exist on the server (surfaced
                by the loader).
        """

        if self.cfg.dry_run:
            self._dry_run_report()
            return

        # Resolve the output path and check whether it already exists
        saving_path = self._prepare_output_path()

        try:
            with MosaicoClient.connect(
                host=self.cfg.host,
                port=self.cfg.port,
                api_key=self.cfg.mosaico_api_key,
                enable_tls=self.cfg.enable_tls,
                tls_cert_path=self.cfg.tls_cert_path,
            ) as mclient:
                logger.info(f"Writing '{saving_path}'")

                with self._open_mosaicoloader(mclient) as ms_loader:
                    with self._open_writer(saving_path) as writer:
                        ui = ProgressManager(ms_loader)
                        ui.setup()

                        with Live(ui.progress, console=self.console):
                            for t_name, ms_msg in ms_loader:
                                self._process_message(
                                    writer, ms_loader, t_name, ms_msg, ui
                                )

        except KeyboardInterrupt:
            logger.warning("Operation cancelled by user. Shutting down...")
            return


# --- CLI Helpers ---


def _add_common_arguments(
    parser: argparse.ArgumentParser, output_flag: str, output_help: str
) -> None:
    """
    Adds the command-line arguments shared by every extractor's entry point to `parser`.

    Args:
        parser (argparse.ArgumentParser): The bridge-specific parser to extend.
        output_flag (str): The name of the output path flag (e.g. `"--rosbag_path"`). Its
            value is stored as `saving_path`.
        output_help (str): The help text of the output path flag.
    """

    # Required Arguments
    parser.add_argument(
        "sequence_name",
        type=str,
        help="Name of the Mosaico sequence to extract",
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
        output_flag,
        dest="saving_path",
        metavar="PATH",
        default=Path("."),
        type=Path,
        help=output_help,
    )

    parser.add_argument(
        "--start_timestamp_ns",
        type=int,
        default=None,
        help="Inclusive timestamp lower-bound (in nanoseconds) from which to start extracting the sequence. None by default",
    )
    parser.add_argument(
        "--end_timestamp_ns",
        type=int,
        default=None,
        help="Exclusive timestamp upper-bound (in nanoseconds) at which to stop extracting the sequence. None by default",
    )

    # Connection Arguments
    parser.add_argument("--host", default="localhost", help="Mosaico Server Host")
    parser.add_argument(
        "--port", type=int, default=6726, help="Mosaico Server Port (Default: 6726)"
    )

    # Advanced Arguments
    parser.add_argument(
        "--mosaico_api_key", default=None, help="The API key for authentication"
    )
    parser.add_argument(
        "--tls_cert_path", default=None, help="Path to the TLS certificate file"
    )
    parser.add_argument(
        "--enable_tls",
        action="store_true",
        help="Whether Mosaico Server requires tls",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Delete and recreate the output path if it already exists",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Resolve topics/adapters/rejections and print a report, without writing any "
            "file or touching the output path."
        ),
    )

    parser.add_argument(
        "--log",
        "-l",
        type=str.upper,  # Automatically converts input (e.g., 'debug') to uppercase
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging verbosity level. Default is INFO",
    )


def _common_config_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    """
    Maps the arguments added by `_add_common_arguments` onto the
    [`ExtractorConfig`][mosaicolabs.bridges.base_sequence_extractor.ExtractorConfig] fields.

    Args:
        args (argparse.Namespace): The parsed command-line arguments.

    Returns:
        dict[str, Any]: The keyword arguments shared by every `ExtractorConfig` subclass.
    """
    return {
        "saving_path": args.saving_path,
        "sequence_name": args.sequence_name,
        "host": args.host,
        "port": args.port,
        "topics": args.topics,
        "log_level": args.log,
        "mosaico_api_key": args.mosaico_api_key,
        "tls_cert_path": args.tls_cert_path,
        "enable_tls": args.enable_tls,
        "start_timestamp_ns": args.start_timestamp_ns,
        "end_timestamp_ns": args.end_timestamp_ns,
        "overwrite": args.overwrite,
        "dry_run": args.dry_run,
    }
