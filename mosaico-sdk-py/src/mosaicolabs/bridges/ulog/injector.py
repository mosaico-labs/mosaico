"""
ULog Injection Tool.

CLI and Python API to inject PX4 ULog (`.ulg`) files into Mosaico: one file becomes one
sequence, one `(format, multi_id)` pair one topic, written in chunks with
`TopicWriter.push_batch`.

Typical usage as a script:
    $ mosaicolabs.ulog_injector ./flight.ulg --name "flight_01"

Typical usage as a library:
    config = ULogInjectionConfig(file_path=Path("flight.ulg"), sequence_name="flight_01")
    ULogInjector(config).run()
"""

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pyarrow as pa
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.enum import TopicLevelErrorPolicy, TopicWriterStatus
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
from ..ui import ProgressManager
from .loader import ULogLoader
from .ulog_message import ULogMessage

logger = get_logger(__name__)


@dataclass
class ULogInjectionConfig(InjectionConfigBase):
    """
    Configuration of the ULog injection, see `InjectionConfigBase` for the shared settings.

    Topic names (keys of `topic_metadata` and `topics_on_error`) are `/{format}/{multi_id}`,
    e.g. `/sensor_accel/0`. Keys of `serialization_formats` are ULog format names
    (e.g. `sensor_accel`), since all instances of a format share one ontology class.
    `update_if_exists` is not supported: a ULog file is a whole flight, i.e. one sequence.

    The `_ulog_` key of the sequence metadata and of each topic metadata is reserved to
    the bridge (see `ULogSequenceMetadata` and `ULogSchemaMetadata`): it replaces a
    user-supplied key with the same name, every other key is user-owned.

    Attributes:
        topics (Optional[List[str]]): Topic names or glob patterns to ingest; patterns
            starting with "!" exclude, evaluated in order. If None, all topics are loaded.
    """

    topics: Optional[List[str]] = None


class ULogInjector:
    """
    Main controller for the ULog ingestion workflow.

    It reads the `.ulg` file with `ULogLoader`, creates the sequence with the file-level
    ULog content (info, parameters, ingestion stats, see `ULogLoader.sequence_metadata`)
    as its `_ulog_` metadata, then creates each topic with its own `_ulog_` metadata and
    writes its rows in chunks with `TopicWriter.push_batch`, each within
    `MosaicoClient.max_batch_size_bytes`.

    Time: the ULog `timestamp` counts microseconds since the boot of the vehicle, not
    since the Unix epoch, and it is stored as it is (in nanoseconds) in `timestamp_ns`.
    Every sequence therefore starts near zero, and time-based queries across sequences
    are meaningless. When the log carries a GPS time, the offset to UTC is stored in the
    sequence metadata, as `ulog_utc_offset_ns` of the `_ulog_` block.

    Example:
        ```python
        from pathlib import Path
        from mosaicolabs.bridges.ulog import ULogInjectionConfig, ULogInjector

        ULogInjector(ULogInjectionConfig(file_path=Path("flight.ulg"), sequence_name="flight_01")).run()
        ```
    """

    def __init__(self, config: ULogInjectionConfig):
        """
        Args:
            config (ULogInjectionConfig): The fully resolved configuration object.
        """
        self._cfg = config
        # One console for the logs and the progress bars, so that they do not overlap
        self._console = Console(stderr=True)
        setup_sdk_logging(
            level=self._cfg.log_level.upper(), pretty=True, console=self._console
        )

    def _open_loader(self, max_batch_size_bytes: int) -> ULogLoader:
        return ULogLoader(
            file_path=self._cfg.file_path,
            max_batch_size_bytes=max_batch_size_bytes,
            topics=self._cfg.topics,
            serialization_formats=self._cfg.serialization_formats,
        )

    def _dry_run_report(self):
        """
        Prints what would be ingested, without connecting to the Mosaico server: per
        topic, the acceptance status, the adapter (or the rejection reason) and the number
        of rows. Also flags the `topic_metadata` entries matching no accepted topic.
        """
        logger.info(f"[DRY RUN] Opening ulog: '{self._cfg.file_path}'")

        # No batch is read in a dry run: the maximum batch size, given by the server, is unused
        with self._open_loader(max_batch_size_bytes=0) as loader:
            table = Table(
                title=f"Dry Run: '{self._cfg.file_path.name}' -> sequence '{self._cfg.sequence_name}'"
            )
            table.add_column("Topics")
            table.add_column("Status")
            table.add_column("Adapter / Reason")
            table.add_column("Messages", justify="right")

            for topic in loader.topics:
                table.add_row(
                    topic,
                    "[bright_green]Accepted",
                    loader.resolve_adapter(topic).__name__,
                    str(loader.msg_count(topic)),
                )
            for topic, status in loader.rejected_topics:
                table.add_row(topic, f"[{to_color(status)}]{status.value}", "-", "-")
            self._console.print(table)

            unused_topic_metadata = set(self._cfg.topic_metadata or {}) - set(
                loader.topics
            )
            if unused_topic_metadata:
                logger.warning(
                    f"'topic_metadata' entries for topics that would NOT be ingested "
                    f"(filtered out or unresolved): {sorted(unused_topic_metadata)}"
                )

            self._console.print(
                f"[bold]{len(loader.topics)}[/bold] topic(s) would be ingested, "
                f"[bold]{len(loader.rejected_topics)}[/bold] rejected. "
                "No connection to the Mosaico server was made."
            )

    def _get_topic_on_error(self, topic: str) -> TopicLevelErrorPolicy:
        if isinstance(self._cfg.topics_on_error, dict):
            return self._cfg.topics_on_error.get(topic, _DEFAULT_TOPIC_ON_ERROR)
        elif isinstance(self._cfg.topics_on_error, TopicLevelErrorPolicy):
            return self._cfg.topics_on_error

        return _DEFAULT_TOPIC_ON_ERROR

    def run(self) -> InjectionStatus:
        """
        Injects the file (or only reports what would be injected, if `dry_run`).

        Returns:
            InjectionStatus: `COMPLETED` if the file was injected, `CANCELLED` if the user
                interrupted the injection (`KeyboardInterrupt`), `DRY_RUN` in dry-run mode.

        Raises:
            ValueError: If `update_if_exists` is set.
            Exception: Any fatal error during connection, loading or upload is logged
                and then re-raised, so callers can detect the failure.
        """
        # sequence_update() takes no metadata, and a second file of the same vehicle
        # would create the same topic names: not supported for now (one flight = one file)
        if self._cfg.update_if_exists:
            raise ValueError(
                "update_if_exists is not supported by the ULog bridge: a .ulg file is one sequence."
            )
        try:
            if self._cfg.dry_run:
                self._dry_run_report()
                return InjectionStatus.DRY_RUN

            logger.info(
                f"Connecting to Mosaico at '{self._cfg.host}:{self._cfg.port}'..."
            )
            with MosaicoClient.connect(
                host=self._cfg.host,
                port=self._cfg.port,
                api_key=self._cfg.mosaico_api_key,
                enable_tls=self._cfg.enable_tls,
                tls_cert_path=self._cfg.tls_cert_path,
            ) as mclient:
                logger.info(f"Opening ulog: '{self._cfg.file_path}'")

                # push_batch sends each call as a single Flight message: the loader cuts
                # the topics into chunks within the limit of this server
                with self._open_loader(mclient.max_batch_size_bytes) as loader:
                    ui = ProgressManager(loader)
                    ui.setup()

                    # `_ulog_` is reserved: it wins over a user-supplied key with the
                    # same name, every other key is user-owned
                    seq_writer = mclient.sequence_create(
                        sequence_name=self._cfg.sequence_name,
                        metadata={
                            **self._cfg.metadata,
                            **loader.sequence_metadata().to_dict(),
                        },
                        on_error=self._cfg.on_error,
                    )

                    with seq_writer:
                        logger.info("Starting upload...")
                        # With this console, the logs are printed above the progress bars
                        with Live(ui.progress, console=self._console):
                            for ulog_msg in loader:
                                self._upload_topic(ulog_msg, loader, seq_writer, ui)

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
            return InjectionStatus.CANCELLED
        except Exception as e:
            logger.exception(f"Fatal error during ingestion: '{e}'")
            raise

        return InjectionStatus.COMPLETED

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

    def _upload_topic(
        self,
        ulog_msg: ULogMessage,
        loader: ULogLoader,
        session_writer: AnySessionWriter,
        ui: ProgressManager,
    ):
        """
        Creates the topic of `ulog_msg` and writes its rows, one `push_batch` per chunk.

        Each chunk is built, translated and pushed inside the topic error scope, so any
        failure follows the topic error policy (see `topics_on_error`): `Raise`
        propagates it and fails the sequence, `Ignore` drops the chunk (and the rest of
        the topic, if the chunk could not be built), `Finalize` closes the topic. The UI
        advances by rows, so the bars count messages.

        Args:
            ulog_msg (ULogMessage): The topic to upload.
            loader (ULogLoader): The loader that yielded `ulog_msg`.
            session_writer (AnySessionWriter): The active session writer for the sequence.
            ui (ProgressManager): The progress manager for updating the UI.
        """
        topic = ulog_msg.topic_name
        n_rows = loader.msg_count(topic)
        adapter = loader.resolve_adapter(topic)
        ontology = adapter.ontology_data_type()

        # `_ulog_` is reserved and wins over the user-supplied topic metadata
        metadata = {
            **(self._cfg.topic_metadata or {}).get(topic, {}),
            **loader.topic_schema_metadata(topic)
            .update(source_file=self._cfg.file_path.name)
            .to_dict(),
        }
        twriter = session_writer.topic_create(
            topic_name=topic,
            metadata=metadata,
            ontology_type=ontology,
            on_error=self._get_topic_on_error(topic),
        )
        # topic_create returned None: skip the whole topic and keep the bars moving
        if twriter is None:
            logger.warning(f"Skipping topic '{topic}': 'topic creation failed'")
            ui.update_status(topic, "Error. Check the logs.", "red")
            ui.advance_global(n_rows)
            return

        payload_schema = pa.schema(list(ontology.__msco_pyarrow_struct__))
        chunks = iter(ulog_msg.data)
        written = 0
        while twriter.is_active:
            # Topic error scope. The chunk is built by `next()`, so it must be requested
            # in here: a failure while building it has to follow the topic error policy
            # too, instead of aborting the whole sequence
            with twriter:
                raw = next(chunks, None)
                if raw is None:
                    break
                # `timestamp_ns` + the payload columns, which push_batch takes apart
                timestamp_ns, *payload = adapter.translate_batch(raw).columns
                twriter.push_batch(
                    timestamp_ns,
                    pa.RecordBatch.from_arrays(payload, schema=payload_schema),
                )
                written += raw.num_rows
                ui.advance_all(topic, raw.num_rows)
            if twriter.status == TopicWriterStatus.IgnoredLastError:
                ui.update_status(topic, "Write Error (Ignored)", "yellow")
            elif twriter.status == TopicWriterStatus.FinalizedWithError:
                ui.update_status(topic, "Fatal Error: Prematurely finalized", "red")

        # Rows left out by the `Ignore` or `Finalize` policy: the bars still count them
        if written < n_rows:
            ui.advance_all(topic, n_rows - written)


def ulog_injector():
    """Console script entry point."""
    parser = argparse.ArgumentParser(
        description="Inject ULog (.ulg) data into Mosaico."
    )
    parser.add_argument("ulog_path", type=Path, help="Path to .ulg file")
    parser.add_argument("--name", "-n", required=True, help="Target Sequence Name")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report the topics that would be ingested, without connecting to the server.",
    )
    parser.add_argument("--host", default="localhost", help="Mosaico Server Host")
    parser.add_argument(
        "--port", type=int, default=6726, help="Mosaico Server Port (Default: 6726)"
    )
    parser.add_argument(
        "--topics",
        nargs="+",
        help="Topic patterns to filter, e.g. '/sensor_*' '!/sensor_baro/*' (evaluated in order).",
    )
    parser.add_argument(
        "--metadata", help="JSON string or path to JSON file with sequence metadata"
    )
    parser.add_argument(
        "--topic-metadata",
        help="JSON string or path to JSON file mapping topic name to metadata, e.g. '{\"/sensor_accel/0\": {...}}'",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Mosaico API-Key. Prefer the MOSAICO_API_KEY environment variable.",
    )
    parser.add_argument(
        "--tls-cert", default=None, help="Path of the .cert file for secure connection"
    )
    parser.add_argument(
        "--log",
        "-l",
        default="INFO",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging verbosity level",
    )
    args = parser.parse_args()

    config = ULogInjectionConfig(
        file_path=args.ulog_path,
        sequence_name=args.name,
        metadata=_parse_json_arg(args.metadata, arg_name="Metadata"),
        topic_metadata=_parse_json_arg(args.topic_metadata, arg_name="Topic metadata")
        or None,
        dry_run=args.dry_run,
        host=args.host,
        port=args.port,
        topics=args.topics,
        log_level=args.log,
        tls_cert_path=args.tls_cert,
        mosaico_api_key=args.api_key or os.environ.get("MOSAICO_API_KEY"),
    )

    try:
        status = ULogInjector(config).run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        # Already logged inside run()
        sys.exit(1)

    if status is InjectionStatus.CANCELLED:
        sys.exit(130)


if __name__ == "__main__":
    ulog_injector()
