"""
ULog Injection Tool.

CLI and Python API to inject PX4 ULog (`.ulg`) files into Mosaico: one file becomes one
sequence, one `(format, multi_id)` pair one topic, written in chunks with
`TopicWriter.push_batch` (from the LeRobot bridge, issue #945).

Typical usage as a script:
    $ mosaicolabs.ulog_injector ./flight.ulg --name "flight_01"

Typical usage as a library:
    config = ULogInjectionConfig(file_path=Path("flight.ulg"), sequence_name="flight_01")
    ULogInjector(config).run()

What is reused from the MCAP bridge (bridges/mcap/injector.py):
    - `ULogInjector` subclasses `MCAPInjector`: `run()` (connection, sequence create,
      progress UI, main loop), `_dry_run_report()`, `_print_summary()` and
      `_get_topic_on_error()` are inherited as they are;
    - `_parse_json_arg` (CLI JSON/file arguments) is imported, not copied;
    - `ulog_injector()` is the same CLI as `mcap_injector()`, with `--topics`
      instead of `--channels` and without `--update-if-exists`.
Only the loader and `_process_message` are ULog-specific.
"""

import argparse
import os
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import List, Optional

import pyarrow as pa

from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.enum import TopicWriterStatus
from mosaicolabs.handlers.base_session_writer import AnySessionWriter
from mosaicolabs.logging_config import get_logger

from ..base_injector import InjectionConfigBase, InjectionStatus
from ..mcap.injector import MCAPInjector, _parse_json_arg
from ..ui import ProgressManager
from .helpers import _chunks
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
    The `ulog_*` keys of the sequence metadata are written by the bridge (see
    `ULogLoader.sequence_metadata`) and replace user-supplied keys with the same name.

    Attributes:
        topics (Optional[List[str]]): Topic names or glob patterns to ingest; patterns
            starting with "!" exclude, evaluated in order. If None, all topics are loaded.
    """

    # Same role as MCAPInjectionConfig.channels
    topics: Optional[List[str]] = None


class ULogInjector(MCAPInjector):
    """
    Main controller for the ULog ingestion workflow.

    Connection, sequence creation, progress UI, dry run and summary are inherited from
    `MCAPInjector`. What changes is the loader (`ULogLoader`) and the unit of work: each
    item is a whole topic, created with its `_ulog_` metadata and written in chunks with
    `TopicWriter.push_batch` (from the LeRobot bridge, issue #945). The file-level ULog
    content (info, parameters, ingestion stats, see `ULogLoader.sequence_metadata`) is
    added to the sequence metadata.

    Example:
        ```python
        from pathlib import Path
        from mosaicolabs.bridges.ulog import ULogInjectionConfig, ULogInjector

        ULogInjector(ULogInjectionConfig(file_path=Path("flight.ulg"), sequence_name="flight_01")).run()
        ```
    """

    # NOTE: reuses MCAPInjector's orchestration instead of copying it; extract a
    # shared BaseInjector when the bridges' injectors diverge

    _cfg: ULogInjectionConfig
    _max_batch_size_bytes: int

    def _open_or_get_loader(self) -> ULogLoader:
        # Override: MCAPInjector.run() and _dry_run_report() get the ULog loader here
        if self._loader is None:
            self._loader = ULogLoader(
                file_path=self._cfg.file_path,
                topics=self._cfg.topics,
                serialization_formats=self._cfg.serialization_formats,
            )
        return self._loader

    def run(self) -> InjectionStatus:
        """
        Injects the file (or only reports what would be injected, if `dry_run`).

        Raises:
            ValueError: If `update_if_exists` is set.
            Exception: Any fatal error, logged and re-raised (see `MCAPInjector.run`).
        """
        # sequence_update() takes no metadata, and a second file of the same vehicle
        # would create the same topic names: not supported for now (one flight = one file)
        if self._cfg.update_if_exists:
            raise ValueError(
                "update_if_exists is not supported by the ULog bridge: a .ulg file is one sequence."
            )
        # Done before the inherited run(): the file-level ULog content is put into the
        # sequence metadata that MCAPInjector.run() passes to sequence_create(), and the
        # server batch limit is read for push_batch. Errors and Ctrl-C are handled like
        # in MCAPInjector.run().
        try:
            ulog_metadata = self._open_or_get_loader().sequence_metadata()
            self._cfg = replace(
                self._cfg, metadata={**self._cfg.metadata, **ulog_metadata}
            )
            if not self._cfg.dry_run:
                # push_batch (from the LeRobot bridge, issue #945) sends each call as a
                # single Flight message: the chunks must stay within this limit
                with MosaicoClient.connect(
                    host=self._cfg.host,
                    port=self._cfg.port,
                    api_key=self._cfg.mosaico_api_key,
                    enable_tls=self._cfg.enable_tls,
                    tls_cert_path=self._cfg.tls_cert_path,
                ) as mclient:
                    self._max_batch_size_bytes = mclient.max_batch_size_bytes
        except KeyboardInterrupt:
            logger.warning("Operation cancelled by user. Shutting down...")
            return InjectionStatus.CANCELLED
        except Exception as e:
            logger.exception(f"Fatal error during ingestion: '{e}'")
            raise
        return super().run()

    def _process_message(
        self,
        ulog_msg: ULogMessage,
        exc: Optional[Exception],
        session_writer: AnySessionWriter,
        ui: ProgressManager,
    ):
        """
        Creates the topic of `ulog_msg` and writes all its rows.

        Same steps as `MCAPInjector._process_message` (error -> skip, resolve the
        adapter, `topic_create` with the reserved metadata merged over the user's,
        push, update the UI on every path), but once per topic: one `topic_create`,
        then one `push_batch` per chunk of rows (see `_chunks`) instead of one `push`
        per message. The UI advances by rows, so the bars still count messages.
        """
        loader = self._open_or_get_loader()
        topic = ulog_msg.topic_name
        n_rows = loader.msg_count(topic)

        adapter = loader.resolve_adapter(topic)
        twriter = None
        if exc is None and adapter is not None:
            ulog_meta = loader.topic_schema_metadata(topic).update(
                source_file=self._cfg.file_path.name
            )
            # `_ulog_` is reserved and wins over the user-supplied topic metadata
            metadata = {
                **(self._cfg.topic_metadata or {}).get(topic, {}),
                **ulog_meta.to_dict(),
            }
            twriter = session_writer.topic_create(
                topic_name=topic,
                metadata=metadata,
                ontology_type=adapter.ontology_data_type(),
                on_error=self._get_topic_on_error(topic),
            )

        # Building the batch failed, or topic_create returned None: count the whole
        # topic as skipped (it shows in the summary table) and keep the bars moving
        if adapter is None or twriter is None:
            logger.warning(
                f"Skipping topic '{topic}': '{exc or 'topic creation failed'}'"
            )
            ui.update_status(topic, "Error. Check the logs.", "red")
            self._malformed_message_counts[topic] = n_rows
            for _ in range(n_rows):
                ui.advance_global()
            return

        # The batch is `timestamp_ns` + the payload columns, which push_batch (from the
        # LeRobot bridge, issue #945) takes apart. One call per chunk: `with twriter`
        # applies the topic error policy (Raise / Ignore / Finalize) to each, as
        # MCAPInjector does around each push()
        payload_schema = pa.schema(
            list(adapter.ontology_data_type().__msco_pyarrow_struct__)
        )
        for chunk in _chunks(adapter.translate(ulog_msg), self._max_batch_size_bytes):
            if twriter.is_active:
                timestamp_ns, *payload = chunk.columns
                with twriter:
                    twriter.push_batch(
                        timestamp_ns,
                        pa.RecordBatch.from_arrays(payload, schema=payload_schema),
                    )
                # Same status handling as MCAPInjector._process_message
                if twriter.status == TopicWriterStatus.IgnoredLastError:
                    ui.update_status(topic, "Write Error (Ignored)", "yellow")
                elif twriter.status == TopicWriterStatus.FinalizedWithError:
                    ui.update_status(topic, "Fatal Error: Prematurely finalized", "red")
            # ProgressManager advances by one message per call
            for _ in range(chunk.num_rows):
                ui.advance_all(topic)


def ulog_injector():
    """Console script entry point (same arguments and exit codes as `mcap_injector`)."""
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
