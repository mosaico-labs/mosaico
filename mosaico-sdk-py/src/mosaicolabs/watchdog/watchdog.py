"""
Watchdog: automatic injection of the MCAP and ROS bag files found in a folder.

The `Watchdog` scans a folder (subfolders included), skips the files it already loaded or
quarantined, and injects the others into a Mosaico server one at a time, choosing the
injector for each file with the `InjectorDispatcher`. What happened to each file is kept by
the `FileStateStore` in `.already_loaded.txt` and `.quarantine_file.txt`, inside the folder.

Two modes are available:

- `single-pass`: scan once, inject every new file, return.
- `daemon`: a producer thread scans the folder every `polling_interval_s` seconds, while the
  calling thread injects the queued files, until Ctrl-C or an error stops it.

Typical usage as a script:
    $ mosaicolabs.watchdog /data/bags --mode daemon

Typical usage as a library:
    config = WatchdogConfig(path_to_monitor=Path("/data/bags"), mode=WatchdogMode.DAEMON)
    Watchdog(config).run()
"""

import argparse
import os
import sys
import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from queue import Empty, Queue
from typing import Optional

from mosaicolabs.logging_config import get_logger, setup_sdk_logging

from ..bridges.base_injector import InjectionStatus
from .dispatcher import InjectorDispatcher
from .file_source import FileRef, FileSource, LocalFileSource
from .file_state_store import FileState, FileStateStore
from .helpers import instanciate_injector, sequence_name_from_fileref

# Set the hierarchical logger
logger = get_logger(__name__)


class WatchdogError(Enum):
    """What the Watchdog does when the injection of a file fails."""

    RAISE = "raise"
    """Stop the Watchdog and re-raise the error to the caller of `run()`."""

    RETRY = "retry"
    """Try the file again, up to `retry_number` more times, then quarantine it."""

    QUARANTINE = "quarantine"
    """Quarantine the file at the first failure and move on to the next one."""


class WatchdogMode(Enum):
    """How long the Watchdog runs."""

    SINGLEPASS = "single-pass"
    """Scan the folder once, inject every new file, then return."""

    DAEMON = "daemon"
    """Scan the folder every `polling_interval_s` seconds until stopped (Ctrl-C or an error)."""


@dataclass(frozen=True)
class WatchdogConfig:
    """
    The configuration of a `Watchdog`: the folder to watch, how to scan it, what to do on
    errors, and how to reach the Mosaico server.

    Example:
        ```python
        from pathlib import Path

        config = WatchdogConfig(
            path_to_monitor=Path("/data/bags"),
            mode=WatchdogMode.DAEMON,
            min_file_age_s=10.0,
            on_error=WatchdogError.RETRY,
        )
        ```
    """

    path_to_monitor: Path
    """The folder to watch, subfolders included. It must exist and be writable: the state
    files (`.already_loaded.txt`, `.quarantine_file.txt`) are written inside it."""

    mode: WatchdogMode = WatchdogMode.SINGLEPASS
    """`SINGLEPASS` scans once and returns, `DAEMON` keeps scanning until stopped."""

    polling_interval_s: float = 5.0
    """Seconds between the start of two scans. Daemon mode only."""

    glob_pattern: Optional[str] = None
    """Only files matching this glob pattern are injected; it is matched at any depth
    (e.g. `"run_*/*.mcap"`). `None` keeps every file with a supported extension."""

    min_file_age_s: Optional[float] = None
    """Files modified less than this many seconds ago are skipped, since they may still be
    written; they are considered again at the next scan. `None` disables the check."""

    on_error: WatchdogError = WatchdogError.QUARANTINE
    """What to do when the injection of a file fails (see `WatchdogError`)."""

    retry_number: int = 2
    """How many times a failed file is tried again before being quarantined. Used only when
    `on_error` is `WatchdogError.RETRY`."""

    max_queue_size: int = 100
    """Maximum number of files waiting to be injected. When the queue is full, the remaining
    new files wait for the next scan. Daemon mode only: in single-pass mode the queue is
    unbounded."""

    host: str = "localhost"
    """Hostname or IP of the Mosaico server."""

    port: int = 6726
    """Port of the Mosaico server."""

    mosaico_api_key: Optional[str] = None
    """API key for the Mosaico server. If provided, it must have the `write` permission."""

    tls_cert_path: Optional[str] = None
    """Path to the TLS certificate file for a secure connection to the Mosaico server."""

    enable_tls: bool = False
    """Use TLS for the connection to the Mosaico server."""

    log_level: str = "INFO"
    """Logging verbosity level ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"). It is also
    passed to the injectors, which reconfigure logging when they start."""

    def is_deamon_mode(self) -> bool:
        """Returns `True` if the Watchdog runs in daemon mode."""
        return self.mode == WatchdogMode.DAEMON


class Watchdog:
    """
    Scans a folder and injects every new MCAP or ROS bag file into a Mosaico server.

    The files are listed by a `LocalFileSource` (filtered by supported extension,
    `glob_pattern` and `min_file_age_s`), skipped if the `FileStateStore` already knows them,
    and injected one at a time with the injector chosen by the `InjectorDispatcher`. Each
    outcome is recorded: `loaded` in `.already_loaded.txt`, `quarantined` in
    `.quarantine_file.txt`.

    - Single-pass: `run()` scans once in the calling thread, injects every new file, then
      returns.
    - Daemon: `run()` starts a producer thread that scans the folder every
      `polling_interval_s` seconds and fills the queue, while the calling thread injects the
      queued files, until Ctrl-C or an error stops it.

    Example:
        ```python
        from pathlib import Path

        watchdog = Watchdog(WatchdogConfig(path_to_monitor=Path("/data/bags")))
        watchdog.run()
        ```
    """

    def __init__(self, watchdog_config: WatchdogConfig):
        """
        Args:
            watchdog_config (WatchdogConfig): The configuration of the Watchdog.

        Raises:
            ValueError: If `path_to_monitor` does not exist or is not a folder.
            RuntimeError: If a state file exists in `path_to_monitor` but cannot be read.
        """
        self._cfg: WatchdogConfig = watchdog_config
        """The configuration of the Watchdog."""

        self._file_source: FileSource = LocalFileSource(
            path_to_monitor=self._cfg.path_to_monitor,
            supported_extensions=InjectorDispatcher.list_supported_ext(),
            glob_pattern=self._cfg.glob_pattern,
            min_file_age_s=self._cfg.min_file_age_s,
        )
        """Lists the candidate files of `path_to_monitor`, filtered by the supported
        extensions, `glob_pattern` and `min_file_age_s`."""

        self._files_queue: Queue[FileRef] = Queue(
            maxsize=self._cfg.max_queue_size if self._cfg.is_deamon_mode() else 0
        )
        """Files waiting to be injected: filled by the producer (`_scan_folder()`), emptied by
        the consumer (`_consumer_loop()`). Bounded by `max_queue_size` in daemon mode,
        unbounded in single-pass mode."""

        self.file_store_state = FileStateStore(self._cfg.path_to_monitor)
        """Records which files are loaded, quarantined or in the queue."""

        self._stop_event = threading.Event()
        """When set, the producer stops scanning and the consumer stops taking files from the
        queue. Cleared at the start of `run()`."""

        self._producer_th: Optional[threading.Thread] = None
        """The producer thread, in daemon mode only. `None` when it is not running."""

    def _scan_folder(self):
        """
        Adds the new files of the monitored folder to the queue.

        Reads the state files again (`FileStateStore.refresh()`), then, for every listed file
        that is not loaded, quarantined or already queued, marks it `in_queue` and adds it to
        the queue. Stops early when the queue is full, so the remaining files wait for the next
        scan, or when the stop event is set. A file whose path cannot be recorded is skipped
        with a warning.
        """
        self.file_store_state.refresh()

        # Do scanning + filtering here
        for f in self._file_source.list_files():
            if self._stop_event.is_set():
                break

            file_state = self.file_store_state.get_state(f)

            if file_state and file_state in (
                FileState.IN_QUEUE,
                FileState.LOADED,
                FileState.QUARANTINED,
            ):
                continue

            if (
                self._files_queue.full()
            ):  # safe: the producer is the only thread that adds
                break  # the rest waits for the next scan
            try:
                self.file_store_state.mark_in_queue(f)  # mark first...
            except RuntimeError as e:
                logger.warning(f"Skipping '{f.relative_path}': {e}")
                continue
            self._files_queue.put_nowait(f)  # ...then add to the queue

    def _inject_file(self, file_ref: FileRef):
        """
        Injects one file into the Mosaico server and records the outcome.

        The file gets its local path (`FileSource.open_local()`), its injector
        (`InjectorDispatcher.get_injector()`) and its sequence name
        (`sequence_name_from_fileref()`), then the injector runs. On success the file is
        marked `loaded`. On failure the `on_error` policy applies: `RAISE` re-raises the error,
        `QUARANTINE` marks the file `quarantined` at once, `RETRY` tries again up to
        `retry_number` more times and then marks it `quarantined`.

        Args:
            file_ref (FileRef): The file to inject.

        Raises:
            KeyboardInterrupt: If the user interrupted the injection (Ctrl-C). The file is not
                marked and stays `in_queue`, so the next run tries it again.
            Exception: Any injection error, when `on_error` is `RAISE`.
        """

        attempts = (
            self._cfg.retry_number + 1
            if self._cfg.on_error is WatchdogError.RETRY
            else 1
        )

        for attempt in range(1, attempts + 1):
            try:
                # Download (if in Object Store) and return its absolute file path in the local filesystem
                file_path = self._file_source.get_local_path(file_ref)
                injector_cls = InjectorDispatcher.get_injector(file_path)
                sequence_name = sequence_name_from_fileref(file_ref)

                injector = instanciate_injector(
                    path=file_path,
                    sequence_name=sequence_name,
                    host=self._cfg.host,
                    port=self._cfg.port,
                    mosaico_api_key=self._cfg.mosaico_api_key,
                    tls_cert_path=self._cfg.tls_cert_path,
                    enable_tls=self._cfg.enable_tls,
                    log_level=self._cfg.log_level,
                    injector_cls=injector_cls,
                )

                status = injector.run()

                if status is InjectionStatus.CANCELLED:
                    # Ctrl-C stops everything and does not mark the file as loaded
                    raise KeyboardInterrupt

                self.file_store_state.mark_loaded(file_ref)
                return

            except Exception as e:
                if self._cfg.on_error is WatchdogError.RAISE:
                    raise
                logger.warning(
                    f"Attempt {attempt}/{attempts} failed for '{file_ref.relative_path}': {e}"
                )

        logger.warning(
            f"File '{file_ref.relative_path}' is quarantined because failed to load in Mosaico too many times."
        )
        self.file_store_state.mark_quarantined(file_ref)

    # Producer functions
    def _start_producer(self):
        """Starts the producer thread (`_producer_loop()`), unless it is already running."""
        logger.info("Start producer thread")

        if not self._producer_th:
            self._producer_th = threading.Thread(target=self._producer_loop)
            self._producer_th.start()

    def _stop_producer(self):
        """
        Sets the stop event and waits for the producer thread to end. The event is shared, so
        the consumer loop stops too. Does nothing if the producer is not running.
        """

        if not self._producer_th:
            return

        logger.info("Requested stop for producer thread")
        self._stop_event.set()

        if self._producer_th.is_alive():
            self._producer_th.join()

        self._producer_th = None
        logger.info("Producer thread correctly joined")

    def _producer_loop(self):
        """
        Body of the producer thread (daemon mode): scans the folder every
        `polling_interval_s` seconds, counted from the start of each scan, until the stop
        event is set. A failed scan is logged and does not stop the loop.
        """
        while not self._stop_event.is_set():
            start_time = time.monotonic()

            # Do the actual work
            try:
                self._scan_folder()
            except Exception as e:  # one bad scan must not kill the producer
                logger.warning(f"Scan failed: {e}")

            elapsed = time.monotonic() - start_time
            self._stop_event.wait(max(0.0, self._cfg.polling_interval_s - elapsed))

    # Consumer functions
    def _consumer_loop(self, single_pass: bool = False):
        """
        Takes the files from the queue and injects them one at a time, in the calling thread,
        until the stop event is set.

        Args:
            single_pass (bool): If `True`, return as soon as the queue is empty, since it was
                filled before; otherwise keep waiting for the files the producer adds.

        Raises:
            KeyboardInterrupt: If the user interrupted an injection (Ctrl-C).
            Exception: Any injection error, when `on_error` is `RAISE`.
        """
        while not self._stop_event.is_set():
            try:
                file_to_inject: FileRef = self._files_queue.get(timeout=0.5)
            except Empty:
                if single_pass:
                    return
                else:
                    continue

            self._inject_file(file_to_inject)

    # --- Public API

    def run(self):
        """
        Runs the Watchdog in the configured mode, blocking the calling thread.

        - Single-pass: scans the folder once, injects every new file, then returns.
        - Daemon: starts the producer thread and injects the queued files until Ctrl-C or an
          error stops it. The producer thread is always stopped and joined before `run()`
          raises.

        Raises:
            KeyboardInterrupt: On Ctrl-C. The file being injected is not marked as loaded.
            Exception: Any injection error, when `on_error` is `RAISE`.
        """
        self._stop_event.clear()

        if self._cfg.mode is WatchdogMode.SINGLEPASS:
            self._scan_folder()
            self._consumer_loop(single_pass=True)

        else:
            self._start_producer()
            try:
                self._consumer_loop()
            finally:
                self._stop_producer()  # sets the event and joins the producer


# --- CLI Entry Point ---


def main():
    """
    Console script entry point (`mosaicolabs.watchdog`): parses the arguments, builds the
    `WatchdogConfig` and runs the Watchdog until the pass ends (`single-pass`) or until it is
    stopped (`daemon`).

    Exits with code 130 on Ctrl-C, and with code 1 if the Watchdog cannot start (e.g. missing
    folder, unreadable state file) or stops on an error with `--on-error raise`.
    """
    parser = argparse.ArgumentParser(
        description="Watch a folder and inject its MCAP and ROS bag files into Mosaico."
    )
    parser.add_argument(
        "path_to_monitor", type=Path, help="Folder to watch, subfolders included"
    )
    parser.add_argument(
        "--mode",
        choices=[mode.value for mode in WatchdogMode],
        default=WatchdogMode.SINGLEPASS.value,
        help="'single-pass': scan once and exit. 'daemon': keep scanning until stopped "
        "(Default: single-pass)",
    )
    parser.add_argument(
        "--polling-interval",
        type=float,
        default=5.0,
        help="Seconds between two scans, daemon mode only (Default: 5.0)",
    )
    parser.add_argument(
        "--glob-pattern",
        default=None,
        help="Only inject files matching this glob pattern, at any depth (e.g. 'run_*/*.mcap')",
    )
    parser.add_argument(
        "--min-file-age",
        type=float,
        default=30.0,
        help="Skip files modified less than this many seconds ago, as they may still be "
        "written (Default: 30.0)",
    )
    parser.add_argument(
        "--on-error",
        choices=[policy.value for policy in WatchdogError],
        default=WatchdogError.QUARANTINE.value,
        help="What to do when an injection fails (Default: quarantine)",
    )
    parser.add_argument(
        "--retry-number",
        type=int,
        default=2,
        help="Retries before quarantining a file, with '--on-error retry' (Default: 2)",
    )
    parser.add_argument(
        "--max-queue-size",
        type=int,
        default=100,
        help="Max files waiting to be injected, daemon mode only (Default: 100)",
    )

    # Connection Arguments
    parser.add_argument("--host", default="localhost", help="Mosaico Server Host")
    parser.add_argument(
        "--port", type=int, default=6726, help="Mosaico Server Port (Default: 6726)"
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help=(
            "Mosaico API-Key. Prefer setting the MOSAICO_API_KEY environment variable "
            "instead, to avoid leaking the key via shell history or the process list "
            "(e.g. `ps aux`); --api-key takes precedence if both are set."
        ),
    )
    parser.add_argument(
        "--tls-cert", default=None, help="Path of the .cert file for secure connection"
    )
    parser.add_argument(
        "--enable-tls",
        action="store_true",
        help="Use TLS for the connection to the Mosaico server",
    )

    parser.add_argument(
        "--log",
        "-l",
        help="Set the logging verbosity level",
        default="INFO",
        type=str.upper,  # Automatically converts input (e.g., 'debug') to uppercase
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    args = parser.parse_args()

    setup_sdk_logging(level=args.log, pretty=True)

    config = WatchdogConfig(
        path_to_monitor=args.path_to_monitor,
        mode=WatchdogMode(args.mode),
        polling_interval_s=args.polling_interval,
        glob_pattern=args.glob_pattern,
        min_file_age_s=args.min_file_age,
        on_error=WatchdogError(args.on_error),
        retry_number=args.retry_number,
        max_queue_size=args.max_queue_size,
        host=args.host,
        port=args.port,
        mosaico_api_key=args.api_key or os.environ.get("MOSAICO_API_KEY"),
        tls_cert_path=args.tls_cert,
        enable_tls=args.enable_tls,
        log_level=args.log,
    )

    try:
        Watchdog(config).run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        # e.g. missing folder, unreadable state file, or an injection error with --on-error raise
        logger.error(f"Watchdog stopped: '{e}'")
        sys.exit(1)


if __name__ == "__main__":
    main()
