"""
Persistent record of what the Watchdog did with each file.

The `FileStateStore` keeps the state of every file the Watchdog has seen, keyed by its path
relative to the monitored folder (e.g. `run_01/drive.mcap`): `loaded` and `quarantined` are
persisted in `.already_loaded.txt` and `.quarantine_file.txt` inside `watched_dir`, while
`in_queue` lives in memory only. A file with no state is new.
"""

import threading
from enum import Enum
from pathlib import Path
from typing import IO, Any, Dict, List, Optional

from mosaicolabs.logging_config import get_logger

from .file_source import FileRef

# Set the hierarchical logger
logger = get_logger(__name__)


class FileState(Enum):
    """The state of a file known to the `FileStateStore`. A file with no state is new."""

    LOADED = 1
    """Injected successfully: listed in `.already_loaded.txt`."""

    QUARANTINED = 2
    """Injection failed: listed in `.quarantine_file.txt`."""

    IN_QUEUE = 3
    """Queued or currently being loaded. Kept in memory only."""


ALREADY_LOADED_FILE_NAME = ".already_loaded.txt"
"""Name of the file, inside `watched_dir`, listing the files loaded successfully."""

QUARANTINED_FILE_NAME = ".quarantine_file.txt"
"""Name of the file, inside `watched_dir`, listing the files whose injection failed. It can
be edited by hand: removing a line makes the Watchdog try that file again."""


class FileStateStore:
    """
    Keeps the state of every file the Watchdog has seen: `loaded`, `quarantined` or
    `in_queue`. A file with no state is new.

    The `loaded` and `quarantined` states are persisted in `.already_loaded.txt` and
    `.quarantine_file.txt` inside `watched_dir`: UTF-8, one relative path per line, only
    appended to by the store. Both files are read again by `refresh()`, so manual edits take
    effect while the Watchdog runs, and a missing file counts as empty. The `in_queue` state
    lives in memory only and starts empty at every Watchdog start.

    A path in more than one state counts as `quarantined`, then `loaded`, then `in_queue`.
    The public methods hold a lock, since the Producer and the Consumer threads use the store
    at the same time.
    """

    def __init__(self, watched_dir: Path):
        """
        Args:
            watched_dir (Path): The folder holding the state files. The Watchdog passes the
                monitored folder itself (`path_to_monitor`), so it must be writable.

        Raises:
            ValueError: If `watched_dir` does not exist or is not a folder.
            RuntimeError: If a state file exists but cannot be read.
        """
        self._watched_dir: Path = watched_dir.resolve()
        self.already_loaded_path = self._watched_dir.joinpath(ALREADY_LOADED_FILE_NAME)
        self.quarantined_path = self._watched_dir.joinpath(QUARANTINED_FILE_NAME)

        if not self._watched_dir.exists() or not self._watched_dir.is_dir():
            raise ValueError(
                f"Cannot instantiate {FileStateStore.__name__} since passed path `{self._watched_dir}` is invalid (does not exist or is not a folder)"
            )

        self._file_cache: Dict[str, FileState] = {}
        """State of every known file, keyed by relative path: the paths listed in the two
        state files plus the in-memory `in_queue` entries."""

        self._lock = threading.Lock()
        """Lock to avoid race conditions"""

        # Fill the cache. Unlike refresh(), an unreadable state file raises here, so the
        # Watchdog refuses to start instead of treating every file as new
        self._file_cache.update(
            dict.fromkeys(self._read_path(self.already_loaded_path), FileState.LOADED)
        )
        self._file_cache.update(
            dict.fromkeys(self._read_path(self.quarantined_path), FileState.QUARANTINED)
        )

    def _validate_file_path(self, r_path: str) -> Optional[str]:
        """
        Strips the line ending from `r_path` and checks that it fits the one-path-per-line
        format of the state files. Returns the stripped path, or `None` if it is blank or
        contains a line break.
        """

        # Strip only the line ending: spaces can be part of a file name
        out = r_path.rstrip("\r\n")

        # Blank lines, and paths that would break the one-path-per-line format, are invalid
        if not out.strip() or "\n" in out or "\r" in out:
            return None

        return out

    def _get_or_create_file(self, path: Path, mode: str) -> IO[Any]:
        """
        Opens `path` with `mode`, in UTF-8. Modes `"a"` and `"w"` create the file if
        missing, mode `"r"` raises `FileNotFoundError`.
        """

        return open(path, mode=mode, encoding="utf-8")

    def _acquire_lock(self):
        """Returns the lock shared by the Producer and Consumer threads."""
        return self._lock

    def _read_path(self, path: Path) -> List[str]:
        """
        Reads the state file at `path` and returns its valid paths, in file order. A missing
        file counts as empty.

        Raises:
            RuntimeError: If the file exists but cannot be read or is not valid UTF-8.
        """

        try:
            with self._get_or_create_file(path, "r") as f_handler:
                sanitized_paths: List[Optional[str]] = [
                    self._validate_file_path(line) for line in f_handler
                ]
        except FileNotFoundError:
            return []  # if file is not present it should be considered empty
        except (OSError, UnicodeDecodeError) as e:
            raise RuntimeError(f"Cannot read state file '{path}': {e}") from e

        return [r_path for r_path in sanitized_paths if r_path]

    def _try_mark(
        self, file_path: str, file_state: FileState, f_handler: Optional[IO] = None
    ):
        """
        Sets the state of `file_path` in the cache and, if `f_handler` is given, appends
        `file_path` to it as a new line. Must be called with the lock held.

        Raises:
            RuntimeError: If `file_path` is blank or contains a line break.
        """

        validated_path = self._validate_file_path(file_path)

        # A path that changes when validated (e.g. a trailing line break) is invalid too
        if validated_path and validated_path == file_path:
            if f_handler:
                f_handler.write(file_path + "\n")
            self._file_cache[file_path] = file_state
        else:
            raise RuntimeError(
                f"Failed to mark {file_path} file as {file_state.name} since it is invalid"
            )

    # Public API

    def refresh(self):
        """
        Reads `.already_loaded.txt` and `.quarantine_file.txt` again, keeping the `in_queue`
        entries. The Watchdog calls it at the start of every scan, so manual edits take
        effect while it runs.

        The cache is replaced only once both files have been read: if one cannot be read, a
        warning is logged and the previous state is kept until the next call.
        """

        with self._acquire_lock():
            try:
                loaded = self._read_path(self.already_loaded_path)
                quarantined = self._read_path(self.quarantined_path)
            except RuntimeError as e:
                logger.warning(f"Skipping cache update because: {e}")
                return

            # Lowest priority first, so later states win: in_queue < loaded < quarantined
            file_cache = {
                p: s for p, s in self._file_cache.items() if s == FileState.IN_QUEUE
            }
            file_cache.update(dict.fromkeys(loaded, FileState.LOADED))
            file_cache.update(dict.fromkeys(quarantined, FileState.QUARANTINED))
            self._file_cache = file_cache

    def mark_loaded(self, file_ref: FileRef):
        """
        Marks the file as `loaded`: appends its relative path to `.already_loaded.txt` and
        replaces its `in_queue` state. The Consumer calls it after a successful injection.

        Args:
            file_ref (FileRef): The file loaded successfully.

        Raises:
            RuntimeError: If its relative path is blank or contains a line break.
            OSError: If `.already_loaded.txt` cannot be written.
        """
        with self._acquire_lock():
            with self._get_or_create_file(self.already_loaded_path, "a") as f_handler:
                self._try_mark(
                    file_ref.relative_path,
                    FileState.LOADED,
                    f_handler,
                )

    def mark_quarantined(self, file_ref: FileRef):
        """
        Marks the file as `quarantined`: appends its relative path to `.quarantine_file.txt`
        and replaces its `in_queue` state. The Consumer calls it when the injection failed
        (see `on_error`).

        Args:
            file_ref (FileRef): The file whose injection failed.

        Raises:
            RuntimeError: If its relative path is blank or contains a line break.
            OSError: If `.quarantine_file.txt` cannot be written.
        """
        with self._acquire_lock():
            with self._get_or_create_file(self.quarantined_path, "a") as f_handler:
                self._try_mark(file_ref.relative_path, FileState.QUARANTINED, f_handler)

    def mark_in_queue(self, file_ref: FileRef):
        """
        Marks the file as `in_queue`, in memory only. The Producer calls it before appending
        the file to the queue, so later scans do not queue it again until its outcome is
        recorded with `mark_loaded()` or `mark_quarantined()`.

        Args:
            file_ref (FileRef): The file about to be queued.

        Raises:
            RuntimeError: If its relative path is blank or contains a line break.
        """
        with self._acquire_lock():
            self._try_mark(file_ref.relative_path, FileState.IN_QUEUE)

    def get_state(self, file_ref: FileRef) -> Optional[FileState]:
        """
        Returns the state of a file.

        Args:
            file_ref (FileRef): The file you want to know the state of.

        Returns:
            Optional[FileState]: The state of the file, or `None` if the file is new.
        """
        with self._acquire_lock():
            return self._file_cache.get(file_ref.relative_path)
