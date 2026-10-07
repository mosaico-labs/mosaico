"""
File Sources for the Watchdog.

A File Source lists the candidate files of a location, subfolders included, and hides
whether that location is a local folder or an object store. Each file is described by an
immutable `FileRef`, which holds no content: `FileSource.open_local()` gives a local path
when the file has to be read.

- `LocalFileSource`: a local folder.
- `ObjectStoreFileSource`: an object store (not implemented yet).
"""

import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Protocol, Tuple


@dataclass(frozen=True)
class FileRef:
    """
    Immutable description of a candidate file found by a File Source. It holds no content.

    Only a File Source creates it. Two `FileRef` are the same file when their
    `relative_path` is equal: `uri`, `size` and `mtime_ns` are excluded from comparison and
    hashing, since size and modification time change between scans.
    """

    relative_path: str
    """Path relative to the monitored folder, with `/` as separator and the extension
    (e.g. `run_01/drive.mcap`). It is the key used by the `FileStateStore` and the base of
    the sequence name."""

    uri: str = field(compare=False)
    """Where the file is: a `file://` URI for local files (e.g.
    `file:///data/bags/run_01/drive.mcap`), or `s3://bucket/key` for object stores (not
    implemented yet). Use `FileSource.open_local()` to get a local path."""

    size: int = field(compare=False)
    """File size in bytes, at scan time."""

    mtime_ns: int = field(compare=False)
    """Last modification time, in nanoseconds since the Unix epoch, at scan time."""

    @property
    def name(self) -> str:
        """The file name, with extension: `run_01/drive.mcap` -> `drive.mcap`."""
        return self.relative_path.split("/")[-1] if self.relative_path else ""


class FileSource(Protocol):
    """
    Contract of a File Source: lists the candidate files of a local folder or an object
    store, and gives access to their content as local files.
    """

    def list_files(self) -> Iterable[FileRef]:
        """
        Lists the candidate files of the location, subfolders included.

        Returns:
            Iterable[FileRef]: One `FileRef` per candidate file.
        """
        ...

    def get_local_path(self, ref: FileRef) -> Path:
        """
        Returns a local path to read the file described by `ref`.

        A local source returns the path of the file itself. An object store source
        downloads the file to a staging folder and returns the path of the copy.

        Args:
            ref (FileRef): A `FileRef` returned by this source's `list_files()`.

        Returns:
            Path: A local path to the content of the file.
        """
        ...


class LocalFileSource:
    """
    `FileSource` for a local folder.

    Lists every file in `path_to_monitor` and, recursively, its subfolders, keeping only the
    files that:

    - have one of the supported extensions;
    - match `glob_pattern`, if set;
    - were last modified at least `min_file_age_s` seconds ago, if set, since a more recent
      file may still be being written.

    A skipped file is not recorded anywhere, so it is evaluated again at the next scan.
    """

    def __init__(
        self,
        path_to_monitor: Path,
        accepted_extensions: Tuple[str, ...],
        glob_pattern: Optional[str] = None,
        min_file_age_s: Optional[float] = None,
    ):
        """
        Args:
            path_to_monitor (Path): The folder to list. It is resolved to an absolute path
                once, so later changes of the working directory do not affect it.
            accepted_extensions (Tuple[str, ...]): The extensions to keep, with or without
                the leading dot (e.g. `InjectorDispatcher.list_supported_ext()`). They are
                normalized to lower case with a leading dot, and file names are matched
                case-sensitively, so `drive.MCAP` is not listed.
            glob_pattern (Optional[str]): A glob pattern the files must also match. As with
                `Path.rglob`, it is matched at any depth (e.g. `"run_*/*.mcap"` also matches
                `a/b/run_01/x.mcap`). Default: None (all files).
            min_file_age_s (Optional[float]): Minimum time, in seconds, since the last
                modification of a file for it to be listed. `None` or `0` disables the
                check. Default: None

        Raises:
            ValueError: If `path_to_monitor` does not exist or is not a folder.
        """

        self._path_to_monitor = path_to_monitor.resolve()

        if not self._path_to_monitor.exists() or not self._path_to_monitor.is_dir():
            raise ValueError(
                f"Cannot instantiate {LocalFileSource.__name__} since passed path `{self._path_to_monitor}` is invalid (does not exist or is not a folder)"
            )

        self._accepted_extensions = tuple(
            e.lower() if e.startswith(".") else f".{e.lower()}"
            for e in accepted_extensions
        )
        self._glob_pattern = glob_pattern
        self._min_file_age_s = min_file_age_s

    def list_files(self) -> Iterable[FileRef]:
        """
        Lists the files of the monitored folder that pass the filters (see the class
        docstring).

        The listing is lazy: the folder is scanned while the result is iterated, and the
        result can be iterated only once, so call `list_files()` again for each scan. A file
        deleted during the scan is skipped.

        Yields:
            FileRef: One per file, with `relative_path` relative to `path_to_monitor`.
        """

        all_files = self._path_to_monitor.rglob(self._glob_pattern or "*")

        for file_path in all_files:
            # Remove what is not a file or is not among the accepted extensions
            if not file_path.is_file() or not file_path.name.endswith(
                self._accepted_extensions
            ):
                continue

            # The file may have been deleted after `rglob` listed it: skip it rather than
            # aborting the whole scan.
            try:
                file_stat = file_path.stat()
            except FileNotFoundError:
                continue

            # Skip files modified less than `_min_file_age_s` seconds ago: they may still be
            # being written. Set the threshold to None to disable the check.
            if (
                self._min_file_age_s
                and time.time() - file_stat.st_mtime < self._min_file_age_s
            ):
                continue

            relative_path = file_path.relative_to(self._path_to_monitor)
            file_uri = file_path.as_uri()
            file_size_bytes = file_stat.st_size
            mtime_ns = file_stat.st_mtime_ns

            file_ref = FileRef(
                relative_path.as_posix(),
                file_uri,
                file_size_bytes,
                mtime_ns,
            )

            yield file_ref

    def get_local_path(self, ref: FileRef) -> Path:
        """
        Returns the path of the file described by `ref`, inside the monitored folder.
        Nothing is copied: the path points to the file itself.

        Args:
            ref (FileRef): A `FileRef` returned by `list_files()`.

        Returns:
            Path: `path_to_monitor / ref.relative_path`.
        """
        return self._path_to_monitor / ref.relative_path


class ObjectStoreFileSource:
    """`FileSource` for an object store (e.g. S3). Not implemented yet."""

    pass
