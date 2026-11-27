"""
LeRobot Dataset Extraction Tool.

`LeRobotSequencesExtractor` exports the Mosaico sequences ingested by the LeRobot bridge (one
per episode, see `LeRobotInjector`) back to a LeRobot v3.0 dataset folder.

The episodes are read from the `_lerobot_` metadata of the sequences and merged by a
`MosaicoToLeRobotLoader`. Their data is then streamed from the sequences and written to the data
files by a `LeRobotWriter`, and the `meta/` folder by `LeRobotDatasetMeta.dump`, with the stats
of the episodes and `meta/stats.json` computed from the data files written. The videos are not
written yet.

Typical usage as a script:
    $ python -m mosaicolabs.bridges.lerobot.sequences_extractor \\
        pusht_episode_000000 pusht_episode_000001 --lerobot-path ./exports --name pusht

Typical usage as a library:
    config = LeRobotExtractorConfig(
        saving_path=Path("exports"),
        sequence_names=["pusht_episode_000000", "pusht_episode_000001"],
        dataset_name="pusht",
    )
    extractor = LeRobotSequencesExtractor(config)
    status = extractor.run()  # `PARTIAL` if some sequences were dropped
    meta = extractor.meta
"""

import argparse
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import List, Optional

from rich.console import Console
from rich.live import Live

from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.logging_config import get_logger, setup_sdk_logging

from .loader import LeRobotLoader
from .metadata import LeRobotDatasetMeta, LeRobotEpisode, LeRobotInfo
from .metadata.stats import compute_episode_stats
from .mosaico_loader import MosaicoToLeRobotLoader
from .ui_lerobot import LeRobotProgressManager
from .writer import (
    _DEFAULT_CHUNKS_SIZE,
    _DEFAULT_DATA_FILES_SIZE_IN_MB,
    LeRobotWriter,
)

# Set the hierarchical logger
logger = get_logger(__name__)


# --- Configuration ---
@dataclass
class LeRobotExtractorConfig:
    """
    The configuration of the LeRobot dataset extraction.

    Attributes:
        saving_path (Path): The folder where the dataset folder `dataset_name` is created.
        sequence_names (List[str]): The sequences to export, one episode each, in episode order.
        dataset_name (str): The name of the dataset folder, created under `saving_path`.
            Defaults to `"LeRobotDataset"`.
        host (str): The hostname of the Mosaico server. Defaults to `"localhost"`.
        port (int): The port of the Mosaico server. Defaults to `6726`.
        log_level (str): The log level. Defaults to `"INFO"`.
        mosaico_api_key (Optional[str]): The API key for authentication on the Mosaico server;
            it must have (at least) the `read` permission. Defaults to None.
        tls_cert_path (Optional[str]): Path to the TLS certificate file for a secure connection
            to the Mosaico server. Defaults to None.
        enable_tls (bool): Enable standard one-way TLS (server authenticated only). Ignored if
            `tls_cert_path` is provided. Defaults to False.
        overwrite (bool): If True, replace the dataset folder if it already exists. Defaults to
            False.
        chunks_size (Optional[int]): The maximum number of data files of each `chunk-XXX`
            folder. Defaults to None: the one of the source dataset, or LeRobot's default
            (1000) if it has none.
        data_files_size_in_mb (Optional[int]): The size, in MB, beyond which a new data file
            is started. Defaults to None: the one of the source dataset, or LeRobot's default
            (100) if it has none.
    """

    saving_path: Path
    """The folder where the dataset folder `dataset_name` is created."""

    sequence_names: List[str]
    """The sequences to export, one episode each, in episode order."""

    dataset_name: str = "LeRobotDataset"
    """The name of the dataset folder, created under `saving_path`."""

    host: str = "localhost"
    """The hostname of the Mosaico server."""

    port: int = 6726
    """The port of the Mosaico server."""

    log_level: str = "INFO"
    """The log level."""

    mosaico_api_key: Optional[str] = None
    """The API key for authentication on the Mosaico server (`read` permission)."""

    tls_cert_path: Optional[str] = None
    """Path to the TLS certificate file for a secure connection to the Mosaico server."""

    enable_tls: bool = False
    """Enable standard one-way TLS (server authenticated only)."""

    overwrite: bool = False
    """If True, replace the dataset folder if it already exists."""

    chunks_size: Optional[int] = None
    """The maximum number of data files of each `chunk-XXX` folder, or None for the one of the
    source dataset (LeRobot's default if it has none)."""

    data_files_size_in_mb: Optional[int] = None
    """The size, in MB, beyond which a new data file is started, or None for the one of the
    source dataset (LeRobot's default if it has none)."""

    def __post_init__(self):
        """
        Raises:
            ValueError: If `chunks_size` or `data_files_size_in_mb` is not a positive integer.
        """
        for name in ("chunks_size", "data_files_size_in_mb"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"`{name}` must be greater than 0, got {value}.")


# --- Main Extractor Class ---


class ExtractionStatus(Enum):
    """
    Outcome of `LeRobotSequencesExtractor.run()`.

    Errors are not a status: `run()` raises them, including when no sequence can be
    exported. `PARTIAL` reports a run that exported only part of the sequences.
    """

    COMPLETED = "completed"
    """Every sequence was exported."""

    PARTIAL = "partial"
    """Only part of the sequences was exported: the others were dropped (see
    `LeRobotSequencesExtractor.dropped_sequences`)."""


def _first_set(*values: Optional[int]) -> int:
    """Returns the first of `values` that is not None: the last one must be set."""
    return next(value for value in values if value is not None)


def _close_tmp_folder(tmp_path: Path, saving_path: Path, save: bool) -> None:
    """
    Closes the temporary folder the dataset was written to: with `save` it replaces
    `saving_path`, otherwise it is removed with the partial files written to it.

    A folder cannot replace a non-empty one: an existing `saving_path` is first renamed aside,
    instantly unlike its removal, and removed only once the new dataset is in place. Hence
    `saving_path` is missing only between two renames.

    Args:
        tmp_path (Path): The temporary folder holding the dataset.
        saving_path (Path): The root folder of the dataset.
        save (bool): Whether the dataset in `tmp_path` is complete and replaces
            `saving_path`.
    """
    if save:
        old_path = tmp_path.with_name(f"{tmp_path.name}.old")
        if saving_path.exists():
            saving_path.rename(old_path)
        tmp_path.rename(saving_path)
        shutil.rmtree(old_path, ignore_errors=True)
    else:
        shutil.rmtree(tmp_path, ignore_errors=True)


class LeRobotSequencesExtractor:
    """
    Exports Mosaico sequences ingested by the LeRobot bridge to a LeRobot v3.0 dataset folder.

    Each sequence is an episode, read from its `_lerobot_` metadata and merged with the previous
    ones by a `MosaicoToLeRobotLoader`: a sequence that does not exist, has no valid `_lerobot_`
    metadata or cannot be merged is dropped with a warning. The data of the episodes is written
    by a `LeRobotWriter`, and the `meta/` folder by `LeRobotDatasetMeta.dump`.

    Example:
        ```python
        from pathlib import Path
        from mosaicolabs.bridges.lerobot.sequences_extractor import (
            LeRobotExtractorConfig,
            LeRobotSequencesExtractor,
        )

        config = LeRobotExtractorConfig(
            saving_path=Path("exports"),
            sequence_names=["pusht_episode_000000", "pusht_episode_000001"],
            dataset_name="pusht",
        )
        extractor = LeRobotSequencesExtractor(config)
        status = extractor.run()
        meta = extractor.meta
        ```
    """

    def __init__(self, config: LeRobotExtractorConfig):
        """
        Args:
            config (LeRobotExtractorConfig): The extraction configuration.
        """
        self._cfg = config

        self._meta: Optional[LeRobotDatasetMeta] = None
        """The metadata written by the last `run()`."""

        self._dropped_sequences: List[str] = []
        """The sequences dropped in the last `run()`."""

        self._console = Console(stderr=True)
        setup_sdk_logging(
            level=self._cfg.log_level.upper(), pretty=True, console=self._console
        )

    @property
    def meta(self) -> Optional[LeRobotDatasetMeta]:
        """
        The metadata written under `saving_path / dataset_name` by the last `run()`, `None`
        before.
        """
        return self._meta

    @property
    def dropped_sequences(self) -> List[str]:
        """
        The sequences dropped in the last `run()`: they do not exist, have no valid
        `_lerobot_` metadata or cannot be merged with the previous ones. Empty before the
        first run, and after a run where every sequence was exported.
        """
        return list(self._dropped_sequences)

    def _write_dataset(
        self,
        loader: MosaicoToLeRobotLoader,
        saving_path: Path,
        ui: LeRobotProgressManager,
    ) -> LeRobotDatasetMeta:
        """
        Writes the dataset under `saving_path`, in three steps:

        1. the data of the episodes, streamed from their sequences (see `LeRobotWriter`), which
           sets the `data/chunk_index` and `data/file_index` of each one to the data file it
           was written to;
        2. the stats of each episode, computed from the data files written (see
           `_compute_episodes_stats`);
        3. the `meta/` folder (see `LeRobotDatasetMeta.dump`), with `meta/stats.json`
           combined from the stats of the episodes.

        Args:
            loader (MosaicoToLeRobotLoader): The resolved loader of the sequences: the
                `info`, episodes and tasks of the dataset, and the streams of its episodes.
            saving_path (Path): The root folder of the dataset.
            ui (LeRobotProgressManager): The progress UI, advanced as the episodes are
                written and their stats computed.

        Returns:
            LeRobotDatasetMeta: The metadata written.

        Raises:
            ValueError: If an episode cannot be written (see `LeRobotWriter.write_episode`).
            KeyError: If a topic of an episode is missing (see `LeRobotWriter.write_episode`).
        """
        # The limits of the data files: from the config, else from the source dataset, else
        # LeRobot's defaults. They are also written to `meta/info.json`.
        info = replace(
            loader.info,
            chunks_size=_first_set(
                self._cfg.chunks_size, loader.info.chunks_size, _DEFAULT_CHUNKS_SIZE
            ),
            data_files_size_in_mb=_first_set(
                self._cfg.data_files_size_in_mb,
                loader.info.data_files_size_in_mb,
                _DEFAULT_DATA_FILES_SIZE_IN_MB,
            ),
        )
        tasks = loader.tasks
        # The episodes as written: with the data file of each one
        written_episodes: List[LeRobotEpisode] = []
        with LeRobotWriter(info, saving_path, tasks) as writer:
            for streamer in loader:
                episode = streamer.episode_metadata
                logger.info(
                    f"Writing episode {episode.episode_index} (sequence "
                    f"'{streamer.sequence_name}')..."
                )

                ui.start_episode(episode.episode_index, episode.length)

                try:
                    written_episodes.append(writer.write_episode(streamer, ui))

                except Exception:
                    ui.finish_episode(succeeded=False)
                    raise

                ui.finish_episode(succeeded=True)

        # Only now it is possible to compute the per episode stats (now that the parquet files are closed)
        episodes = self._compute_episodes_stats(info, written_episodes, saving_path, ui)

        # Every episode has its stats: `dump` reads no data file, it only combines them into
        # `meta/stats.json` and writes the `meta/` folder
        logger.info("Writing the 'meta/' folder...")
        meta = LeRobotDatasetMeta.from_data(saving_path, info, episodes, tasks)
        meta.dump()
        return meta

    def _compute_episodes_stats(
        self,
        info: LeRobotInfo,
        episodes: List[LeRobotEpisode],
        saving_path: Path,
        ui: LeRobotProgressManager,
    ) -> List[LeRobotEpisode]:
        """
        Computes the stats of each episode from its rows in its data file (see
        `compute_episode_stats`), advancing the episode stats bar once per episode.

        Args:
            info (LeRobotInfo): The `info` of the dataset.
            episodes (List[LeRobotEpisode]): The episodes of the dataset, as written (see
                `LeRobotWriter.write_episode`).
            saving_path (Path): The root folder of the dataset, holding the data files.
            ui (LeRobotProgressManager): The progress UI, whose episode stats bar is created
                here.

        Returns:
            List[LeRobotEpisode]: The episodes of the dataset, in episode order, with their
                stats. The episodes given are left unchanged.

        Raises:
            RuntimeError: If the data file of an episode holds no row of it.
            FileNotFoundError: If the data file of an episode is missing.
            pa.ArrowInvalid: If the data file of an episode is not a readable parquet file
                (e.g. still being written).
        """
        logger.info("Computing the stats of the episodes...")
        ui.setup_episode_stats()

        episodes_with_stats: List[LeRobotEpisode] = []
        for episode in episodes:
            data_path = info.get_path_to_data(
                saving_path, episode.data_chunk_index, episode.data_file_index
            )
            stats = compute_episode_stats(
                data_path, episode.episode_index, info.features
            )
            episodes_with_stats.append(replace(episode, stats=stats))
            ui.advance_episode_stats()

        return episodes_with_stats

    def run(self) -> ExtractionStatus:
        """
        Connects to the Mosaico server, builds the metadata of the dataset from the sequences
        and writes the dataset under `saving_path / dataset_name` (see `_write_dataset`).

        An existing dataset folder is checked before connecting: it is replaced only with
        `overwrite`, and only if it is empty or a LeRobot dataset (see
        `LeRobotLoader.is_dataset`). The dataset is written to a temporary folder in
        `saving_path`, next to the dataset folder, and swapped in only once complete (see
        `_close_tmp_folder`): a failure leaves the dataset folder untouched and discards the
        partial files, which is logged as an error.

        A sequence that cannot be exported is dropped with a warning, and the dropped ones
        are listed again at the end (see `dropped_sequences`). The metadata written is in
        `meta`.

        Note:
            If the process is killed without a chance to clean up (e.g. `SIGKILL`, a power
            loss), the hidden temporary folders `.{dataset_name}.*` (and
            `.{dataset_name}.*.old` during the swap) are left in `saving_path`, where they
            can take as much space as a whole dataset. They are not removed by the next
            extraction and must be deleted by hand.

        Returns:
            ExtractionStatus: `COMPLETED` if every sequence was exported, `PARTIAL` if some
                were dropped.

        Raises:
            FileExistsError: If the dataset folder exists and `overwrite` is False.
            ValueError: If the dataset folder is neither empty nor a LeRobot dataset, or an
                episode cannot be written (see `LeRobotWriter.write_episode`).
            RuntimeError: If no sequence can be exported.
            KeyError: If a topic of an episode is missing (see `LeRobotWriter.write_episode`).
        """

        # Resetting internal data
        self._meta = None
        self._dropped_sequences = []

        saving_path = self._cfg.saving_path / self._cfg.dataset_name
        if saving_path.exists():
            if not self._cfg.overwrite:
                raise FileExistsError(
                    f"Impossible to save to '{saving_path}' since it already exists. "
                    "Pass overwrite=True to replace it."
                )
            if not saving_path.is_dir() or (
                any(saving_path.iterdir()) and not LeRobotLoader.is_dataset(saving_path)
            ):
                raise ValueError(
                    f"Refusing to overwrite '{saving_path}': it is not empty and not a "
                    "LeRobot dataset."
                )

        logger.info(f"Connecting to Mosaico at '{self._cfg.host}:{self._cfg.port}'...")
        with MosaicoClient.connect(
            host=self._cfg.host,
            port=self._cfg.port,
            api_key=self._cfg.mosaico_api_key,
            enable_tls=self._cfg.enable_tls,
            tls_cert_path=self._cfg.tls_cert_path,
        ) as mclient:
            with MosaicoToLeRobotLoader(mclient, self._cfg.sequence_names) as loader:
                saving_path.parent.mkdir(parents=True, exist_ok=True)
                tmp_path = Path(
                    tempfile.mkdtemp(
                        prefix=f".{saving_path.name}.", dir=saving_path.parent
                    )
                )

                succeeded = False
                try:
                    ui = LeRobotProgressManager(loader, len(loader.episodes))
                    ui.setup()

                    with Live(ui.progress, console=self._console):
                        meta = self._write_dataset(loader, tmp_path, ui)
                    succeeded = True

                finally:
                    if not succeeded:
                        logger.error(
                            f"The extraction to '{saving_path}' stopped before writing "
                            "all of the LeRobot dataset: the partial files are discarded."
                        )
                    _close_tmp_folder(tmp_path, saving_path, save=succeeded)

                self._meta = replace(meta, root=saving_path)
                # Before the loader is closed: it cannot be used afterwards
                self._dropped_sequences = loader.dropped_sequences

        logger.info(
            f"LeRobot dataset of {meta.n_episodes} episode(s) written to '{saving_path}'."
        )

        if self._dropped_sequences:
            logger.warning(
                f"{len(self._dropped_sequences)} of {len(self._cfg.sequence_names)} "
                f"sequence(s) dropped (see the warnings above): {self._dropped_sequences}"
            )
            return ExtractionStatus.PARTIAL
        return ExtractionStatus.COMPLETED


# --- CLI Entry Point ---


def lerobot_sequences_extractor(argv: Optional[List[str]] = None):
    """
    Command-line entry point.
    Parses arguments, sets up configuration, and initiates the sequences extractor.

    Args:
        argv (Optional[List[str]]): The command-line arguments (`sys.argv[1:]` if `None`).
    """
    parser = argparse.ArgumentParser(
        description=(
            "Extracts sequences ingested by the LeRobot bridge from Mosaico into a LeRobot "
            "v3.0 dataset"
        )
    )

    # Required Arguments
    parser.add_argument(
        "sequence_names",
        nargs="+",
        help="Names of the Mosaico sequences to extract, one episode each, in episode order",
    )
    parser.add_argument(
        "--lerobot-path",
        dest="saving_path",
        metavar="PATH",
        type=Path,
        required=True,
        help="Folder where the LeRobot dataset folder is created",
    )
    parser.add_argument(
        "--name",
        "-n",
        dest="dataset_name",
        default="LeRobotDataset",
        help="Name of the LeRobot dataset folder, created under --lerobot-path",
    )

    # Connection Arguments
    parser.add_argument("--host", default="localhost", help="Mosaico Server Host")
    parser.add_argument(
        "--port", type=int, default=6726, help="Mosaico Server Port (Default: 6726)"
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
    parser.add_argument(
        "--tls-cert", default=None, help="Path to the TLS certificate file"
    )
    parser.add_argument(
        "--enable-tls",
        action="store_true",
        help="Whether Mosaico Server requires tls",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Replace the LeRobot dataset folder if it already exists",
    )
    parser.add_argument(
        "--log",
        "-l",
        type=str.upper,  # Automatically converts input (e.g., 'debug') to uppercase
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging verbosity level. Default is INFO",
    )

    args = parser.parse_args(argv)

    config = LeRobotExtractorConfig(
        saving_path=args.saving_path,
        sequence_names=args.sequence_names,
        dataset_name=args.dataset_name,
        host=args.host,
        port=args.port,
        log_level=args.log,
        mosaico_api_key=args.api_key or os.environ.get("MOSAICO_API_KEY"),
        tls_cert_path=args.tls_cert,
        enable_tls=args.enable_tls,
        overwrite=args.overwrite,
    )

    # --- Execution ---
    extractor = LeRobotSequencesExtractor(config)
    try:
        status = extractor.run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        # Exit non-zero so calling scripts/CI can detect the failure
        logger.error(f"LeRobot extraction failed: {e}")
        sys.exit(1)

    # Some sequences were dropped (already logged): exit non-zero, as for a failure
    if status is ExtractionStatus.PARTIAL:
        sys.exit(1)


if __name__ == "__main__":
    lerobot_sequences_extractor()
