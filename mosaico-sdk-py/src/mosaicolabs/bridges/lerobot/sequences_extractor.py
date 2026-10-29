"""
LeRobot Dataset Extraction Tool.

`LeRobotSequencesExtractor` exports the Mosaico sequences ingested by the LeRobot bridge (one
per episode, see `LeRobotInjector`) back to a LeRobot v3.0 dataset folder.

The episodes are read from the `_lerobot_` metadata of the sequences and merged by a
`MosaicoToLeRobotLoader`. Their data is then streamed from the sequences and written to the data
files by a `LeRobotWriter`, and the `meta/` folder by `LeRobotDatasetMeta.dump`. The videos and
`meta/stats.json` are not written yet.

Typical usage as a script:
    $ python -m mosaicolabs.bridges.lerobot.sequences_extractor \\
        pusht_episode_000000 pusht_episode_000001 --lerobot_path ./exports/pusht

Typical usage as a library:
    config = LeRobotExtractorConfig(
        saving_path=Path("exports/pusht"),
        sequence_names=["pusht_episode_000000", "pusht_episode_000001"],
    )
    meta = LeRobotSequencesExtractor(config).run()
"""

import argparse
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.logging_config import get_logger, setup_sdk_logging

from .metadata import LeRobotDatasetMeta
from .mosaico_loader import MosaicoToLeRobotLoader
from .writer import LeRobotWriter

# Set the hierarchical logger
logger = get_logger(__name__)


# --- Configuration ---
@dataclass
class LeRobotExtractorConfig:
    """
    The configuration of the LeRobot dataset extraction.

    Attributes:
        saving_path (Path): The root folder of the exported LeRobot dataset.
        sequence_names (List[str]): The sequences to export, one episode each, in episode order.
        host (str): The hostname of the Mosaico server. Defaults to `"localhost"`.
        port (int): The port of the Mosaico server. Defaults to `6726`.
        log_level (str): The log level. Defaults to `"INFO"`.
        mosaico_api_key (Optional[str]): The API key for authentication on the Mosaico server;
            it must have (at least) the `read` permission. Defaults to None.
        tls_cert_path (Optional[str]): Path to the TLS certificate file for a secure connection
            to the Mosaico server. Defaults to None.
        enable_tls (bool): Enable standard one-way TLS (server authenticated only). Ignored if
            `tls_cert_path` is provided. Defaults to False.
        overwrite (bool): If True, replace `saving_path` if it already exists. Defaults to False.
    """

    saving_path: Path
    """The root folder of the exported LeRobot dataset."""

    sequence_names: List[str]
    """The sequences to export, one episode each, in episode order."""

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
    """If True, replace `saving_path` if it already exists."""


# --- Main Extractor Class ---


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
            saving_path=Path("exports/pusht"),
            sequence_names=["pusht_episode_000000", "pusht_episode_000001"],
        )
        meta = LeRobotSequencesExtractor(config).run()
        ```
    """

    def __init__(self, config: LeRobotExtractorConfig):
        """
        Args:
            config (LeRobotExtractorConfig): The extraction configuration.
        """
        self._cfg = config

        from rich.console import Console

        self._console = Console(stderr=True)
        setup_sdk_logging(
            level=self._cfg.log_level.upper(), pretty=True, console=self._console
        )

    def run(self) -> LeRobotDatasetMeta:
        """
        Connects to the Mosaico server, builds the metadata of the dataset from the sequences
        and writes the dataset under `saving_path` (see `_write_dataset`).

        An existing `saving_path` is checked before connecting: a failed resolution of the sequences leaves it untouched.

        Returns:
            LeRobotDatasetMeta: The metadata written under `saving_path`.

        Raises:
            FileExistsError: If `saving_path` exists and `overwrite` is False.
            ValueError: If no sequence can be exported, or an episode cannot be written (see
                `LeRobotWriter.write_episode`).
        """
        saving_path = self._cfg.saving_path
        if saving_path.exists() and not self._cfg.overwrite:
            raise FileExistsError(
                f"Impossible to save to '{saving_path}' since it already exists. "
                "Pass overwrite=True to replace it."
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
                meta = LeRobotDatasetMeta.from_data(
                    saving_path, loader.info, loader.episodes
                )

                if saving_path.exists():
                    shutil.rmtree(saving_path)
                try:
                    self._write_dataset(loader, meta)
                except Exception:
                    # Interruptions included: the files written so far are left as they are
                    logger.error(
                        f"The LeRobot dataset in '{saving_path}' is incomplete: the "
                        "extraction stopped before writing all of it."
                    )
                    raise

        logger.info(
            f"LeRobot dataset of {meta.n_episodes} episode(s) written to '{saving_path}'."
        )
        return meta

    @staticmethod
    def _write_dataset(
        loader: MosaicoToLeRobotLoader, meta: LeRobotDatasetMeta
    ) -> None:
        """
        Writes the dataset under the `root` of `meta`: the data of the episodes, streamed from
        their sequences (see `LeRobotWriter`), then the `meta/` folder (see
        `LeRobotDatasetMeta.dump`).

        Raises:
            ValueError: If an episode cannot be written (see `LeRobotWriter.write_episode`).
        """
        with LeRobotWriter(meta) as writer:
            for streamer in loader:
                logger.info(
                    f"Writing episode {streamer.episode_metadata.episode_index} (sequence "
                    f"'{streamer.sequence_name}')..."
                )
                writer.write_episode(streamer)

        meta.dump()


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
        "--lerobot_path",
        dest="saving_path",
        metavar="PATH",
        type=Path,
        required=True,
        help="Root folder of the LeRobot dataset to write",
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
        host=args.host,
        port=args.port,
        log_level=args.log,
        mosaico_api_key=args.mosaico_api_key,
        tls_cert_path=args.tls_cert_path,
        enable_tls=args.enable_tls,
        overwrite=args.overwrite,
    )

    # --- Execution ---
    extractor = LeRobotSequencesExtractor(config)
    try:
        extractor.run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        # Exit non-zero so calling scripts/CI can detect the failure
        logger.error(f"LeRobot extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    lerobot_sequences_extractor()
