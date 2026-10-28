"""
LeRobot dataset metadata.

This module reads the `meta/` folder of a LeRobot v3.0 dataset and exposes it as typed objects:

- `LeRobotFeature` (`feature.py`): one entry of the `features` dict of `meta/info.json`.
- `LeRobotEpisode` (`episode.py`): one row of the `meta/episodes/chunk-XXX/file-XXX.parquet` files.
- `LeRobotInfo` (`info.py`): the content of `meta/info.json`.
- `LeRobotDatasetMeta`: the whole `meta/` folder (`info.json`, the optional `stats.json`, the
  tasks and the episodes), read from a dataset folder (`load`) or written to one (`dump`).
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .episode import LeRobotEpisode
from .feature import LeRobotFeature
from .helpers import _flatten
from .info import LeRobotInfo

_META_DIR = "meta"
_EPISODES_DIR = "episodes"
_EPISODES_FILE = "chunk-{chunk_index:03d}/file-{file_index:03d}.parquet"
_INFO_FILE = "info.json"
_STATS_FILE = "stats.json"
_TASKS_FILE = "tasks.parquet"


def _load_json_object(path: Path) -> Dict[str, Any]:
    """Loads a JSON file, raising if its top-level value is not an object."""
    with open(path, "r", encoding="utf-8") as f:
        content = json.load(f)

    if not isinstance(content, dict):
        raise ValueError(
            f"'{path}' is expected to hold a JSON object, but holds a "
            f"`{type(content).__name__}`."
        )
    return content


def _write_parquet(rows: List[Dict[str, Any]], path: Path) -> None:
    """Writes `rows` as a parquet file, with the columns of every row (a missing value is null)."""
    columns = list(dict.fromkeys(key for row in rows for key in row))
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({col: [row.get(col) for row in rows] for col in columns}), path
    )


@dataclass(frozen=True)
class LeRobotDatasetMeta:
    """
    The content of the `meta/` folder of a LeRobot v3.0 dataset.
    """

    root: Path
    """The root folder of the dataset."""

    info: LeRobotInfo
    """The content of `meta/info.json`."""

    stats: Optional[Dict[str, Any]]
    """The content of `meta/stats.json`, or `None` if the dataset has no `stats.json`."""

    tasks: List[str]
    """The content of `meta/tasks.parquet`: the dataset tasks, in task index order."""

    episodes_map: Dict[int, LeRobotEpisode]
    """The dataset episodes, keyed by episode index."""

    @property
    def features(self) -> List[LeRobotFeature]:
        """The dataset features, in the order of `features` in `meta/info.json`."""
        return self.info.features

    @property
    def codebase_version(self) -> str:
        """The LeRobot dataset format version (`codebase_version` in `meta/info.json`)."""
        return self.info.codebase_version

    @property
    def n_episodes(self) -> int:
        """The total number of episodes (`total_episodes` in `meta/info.json`)."""
        return self.info.total_episodes

    @property
    def episode_indexes(self) -> List[int]:
        """The indexes of the episodes defined in `meta/episodes`, in increasing order."""
        return list(self.episodes_map.keys())

    @property
    def fps(self) -> int:
        """The dataset frame rate (`fps` in `meta/info.json`)."""
        return self.info.fps

    @property
    def column_names(self) -> List[str]:
        """
        The data columns read by the bridge: the image and timeseries features. The `timestamp`
        column (the time of every message) and the video features (stored in separate video
        files, not supported yet) are excluded.
        """
        return [
            feature.key
            for feature in self.features
            if feature.is_image or feature.is_timeserie
        ]

    def get_parquet_file(self, episode_index: int) -> Path:
        """
        Returns the path of the data parquet file holding an episode, as LeRobot finds it: the
        `data_path` template of `meta/info.json` (e.g.
        `data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet`), filled with the
        `data/chunk_index` and `data/file_index` of the episode. The file can hold other
        episodes too.

        Args:
            episode_index (int): The episode index.

        Returns:
            Path: The path of the data file, under `root`.

        Raises:
            ValueError: If the episode is not defined in `meta/episodes`.
        """
        episode = self.episodes_map.get(episode_index)
        if episode is None:
            raise ValueError(
                f"Episode {episode_index} is not defined in 'meta/episodes'."
            )

        return self.root / self.info.data_path.format(
            chunk_index=episode.data_chunk_index, file_index=episode.data_file_index
        )

    def read_schema(self) -> pa.Schema:
        """
        Reads the `pyarrow` schema of the data files from the footer of the data file of the
        first episode: LeRobot writes every data file with the same features.

        Returns:
            pa.Schema: The schema of the data files.

        Raises:
            FileNotFoundError: If the data file of the first episode is missing.
            ValueError: If the dataset has no episode.
        """
        if not self.episode_indexes:
            raise ValueError(
                f"The LeRobot dataset '{self.root}' has no episode in 'meta/episodes': the "
                "schema of its data files cannot be read."
            )

        return pq.read_schema(self.get_parquet_file(self.episode_indexes[0]))

    def n_frames(self, episode_index: Optional[int] = None) -> int:
        """
        Returns the number of frames of an episode (its `length` in `meta/episodes`), or of
        every episode if `episode_index` is `None`.

        Args:
            episode_index (Optional[int]): The episode to count (every episode if `None`).

        Returns:
            int: The number of frames, or 0 if the episode is not defined in `meta/episodes`.
        """
        if episode_index is None:
            return sum(episode.length for episode in self.episodes_map.values())

        episode = self.episodes_map.get(episode_index)
        return episode.length if episode is not None else 0

    @classmethod
    def load(cls, root: Path) -> "LeRobotDatasetMeta":
        """
        Loads the `meta/` folder of a LeRobot dataset.

        Args:
            root (Path): The root folder of the dataset.

        Returns:
            LeRobotDatasetMeta: The dataset metadata.

        Raises:
            FileNotFoundError: If `meta/`, `meta/info.json`, `meta/tasks.parquet` or the episodes
                parquet files are missing.
            ValueError: If the dataset format version is not supported, or the metadata is malformed.
        """
        meta_dir = Path(root) / _META_DIR
        # `info.json` is parsed, and its version checked, before reading the episodes, whose
        # layout differs in other versions
        info, stats = cls._load_meta_json(meta_dir)

        episodes_files = sorted((meta_dir / _EPISODES_DIR).rglob("*.parquet"))
        if not episodes_files:
            raise FileNotFoundError(
                f"No episodes parquet file found in '{meta_dir / _EPISODES_DIR}'."
            )

        episodes = [
            LeRobotEpisode.from_row(row)
            for episodes_file in episodes_files
            for row in pq.read_table(episodes_file).to_pylist()
        ]
        tasks = pd.read_parquet(meta_dir / _TASKS_FILE).index.tolist()

        return cls.from_data(root, info, episodes, tasks, stats)

    @classmethod
    def from_data(
        cls,
        root: Path,
        info: LeRobotInfo,
        episodes: List[LeRobotEpisode],
        tasks: List[str],
        stats: Optional[Dict[str, Any]] = None,
    ) -> "LeRobotDatasetMeta":
        """
        Creates the metadata of a LeRobot dataset from its content, without reading any file.

        Args:
            root (Path): The root folder of the dataset.
            info (LeRobotInfo): The content of `meta/info.json`.
            episodes (List[LeRobotEpisode]): The episodes of the dataset.
            tasks (List[str]): The dataset tasks, in task index order.
            stats (Optional[Dict[str, Any]]): The content of `meta/stats.json`, if any.

        Returns:
            LeRobotDatasetMeta: The dataset metadata.

        Raises:
            ValueError: If an episode is defined more than once.
        """
        episodes_map: Dict[int, LeRobotEpisode] = {}
        for episode in episodes:
            if episode.episode_index in episodes_map:
                raise ValueError(
                    f"Episode {episode.episode_index} is defined more than once in "
                    f"the LeRobot dataset '{root}'."
                )
            episodes_map[episode.episode_index] = episode

        return cls(
            root=Path(root),
            info=info,
            stats=stats,
            tasks=tasks,
            episodes_map=dict(sorted(episodes_map.items())),
        )

    def dump(self) -> None:
        """
        Writes the `meta/` folder of the dataset under `root`, as `load` reads it:

        - `info.json`, and `stats.json` if `stats` is not `None`;
        - `tasks.parquet`: `tasks`, indexed by task as LeRobot reads them;
        - the episodes parquet files, each episode in the one given by its
          `meta/episodes/chunk_index` and `meta/episodes/file_index` (0 if not set).

        Existing files are overwritten.
        """
        meta_dir = self.root / _META_DIR
        meta_dir.mkdir(parents=True, exist_ok=True)

        (meta_dir / _INFO_FILE).write_text(json.dumps(self.info.to_dict(), indent=4))
        if self.stats is not None:
            (meta_dir / _STATS_FILE).write_text(json.dumps(self.stats, indent=4))

        pd.DataFrame(
            {"task_index": range(len(self.tasks))},
            index=pd.Index(self.tasks, name="task"),
        ).to_parquet(meta_dir / _TASKS_FILE)

        # The `meta/episodes` rows, grouped by the episodes file holding them
        files: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
        for episode in self.episodes_map.values():
            chunk_index = episode.meta_episodes_chunk_index or 0
            file_index = episode.meta_episodes_file_index or 0
            files.setdefault((chunk_index, file_index), []).append(
                _flatten(episode.to_dict(), sep="/")
            )

        for (chunk_index, file_index), rows in files.items():
            file_name = _EPISODES_FILE.format(
                chunk_index=chunk_index, file_index=file_index
            )
            _write_parquet(rows, meta_dir / _EPISODES_DIR / file_name)

    @staticmethod
    def _load_meta_json(
        meta_dir: Path,
    ) -> Tuple[LeRobotInfo, Optional[Dict[str, Any]]]:
        """
        Loads the JSON files of the `meta/` folder: `info.json` (required) and `stats.json`
        (optional, as in LeRobot).

        Args:
            meta_dir (Path): The `meta/` folder of the dataset.

        Returns:
            Tuple[LeRobotInfo, Optional[Dict[str, Any]]]: The parsed `info.json`, and the
                `stats.json` content or `None` if the dataset has no `stats.json`.

        Raises:
            FileNotFoundError: If `meta_dir` or `meta/info.json` is missing.
            ValueError: If a file does not hold a JSON object, or `info.json` is not a supported
                LeRobot info (see `LeRobotInfo.from_dict`).
        """
        if not meta_dir.is_dir():
            raise FileNotFoundError(f"'{meta_dir}' is not a directory.")

        info_path = meta_dir / _INFO_FILE
        if not info_path.is_file():
            raise FileNotFoundError(f"'{info_path}' not found.")
        info = LeRobotInfo.from_dict(_load_json_object(info_path))

        stats_path = meta_dir / _STATS_FILE
        stats = _load_json_object(stats_path) if stats_path.is_file() else None

        return info, stats
