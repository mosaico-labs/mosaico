"""
LeRobot dataset metadata.

This module reads the `meta/` folder of a LeRobot v3.0 dataset and exposes it as typed objects:

- `LeRobotFeature`: one entry of the `features` dict of `meta/info.json`.
- `LeRobotEpisode`: one row of the `meta/episodes/chunk-XXX/file-XXX.parquet` files.
- `LeRobotDatasetMeta`: the whole `meta/` folder (`info.json`, the optional `stats.json`, the features
  and the episodes).

It also defines `LeRobotSchemaMetadata`, the reserved `_lerobot_` metadata namespace attached to the
sequences and topics created by the LeRobot bridge. LeRobot metadata is keyed by feature names such as
`observation.state`, while the Mosaico server rejects metadata keys containing `.`: the namespace
therefore nests dotted keys (`observation.state` -> `observation` / `state`) and drops non-finite floats
(rejected by the server JSON parser), so that its content is always accepted by the server.
"""

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Tuple

import pyarrow.parquet as pq

from mosaicolabs.handlers.helpers import _validate_metadata_keys
from mosaicolabs.logging_config import get_logger

from ..base_schema_metadata import BaseSchemaMetadata

# Set the hierarchical logger
logger = get_logger(__name__)

SUPPORTED_CODEBASE_VERSION = "v3.0"
"""The only LeRobot dataset format version supported by the bridge."""

_META_DIR = "meta"
_EPISODES_DIR = "episodes"
_INFO_FILE = "info.json"
_STATS_FILE = "stats.json"
_STATS_COLUMN_PREFIX = "stats/"
_TIMESTAMP_COLUMN_NAME = "timestamp"


def _merge_nested(dst: Dict[str, Any], src: Dict[str, Any], path: str) -> None:
    """Recursively merges `src` into `dst`, raising if a key is both a value and a nested object."""
    for key, value in src.items():
        key_path = f"{path}.{key}" if path else key
        if key not in dst:
            dst[key] = value
        elif isinstance(dst[key], dict) and isinstance(value, dict):
            _merge_nested(dst[key], value, key_path)
        else:
            raise ValueError(
                f"Metadata key conflict on '{key_path}': it is defined both as a value "
                "and as a nested object."
            )


def _unflatten(flat: Dict[str, Any], sep: str) -> Dict[str, Any]:
    """Turns `{"a/b": 1, "a/c": 2}` into `{"a": {"b": 1, "c": 2}}` (with `sep="/"`)."""
    out: Dict[str, Any] = {}
    for key, value in flat.items():
        *parents, leaf = key.split(sep)
        nested: Dict[str, Any] = {leaf: value}
        for parent in reversed(parents):
            nested = {parent: nested}
        _merge_nested(out, nested, "")
    return out


def _to_mosaico_metadata(value: Any) -> Any:
    """
    Makes a JSON-like value storable as Mosaico metadata.

    Keys containing `.` are nested (`{"observation.state": x}` -> `{"observation": {"state": x}}`),
    and non-finite floats (`NaN`, `inf`), which are not valid JSON, are replaced by `None`.
    """
    if isinstance(value, dict):
        return _unflatten(
            {str(key): _to_mosaico_metadata(val) for key, val in value.items()},
            sep=".",
        )
    if isinstance(value, (list, tuple)):
        return [_to_mosaico_metadata(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


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


class LeRobotSchemaMetadata(BaseSchemaMetadata):
    """
    Encapsulates Mosaico's reserved ``_lerobot_`` metadata namespace in a single place.

    Sequences created by the LeRobot bridge carry the dataset `meta/*.json` files and the
    episode information, topics carry the description of the LeRobot features they store.

    Every field set on this namespace is made server-compatible on the fly: keys containing
    `.` (e.g. the feature name `observation.state`) are nested, non-finite floats are replaced
    by `None`, and the resulting keys are validated, so that an invalid key is reported before
    any request reaches the server.

    Example:
        ```python
        meta = LeRobotSchemaMetadata(info={"features": {"observation.state": {"dtype": "float32"}}})
        sequence_metadata = meta.merge_into({"operator": "John"})
        # sequence_metadata == {
        #     "operator": "John",
        #     "_lerobot_": {"info": {"features": {"observation": {"state": {"dtype": "float32"}}}}},
        # }
        ```
    """

    KEY: ClassVar[str] = "_lerobot_"
    """The reserved metadata key."""

    def __init__(self, **fields: Any):
        super().__init__(**self._sanitize(fields))

    def update(self, **fields: Any) -> "LeRobotSchemaMetadata":
        super().update(**self._sanitize(fields))
        return self

    @staticmethod
    def _sanitize(fields: Dict[str, Any]) -> Dict[str, Any]:
        sanitized = _to_mosaico_metadata(fields)
        _validate_metadata_keys(sanitized)
        return sanitized


@dataclass(frozen=True)
class LeRobotFeature:
    """
    A feature of a LeRobot dataset, as described in the `features` dict of `meta/info.json`.
    """

    key: str
    """The feature name (e.g. `observation.state`), which is also the name of its parquet column."""

    dtype: str
    """The LeRobot data type (e.g. `float32`, `int64`, `image`, `video`)."""

    shape: Tuple[int, ...]
    """The shape of a single value of the feature."""

    names: Optional[Any] = None
    """The names of the feature dimensions (a list, a dict of lists or `None`)."""

    info: Optional[Dict[str, Any]] = None
    """Additional information about the feature (e.g. video encoding parameters)."""

    @classmethod
    def from_dict(cls, key: str, feature: Dict[str, Any]) -> "LeRobotFeature":
        """
        Creates a `LeRobotFeature` from its `meta/info.json` description.

        Args:
            key (str): The feature name.
            feature (Dict[str, Any]): The feature description (`dtype`, `shape`, `names`, `info`).

        Returns:
            LeRobotFeature: The parsed feature.

        Raises:
            ValueError: If the description has no `dtype`.
        """
        if "dtype" not in feature:
            raise ValueError(f"Feature '{key}' has no 'dtype' in 'meta/info.json'.")

        return cls(
            key=key,
            dtype=feature["dtype"],
            shape=tuple(feature.get("shape") or ()),
            names=feature.get("names"),
            info=feature.get("info"),
        )

    @property
    def is_image(self) -> bool:
        """Whether the feature is an image embedded in the data parquet files."""
        return self.dtype == "image"

    @property
    def is_video(self) -> bool:
        """Whether the feature is stored in separate video files."""
        return self.dtype == "video"

    @property
    def is_timestamp(self) -> bool:
        """Whether the feature is the `timestamp` column (seconds since the episode start)."""
        return self.key == _TIMESTAMP_COLUMN_NAME

    @property
    def is_timeserie(self) -> bool:
        """Whether the feature is a timeseries: neither an image, a video nor the timestamp."""
        return not self.is_image and not self.is_video and not self.is_timestamp

    def to_dict(self) -> Dict[str, Any]:
        """Returns the feature description, as in `meta/info.json`, including its `key`."""
        return {
            "key": self.key,
            "dtype": self.dtype,
            "shape": list(self.shape),
            "names": self.names,
            "info": self.info,
        }


@dataclass(frozen=True)
class LeRobotEpisode:
    """
    An episode of a LeRobot dataset: one row of the `meta/episodes/*.parquet` files.

    LeRobot flattens the nested episode information into column names separated by `/`
    (e.g. `data/chunk_index`, `stats/observation.state/mean`).
    """

    episode_index: int
    """The episode index."""

    tasks: List[str]
    """The natural-language tasks performed in the episode."""

    length: int
    """The number of frames of the episode."""

    data_chunk_index: int
    """The chunk index of the data parquet file holding the episode (`data/chunk_index`)."""

    data_file_index: int
    """The file index of the data parquet file holding the episode (`data/file_index`)."""

    dataset_from_index: int
    """The global index of the first frame of the episode (inclusive)."""

    dataset_to_index: int
    """The global index of the last frame of the episode (exclusive)."""

    meta_episodes_chunk_index: Optional[int] = None
    """The chunk index of the episodes parquet file holding this row (`meta/episodes/chunk_index`)."""

    meta_episodes_file_index: Optional[int] = None
    """The file index of the episodes parquet file holding this row (`meta/episodes/file_index`)."""

    stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    """The per-feature statistics of the episode (`stats/<feature>/<stat>` columns)."""

    extra: Dict[str, Any] = field(default_factory=dict)
    """Any other column of the row, keyed by its original name."""

    _REQUIRED_COLUMNS: ClassVar[Dict[str, str]] = {
        "episode_index": "episode_index",
        "tasks": "tasks",
        "length": "length",
        "data/chunk_index": "data_chunk_index",
        "data/file_index": "data_file_index",
        "dataset_from_index": "dataset_from_index",
        "dataset_to_index": "dataset_to_index",
    }
    _OPTIONAL_COLUMNS: ClassVar[Dict[str, str]] = {
        "meta/episodes/chunk_index": "meta_episodes_chunk_index",
        "meta/episodes/file_index": "meta_episodes_file_index",
    }

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "LeRobotEpisode":
        """
        Creates a `LeRobotEpisode` from a row of the `meta/episodes/*.parquet` files.

        Args:
            row (Dict[str, Any]): The row, as returned by `pyarrow.Table.to_pylist()`.

        Returns:
            LeRobotEpisode: The parsed episode.

        Raises:
            ValueError: If a required column is missing.
        """
        missing = [col for col in cls._REQUIRED_COLUMNS if col not in row]
        if missing:
            raise ValueError(
                f"Episode row is missing the required columns {missing}. "
                f"Available columns: {sorted(row)}"
            )

        fields: Dict[str, Any] = {
            attr: row[col] for col, attr in cls._REQUIRED_COLUMNS.items()
        }
        fields.update(
            {
                attr: row[col]
                for col, attr in cls._OPTIONAL_COLUMNS.items()
                if col in row
            }
        )

        stats: Dict[str, Dict[str, Any]] = {}
        extra: Dict[str, Any] = {}
        known_columns = cls._REQUIRED_COLUMNS.keys() | cls._OPTIONAL_COLUMNS.keys()
        for col, value in row.items():
            if col in known_columns:
                continue
            if col.startswith(_STATS_COLUMN_PREFIX):
                # Feature names contain '.', not '/': the stat name is the last component
                feature, _, stat = col[len(_STATS_COLUMN_PREFIX) :].rpartition("/")
                stats.setdefault(feature, {})[stat] = value
            else:
                extra[col] = value

        return cls(
            tasks=list(fields.pop("tasks") or []), stats=stats, extra=extra, **fields
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Returns the episode information, with the `/`-separated column names nested
        (e.g. `data/chunk_index` -> `data` / `chunk_index`).
        """
        flat: Dict[str, Any] = {
            col: getattr(self, attr) for col, attr in self._REQUIRED_COLUMNS.items()
        }
        flat.update(
            {
                col: getattr(self, attr)
                for col, attr in self._OPTIONAL_COLUMNS.items()
                if getattr(self, attr) is not None
            }
        )
        flat.update(self.extra)

        out = _unflatten(flat, sep="/")
        if self.stats:
            out["stats"] = self.stats
        return out


@dataclass(frozen=True)
class LeRobotDatasetMeta:
    """
    The content of the `meta/` folder of a LeRobot v3.0 dataset.
    """

    root: Path
    """The root folder of the dataset."""

    info: Dict[str, Any]
    """The content of `meta/info.json`."""

    stats: Optional[Dict[str, Any]]
    """The content of `meta/stats.json`, or `None` if the dataset has no `stats.json`."""

    features: Dict[str, LeRobotFeature]
    """The dataset features, keyed by name."""

    episodes_map: Dict[int, LeRobotEpisode]
    """The dataset episodes, keyed by episode index."""

    @property
    def codebase_version(self) -> str:
        """The LeRobot dataset format version (`codebase_version` in `meta/info.json`)."""
        return self.info["codebase_version"]

    @property
    def n_episodes(self) -> int:
        """The total number of episodes."""
        return self.info["total_episodes"]

    @property
    def episode_indexes(self) -> List[int]:
        """Return all the episodes by their associated index"""
        return list(self.episodes_map.keys())

    @property
    def fps(self) -> float:
        """The dataset frame rate."""
        return self.info["fps"]

    @property
    def column_names(self) -> List[str]:
        """
        The data columns read by the bridge: the image and timeseries features. The `timestamp`
        column (the time of every message) and the video features (stored in separate video
        files, not supported yet) are excluded.
        """
        return [
            key
            for key, feature in self.features.items()
            if feature.is_image or feature.is_timeserie
        ]

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
            FileNotFoundError: If `meta/`, `meta/info.json` or the episodes parquet files are missing.
            ValueError: If the dataset format version is not supported, or the metadata is malformed.
        """
        meta_dir = Path(root) / _META_DIR
        info, stats = cls._load_meta_json(meta_dir)

        codebase_version = info.get("codebase_version")
        if codebase_version != SUPPORTED_CODEBASE_VERSION:
            raise ValueError(
                f"Unsupported LeRobot dataset version '{codebase_version}': only "
                f"'{SUPPORTED_CODEBASE_VERSION}' is supported."
            )

        features = {
            key: LeRobotFeature.from_dict(key, feature)
            for key, feature in (info.get("features") or {}).items()
        }

        episodes_files = sorted((meta_dir / _EPISODES_DIR).rglob("*.parquet"))
        if not episodes_files:
            raise FileNotFoundError(
                f"No episodes parquet file found in '{meta_dir / _EPISODES_DIR}'."
            )

        episodes: Dict[int, LeRobotEpisode] = {}
        for episodes_file in episodes_files:
            for row in pq.read_table(episodes_file).to_pylist():
                episode = LeRobotEpisode.from_row(row)
                if episode.episode_index in episodes:
                    raise ValueError(
                        f"Episode {episode.episode_index} is defined more than once in "
                        f"'{meta_dir / _EPISODES_DIR}'."
                    )
                episodes[episode.episode_index] = episode

        return cls(
            root=Path(root),
            info=info,
            stats=stats,
            features=features,
            episodes_map=dict(sorted(episodes.items())),
        )

    @staticmethod
    def _load_meta_json(
        meta_dir: Path,
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        Loads the JSON files of the `meta/` folder: `info.json` (required) and `stats.json`
        (optional, as in LeRobot).

        Args:
            meta_dir (Path): The `meta/` folder of the dataset.

        Returns:
            Tuple[Dict[str, Any], Optional[Dict[str, Any]]]: The `info.json` content, and the
                `stats.json` content or `None` if the dataset has no `stats.json`.

        Raises:
            FileNotFoundError: If `meta_dir` or `meta/info.json` is missing.
            ValueError: If a file does not hold a JSON object.
        """
        if not meta_dir.is_dir():
            raise FileNotFoundError(f"'{meta_dir}' is not a directory.")

        info_path = meta_dir / _INFO_FILE
        if not info_path.is_file():
            raise FileNotFoundError(f"'{info_path}' not found.")
        info = _load_json_object(info_path)

        stats_path = meta_dir / _STATS_FILE
        stats = _load_json_object(stats_path) if stats_path.is_file() else None

        return info, stats
