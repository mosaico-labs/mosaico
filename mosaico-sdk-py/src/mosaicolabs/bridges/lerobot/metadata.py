"""
LeRobot dataset metadata.

This module reads the `meta/` folder of a LeRobot v3.0 dataset and exposes it as typed objects:

- `LeRobotFeature`: one entry of the `features` dict of `meta/info.json`.
- `LeRobotEpisode`: one row of the `meta/episodes/chunk-XXX/file-XXX.parquet` files.
- `LeRobotDatasetMeta`: the whole `meta/` folder (`info.json`, the optional `stats.json`, the features
  and the episodes).

It also defines `LeRobotSchemaMetadata`, the reserved `_lerobot_` metadata namespace attached to the
sequences created by the LeRobot bridge. LeRobot metadata is keyed by feature names such as
`observation.state`, while the Mosaico server rejects metadata keys containing `.`: the namespace
therefore nests dotted keys (`observation.state` -> `observation` / `state`) and drops non-finite floats
(rejected by the server JSON parser), so that its content is always accepted by the server.
"""

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from mosaicolabs.handlers.helpers import _validate_metadata_keys
from mosaicolabs.logging_config import get_logger

from ..base_schema_metadata import BaseSchemaMetadata

if TYPE_CHECKING:
    from mosaicolabs.handlers import SequenceHandler

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


def _holds_feature(value: Any) -> bool:
    """Whether `value` is a feature description, or a dict holding one at any depth."""
    return isinstance(value, dict) and (
        "dtype" in value or any(_holds_feature(v) for v in value.values())
    )


def _flatten_features(nested: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    Rebuilds the `features` dict of `meta/info.json` from its `_lerobot_` form, where ingestion
    nested the dotted feature names: a name is the path, joined with `.`, down to the first dict
    holding a `dtype` (`{"observation": {"state": {"dtype": ...}}}` -> `observation.state`).

    Raises:
        ValueError: If a feature holds another feature: a dataset with both `a` and `a.b`, which
            ingestion merges into the description of `a`.
    """
    flat: Dict[str, Any] = {}
    for key, value in nested.items():
        name = f"{prefix}.{key}" if prefix else key
        if "dtype" not in value:
            flat.update(_flatten_features(value, name))
        elif any(_holds_feature(v) for v in value.values()):
            raise ValueError(
                f"Feature '{name}' holds another feature: its name cannot be rebuilt."
            )
        else:
            flat[name] = value
    return flat


def _feature_signature(features: Dict[str, Any]) -> Dict[str, Tuple[Any, ...]]:
    """The part of each feature that must match to merge datasets: dtype, shape and names."""
    return {
        key: (feature["dtype"], list(feature.get("shape") or []), feature.get("names"))
        for key, feature in features.items()
    }


class LeRobotSchemaMetadata(BaseSchemaMetadata):
    """
    Encapsulates Mosaico's reserved ``_lerobot_`` metadata namespace in a single place.

    It holds, on each sequence created by the LeRobot bridge (one per episode), the content of
    `meta/info.json` (`info`) and `meta/stats.json` (`stats`), the `meta/episodes` row of the
    episode (`episode`) and the name of the dataset folder (`source_file`). The topics carry no
    `_lerobot_` metadata.

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
        """The total number of episodes (`total_episodes` in `meta/info.json`)."""
        return self.info["total_episodes"]

    @property
    def episode_indexes(self) -> List[int]:
        """The indexes of the episodes defined in `meta/episodes`, in increasing order."""
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

        return self.root / self.info["data_path"].format(
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
            FileNotFoundError: If `meta/`, `meta/info.json` or the episodes parquet files are missing.
            ValueError: If the dataset format version is not supported, or the metadata is malformed.
        """
        meta_dir = Path(root) / _META_DIR
        info, stats = cls._load_meta_json(meta_dir)
        # Checked before reading the episodes, whose layout differs in other versions
        cls._check_info(root, info)

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
        return cls.from_data(root, info, episodes, stats)

    @classmethod
    def from_data(
        cls,
        root: Path,
        info: Dict[str, Any],
        episodes: List[LeRobotEpisode],
        stats: Optional[Dict[str, Any]] = None,
    ) -> "LeRobotDatasetMeta":
        """
        Creates the metadata of a LeRobot dataset from its content, without reading any file.

        Args:
            root (Path): The root folder of the dataset.
            info (Dict[str, Any]): The content of `meta/info.json`.
            episodes (List[LeRobotEpisode]): The episodes of the dataset.
            stats (Optional[Dict[str, Any]]): The content of `meta/stats.json`, if any.

        Returns:
            LeRobotDatasetMeta: The dataset metadata.

        Raises:
            ValueError: If the dataset format version is not supported, or the metadata is malformed.
        """
        cls._check_info(root, info)

        features = {
            key: LeRobotFeature.from_dict(key, feature)
            for key, feature in (info.get("features") or {}).items()
        }

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
            features=features,
            episodes_map=dict(sorted(episodes_map.items())),
        )

    @staticmethod
    def _check_info(root: Path, info: Dict[str, Any]) -> None:
        """
        Checks the dataset format version and the `data_path` template of `meta/info.json`.

        Raises:
            ValueError: If the version is not supported, or `data_path` is missing.
        """
        codebase_version = info.get("codebase_version")
        if codebase_version != SUPPORTED_CODEBASE_VERSION:
            raise ValueError(
                f"Unsupported LeRobot dataset version '{codebase_version}': only "
                f"'{SUPPORTED_CODEBASE_VERSION}' is supported."
            )

        # The template of the data file paths (see `get_parquet_file`)
        if not isinstance(info.get("data_path"), str):
            raise ValueError(
                f"The 'meta/info.json' of the LeRobot dataset '{root}' has no 'data_path' "
                "(the template of the data file paths)."
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


def sequences_to_lerobot_meta(
    root: Path, sequences: List["SequenceHandler"]
) -> LeRobotDatasetMeta:
    """
    Creates the metadata of the dataset exported from Mosaico sequences, one episode per
    sequence, from their `_lerobot_` metadata (see `LeRobotSchemaMetadata`).

    The episodes are renumbered in the order of `sequences`: the position of a sequence is its
    new `episode_index`, and `dataset_from_index` / `dataset_to_index` are the running sum of
    the episode lengths. Only `tasks` and `length` are kept from the source episode; the
    source sequence name and episode index are kept in `extra` for traceability. The video
    features are dropped, and the totals and `splits` of `info` are rewritten.

    Args:
        root (Path): The output folder of the export.
        sequences (List[SequenceHandler]): The sequences to export, in episode order.

    Returns:
        LeRobotDatasetMeta: The metadata of the exported dataset, with no `stats`.

    Raises:
        ValueError: If `sequences` is empty, if the `_lerobot_` metadata of a sequence is
            missing or malformed, or if two sequences differ in `fps` or features.
    """
    if not sequences:
        raise ValueError("No sequence to export.")

    info: Dict[str, Any] = {}
    #TODO - features will become LeRobotFeature
    features: Dict[str, Any] = {}
    episodes: List[LeRobotEpisode] = []
    for position, sequence in enumerate(sequences):
        start = episodes[-1].dataset_to_index if episodes else 0
        seq_info, seq_features, fps, episode = _read_sequence(sequence, position, start)

        # merge_info: the first sequence sets fps and features, the others must match (FR-3)
        if not info:
            info, features = seq_info, seq_features
        else:
            #TODO -> feature != seq_features beacuse the two are DataClass
            differing = _differing_features(features, seq_features)
            if fps != info["fps"] or differing:
                raise ValueError(
                    f"Sequence '{sequence.name}' cannot be exported with "
                    f"'{sequences[0].name}': fps {fps} vs {info['fps']}, differing "
                    f"features {differing}."
                )
        episodes.append(episode)

    return LeRobotDatasetMeta.from_data(
        root, _export_info(info, features, episodes), episodes
    )


def _read_sequence(
    sequence: "SequenceHandler", position: int, start: int
) -> Tuple[Dict[str, Any], Dict[str, Any], Any, LeRobotEpisode]:
    """
    Reads the `_lerobot_` metadata of a sequence: its `info`, its flattened features, its
    `fps` and its episode, renumbered at `position` from the frame `start` (FR-7).

    Raises:
        ValueError: If the `_lerobot_` metadata is missing or malformed.
    """
    block = LeRobotSchemaMetadata.extract(sequence.user_metadata)
    try:
        info = block["info"]
        #TODO - use LeRobotFeature 
        features = _flatten_features(info["features"])
        fps = info["fps"]
        episode = block["episode"]
        tasks = list(episode["tasks"])
        length = int(episode["length"])
        source_index = episode["episode_index"]
    except (AttributeError, KeyError, TypeError, ValueError) as e:
        raise ValueError(
            f"Sequence '{sequence.name}' has missing or malformed "
            f"'{LeRobotSchemaMetadata.KEY}' metadata: {e!r}"
        ) from e

    return (
        info,
        features,
        fps,
        LeRobotEpisode(
            episode_index=position,
            tasks=tasks,
            length=length,
            # v1: one data file for the whole export; split by data_files_size_in_mb when needed
            data_chunk_index=0,
            data_file_index=0,
            dataset_from_index=start,
            dataset_to_index=start + length,
            extra={
                "source_sequence": sequence.name,
                "source_episode_index": source_index,
            },
        ),
    )


def _differing_features(expected: Dict[str, Any], found: Dict[str, Any]) -> List[str]:
    """Returns the sorted keys of the features that differ between two feature sets."""
    expected_sig, found_sig = _feature_signature(expected), _feature_signature(found)
    return sorted(
        key
        for key in expected_sig.keys() | found_sig.keys()
        if expected_sig.get(key) != found_sig.get(key)
    )


def _export_info(
    info: Dict[str, Any], features: Dict[str, Any], episodes: List[LeRobotEpisode]
) -> Dict[str, Any]:
    """Returns `info` without the video features, with the totals and `splits` of the
    renumbered `episodes`."""
    #TODO -Convert Dict to LeRobotFeature
    video_features = [key for key, f in features.items() if f["dtype"] == "video"]
    if video_features:
        logger.warning(
            f"Skipping video features {video_features}: video files are not supported yet."
        )

    return {
        **info,
        "features": {
            key: f for key, f in features.items() if key not in video_features
        },
        "total_episodes": len(episodes),
        "total_frames": episodes[-1].dataset_to_index,
        "total_tasks": len(set({task for e in episodes for task in e.tasks})),
        "splits": {"train": f"0:{len(episodes)}"},
    }
