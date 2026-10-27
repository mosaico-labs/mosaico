"""
The episodes of a LeRobot dataset: the rows of the `meta/episodes/chunk-XXX/file-XXX.parquet`
files.
"""

from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, List, Optional

from .helpers import _flatten, _unflatten

_STATS_COLUMN_PREFIX = "stats/"


@dataclass(frozen=True)
class LeRobotEpisode:
    """
    An episode of a LeRobot dataset: one row of the `meta/episodes/*.parquet` files.

    LeRobot flattens the nested episode information into column names separated by `/`
    (e.g. `data/chunk_index`, `stats/observation.state/mean`). The `videos/<feature>/...`
    columns of the video features, not supported yet, are not read.
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
        for col, value in row.items():
            if col.startswith(_STATS_COLUMN_PREFIX):
                # Feature names contain '.', not '/': the stat name is the last component
                feature, _, stat = col[len(_STATS_COLUMN_PREFIX) :].rpartition("/")
                stats.setdefault(feature, {})[stat] = value

        return cls(tasks=list(fields.pop("tasks") or []), stats=stats, **fields)

    @classmethod
    def from_dict(cls, ep_dict: Dict[str, Any]) -> "LeRobotEpisode":
        """
        Creates a `LeRobotEpisode` from its `to_dict()` form (e.g. the `episode` of the `_lerobot_`
        metadata, restored by `LeRobotSchemaMetadata.restore`): the nested information is
        flattened back into the `/`-separated column names of a `meta/episodes` row (e.g.
        `data` / `chunk_index` -> `data/chunk_index`), then parsed by `from_row`.

        Args:
            ep_dict (Dict[str, Any]): The episode information, as returned by `to_dict()`.

        Returns:
            LeRobotEpisode: The parsed episode.

        Raises:
            ValueError: If a required column is missing.
        """
        return cls.from_row(_flatten(ep_dict, "/"))

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

        out = _unflatten(flat, sep="/")
        if self.stats:
            out["stats"] = self.stats
        return out
