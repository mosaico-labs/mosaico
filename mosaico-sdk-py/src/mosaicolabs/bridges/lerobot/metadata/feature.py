"""
The features of a LeRobot dataset: the entries of the `features` dict of `meta/info.json`.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

_TIMESTAMP_COLUMN_NAME = "timestamp"
_INDEX_COLUMN = "index"
_FRAME_INDEX_COLUMN = "frame_index"
_EPISODE_INDEX_COLUMN = "episode_index"
_TASK_INDEX_COLUMN = "task_index"


_MAX_ARRAY_DIMENSIONS = 5
"""The maximum number of dimensions of a feature value."""


@dataclass(frozen=True)
class LeRobotFeature:
    """
    A feature of a LeRobot dataset, as described in the `features` dict of `meta/info.json`.

    `info` is excluded from equality: it describes the feature (e.g. its video encoding), not
    its data, so that datasets differing only there can be merged.
    """

    key: str
    """The feature name (e.g. `observation.state`), which is also the name of its parquet column."""

    dtype: str
    """The LeRobot data type (e.g. `float32`, `int64`, `image`, `video`)."""

    shape: Tuple[int, ...]
    """The shape of a single value of the feature."""

    names: Optional[Any] = None
    """The names of the feature dimensions (a list, a dict of lists or `None`)."""

    info: Optional[Dict[str, Any]] = field(default=None, compare=False)
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

    def is_image(self) -> bool:
        """Whether the feature is an image embedded in the data parquet files."""
        return self.dtype == "image"

    def is_video(self) -> bool:
        """Whether the feature is stored in separate video files."""
        return self.dtype == "video"

    def is_timestamp(self) -> bool:
        """Whether the feature is the `timestamp` column (seconds since the episode start)."""
        return self.key == _TIMESTAMP_COLUMN_NAME

    def is_index(self) -> bool:
        """Whether the feature is the `index` column."""
        return self.key == _INDEX_COLUMN

    def is_frame_index(self) -> bool:
        """Whether the feature is the `frame_index` column."""
        return self.key == _FRAME_INDEX_COLUMN

    def is_episode_index(self) -> bool:
        """Whether the feature is the `episode_index` column."""
        return self.key == _EPISODE_INDEX_COLUMN

    def is_task_index(self) -> bool:
        """Whether the feature is the `task_index` column."""
        return self.key == _TASK_INDEX_COLUMN

    def is_timeserie(self) -> bool:
        """
        Whether the feature is a timeseries: neither an image, a video, the timestamp, nor the
        position of the frame (`index`, `frame_index` and `episode_index`), which is rebuilt
        when the episodes are exported (see `LeRobotWriter`).
        """
        return (
            not self.is_image()
            and not self.is_video()
            and not self.is_timestamp()
            and not self.is_index()
            and not self.is_frame_index()
            and not self.is_episode_index()
        )

    def is_scalar(self) -> bool:
        """Whether the feature is a scalar: a single value, of shape `(1,)`."""
        return (
            not self.is_image()
            and not self.is_video()
            and len(self.shape) == 1
            and self.shape[0] == 1
        )

    def is_list(self) -> bool:
        """Whether the feature is a vector: a fixed size list, of shape `(n,)` with `n > 1`."""
        return (
            not self.is_image()
            and not self.is_video()
            and len(self.shape) == 1
            and self.shape[0] > 1
        )

    def is_nested_list(self) -> bool:
        """Whether the feature is an array of 2 to 5 dimensions: a nested list, one level per
        dimension."""
        return (
            not self.is_image()
            and not self.is_video()
            and 2 <= len(self.shape) <= _MAX_ARRAY_DIMENSIONS
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Returns the feature description, as in `meta/info.json`: the `key` is the name the
        description is stored under, and `info` is only set if not `None`.
        """
        description: Dict[str, Any] = {
            "dtype": self.dtype,
            "shape": list(self.shape),
            "names": self.names,
        }
        if self.info is not None:
            description["info"] = self.info
        return description
