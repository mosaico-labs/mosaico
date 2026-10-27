"""
The features of a LeRobot dataset: the entries of the `features` dict of `meta/info.json`.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

_TIMESTAMP_COLUMN_NAME = "timestamp"


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
