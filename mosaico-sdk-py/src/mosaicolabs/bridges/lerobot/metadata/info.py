"""
The `meta/info.json` file of a LeRobot v3.0 dataset.
"""

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Tuple

from .feature import LeRobotFeature

SUPPORTED_CODEBASE_VERSION = "v3.0"
"""The only LeRobot dataset format version supported by the bridge."""


@dataclass(frozen=True)
class LeRobotInfo:
    """
    The content of the `meta/info.json` file of a LeRobot v3.0 dataset.

    Each field is a key of `meta/info.json`. Only the keys needed to read the data are
    required (`codebase_version`, `fps`, `features`, `total_episodes`, `data_path`), and their
    type is checked. The others are kept as found, or `None` if missing. Any key outside the
    v3.0 format is kept, as found, in `extra`.
    """

    codebase_version: str
    """The LeRobot dataset format version (e.g. `v3.0`)."""

    fps: int
    """The dataset frame rate."""

    features: List[LeRobotFeature]
    """The dataset features, in the order of `meta/info.json`."""

    total_episodes: int
    """The number of episodes of the dataset."""

    data_path: str
    """The template of the data file paths (see `get_path_to_data`)."""

    total_frames: Optional[int] = None
    """The number of frames of the dataset, over every episode."""

    total_tasks: Optional[int] = None
    """The number of distinct tasks of the dataset."""

    chunks_size: Optional[int] = None
    """The maximum number of files of each `chunk-XXX` folder."""

    data_files_size_in_mb: Optional[int] = None
    """The size, in MB, beyond which LeRobot starts a new data file."""

    video_files_size_in_mb: Optional[int] = None
    """The size, in MB, beyond which LeRobot starts a new video file."""

    splits: Optional[Dict[str, str]] = None
    """The dataset splits, as ranges of episode indexes (e.g. `{"train": "0:50"}`)."""

    video_path: Optional[str] = None
    """The template of the video file paths, or `None` if the dataset has no video feature."""

    robot_type: Optional[str] = None
    """The robot that recorded the dataset (e.g. `so_follower`), if known."""

    extra: Dict[str, Any] = field(default_factory=dict)
    """The keys of `meta/info.json` outside the v3.0 format (e.g. added by the dataset author),
    kept as found and written back by `to_dict`."""

    _REQUIRED_KEYS: ClassVar[Dict[str, type]] = {
        "codebase_version": str,
        "fps": int,
        "features": dict,
        "total_episodes": int,
        "data_path": str,
    }
    """The keys needed to read the data, with their expected type. Each is a field."""

    _OPTIONAL_KEYS: ClassVar[Tuple[str, ...]] = (
        "total_frames",
        "total_tasks",
        "chunks_size",
        "data_files_size_in_mb",
        "video_files_size_in_mb",
        "splits",
        "video_path",
        "robot_type",
    )
    """The other keys of the v3.0 format, `None` if missing. Each is a field."""

    @classmethod
    def from_dict(cls, info: Dict[str, Any]) -> "LeRobotInfo":
        """
        Creates a `LeRobotInfo` from the content of `meta/info.json`.

        Args:
            info (Dict[str, Any]): The content of `meta/info.json`.

        Returns:
            LeRobotInfo: The parsed info.

        Raises:
            ValueError: If the dataset format version is not supported, a required key is
                missing or has the wrong type, or a feature description is not valid (see
                `LeRobotFeature.from_dict`).
        """

        # Checking missing required elements
        missing = [key for key in cls._REQUIRED_KEYS if key not in info]

        if missing:
            raise ValueError(
                f"'meta/info.json' is missing the required keys {missing}. "
                f"Available keys: {sorted(info)}"
            )

        # Check on the code version
        codebase_version = info["codebase_version"]
        if codebase_version != SUPPORTED_CODEBASE_VERSION:
            raise ValueError(
                f"Unsupported LeRobot dataset version '{codebase_version}': only "
                f"'{SUPPORTED_CODEBASE_VERSION}' is supported."
            )

        # Checking required elements types
        for key, expected_type in cls._REQUIRED_KEYS.items():
            value = info[key]

            if not isinstance(value, expected_type):
                raise ValueError(
                    f"'meta/info.json' key '{key}' must be a {expected_type.__name__}, "
                    f"got {type(value).__name__}: {value!r}."
                )

        # A copy, so that the caller's dict is left unchanged: what is left once the v3.0 keys
        # are popped are the extra keys
        remaining = dict(info)
        required = {key: remaining.pop(key) for key in cls._REQUIRED_KEYS}
        optional = {
            key: remaining.pop(key) for key in cls._OPTIONAL_KEYS if key in remaining
        }
        features = required.pop("features")

        return cls(
            features=[
                LeRobotFeature.from_dict(key, feature)
                for key, feature in features.items()
            ],
            **required,
            **optional,
            extra=remaining,
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Returns the content of `meta/info.json`: the `extra` keys and the v3.0 ones.
        """
        return {
            **self.extra,
            "codebase_version": self.codebase_version,
            "fps": self.fps,
            "features": {feature.key: feature.to_dict() for feature in self.features},
            "total_episodes": self.total_episodes,
            "total_frames": self.total_frames,
            "total_tasks": self.total_tasks,
            "chunks_size": self.chunks_size,
            "data_files_size_in_mb": self.data_files_size_in_mb,
            "video_files_size_in_mb": self.video_files_size_in_mb,
            "data_path": self.data_path,
            "splits": self.splits,
            "video_path": self.video_path,
            "robot_type": self.robot_type,
        }

    def check_compatible(self, other_info: "LeRobotInfo") -> None:
        """
        Checks that the episodes of `other_info` can be merged with the ones of this info: same
        features (see `LeRobotFeature` for what is compared), fps and robot type. The `extra`
        keys are not compared.

        Raises:
            ValueError: If the two infos differ in features, fps or robot type.
        """

        # Check that features are the same
        expected_by_key = {feature.key: feature for feature in self.features}
        found_by_key = {feature.key: feature for feature in other_info.features}

        differing = sorted(
            key
            for key in expected_by_key.keys() | found_by_key.keys()
            if expected_by_key.get(key) != found_by_key.get(key)
        )

        if differing:
            raise ValueError(
                f"Info are not compatible. "
                f"They contain differing features: {differing}."
            )

        # Check that FPS numbers are the same
        if self.fps != other_info.fps:
            raise ValueError(
                f"Info are not compatible. "
                f"Current info has {self.fps} fps while the other has {other_info.fps} fps."
            )

        # Check that the episodes come from the same robot
        if self.robot_type != other_info.robot_type:
            raise ValueError(
                f"Info are not compatible. "
                f"Current info has robot type '{self.robot_type}' while the other has "
                f"'{other_info.robot_type}'."
            )

    def drop_features(self, to_drop: List[str]) -> "LeRobotInfo":
        """
        Returns a copy of the info without the given features. Unknown features are ignored,
        and this info is left unchanged.

        Args:
            to_drop (List[str]): The names of the features to drop.

        Returns:
            LeRobotInfo: The info without the given features.
        """
        return replace(
            self, features=[f for f in self.features if f.key not in to_drop]
        )

    def get_path_to_data(self, root: Path, chunk_index: int, file_index: int) -> Path:
        """
        Returns the path of a data parquet file, as LeRobot finds it: the `data_path` template
        (e.g. `data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet`) filled with
        `chunk_index` and `file_index`, under `root`.

        Args:
            root (Path): The root folder of the dataset.
            chunk_index (int): The chunk of the file (the `data/chunk_index` of its episodes).
            file_index (int): The file in its chunk (the `data/file_index` of its episodes).

        Returns:
            Path: The path of the data file, under `root`.
        """
        return root / self.data_path.format(
            chunk_index=chunk_index, file_index=file_index
        )
