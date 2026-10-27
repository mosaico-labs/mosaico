"""
The `meta/info.json` file of a LeRobot v3.0 dataset.
"""

from dataclasses import MISSING, dataclass, fields
from typing import Any, Dict, List, Optional

from .feature import LeRobotFeature

"""The only LeRobot dataset format version supported by the bridge."""
SUPPORTED_CODEBASE_VERSION = "v3.0"


@dataclass(frozen=True)
class LeRobotInfo:
    """
    The content of the `meta/info.json` file of a LeRobot v3.0 dataset.

    Each field is a key of `meta/info.json`: the fields with no default are the keys LeRobot
    always writes, and are required. Any key outside the v3.0 format is dropped.
    """

    codebase_version: str
    """The LeRobot dataset format version (e.g. `v3.0`)."""

    fps: int
    """The dataset frame rate."""

    features: List[LeRobotFeature]
    """The dataset features, in the order of `meta/info.json`."""

    total_episodes: int
    """The number of episodes of the dataset."""

    total_frames: int
    """The number of frames of the dataset, over every episode."""

    total_tasks: int
    """The number of distinct tasks of the dataset."""

    chunks_size: int
    """The maximum number of files of each `chunk-XXX` folder."""

    data_files_size_in_mb: int
    """The size, in MB, beyond which LeRobot starts a new data file."""

    video_files_size_in_mb: int
    """The size, in MB, beyond which LeRobot starts a new video file."""

    data_path: str
    """The template of the data file paths (see `LeRobotDatasetMeta.get_parquet_file`)."""

    splits: Dict[str, str]
    """The dataset splits, as ranges of episode indexes (e.g. `{"train": "0:50"}`)."""

    video_path: Optional[str] = None
    """The template of the video file paths, or `None` if the dataset has no video feature."""

    robot_type: Optional[str] = None
    """The robot that recorded the dataset (e.g. `so_follower`), if known."""

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
                missing, or a feature has no `dtype`.
        """
        codebase_version = info.get("codebase_version")
        if codebase_version != SUPPORTED_CODEBASE_VERSION:
            raise ValueError(
                f"Unsupported LeRobot dataset version '{codebase_version}': only "
                f"'{SUPPORTED_CODEBASE_VERSION}' is supported."
            )

        missing = [
            f.name for f in fields(cls) if f.default is MISSING and f.name not in info
        ]
        if missing:
            raise ValueError(
                f"'meta/info.json' is missing the required keys {missing}. "
                f"Available keys: {sorted(info)}"
            )

        return cls(
            codebase_version=codebase_version,
            fps=info["fps"],
            features=[
                LeRobotFeature.from_dict(key, feature)
                for key, feature in info["features"].items()
            ],
            total_episodes=info["total_episodes"],
            total_frames=info["total_frames"],
            total_tasks=info["total_tasks"],
            chunks_size=info["chunks_size"],
            data_files_size_in_mb=info["data_files_size_in_mb"],
            video_files_size_in_mb=info["video_files_size_in_mb"],
            data_path=info["data_path"],
            splits=info["splits"],
            video_path=info.get("video_path"),
            robot_type=info.get("robot_type"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Returns the content of `meta/info.json`."""
        return {
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
        features (see `LeRobotFeature` for what is compared), fps and robot type.

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
