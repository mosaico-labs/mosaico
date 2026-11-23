"""
The merge of LeRobot episodes coming from different Mosaico sequences, possibly of different
source datasets, into the metadata of a single LeRobot dataset.
"""

from dataclasses import replace
from pathlib import Path
from typing import Dict, List, Optional

from mosaicolabs.logging_config import get_logger

from .episode import LeRobotEpisode
from .info import LeRobotInfo
from .metadata import LeRobotDatasetMeta

# Set the hierarchical logger
logger = get_logger(__name__)


class LeRobotDatasetBuilder:
    """
    Builds the metadata of a LeRobot dataset from episodes added one at a time (see
    `add_episode`).

    The first episode sets the reference `info`: every next episode must be compatible with it
    (see `LeRobotInfo.check_compatible`), or `add_episode` raises. Each accepted episode is
    renumbered after the previous ones, without its stats, and `info` is kept consistent with
    them (totals and `splits`). The new index of each episode is kept by the name of its
    sequence (`episode_index_by_sequence`). The dataset is written as a single data file and a
    single episodes file. The video features, not supported yet, are dropped.

    Example:
        ```python
        builder = LeRobotDatasetBuilder()
        for sequence_name, info, tasks, length in sources:
            builder.add_episode(sequence_name, info, tasks, length)

        info, episodes = builder.info, builder.episodes
        ```
    """

    def __init__(self):
        self._info: Optional[LeRobotInfo] = None
        """The `info` of the first episode, without the video features."""

        self._episode_by_sequence: Dict[str, LeRobotEpisode] = {}
        """The new `episode_index` of each accepted episode, keyed by the name of its sequence."""

        self._current_index = 0
        """The current episode index. Increases by episode length for each time a new episode is added"""

    @property
    def info(self) -> Optional[LeRobotInfo]:
        """
        The `info` of the dataset, with the totals and `splits` of the accepted episodes, or
        `None` if no episode was accepted yet.
        """
        if self._info is None:
            return None

        return replace(
            self._info,
            video_path=None,
            total_episodes=len(self._episode_by_sequence),
            total_frames=sum(
                episode.length for episode in self._episode_by_sequence.values()
            ),
            total_tasks=len(self.tasks),
            splits={"train": f"0:{len(self._episode_by_sequence)}"},
        )

    @property
    def episodes(self) -> List[LeRobotEpisode]:
        """The accepted episodes, renumbered in the order they were added."""
        return list(self._episode_by_sequence.values())

    @property
    def tasks(self) -> List[str]:
        """The distinct tasks of the accepted episodes, in the order they first appear."""
        return list(
            dict.fromkeys(
                task for e in self._episode_by_sequence.values() for task in e.tasks
            )
        )

    @property
    def episode_index_by_sequence(self) -> Dict[str, int]:
        """
        The new `episode_index` of each accepted episode, keyed by the name of its sequence, in
        episode order.
        """
        return {
            s_name: e.episode_index for s_name, e in self._episode_by_sequence.items()
        }

    def add_episode(
        self, sequence_name: str, info: LeRobotInfo, tasks: List[str], length: int
    ) -> None:
        """
        Adds the episode of a sequence to the dataset, after the previous ones, renumbered.

        Episode `length` is the number of rows extracted from Mosaico: its indexes are set by
        its position in the new dataset. Only its `tasks` is taken from its source dataset.
        Its stats are not taken either: those of its source dataset do not apply to the new
        one (it can hold only part of the episode). They are computed from the data of the
        new dataset instead (see `compute_episode_stats`).

        The video features are dropped from `info` before comparing it with the one of the
        first episode (see `LeRobotInfo.check_compatible`).

        Args:
            sequence_name (str): The name of the sequence holding the episode.
            info (LeRobotInfo): The `info` of the source dataset of the episode.
            tasks (List[str]): The natural-language tasks performed in the episode.
            length (int): The number of frames of the episode.

        Raises:
            ValueError: If the sequence was already added, its `info` is not compatible with
                the one of the first episode or length is <= 0. The dataset is left unchanged.
        """
        if sequence_name in self._episode_by_sequence:
            raise ValueError(f"Sequence '{sequence_name}' has already been added.")

        if length <= 0:
            raise ValueError(f"Sequence '{sequence_name}' has length <=0: {length}.")

        video_features = [f.key for f in info.features if f.is_video()]

        if video_features:
            logger.warning(
                f"Skipping video features {video_features}: video files are not "
                "supported yet."
            )

        info = replace(info, features=[f for f in info.features if not f.is_video()])

        if self._info is None:
            self._info = info
        else:
            self._info.check_compatible(info)

        episode = LeRobotEpisode(
            episode_index=len(self._episode_by_sequence),
            tasks=list(tasks),
            length=length,
            dataset_from_index=self._current_index,
            dataset_to_index=self._current_index + length,
            # A single data file and a single episodes file
            data_chunk_index=0,
            data_file_index=0,
            meta_episodes_chunk_index=0,
            meta_episodes_file_index=0,
            # Computed from the data of the new dataset
            stats={},
        )

        self._current_index += length
        self._episode_by_sequence[sequence_name] = episode

    def build_lerobot_metadata(self, root: Path) -> LeRobotDatasetMeta:
        """
        Creates a LeRobotDatasetMeta from LeRobotDatasetBuilder. Stats are not computed
        and should be computed separately.
        Raises RuntimeError if not episode has been added yet.
        """
        if self.info is None:
            raise RuntimeError(
                "Cannot generate `LeRobotDatasetMeta` if no episode has been added. "
                "Add episodes using add_episode()"
            )

        return LeRobotDatasetMeta.from_data(root, self.info, self.episodes, self.tasks)

    def get_episode_by_sequence_name(self, seq_name: str):
        """
        Returns the LeRobotEpisode associated to Mosaico sequence name. None if none is found
        """
        return self._episode_by_sequence.get(seq_name)
