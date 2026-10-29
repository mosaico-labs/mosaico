"""
The merge of LeRobot episodes coming from different Mosaico sequences, possibly of different
source datasets, into the metadata of a single LeRobot dataset.
"""

from dataclasses import replace
from typing import Dict, List, Optional

from mosaicolabs.logging_config import get_logger

from .episode import LeRobotEpisode
from .info import LeRobotInfo

# Set the hierarchical logger
logger = get_logger(__name__)


class LeRobotDatasetBuilder:
    """
    Builds the metadata of a LeRobot dataset from episodes added one at a time (see
    `add_episode`).

    The first episode sets the reference `info`: every next episode must be compatible with it
    (see `LeRobotInfo.check_compatible`), or `add_episode` raises. Each accepted episode is
    renumbered after the previous ones, and `info` is kept consistent with them (totals and
    `splits`). The new index of each episode is kept by the name of its sequence
    (`episode_index_by_sequence`). The dataset is written as a single data file and a single
    episodes file. The video features, not supported yet, are dropped.

    Example:
        ```python
        builder = LeRobotDatasetBuilder()
        for sequence_name, info, episode in sources:
            builder.add_episode(sequence_name, info, episode)

        info, episodes = builder.info, builder.episodes
        ```
    """

    def __init__(self):
        self._info: Optional[LeRobotInfo] = None
        """The `info` of the first episode, without the video features."""

        self._episodes: List[LeRobotEpisode] = []
        """The accepted episodes, renumbered in the order they were added."""

        self._episode_index_by_sequence: Dict[str, int] = {}
        """The new `episode_index` of each accepted episode, keyed by the name of its sequence."""

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
            total_episodes=len(self._episodes),
            total_frames=sum(episode.length for episode in self._episodes),
            total_tasks=len({task for e in self._episodes for task in e.tasks}),
            splits={"train": f"0:{len(self._episodes)}"},
        )

    @property
    def episodes(self) -> List[LeRobotEpisode]:
        """The accepted episodes, renumbered in the order they were added."""
        return list(self._episodes)

    @property
    def episode_index_by_sequence(self) -> Dict[str, int]:
        """
        The new `episode_index` of each accepted episode, keyed by the name of its sequence, in
        episode order.
        """
        return dict(self._episode_index_by_sequence)

    def add_episode(
        self, sequence_name: str, info: LeRobotInfo, episode: LeRobotEpisode
    ) -> None:
        """
        Adds the episode of a sequence to the dataset, after the previous ones, renumbered.

        The video features are dropped from `info` before comparing it with the one of the
        first episode (see `LeRobotInfo.check_compatible`).

        Args:
            sequence_name (str): The name of the sequence holding the episode.
            info (LeRobotInfo): The `info` of the source dataset of the episode.
            episode (LeRobotEpisode): The episode, as in its source dataset.

        Raises:
            ValueError: If the sequence was already added, or its `info` is not compatible with
                the one of the first episode. The dataset is left unchanged.
        """
        if sequence_name in self._episode_index_by_sequence:
            raise ValueError(f"Sequence '{sequence_name}' has already been added.")

        video_features = [f.key for f in info.features if f.is_video()]
        info = replace(info, features=[f for f in info.features if not f.is_video()])

        if self._info is None:
            if video_features:
                logger.warning(
                    f"Skipping video features {video_features}: video files are not "
                    "supported yet."
                )
            self._info = info
        else:
            self._info.check_compatible(info)

        start = self._episodes[-1].dataset_to_index if self._episodes else 0
        episode = replace(
            episode,
            episode_index=len(self._episodes),
            dataset_from_index=start,
            dataset_to_index=start + episode.length,
            # A single data file and a single episodes file
            data_chunk_index=0,
            data_file_index=0,
            meta_episodes_chunk_index=0,
            meta_episodes_file_index=0,
        )

        self._episodes.append(episode)
        self._episode_index_by_sequence[sequence_name] = episode.episode_index
