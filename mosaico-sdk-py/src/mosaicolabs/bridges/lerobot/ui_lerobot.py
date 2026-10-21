"""
Progress UI of the LeRobot bridge.

A LeRobot dataset is ingested one episode at a time, each episode as its own sequence. Instead of
one progress UI per episode, `LeRobotProgressManager` shows a single UI for the whole dataset: the
column bars and the global bar count the rows of every episode, and an extra bar tracks the
episodes, with the number of succeeded and failed ones.
"""

from typing import Dict, Optional

from rich.progress import TaskID

from ..ui import ProgressManager
from .loader import LeRobotLoader


class LeRobotProgressManager(ProgressManager):
    """
    Progress UI for a LeRobot dataset, ingested one episode (sequence) at a time.

    On top of the bars of `ProgressManager`, here sized on every episode of the dataset (one bar
    per column, and the global one), it shows an episode bar, labelled with the index of the
    episode being uploaded and the number of succeeded and failed episodes.

    Expected usage, around the upload loop:

    1. `setup()` once, before the first episode.
    2. `start_episode()` before uploading an episode.
    3. `finish_episode()` once the episode is done, succeeded or failed: the rows the episode
       did not upload are filled in, so that every bar reaches its total once all the episodes
       are done.
    """

    def __init__(self, loader: LeRobotLoader):
        """
        Args:
            loader (LeRobotLoader): The loader of the dataset, used to size the bars.
        """
        super().__init__(loader)
        self._lerobot_loader = loader
        """The loader, with its LeRobot specific API (the message counts of an episode)."""
        self._n_episodes = loader.n_episodes
        """The number of episodes of the dataset."""
        self.episode_task: Optional[TaskID] = None
        """The episode bar, created by `setup()`."""
        self._current_episode: Optional[int] = None
        """The index of the episode being uploaded, `None` between two episodes."""
        self._n_succeeded = 0
        """The number of episodes uploaded."""
        self._n_failed = 0
        """The number of episodes failed."""
        self._rows_done: Dict[str, int] = {}
        """The rows of the finished episodes, per accepted column."""

    def setup(self):
        """
        Creates the episode bar, then the column bars and the global bar (see
        `ProgressManager.setup`). Must be called before the first episode.
        """
        # Created first, so that it is shown above the column bars
        self.episode_task = self.progress.add_task(
            "", total=self._n_episodes, name=self._episode_label()
        )
        super().setup()
        self._rows_done = {topic: 0 for topic in self.loader.topics}

    def start_episode(self, episode_index: int):
        """
        Shows `episode_index` as the episode being uploaded.

        Args:
            episode_index (int): The index of the episode.
        """
        self._current_episode = episode_index
        self._refresh_episode_label()

    def finish_episode(self, succeeded: bool):
        """
        Closes the episode being uploaded: counts it as succeeded or failed, advances the episode
        bar, and fills in the rows the episode did not upload (the rows after the error of a
        failed episode), so that the column bars and the global bar account for every finished
        episode.

        Args:
            succeeded (bool): Whether the episode was fully uploaded.
        """
        if self._current_episode is None:
            return

        if succeeded:
            self._n_succeeded += 1
        else:
            self._n_failed += 1

        for topic in self.loader.topics:
            self._rows_done[topic] += self._lerobot_loader.msg_count(
                topic, self._current_episode
            )
            self.progress.update(self.tasks[topic], completed=self._rows_done[topic])

        if self.global_task is not None:
            # The global bar is advanced along with every column bar (see `advance_all`)
            self.progress.update(
                self.global_task, completed=sum(self._rows_done.values())
            )

        if self.episode_task is not None:
            self.progress.advance(self.episode_task)

        self._current_episode = None
        self._refresh_episode_label()

    def _episode_label(self) -> str:
        """The label of the episode bar: the episode being uploaded and the counters."""
        current = (
            ""
            if self._current_episode is None
            else f" (uploading index {self._current_episode})"
        )
        return (
            f"Episodes{current}  "
            f"[green]✓ {self._n_succeeded}[/]  [red]✗ {self._n_failed}[/]"
        )

    def _refresh_episode_label(self):
        if self.episode_task is not None:
            self.progress.update(self.episode_task, name=self._episode_label())
