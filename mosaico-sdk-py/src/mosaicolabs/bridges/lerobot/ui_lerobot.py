"""
Progress UI of the LeRobot bridge.

A LeRobot dataset is ingested one episode at a time, each episode as its own sequence. Instead of
one progress UI per episode, `LeRobotProgressManager` shows a single UI for the whole dataset: the
column bars and the global bar count the rows of every episode, and an extra bar tracks the
episodes, with the number of succeeded and failed ones.
"""

from typing import Dict, Optional

from rich.progress import TaskID

from ..ui import LoaderUIAPI, ProgressManager


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

    def __init__(self, loader: LoaderUIAPI, n_episodes: int):
        """
        Args:
            loader (LoaderUIAPI): The loader of the dataset, used to size the bars.
        """
        super().__init__(loader)
        self.episode_task: Optional[TaskID] = None
        """The episode bar, created by `setup()`."""
        self._n_episodes = n_episodes
        """The total number of episodes that will be ingested"""
        self._current_episode: Optional[int] = None
        """The index of the episode being uploaded, `None` between two episodes."""
        self._expected_rows_at_end: Optional[Dict[str, int]] = None
        """Mapper tracking the expected number of ingested message at episode end for each accepted column"""
        self._n_succeeded = 0
        """The number of episodes uploaded."""
        self._n_failed = 0
        """The number of episodes failed."""

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

        self._expected_rows_at_end = {col_name: 0 for col_name in self.loader.topics}

    def start_episode(self, episode_index: int, episode_rows: int):
        """
        Shows `episode_index` as the episode being uploaded.

        Args:
            episode_index (int): The index of the episode.
        """
        self._current_episode = episode_index

        self._refresh_episode_label()

        if not self._expected_rows_at_end:
            return

        # Update expected progress bar at episode end
        for k in self._expected_rows_at_end.keys():
            self._expected_rows_at_end[k] += episode_rows

    def finish_episode(self, succeeded: bool):
        """
        Closes the episode being uploaded: counts it as succeeded or failed, advances the episode
        bar, and fills in the rows the episode did not upload (the rows after the error of a
        failed episode), so that the column bars and the global bar account for every finished
        episode.

        Args:
            succeeded (bool): Whether the episode was fully uploaded.
        """
        if self._current_episode is None or self._expected_rows_at_end is None:
            return

        if succeeded:
            self._n_succeeded += 1
        else:
            self._n_failed += 1

        # Update remaining columns that have not reached the expected counter + global bar
        # Remains zero if all columns are at the expected message counter
        global_bar_to_fill = 0

        for col_name, expected_val in self._expected_rows_at_end.items():
            task_id = self.tasks[col_name]
            task = next(t for t in self.progress.tasks if t.id == task_id)

            if task.completed == expected_val:
                continue  # In this case the column has the expected message counter
            else:
                msg_remaining = expected_val - task.completed
                self.progress.advance(task_id, msg_remaining)
                global_bar_to_fill += msg_remaining

        # Update global bar
        self.advance_global(int(global_bar_to_fill))

        # Update episode bar
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
