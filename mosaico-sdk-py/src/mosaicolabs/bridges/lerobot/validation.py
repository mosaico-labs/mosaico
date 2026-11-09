"""
LeRobot episode validation.

Checks each episode before it is injected, so that an invalid one is skipped. The `timestamp`
column is checked a batch at a time, as it is read: the last value of each batch is kept to check
the interval to the next batch.
"""

from typing import List

import numpy as np

from .metadata import _TIMESTAMP_COLUMN_NAME
from .reader import LeRobotReader

_DEFAULT_TOLERANCE_S = 1e-4
"""The LeRobot default (`LeRobotDataset(tolerance_s=1e-4)`)."""


def _validate_timestamps(
    timestamps_s: np.ndarray, fps: float, is_start: bool = True
) -> List[str]:
    """
    Runs every check on consecutive timestamps of an episode.

    Periods and start are checked within `_DEFAULT_TOLERANCE_S`. A NaN also fails the checks on
    consecutive values. Fewer than two timestamps have no intervals to check.

    Args:
        timestamps_s (np.ndarray): The LeRobot timestamps, in float seconds (NaN for nulls).
        fps (float): The frame rate of the dataset.
        is_start (bool): Whether the timestamps are the start of the episode, which has to be 0.

    Returns:
        List[str]: The problems found, empty if the timestamps are valid.
    """
    periods_s = np.diff(timestamps_s)

    checks = {
        "null or NaN timestamps": not np.isnan(timestamps_s).any(),
        "timestamps not strictly increasing": (periods_s > 0).all(),
        "timestamps not 1 / fps apart": (
            np.abs(periods_s - 1.0 / fps) <= _DEFAULT_TOLERANCE_S
        ).all(),
        "timestamps not starting at 0": not is_start
        or timestamps_s.size == 0
        or abs(timestamps_s[0]) <= _DEFAULT_TOLERANCE_S,
    }
    return [message for message, passed in checks.items() if not passed]


def validate_episode(reader: LeRobotReader, episode_index: int) -> List[str]:
    """
    Runs every check on an episode, before it is injected, one batch at a time.

    An episode that cannot be read (e.g. its data file is missing) is a problem.

    Args:
        reader (LeRobotReader): The reader of the dataset holding the episode.
        episode_index (int): The episode index.

    Returns:
        List[str]: The problems found, empty if the episode is valid.
    """
    issues: List[str] = []  # a problem found in several batches is reported once
    previous_s = None
    try:
        for batch in reader._iter_episode_batches(
            episode_index, [_TIMESTAMP_COLUMN_NAME]
        ):
            # Arrow -> numpy float64: nulls become NaN, caught by the NaN check
            timestamps_s = np.asarray(batch.column(0), dtype=np.float64)
            if previous_s is not None:
                timestamps_s = np.concatenate([[previous_s], timestamps_s])

            batch_issues = _validate_timestamps(
                timestamps_s, reader._meta.fps, is_start=previous_s is None
            )
            issues += [issue for issue in batch_issues if issue not in issues]
            previous_s = timestamps_s[-1]
    except (FileNotFoundError, ValueError) as e:
        return [f"unreadable: {e}"]

    return issues
