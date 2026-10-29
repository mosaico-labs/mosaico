"""
Validation of LeRobot episodes before they are injected.
"""

from typing import List

import numpy as np
import pyarrow as pa

from .reader import LeRobotReader

_DEFAULT_TOLERANCE_S = 1e-4
"""The LeRobot default (`LeRobotDataset(tolerance_s=1e-4)`)."""


def _validate_timestamps(timestamp: pa.ChunkedArray, fps: float) -> List[str]:
    """
    Runs every check on the timestamps of an episode.

    Periods and start are checked within `_DEFAULT_TOLERANCE_S`. A NaN also fails the checks on
    consecutive values. An episode with fewer than two frames has no intervals to check.

    Args:
        timestamp (pa.ChunkedArray): The LeRobot timestamps of the episode, in float seconds.
        fps (float): The frame rate of the dataset.

    Returns:
        List[str]: The problems found, empty if the timestamps are valid.
    """
    # float64 for the arithmetic; nulls become NaN
    timestamp_s = np.asarray(timestamp, dtype=np.float64)
    periods_s = np.diff(timestamp_s)

    checks = {
        "null or NaN timestamps": not np.isnan(timestamp_s).any(),
        "timestamps not strictly increasing": (periods_s > 0).all(),
        "timestamps not 1 / fps apart": (
            np.abs(periods_s - 1.0 / fps) <= _DEFAULT_TOLERANCE_S
        ).all(),
        "timestamps not starting at 0": timestamp_s.size == 0
        or abs(timestamp_s[0]) <= _DEFAULT_TOLERANCE_S,
    }
    return [message for message, passed in checks.items() if not passed]


def validate_episode(reader: LeRobotReader, episode_index: int) -> List[str]:
    """
    Runs every check on an episode, before it is injected.

    An episode that cannot be read (e.g. its data file is missing) is a problem.

    Args:
        reader (LeRobotReader): The reader of the dataset holding the episode.
        episode_index (int): The episode index.

    Returns:
        List[str]: The problems found, empty if the episode is valid.
    """
    try:
        timestamp = reader.read_episode_timestamps(episode_index)
    except (FileNotFoundError, ValueError) as e:
        return [f"unreadable: {e}"]

    return _validate_timestamps(timestamp, reader._meta.fps)
