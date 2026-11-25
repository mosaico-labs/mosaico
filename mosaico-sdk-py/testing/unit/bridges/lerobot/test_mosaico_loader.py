"""
Tests of `LeRobotEpisodeStreamer`: how the rows of an episode, streamed by Mosaico one topic at
a time in batches of their own size, are lined up across the topics.

The `SequenceDataStreamer` is faked (see `episode_streamer_fakes`): only the batches it returns
matter here. Its real counterpart is tested by the integration tests of the LeRobot extraction.
"""

from typing import List, Sequence

import pyarrow as pa
import pytest

from testing.unit.bridges.lerobot.episode_streamer_fakes import episode_streamer

TIMESTAMPS = [0, 1, 2, 3, 4]
"""The `timestamp_ns` of the rows of the episode, one per frame."""


def _batches(
    column: str, timestamps: Sequence[int], sizes: Sequence[int]
) -> List[pa.RecordBatch]:
    """The rows of `timestamps`, split into batches of `sizes` rows. Each row holds its
    `timestamp_ns` and, in `column`, the same value: a row can be traced after realignment."""
    batches, start = [], 0
    for size in sizes:
        rows = list(timestamps[start : start + size])
        batches.append(
            pa.record_batch(
                {
                    "timestamp_ns": pa.array(rows, pa.int64()),
                    column: pa.array(rows, pa.int64()),
                }
            )
        )
        start += size
    return batches


def test_episode_streamer_realigns_batches_of_different_sizes():
    """Each yield holds the rows every topic has: `/a` (3 + 2 rows) and `/b` (1 + 4 rows)
    are sliced into 1, 2 and 2 rows."""
    streamer = episode_streamer(
        {
            "/a": _batches("a", TIMESTAMPS, [3, 2]),
            "/b": _batches("b", TIMESTAMPS, [1, 4]),
        },
        length=len(TIMESTAMPS),
    )

    yielded = list(streamer)

    assert [len(timestamps) for timestamps, _ in yielded] == [1, 2, 2]
    assert [
        t for timestamps, _ in yielded for t in timestamps.to_pylist()
    ] == TIMESTAMPS
    for timestamps, batches in yielded:
        # The rows of each topic are those of the timestamps, without `timestamp_ns`
        assert batches["/a"].column_names == ["a"]
        assert batches["/b"].column_names == ["b"]
        assert batches["/a"].column("a").to_pylist() == timestamps.to_pylist()
        assert batches["/b"].column("b").to_pylist() == timestamps.to_pylist()


def test_episode_streamer_rejects_a_topic_ending_before_the_others():
    streamer = episode_streamer(
        {
            "/a": _batches("a", TIMESTAMPS, [5]),
            "/b": _batches("b", TIMESTAMPS[:3], [3]),
        },
        length=len(TIMESTAMPS),
    )

    with pytest.raises(ValueError, match=r"topics \['/b'\] ended before the others"):
        list(streamer)


def test_episode_streamer_rejects_different_timestamps_across_topics():
    streamer = episode_streamer(
        {
            "/a": _batches("a", TIMESTAMPS, [5]),
            "/b": _batches("b", [t + 10 for t in TIMESTAMPS], [5]),
        },
        length=len(TIMESTAMPS),
    )

    with pytest.raises(ValueError, match="hold different timestamps"):
        list(streamer)


def test_episode_streamer_rejects_a_row_count_different_from_the_length():
    streamer = episode_streamer(
        {
            "/a": _batches("a", TIMESTAMPS, [5]),
            "/b": _batches("b", TIMESTAMPS, [5]),
        },
        length=4,
    )

    with pytest.raises(
        ValueError, match="5 rows streamed, while its episode has length 4"
    ):
        list(streamer)
