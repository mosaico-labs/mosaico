"""
Tests of `LeRobotEpisodeStreamer`: how the rows of an episode, streamed by Mosaico one topic at
a time in batches of their own size, are lined up across the topics.

The `SequenceDataStreamer` is faked: only the batches it returns matter here. Its real
counterpart is tested by the integration tests of the LeRobot extraction.
"""

from typing import Dict, List, Optional, Sequence

import pyarrow as pa
import pytest

from mosaicolabs.bridges.lerobot.metadata import LeRobotEpisode
from mosaicolabs.bridges.lerobot.mosaico_loader import LeRobotEpisodeStreamer

SEQUENCE_NAME = "sequence"

TIMESTAMPS = [0, 1, 2, 3, 4]
"""The `timestamp_ns` of the rows of the episode, one per frame."""


class _FakeTopicStreamer:
    """Stands for a `TopicDataStreamer` in batch mode: returns its batches, then `None`."""

    def __init__(self, batches: List[pa.RecordBatch]):
        self._batches = iter(batches)

    def _fetch_next_batch(self) -> Optional[pa.RecordBatch]:
        return next(self._batches, None)


class _FakeSequenceStreamer:
    """Stands for a `SequenceDataStreamer`: the parts `LeRobotEpisodeStreamer` uses."""

    def __init__(self, batches_by_topic: Dict[str, List[pa.RecordBatch]]):
        self._topics = {
            topic: _FakeTopicStreamer(batches)
            for topic, batches in batches_by_topic.items()
        }

    def _as_batch_provider(self) -> Dict[str, _FakeTopicStreamer]:
        return self._topics

    def close(self):
        pass


class _FakeSequenceHandler:
    """Stands for a `SequenceHandler`: the parts `LeRobotEpisodeStreamer` uses."""

    def __init__(self, name, streamer: _FakeSequenceStreamer):
        self._name = name
        self._streamer = streamer

    @property
    def name(self) -> str:
        return self._name

    def get_data_streamer(self) -> _FakeSequenceStreamer:
        return self._streamer

    def close(self):
        pass


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


def _episode_streamer(
    batches_by_topic: Dict[str, List[pa.RecordBatch]], length: int = len(TIMESTAMPS)
) -> LeRobotEpisodeStreamer:
    episode = LeRobotEpisode(
        episode_index=0,
        tasks=["task"],
        length=length,
        data_chunk_index=0,
        data_file_index=0,
        dataset_from_index=0,
        dataset_to_index=length,
    )
    return LeRobotEpisodeStreamer(
        _FakeSequenceHandler(SEQUENCE_NAME, _FakeSequenceStreamer(batches_by_topic)),
        episode,
    )


def test_episode_streamer_realigns_batches_of_different_sizes():
    """Each yield holds the rows every topic has: `/a` (3 + 2 rows) and `/b` (1 + 4 rows)
    are sliced into 1, 2 and 2 rows."""
    streamer = _episode_streamer(
        {
            "/a": _batches("a", TIMESTAMPS, [3, 2]),
            "/b": _batches("b", TIMESTAMPS, [1, 4]),
        }
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
    streamer = _episode_streamer(
        {
            "/a": _batches("a", TIMESTAMPS, [5]),
            "/b": _batches("b", TIMESTAMPS[:3], [3]),
        }
    )

    with pytest.raises(ValueError, match=r"topics \['/b'\] ended before the others"):
        list(streamer)


def test_episode_streamer_rejects_different_timestamps_across_topics():
    streamer = _episode_streamer(
        {
            "/a": _batches("a", TIMESTAMPS, [5]),
            "/b": _batches("b", [t + 10 for t in TIMESTAMPS], [5]),
        }
    )

    with pytest.raises(ValueError, match="hold different timestamps"):
        list(streamer)


def test_episode_streamer_rejects_a_row_count_different_from_the_length():
    streamer = _episode_streamer(
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
