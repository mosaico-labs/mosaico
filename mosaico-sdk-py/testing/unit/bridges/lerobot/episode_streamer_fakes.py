"""
Fakes of the Mosaico streaming read by `LeRobotEpisodeStreamer`, shared by the unit tests of
the streamer and of the writer: only the batches the topics return matter here.

The real streaming is tested by the integration tests of the LeRobot extraction.
"""

from typing import Dict, List, Optional

import pyarrow as pa

from mosaicolabs.bridges.lerobot.metadata import LeRobotEpisode
from mosaicolabs.bridges.lerobot.mosaico_loader import LeRobotEpisodeStreamer

SEQUENCE_NAME = "sequence"
"""The name of the sequence of the faked episodes."""


class FakeTopicStreamer:
    """Stands for a `TopicDataStreamer` in batch mode: returns its batches, then `None`."""

    def __init__(self, batches: List[pa.RecordBatch]):
        self._batches = iter(batches)

    def _fetch_next_batch(self) -> Optional[pa.RecordBatch]:
        return next(self._batches, None)


class FakeSequenceStreamer:
    """Stands for a `SequenceDataStreamer`: the parts `LeRobotEpisodeStreamer` uses."""

    def __init__(self, batches_by_topic: Dict[str, List[pa.RecordBatch]]):
        self._topics = {
            topic: FakeTopicStreamer(batches)
            for topic, batches in batches_by_topic.items()
        }

    def _as_batch_provider(self) -> Dict[str, FakeTopicStreamer]:
        return self._topics

    def close(self):
        pass


class FakeSequenceHandler:
    """Stands for a `SequenceHandler`: the parts `LeRobotEpisodeStreamer` uses. Its streamer
    is returned as it is: the episode can be streamed once."""

    def __init__(self, name: str, streamer: FakeSequenceStreamer):
        self._name = name
        self._streamer = streamer

    @property
    def name(self) -> str:
        return self._name

    def get_data_streamer(self) -> FakeSequenceStreamer:
        return self._streamer

    def close(self):
        pass


def episode_streamer(
    batches_by_topic: Dict[str, List[pa.RecordBatch]],
    length: int,
    dataset_to_index: Optional[int] = None,
) -> LeRobotEpisodeStreamer:
    """
    The episode 0 of a dataset, starting at row 0, streamed from `batches_by_topic` (each
    batch with its `timestamp_ns` column).

    `dataset_to_index` is `length` by default: give another one for an episode whose
    metadata do not match its rows.
    """
    episode = LeRobotEpisode(
        episode_index=0,
        tasks=["task"],
        length=length,
        data_chunk_index=0,
        data_file_index=0,
        dataset_from_index=0,
        dataset_to_index=length if dataset_to_index is None else dataset_to_index,
    )
    return LeRobotEpisodeStreamer(
        FakeSequenceHandler(SEQUENCE_NAME, FakeSequenceStreamer(batches_by_topic)),
        episode,
    )
