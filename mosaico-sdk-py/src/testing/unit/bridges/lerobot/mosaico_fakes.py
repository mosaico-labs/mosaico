"""
Fakes of the Mosaico client and sequence handlers, to test the LeRobot dataset extraction
without a Mosaico server.
"""

from typing import Any, Dict, Iterable, Optional

from mosaicolabs.bridges.lerobot.metadata import LeRobotEpisode, LeRobotInfo
from mosaicolabs.bridges.lerobot.schema_metadata import LeRobotSchemaMetadata

TOPICS = ["/timeseries", "/observation/images/front"]
"""The default topics of a fake sequence."""


class FakeSequenceHandler:
    """Stands for a `SequenceHandler`: its name, user metadata and topics, and whether it is closed."""

    def __init__(
        self, name: str, user_metadata: Dict[str, Any], topics: Iterable[str] = TOPICS
    ):
        self.name = name
        self.user_metadata = user_metadata
        self.topics = list(topics)
        self.closed = False

    def close(self):
        self.closed = True


class FakeClient:
    """Stands for a connected `MosaicoClient`: returns the given sequences by name."""

    def __init__(self, sequences: Iterable[FakeSequenceHandler]):
        self._sequences = {sequence.name: sequence for sequence in sequences}

    def sequence_handler(self, sequence_name: str) -> Optional[FakeSequenceHandler]:
        return self._sequences.get(sequence_name)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def lerobot_sequence(
    name: str, info: LeRobotInfo, episode: LeRobotEpisode
) -> FakeSequenceHandler:
    """A sequence as ingested by the LeRobot bridge (see `LeRobotInjector`)."""
    meta = LeRobotSchemaMetadata(info=info.to_dict(), episode=episode.to_dict())
    return FakeSequenceHandler(name, meta.merge_into({"operator": "x"}))
