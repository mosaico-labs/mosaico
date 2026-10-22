from dataclasses import dataclass
from typing import List, Optional, Type

import pyarrow as pa

from mosaicolabs import Serializable


@dataclass
class LeRobotMessage:
    """
    A batch of rows of a LeRobot episode, ready to be pushed to its Mosaico topic with
    `TopicWriter.push_batch(timestamp_ns, data)`.

    It is yielded by the `LeRobotReader` batch iterators. When the rows of a topic cannot be read,
    `LeRobotEpisodeLoader` yields a message with its topic, columns and ontology only: `timestamp_ns`,
    `data` and `n_rows` are then `None`.
    """

    msco_topic_name: str
    """The Mosaico topic the batch is pushed to (e.g. `/timeseries`, `/observation/images/front`)."""

    column_names: List[str]
    """The data columns the batch comes from: every selected timeseries column for the timeseries
    topic, the image column for a camera topic."""

    timestamp_ns: Optional[pa.Array]
    """The `int64` timestamps of the rows, in nanoseconds since the episode start."""

    data: Optional[pa.RecordBatch]
    """The rows, with the fields of the topic ontology."""

    msco_ontology: Type[Serializable]
    """The ontology of the topic: the `Unmodeled` timeseries ontology, or `CompressedImage`."""

    n_rows: Optional[int]
    """The number of rows of the batch."""
