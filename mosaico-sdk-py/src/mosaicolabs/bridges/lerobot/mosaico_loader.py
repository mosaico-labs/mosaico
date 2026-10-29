"""
Loader of the LeRobot dataset extraction: reads the Mosaico sequences ingested by the LeRobot
bridge (one per episode, see `LeRobotInjector`) to export them back to a LeRobot dataset.

As `LeRobotLoader` resolves the metadata of a dataset from its `meta/` folder, this loader
resolves the metadata of the exported dataset from the `_lerobot_` metadata of the sequences
(see `LeRobotSchemaMetadata`), merging their episodes with a `LeRobotDatasetBuilder`.
"""

from typing import Dict, Iterator, List, Optional, Tuple

import pyarrow as pa

from mosaicolabs import TopicDataStreamer
from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.handlers import SequenceHandler
from mosaicolabs.logging_config import get_logger

from ..helpers import _validate_sequence
from ..loader_base import BaseLoader
from .metadata import LeRobotEpisode, LeRobotInfo
from .metadata.dataset_builder import LeRobotDatasetBuilder
from .schema_metadata import LeRobotSchemaMetadata

# Set the hierarchical logger
logger = get_logger(__name__)

_TIMESTAMP_NS_COLUMN_NAME = "timestamp_ns"
"""The column of the timestamp of the messages, in every topic."""


def _unpack_sequence(sequence: SequenceHandler) -> Tuple[LeRobotInfo, LeRobotEpisode]:
    """
    Reads the `_lerobot_` metadata of a sequence (see `LeRobotSchemaMetadata.restore`): its
    `info` and its episode, as in their source dataset.

    Raises:
        ValueError: If the `_lerobot_` metadata is missing or malformed.
    """
    try:
        block = LeRobotSchemaMetadata.restore(sequence.user_metadata)
        info = LeRobotInfo.from_dict(block["info"])
        episode = LeRobotEpisode.from_dict(block["episode"])
    except (AttributeError, KeyError, TypeError, ValueError) as e:
        raise ValueError(
            f"Sequence '{sequence.name}' has missing or malformed "
            f"'{LeRobotSchemaMetadata.KEY}' metadata: {e!r}"
        ) from e

    return info, episode


def _pop_timestamps(
    sequence_name: str, batches: Dict[str, pa.RecordBatch]
) -> Tuple[pa.Array, Dict[str, pa.RecordBatch]]:
    """
    Splits the `timestamp_ns` column, the same in every topic, from the batches of the topics
    of a sequence.

    Returns:
        Tuple[pa.Array, Dict[str, pa.RecordBatch]]: The `timestamp_ns` of the rows, and the
            batches without it.

    Raises:
        ValueError: If a batch holds different timestamps than the others.
    """
    (first_topic, first_batch), *others = batches.items()
    timestamps = first_batch.column(_TIMESTAMP_NS_COLUMN_NAME)
    for topic, batch in others:
        if not batch.column(_TIMESTAMP_NS_COLUMN_NAME).equals(timestamps):
            raise ValueError(
                f"Sequence '{sequence_name}': topics '{first_topic}' and '{topic}' hold "
                "different timestamps."
            )

    return timestamps, {
        topic: batch.drop_columns([_TIMESTAMP_NS_COLUMN_NAME])
        for topic, batch in batches.items()
    }


class MosaicoToLeRobotLoader(BaseLoader):
    """
    Reads the Mosaico sequences of a LeRobot dataset extraction, one episode each.

    On resolution (see `_ensure_resolved`), the episode of each sequence is read from its
    `_lerobot_` metadata and merged with the previous ones by a `LeRobotDatasetBuilder`. The
    metadata of the exported dataset is exposed by `info`, `episodes` and
    `episode_index_by_sequence`.

    Its topics (in the `BaseLoader` sense) are the topics of the first accepted sequence. The
    handlers of the accepted sequences stay open until `close` is called (or the `with` block
    exits).
    """

    def __init__(self, m_client: MosaicoClient, sequence_names: List[str]):
        """
        Initializes the loader against Mosaico sequences.

        Nothing is fetched here: the sequences are resolved lazily on first access to any
        property (see `_ensure_resolved`).

        Args:
            m_client (MosaicoClient): An open connection to the Mosaico server.
            sequence_names (List[str]): The sequences to export, one episode each, in episode
                order.
        """
        super().__init__(container_type=list)

        self._client = m_client
        """The MosaicoClient used to fetch the sequences."""
        self._sequence_names = sequence_names
        """The sequences to export, one episode each, in episode order."""
        self._seq_handlers: Optional[List[SequenceHandler]] = None
        """List each accepted sequence handler: `None` until resolved by
        `_ensure_resolved`."""
        self._builder = LeRobotDatasetBuilder()
        """The builder of the metadata of the exported dataset, filled by `_ensure_resolved`."""

    @property
    def info(self) -> LeRobotInfo:
        """
        The `info` of the exported dataset, with the totals and `splits` of its episodes.

        Raises:
            ValueError: If no sequence can be exported.
        """
        self._ensure_resolved()
        info = self._builder.info
        if info is None:
            raise ValueError("No episode to export: the dataset has no `info`.")
        return info

    @property
    def episodes(self) -> List[LeRobotEpisode]:
        """The episodes of the exported dataset, renumbered in the order of the sequences."""
        self._ensure_resolved()
        return self._builder.episodes

    @property
    def episode_index_by_sequence(self) -> Dict[str, int]:
        """The `episode_index` of each accepted sequence in the exported dataset."""
        self._ensure_resolved()
        return self._builder.episode_index_by_sequence

    def _resolve_sequence_handlers(self) -> List[SequenceHandler]:
        """
        Lazily resolves the sequences: their handlers, the episodes of the exported dataset and
        their topics.

        Called automatically on first access to any property. Each sequence, in order, is
        dropped with a warning (and its handler closed) if it does not exist, has no valid
        `_lerobot_` metadata, or cannot be merged with the previous ones (see
        `LeRobotDatasetBuilder.add_episode`). If the resolution fails, the handlers opened so
        far are closed.

        Raises:
            ValueError: If no sequence can be exported.
        """
        if self._seq_handlers is not None:
            return self._seq_handlers

        seq_handlers: List[SequenceHandler] = []
        builder = LeRobotDatasetBuilder()
        topics: Optional[List[str]] = None
        try:
            for sequence_name in self._sequence_names:
                seq_handler = self._client.sequence_handler(sequence_name)
                if not _validate_sequence(seq_handler):
                    logger.warning(
                        f"Episode of sequence '{sequence_name}' dropped: the sequence does "
                        "not exist."
                    )
                    continue

                try:
                    info, episode = _unpack_sequence(seq_handler)
                    # TODO: a sequence ingested with a column filter
                    # (`LeRobotInjectionConfig.column_names`) has features in its `info` with no
                    # topic: drop them from `info`, or drop the sequence
                    builder.add_episode(sequence_name, info, episode)
                except ValueError as e:
                    logger.warning(
                        f"Episode of sequence '{sequence_name}' dropped: {e}"
                    )
                    seq_handler.close()
                    continue
                except Exception:
                    seq_handler.close()
                    raise

                seq_handlers.append(seq_handler)
                if topics is None:
                    topics = list(seq_handler.topics)
        except Exception:
            # Release the handlers opened so far
            for seq_handler in seq_handlers:
                seq_handler.close()
            raise

        if topics is None:
            raise ValueError("No episode to export.")

        self._builder = builder
        self._resolved_topics = list(topics)
        self._accepted_topics = list(topics)
        self._seq_handlers = seq_handlers

        return self._seq_handlers

    def _ensure_resolved(self) -> None:
        """
        Resolves the sequences, if not done yet (see `_resolve_sequence_handlers`).

        Raises:
            ValueError: If no sequence can be exported.
        """
        self._resolve_sequence_handlers()

    def __iter__(self) -> Iterator["LeRobotEpisodeStreamer"]:
        """
        Streams the episodes of the exported dataset, one per accepted sequence, in episode
        order.

        Yields:
            LeRobotEpisodeStreamer: The streamer of the data of the episode.
        """
        seq_handlers = self._resolve_sequence_handlers()

        # The handlers and the episodes are accepted together, in the same order
        for s_handler, episode in zip(
            seq_handlers, self._builder.episodes, strict=True
        ):
            yield LeRobotEpisodeStreamer(s_handler, episode)

    def close(self):
        """
        Closes the handlers of the sequences (and their topic handlers and streams), releasing
        their resources.
        """
        if self._seq_handlers is not None:
            for seq_handler in self._seq_handlers:
                seq_handler.close()
            self._seq_handlers = None

    def __enter__(self):
        """Context manager support."""
        self._ensure_resolved()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensures resources are released even if an error occurs in the `with` block."""
        self.close()


class LeRobotEpisodeStreamer:
    """
    A single episode of the exported dataset, as yielded by iterating over a
    `MosaicoToLeRobotLoader`: the opposite of `LeRobotEpisodeLoader`.

    Iterating over it streams the data of the episode, lined up across the topics of its
    sequence: each row of the episode is spread over all the topics (see
    `LeRobotEpisodeLoader`), while Mosaico streams each topic on its own, in batches of its own
    size.
    """

    def __init__(self, s_handler: SequenceHandler, episode: LeRobotEpisode):
        """
        Args:
            s_handler (SequenceHandler): The handler of the sequence of the episode.
            episode (LeRobotEpisode): The episode, renumbered in the exported dataset.
        """
        self._sequence_name: str = s_handler.name
        """The name of the sequence of the episode."""
        self.episode_metadata: LeRobotEpisode = episode
        """The episode, renumbered in the exported dataset."""

        # Get the streamers and set them as `batch` mode. The topics with no data have none
        self._streamers: Dict[str, TopicDataStreamer] = (
            s_handler.get_data_streamer()._as_batch_provider()
        )

    @property
    def sequence_name(self) -> str:
        """The name of the sequence of the episode."""
        return self._sequence_name

    @staticmethod
    def _get_finished_topics(
        batches_map: Dict[str, Optional[pa.RecordBatch]],
    ) -> List[str]:
        """Returns the list of topics whose streaming is over (meaning their RecordBatch is None)"""
        return [topic for topic, batch in batches_map.items() if batch is None]

    @staticmethod
    def _is_episode_over(batches_map: Dict[str, Optional[pa.RecordBatch]]) -> bool:
        """Check whether the episode is over. An episode is over when all the RecordBatch within the map are None"""
        finished_topics = LeRobotEpisodeStreamer._get_finished_topics(batches_map)
        return len(finished_topics) == len(batches_map)

    def __iter__(self) -> Iterator[Tuple[pa.Array, Dict[str, pa.RecordBatch]]]:
        """
        Streams the data of the episode, lined up across its topics.

        Each yielded batch is a slice of the batches received from the topics: about one batch
        per topic is held in memory.

        Yields:
            Tuple[pa.Array, Dict[str, pa.RecordBatch]]: The `timestamp_ns` of the rows, shared
                by all the topics, and a batch per topic holding those rows, as stored in Mosaico
                but without `timestamp_ns`.

        Raises:
            ValueError: If the topics do not hold the same rows (a topic ends before the others,
                or holds different timestamps), or their number of rows is not the `length` of
                the episode.
        """
        batches_map: Dict[str, Optional[pa.RecordBatch]] = dict.fromkeys(
            self._streamers
        )
        n_streamed = 0
        while True:
            # 1) Refill the topics with no rows left: `_fetch_next_batch` never returns an
            # empty batch, `None` means the stream is exhausted
            for topic, batch in batches_map.items():
                if batch is None or batch.num_rows == 0:
                    batches_map[topic] = self._streamers[topic]._fetch_next_batch()

            # 2) All the topics exhausted: the episode is over. Only some: their rows differ
            if self._is_episode_over(batches_map):
                # The exported metadata place the rows of the episode by its `length`
                if n_streamed != self.episode_metadata.length:
                    raise ValueError(
                        f"Sequence '{self._sequence_name}': {n_streamed} rows streamed, "
                        f"while its episode has length {self.episode_metadata.length}."
                    )
                return

            ended = self._get_finished_topics(batches_map)
            if len(ended) > 0:
                raise ValueError(
                    f"Sequence '{self._sequence_name}': topics {ended} ended before the "
                    "others, the topics have a different number of rows."
                )

            # 3) The rows all the topics have, sliced out of their batches. No batch is `None`
            # here: the filter only narrows the type
            batches = {t: b for t, b in batches_map.items() if b is not None}
            n_rows = min(b.num_rows for b in batches.values())
            sliced = {t: b.slice(0, n_rows) for t, b in batches.items()}
            # Keep the rows not sliced yet for the next round
            batches_map = {t: b.slice(n_rows) for t, b in batches.items()}

            timestamps, sliced = _pop_timestamps(self._sequence_name, sliced)
            n_streamed += n_rows
            yield timestamps, sliced
