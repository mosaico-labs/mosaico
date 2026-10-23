from pathlib import Path
from typing import (
    Generator,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from mosaicolabs.logging_config import get_logger

from ..helpers import (
    _filter_from_list,
)
from ..loader_base import BaseLoader
from ..topic_status import TopicStatus
from .lerobot_message import LeRobotMessage
from .metadata import LeRobotDatasetMeta, LeRobotEpisode
from .reader import LeRobotReader

# Set the hierarchical logger
logger = get_logger(__name__)


class LeRobotLoader(BaseLoader):
    """
    Loader of a LeRobot v3.0 dataset folder, ingested one episode at a time.

    Its topics (in the `BaseLoader` sense) are the data columns of the dataset: the image and
    timeseries features of `meta/info.json`, filtered by the `columns` patterns. The `timestamp`
    column (the time of every message) and the video features (not supported yet) are not part of
    them. Iterating over the loader yields one `LeRobotEpisodeLoader` per episode, which streams
    the messages of that episode: each episode is ingested as its own sequence.

    The message counts are per column, every column having one row per frame. The loader conforms
    to the `LoaderUIAPI` protocol, making it usable with `LeRobotProgressManager`.

    Example:
        ```python
        loader = LeRobotLoader(
            "datasets/pusht", client.max_batch_size_bytes, columns=["observation.*"]
        )
        for episode_loader in loader:
            for msg, exc in episode_loader:
                if exc is None:
                    print(f"{msg.n_rows} rows for topic {msg.msco_topic_name}")
        ```
    """

    def __init__(
        self,
        file_path: Union[str, Path],
        max_batch_size_bytes: int,
        columns: Optional[Union[str, List[str]]] = None,
    ):
        """
        Initializes the loader. Nothing is read here: the dataset is opened lazily, on the first
        access to a property or on iteration.

        Args:
            file_path (Union[str, Path]): The root folder of the LeRobot dataset.
            max_batch_size_bytes (int): The maximum size of the batches sent to the server
                (see `MosaicoClient.max_batch_size_bytes`).
            columns (Optional[Union[str, List[str]]]): A single column name, a list of names, or
                glob patterns (e.g. `observation.images.*`), with `!`-prefixed patterns for
                exclusion. Patterns are evaluated in ORDER (gitignore-like semantics). If None,
                every column is loaded.
        """

        super().__init__(
            container_type=list[str]
        )  # Initialize the base class to set up topic resolution state
        self._file_path = Path(file_path)
        """The path to the LeRobot dataset directory."""

        # Configuration
        self._requested_columns = [columns] if isinstance(columns, str) else columns
        """The user-specified column patterns (`None` loads every column)."""

        # State
        self._max_batch_size_bytes = max_batch_size_bytes
        """The maximum record batch size in bytes that the reader should return."""

        self._reader: Optional[LeRobotReader] = None
        """The underlying `lerobot` reader instance, lazily initialized."""

        self._meta: Optional[LeRobotDatasetMeta] = None
        """The dataset metadata, lazily loaded (see `_resolve_metadata`)."""

    def _validate_folder(self):
        """Raises `FileNotFoundError` if the dataset folder does not exist."""
        if not self._file_path.exists():
            raise FileNotFoundError(f"LeRobot dataset not found: {self._file_path}")

    def _resolve_columns(self) -> LeRobotReader:
        """
        Lazily opens the LeRobot dataset and resolves the requested column patterns.

        The columns that do not match the patterns are rejected as `FILTERED`, and the reader is
        restricted to the accepted ones.

        Returns:
            LeRobotReader: The reader of the dataset data files, restricted to the accepted
                columns.

        Raises:
            IOError: If the dataset cannot be opened.
            RuntimeError: If no column matches the requested patterns.
        """
        if self._reader is not None:
            return self._reader

        metadata = self._resolve_metadata()

        video_columns = [
            key for key, feature in metadata.features.items() if feature.is_video
        ]
        if video_columns:
            logger.warning(
                f"Skipping video features {video_columns}: video files are not supported yet."
            )

        # Resolved topics are all the columns from the .parquet (except timestamp and videos)
        self._resolved_topics = metadata.column_names

        matched_columns = _filter_from_list(
            self._resolved_topics, self._requested_columns
        )

        for col_name in self._resolved_topics:
            if col_name not in matched_columns:
                self._reject(col_name, TopicStatus.FILTERED)

        if not matched_columns:
            raise RuntimeError(
                "Unable to initialize LeRobotLoader: No columns matched criteria. Try checking the columns filter, if any."
            )

        self._accepted_topics = matched_columns

        try:
            self._reader = LeRobotReader(
                self._file_path,
                self._max_batch_size_bytes,
                metadata,
                self._accepted_topics,
            )

        except Exception as e:
            raise IOError(f"Could not open LeRobot dataset: '{e}'") from e

        return self._reader

    def _resolve_metadata(self) -> LeRobotDatasetMeta:
        """
        Returns the dataset metadata (the content of the `meta/` folder), lazily loaded on
        first access.

        Returns:
            LeRobotDatasetMeta: The dataset metadata.

        Raises:
            IOError: If the dataset folder does not exist, or its `meta/` folder cannot be loaded.
        """
        if self._meta is None:
            try:
                self._validate_folder()
                self._meta = LeRobotDatasetMeta.load(self._file_path)

            except Exception as e:
                raise IOError(f"Could not open LeRobot dataset: '{e}'") from e

        return self._meta

    def _ensure_resolved(self) -> None:
        """Lazily opens the dataset and resolves its columns (see `_resolve_columns`)."""
        self._resolve_columns()

    # --- Properties ---

    @property
    def n_episodes(self) -> int:
        """The number of episodes of the dataset (`total_episodes` in `meta/info.json`)."""
        meta = self._resolve_metadata()
        return meta.n_episodes

    def msco_topic_name(self, column_name: str) -> Optional[str]:
        """
        Returns the Mosaico topic an accepted column is pushed to: `/timeseries` for a
        timeseries column, its own topic for an image column (e.g. `observation.images.front`
        -> `/observation/images/front`).

        Args:
            column_name (str): The column name.

        Returns:
            Optional[str]: The Mosaico topic name, or `None` if the column is not accepted.
        """
        reader = self._resolve_columns()
        return reader.get_msco_topic_name(column_name)

    def msg_count(
        self, topic: Optional[str] = None, episode_index: Optional[int] = None
    ) -> int:
        """
        Returns the number of messages to be processed based on active filters.

        Args:
            topic (Optional[str]): If provided, returns the count for that specific column, even if filtered.
                If None, returns the sum of the counts of all accepted columns, as the other loaders
                do: each timeseries column is counted, although they are all pushed together within
                the single timeseries topic.
            episode_index (Optional[int]): The episode to count (every episode if `None`).

        Returns:
            int: The message count. Returns 0 (and logs an error) if `topic` is not a column of the
                dataset, or 0 if the episode is not defined in `meta/episodes`.
        """

        self._resolve_columns()
        if not topic:
            return sum(
                self.msg_count(column, episode_index)
                for column in self._accepted_topics
            )

        if topic not in self._resolved_topics:
            logger.error(f"Column '{topic}' not found in the dataset.")
            return 0

        # Every column has one row per frame: a filtered column is counted as well
        return self._resolve_metadata().n_frames(episode_index)

    # --- Core Logic ---

    def __iter__(self) -> Iterator["LeRobotEpisodeLoader"]:
        """
        Iterates over the episodes of the dataset, in episode index order.

        Each episode is ingested as its own sequence: the yielded `LeRobotEpisodeLoader` streams
        the messages of that episode only.

        Yields:
            LeRobotEpisodeLoader: One loader per episode.
        """
        reader = self._resolve_columns()

        for episode_index in reader.episode_indexes:
            yield LeRobotEpisodeLoader(reader, episode_index)


class LeRobotEpisodeLoader:
    """
    A single episode of a LeRobot dataset, as yielded by iterating over a `LeRobotLoader`.

    Iterating over it streams the messages of the episode, which is ingested as its own sequence.
    """

    def __init__(self, reader: LeRobotReader, episode_index: int):
        """
        Args:
            reader (LeRobotReader): The reader of the dataset data files, restricted to the
                accepted columns.
            episode_index (int): The episode index.
        """
        self._reader = reader
        self._episode_index = episode_index
        self.episode_metadata: LeRobotEpisode = self._reader._meta.episodes_map[
            self._episode_index
        ]

    @property
    def episode_index(self) -> int:
        """The episode index."""
        return self._episode_index

    @property
    def n_rows(self) -> int:
        """The number of rows (frames) of the episode: its `length` in `meta/episodes`."""
        return self.episode_metadata.length

    def msco_seq_name(self, prefix: Optional[str] = None) -> str:
        """
        Returns the name of the Mosaico sequence of the episode: `<prefix>_episode_<index>`, the
        episode index being zero-padded to 6 digits (e.g. `pusht_episode_000012`).

        Args:
            prefix (Optional[str]): The prefix of the sequence name. If `None`, the name of the
                dataset root folder is used.

        Returns:
            str: The sequence name.
        """
        if prefix is None:
            # `resolve()` gives the folder name even for a relative root such as `.`
            prefix = self._reader._meta.root.resolve().name

        width = 6
        return f"{prefix}_episode_{self.episode_index:0{width}d}"

    def __iter__(
        self,
    ) -> Generator[Tuple[LeRobotMessage, Optional[Exception]], None, None]:
        """
        Streams the messages of the episode: the timeseries batches, then the image batches,
        one camera at a time.

        Yields:
            A tuple of (LeRobotMessage, Exception). If reading succeeds, Exception is None.
            If it fails, the remaining batches of that topic are skipped, and LeRobotMessage
            still contains its topic and ontology, but `data` and `timestamp_ns` are None.
        """
        reader = self._reader

        # 1) loading timeseries batches
        try:
            for msg in reader.iter_timeseries_batches(self._episode_index):
                yield msg, None
        except Exception as ex:
            yield (
                LeRobotMessage(
                    reader.timeseries_msco_topic_name,
                    reader.timeseries_column_names,
                    None,
                    None,
                    reader.timeseries_ontology,
                    None,
                ),
                ex,
            )

        # 2) loading images batches one camera at a time
        for camera_col_name in reader.cameras_column_names:
            try:
                for msg in reader.iter_image_by_name_batches(
                    self._episode_index, camera_col_name
                ):
                    yield msg, None
            except Exception as ex:
                yield (
                    LeRobotMessage(
                        reader.get_msco_topic_name(camera_col_name) or "?",
                        [camera_col_name],
                        None,
                        None,
                        reader.image_ontology,
                        None,
                    ),
                    ex,
                )
