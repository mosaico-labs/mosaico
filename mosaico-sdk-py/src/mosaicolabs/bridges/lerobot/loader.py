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
from .metadata import LeRobotDatasetMeta
from .reader import LeRobotReader

# Set the hierarchical logger
logger = get_logger(__name__)


class LeRobotLoader(BaseLoader):
    """
    FIXME
    Unified loader for reading and deserializing ROS 1 (.bag) and ROS 2 (.mcap, .db3) data.

    The `ROSLoader` acts as a resource manager that abstracts the underlying `rosbags` library.
    It provides a standardized Pythonic interface for filtering topics and streaming data
    into the Mosaico adaptation pipeline, against a caller-supplied `Typestore`.

    Note: `ROSLoader` does not itself consult the [`ROSTypeRegistry`][mosaicolabs.bridges.ros.ROSTypeRegistry].
        Resolving `ros_distro`/`custom_msgs` into a concrete `Typestore` (including any custom
        `.msg` registration) is the caller's responsibility — [`RosbagInjector`][mosaicolabs.bridges.ros.RosbagInjector]
        does this internally before constructing a `ROSLoader`.

    ### Key Features
    * **Multi-Format Support**: Automatically detects and handles ROS 1 and ROS 2 bag containers.
    * **Semantic Filtering**: Supports glob-style patterns (e.g., `/sensors/*`, `*camera_info`) to include relevant data channels,
        with `!`-prefixed patterns for exclusion (e.g., `!/sensors/debug*`). Patterns are evaluated in ORDER (gitignore-like semantics).
    * **Configurable Serialization**: Non-adapted message types can be assigned a specific
        [`SerializationFormat`][mosaicolabs.enum.serialization_format.SerializationFormat] via `serialization_formats`,
        overriding the `SerializationFormat.Default` used otherwise.
    * **Memory Efficient**: Implements a generator-based iteration pattern to process large bags without loading them into RAM.

    Attributes:
        ACCEPTED_EXTENSIONS: Set of supported file extensions {'.bag', '.db3', '.mcap'}.
    """

    def __init__(
        self,
        file_path: Union[str, Path],
        max_batch_size_bytes: int,
        columns: Optional[Union[str, List[str]]] = None,
    ):
        """
        FIXME
        Initializes the ROSbag loader against a caller-supplied `Typestore` or ROS distro.

        `ROSLoader` performs no `ROSTypeRegistry` lookups itself — pass in a `Typestore`
        that already has any custom `.msg` definitions registered (e.g. via
        `get_typestore(ros_distro)` plus `Typestore.register(...)`, or the `Typestore`
        that `RosbagInjector` builds internally from `ROSInjectionConfig.ros_distro`/
        `custom_msgs`).

        Example:
            ```python
            from rosbags.typesys import Stores
            from mosaicolabs.enum.serialization_format import SerializationFormat
            from mosaicolabs.bridges.ros import ROSLoader

            # Initialize to read only IMU and GPS data from an MCAP file
            with ROSLoader(
                file_path="mission_01.mcap",
                topics=["/imu*", "/gps/fix"],
                typestore_or_distro=Stores.ROS2_HUMBLE,
                # Non-adapted (Unmodeled) messages of this type will be
                # serialized as Ragged instead of the Default format
                serialization_formats={
                    "sensor_msgs/msg/CustomPointCloud2": SerializationFormat.Ragged,
                },
            ) as loader:
                for msg, exc in loader:
                    if not exc:
                        print(f"Read {msg.msg_type} from {msg.topic}")
            ```

        Args:
            file_path (Union[str, Path]): Path to the bag file or directory.
            typestore_or_distro (Typestore | Stores): The typestore or ROS distro to use for message type resolution.
            topics (Optional[Union[str, List[str]]]): A single topic name, a list of names, or glob patterns. Patterns are evaluated in ORDER (gitignore-like semantics).
                If None, all available topics are loaded.
            serialization_formats (Optional[Dict[str, SerializationFormat]]): Maps a ROS message type string (e.g. `sensor_msgs/msg/CustomPointCloud2`)
                to the [`SerializationFormat`][mosaicolabs.enum.serialization_format.SerializationFormat]
                used when synthesizing an [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled]
                ontology for that type. Only applies to topics that have **no** hand-written Mosaico
                adapter. Message types not present in this mapping default to `SerializationFormat.Default`.
        """

        super().__init__(
            container_type=list[str]
        )  # Initialize the base class to set up topic resolution state
        self._file_path = Path(file_path)
        """The path to the LeRobot dataset directory."""

        # Configuration
        self._requested_columns = [columns] if isinstance(columns, str) else columns

        # State
        self._max_batch_size_bytes = max_batch_size_bytes
        """The maximum record batch size in bytes that the reader should return."""

        self._reader: Optional[LeRobotReader] = None
        """The underlying `lerobot` reader instance, lazily initialized."""

        self._meta: Optional[LeRobotDatasetMeta] = None
        """The dataset metadata, lazily loaded (see `_resolve_metadata`)."""

    def _validate_folder(self):
        if not self._file_path.exists():
            raise FileNotFoundError(f"LeRobot dataset not found: {self._file_path}")

    def _resolve_columns(self) -> LeRobotReader:
        """
        Lazily opens the LeRobot dataset and resolves requested column patterns.

        This method performs "Smart Filtering" by matching requested glob patterns against
        the actual columns available in the .parquet file.
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

    # --- Properties ---

    @property
    def n_episodes(self):
        meta = self._resolve_metadata()
        return meta.n_episodes

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

    def _ensure_resolved(self) -> None:
        """Lazily opens the bag file and resolves topics (see `_resolve_columns`)."""
        self._resolve_columns()

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

    @property
    def episode_index(self) -> int:
        """The episode index."""
        return self._episode_index

    @property
    def n_rows(self) -> int:
        """The number of rows for the current episode"""
        return self._reader.get_episode_rows(self._episode_index)

    def msco_seq_name(self, prefix: Optional[str] = None) -> str:
        """
        Returns the name of the Mosaico sequence of the episode: the prefix followed by the
        episode index, zero-padded to 5 digits (e.g. `pusht_00012`).

        Args:
            prefix (Optional[str]): The prefix of the sequence name. If `None`, the name of the
                dataset root folder is used.

        Returns:
            str: The sequence name.
        """
        if prefix is None:
            # `resolve()` gives the folder name even for a relative root such as `.`
            prefix = self._reader._meta.root.resolve().name

        width = 5
        return f"{prefix}_{self.episode_index:0{width}d}"

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
                        reader.image_msco_topic_name(camera_col_name),
                        [camera_col_name],
                        None,
                        None,
                        reader.image_ontology,
                        None,
                    ),
                    ex,
                )
