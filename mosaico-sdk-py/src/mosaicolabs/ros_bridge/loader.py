from enum import Enum
from pathlib import Path
from typing import Dict, Generator, List, Optional, Protocol, Tuple, Union

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rosbags.highlevel import AnyReader
from rosbags.interfaces import Connection, TopicInfo
from rosbags.typesys import Stores, get_typestore

from mosaicolabs import MosaicoClient, SequenceDataStreamer
from mosaicolabs.logging_config import get_logger

from .helpers import _filter_topics_from_dict, _filter_topics_from_list, _to_dict
from .registry import ROSTypeRegistry
from .ros_bridge import ROSMessage

# Set the hierarchical logger
logger = get_logger(__name__)


class LoaderErrorPolicy(Enum):
    """
    Defines the strategy for handling deserialization failures during bag playback.

    In heterogeneous datasets, it is common to encounter corrupted messages or missing
    type definitions for specific topics. This policy allows the user to balance
    system robustness against data integrity.

    Attributes:
        IGNORE: Silently skips any message that fails to deserialize. The pipeline continues
            uninterrupted without any log output.
        LOG_WARN: (Default) Logs a warning containing the topic name and error details, then
            skips the message and continues.
        RAISE: Immediately halts execution and raises the exception. Best used for critical
            data ingestion where missing even a single record is unacceptable.
    """

    IGNORE = "ignore"
    """Silently skips any message that fails to deserialize."""

    LOG_WARN = "log_warn"
    """Logs a warning containing the topic name and error details, then skips the message and continues."""

    RAISE = "raise"
    """Immediately halts execution and raises the exception. Best used for critical data ingestion where missing even a single record is unacceptable."""


class Loader(Protocol):
    """
    Structural protocol for data loaders consumed by :class:`ProgressManager`.

    Both :class:`ROSLoader` and :class:`MosaicoLoader` satisfy this protocol,
    allowing :class:`ProgressManager` to set up progress bars without depending
    on a concrete loader class.
    """

    @property
    def duration(self) -> int:
        """This should return the duration of the loaded data as int"""

    @property
    def topics(self) -> List[str]:
        """This should return the topics of the loaded data as strings"""

    @property
    def msg_types(self) -> List[str | None]:
        """This should return the types of the loaded data as strings"""

    def msg_count(self, topic: Optional[str] = None) -> int:
        """This should return the total number of messages in the passed
        topic if not None. Otherwise returns all messages in all topics"""


# --- UI / Progress Helper ---


class ProgressManager:
    """
    Visual management system for loader tracking.

    This class decouples the UI presentation logic from the data processing pipeline.
    It utilizes the `rich` library to provide real-time feedback through progress bars,
    tracking individual topic throughput and aggregate global progress.


    Methods:
        setup(): Initializes the progress tracking tasks by querying message counts from the loader.
        update_status(topic, status, style): Modifies the label of a specific topic bar (e.g., to show "No Adapter").
        advance_global(): Increments the master progress bar without affecting individual topic bars.
        advance_all(topic): Increments both the specific topic task and the global master task.
    """

    def __init__(self, loader: Loader):
        """
        Initialize the progress manager.

        Args:
            loader (Loader): The initialized data loader. Used to query total
                                message counts for setting up progress bars.
        """
        self.loader = loader
        self.progress = Progress(
            TextColumn("[bold cyan]{task.fields[name]}"),
            BarColumn(),
            MofNCompleteColumn(),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            TimeRemainingColumn(),
            "•",
            TimeElapsedColumn(),
            expand=True,
        )
        self.tasks: Dict[str, TaskID] = {}
        self.global_task: Optional[TaskID] = None

    def setup(self):
        """
        Calculates totals and creates the visual progress tasks.
        Must be called before the main processing loop starts.
        """
        # Create individual progress bars for each topic
        for topic in self.loader.topics:
            count = self.loader.msg_count(topic)
            self.tasks[topic] = self.progress.add_task("", total=count, name=topic)

        # Create a master progress bar for the aggregate total
        total_msgs = sum(self.loader.msg_count(t) for t in self.loader.topics)
        self.global_task = self.progress.add_task(
            "Total", total=total_msgs, name="Total Upload"
        )

    def update_status(self, topic: str, status: str, style: str = "white"):
        """
        Updates the text description of a specific topic's progress bar.
        Useful for indicating errors or skipped topics (e.g. "[red]No Adapter").

        Args:
            topic: The topic name.
            status: The status message to display.
            style: The rich style string (e.g., 'red', 'bold yellow').
        """
        if topic in self.tasks:
            self.progress.update(
                self.tasks[topic],
                name=f"[{style}]{topic}: {status}",
            )

    def advance_global(self):
        """Advances only the global progress bar (used when skipping messages)."""
        if self.global_task is not None:
            self.progress.advance(self.global_task)

    def advance_all(self, topic: str):
        """Advances both the specific topic's bar and the global bar."""
        if topic in self.tasks:
            self.progress.advance(self.tasks[topic])
        if self.global_task is not None:
            self.progress.advance(self.global_task)


class ROSLoader:
    """
    Unified loader for reading and deserializing ROS 1 (.bag) and ROS 2 (.mcap, .db3) data.

    The `ROSLoader` acts as a resource manager that abstracts the underlying `rosbags` library.
    It provides a standardized Pythonic interface for filtering topics, managing custom message
    registries, and streaming data into the Mosaico adaptation pipeline.


    ### Key Features
    * **Multi-Format Support**: Automatically detects and handles ROS 1 and ROS 2 bag containers.
    * **Semantic Filtering**: Supports glob-style patterns (e.g., `/sensors/*`, `*camera_info`) to include relevant data channels,
        with `!`-prefixed patterns for exclusion (e.g., `!/sensors/debug*`). Patterns are evaluated in ORDER (gitignore-like semantics).
    * **Dynamic Schema Resolution**: Integrates with the [`ROSTypeRegistry`][mosaicolabs.ros_bridge.ROSTypeRegistry] to resolve proprietary message types on-the-fly.
    * **Memory Efficient**: Implements a generator-based iteration pattern to process large bags without loading them into RAM.

    Attributes:
        ACCEPTED_EXTENSIONS: Set of supported file extensions {'.bag', '.db3', '.mcap'}.
    """

    ACCEPTED_EXTENSIONS = {".bag", ".db3", ".mcap"}

    def __init__(
        self,
        file_path: Union[str, Path],
        topics: Optional[Union[str, List[str]]] = None,
        typestore_name: Stores = Stores.EMPTY,
        error_policy: LoaderErrorPolicy = LoaderErrorPolicy.LOG_WARN,
        custom_types: Optional[Dict[str, Union[str, Path]]] = None,
    ):
        """
        Initializes the loader and prepares the type registry.

        Upon initialization, the loader merges the global definitions from the
        [`ROSTypeRegistry`][mosaicolabs.ros_bridge.ROSTypeRegistry]
        with any `custom_types` provided specifically for this session.

        Example:
            ```python
            from rosbags.typesys import Stores
            from mosaicolabs.ros_bridge import ROSLoader, LoaderErrorPolicy

            # Initialize to read only IMU and GPS data from an MCAP file
            with ROSLoader(
                file_path="mission_01.mcap",
                topics=["/imu*", "/gps/fix"],
                typestore_name=Stores.ROS2_HUMBLE,
                error_policy=LoaderErrorPolicy.RAISE
            ) as loader:
                for msg, exc in loader:
                    if not exc:
                        print(f"Read {msg.msg_type} from {msg.topic}")
            ```

        Args:
            file_path: Path to the bag file or directory.
            topics: A single topic name, a list of names, or glob patterns. Patterns are evaluated in ORDER (gitignore-like semantics).
                If None, all available topics are loaded.
            typestore_name: The target ROS distribution for default message schemas.
                See [`rosbags.typesys.Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores).
            error_policy: How to handle errors during message iteration.
            custom_types: Local overrides for message definitions (type_name: path/to/msg).
        """

        self._file_path = Path(file_path)
        self._validate_file()

        # Configuration
        self._requested_topics = [topics] if isinstance(topics, str) else topics
        self._typestore = get_typestore(typestore_name)
        self._error_policy = error_policy

        # State
        self._reader: Optional[AnyReader] = None
        self._connections: List[Connection] = []
        self._resolved_topics: Dict[
            str, TopicInfo
        ] = {}  # The actual topics matched after globbing

        # Register Global Types (Registry Pattern)
        global_types = ROSTypeRegistry.get_types(typestore_name)
        if global_types:
            self._register_definitions(global_types)

        # Register Local Overrides
        if custom_types:
            # Resolve paths to strings immediately
            resolved = {
                k: ROSTypeRegistry._resolve_source(v) for k, v in custom_types.items()
            }
            self._register_definitions(resolved)

    def _validate_file(self):
        if not self._file_path.exists():
            raise FileNotFoundError(f"ROS bag not found: {self._file_path}")
        if self._file_path.suffix not in self.ACCEPTED_EXTENSIONS:
            raise ValueError(
                f"Unsupported format '{self._file_path.suffix}'. Supported: {self.ACCEPTED_EXTENSIONS}"
            )

    def _register_definitions(self, types_map: Dict[str, str]):
        """Safe registration wrapper."""
        from rosbags.typesys import get_types_from_msg

        for msg_type, msg_def in types_map.items():
            try:
                add_types = get_types_from_msg(msg_def, msg_type)
                self._typestore.register(add_types)
            except Exception as e:
                logger.warning(f"Failed to register type '{msg_type}': '{e}'")

    def _resolve_connections(self):
        """
        Lazily opens the bag file and resolves requested topic patterns.

        This method performs "Smart Filtering" by matching requested glob patterns against
        the actual topics available in the bag file. It populates the
        internal `_connections` list used for optimized iteration.
        """
        if self._reader is not None:
            return

        try:
            self._reader = AnyReader(
                [self._file_path], default_typestore=self._typestore
            )
            self._reader.open()
        except Exception as e:
            raise IOError(f"Could not open bag file: '{e}'") from e

        self._connections = []
        self._resolved_topics = _filter_topics_from_dict(
            self._reader.topics, self._requested_topics
        )

        # Filter connections
        for conn in self._reader.connections:
            if conn.topic in self._resolved_topics:
                self._connections.append(conn)

        if not self._connections:
            raise RuntimeError(
                "Unable to initialize ROSLoader: No connections matched criteria. Try checking the topics filter, if any."
            )

    # --- Properties ---
    def msg_count(self, topic: Optional[str] = None) -> int:
        """
        Returns the total number of messages to be processed based on active filters.

        Args:
            topic: If provided, returns the count for that specific topic. If None, returns
                the aggregate count for all filtered topics.

        Returns:
            The total message count.
        """
        self._resolve_connections()
        if not topic:
            return sum(c.msgcount for c in self._connections)
        try:
            return next(c.msgcount for c in self._connections if c.topic == topic)
        except StopIteration:
            logger.error(f"Topic '{topic}' not found in the loaded connections.")
            return 0

    @property
    def duration(self) -> int:
        """
        Returns the duration of the bag file in nanoseconds.

        Returns:
            int: The duration of the bag file in nanoseconds.
        """
        self._resolve_connections()
        if not self._reader:
            raise ValueError(
                "Loader not initialized. Call .open() or use as context manager first."
            )
        return self._reader.duration

    @property
    def topics(self) -> List[str]:
        """
        Retrieves the list of canonical topic names that will be processed.

        This property returns the result of the "Smart Filtering" process, which resolves
        any glob patterns (e.g., `/camera/*`) provided during initialization against
         the actual metadata contained within the bag file.

        Example:
            ```python
            with ROSLoader(file_path="data.mcap", topics=["/sensors/*"]) as loader:
                # If the bag contains /sensors/imu and /sensors/gps,
                # this property returns ['/sensors/imu', '/sensors/gps']
                print(f"Loading topics: {loader.topics}")
            ```

        Returns:
            List[str]: A list of topic names currently matched and scheduled for loading.
        """
        self._resolve_connections()
        return list(self._resolved_topics.keys())

    @property
    def msg_types(self) -> List[str | None]:
        """
        Retrieves the list of ROS message types corresponding to the resolved topics.

        Each entry in this list represents the schema name (e.g., `sensor_msgs/msg/Image`)
        required to correctly deserialize the messages for the topics returned by
        the `.topics` property.

        Example:
            ```python
            with ROSLoader(file_path="data.mcap") as loader:
                for topic, msg_type in zip(loader.topics, loader.msg_types):
                    print(f"Topic {topic} requires schema: {msg_type}")
            ```

        Returns:
            List[str]: A list of ROS message type strings in the same order
            as the resolved topics.
        """
        self._resolve_connections()
        return [val.msgtype for val in self._resolved_topics.values()]

    # --- Core Logic ---

    def __iter__(self) -> Generator[Tuple[ROSMessage, Optional[Exception]], None, None]:
        """
        The primary data streaming loop.

        This generator iterates through the bag chronologically, deserializing raw binary
        payloads into standard `ROSMessage` containers.

        Yields:
            A tuple of (ROSMessage, Exception). If deserialization succeeds, Exception is None.
            If it fails, ROSMessage still contains metadata (topic, timestamp) but `data` is None.
        """

        self._resolve_connections()

        if (
            not self._connections or not self._reader
        ):  # just for remove IDE errors on reader usage
            return

        # We allow an external observer hook for progress bars
        # This removes `rich` dependency from the core class

        for connection, bag_timestamp_ns, rawdata in self._reader.messages(
            connections=self._connections
        ):
            try:
                msg_obj = self._reader.deserialize(rawdata, connection.msgtype)

                # Yield the standard SDK message
                yield (
                    ROSMessage(
                        bag_timestamp_ns=bag_timestamp_ns,
                        topic=connection.topic,
                        msg_type=connection.msgtype,
                        data=_to_dict(msg_obj),
                    ),
                    None,
                )

            except Exception as e:
                self._handle_error(connection.topic, connection.msgtype, e)
                yield (
                    ROSMessage(
                        bag_timestamp_ns=bag_timestamp_ns,
                        topic=connection.topic,
                        msg_type=connection.msgtype,
                        data=None,
                    ),
                    e,
                )

    def _handle_error(self, topic: str, msg_type: str, exc: Exception):
        msg = f"Deserialization error on {topic} ({msg_type}): {exc}"

        if self._error_policy == LoaderErrorPolicy.RAISE:
            raise ValueError(msg) from exc
        elif self._error_policy == LoaderErrorPolicy.LOG_WARN:
            logger.warning(msg)
        # If IGNORE, do nothing

    def close(self):
        """
        Explicitly closes the bag file and releases system resources.
        """
        if self._reader:
            self._reader.close()
            self._reader = None

    def __enter__(self):
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensures resources are released even if an error occurs in the `with` block."""
        self.close()


class MosaicoLoader:
    """
    Lazy data loader that streams messages from a Mosaico sequence.

    Connects to the Mosaico server on first access, resolves the requested
    sequence and topic filter, clips the time window to valid sequence bounds,
    and exposes a :class:`SequenceDataStreamer` for iteration.

    Conforms to the :class:`Loader` protocol, making it usable with
    :class:`ProgressManager` for live progress reporting.

    Args:
        m_client: An open :class:`MosaicoClient` connection.
        sequence_name: Name of the Mosaico sequence to load.
        topics: Optional topic-name filter patterns (glob-style, ``!``-prefixed for
            exclusions). ``None`` loads all topics.
        start_timestamp_ns: Lower bound for the time window (nanoseconds). Clipped
            to the sequence minimum if out of range.
        end_timestamp_ns: Upper bound for the time window (nanoseconds). Clipped to
            the sequence maximum if out of range.
    """

    def __init__(
        self,
        m_client: MosaicoClient,
        sequence_name: str,
        topics: Optional[List[str]],
        start_timestamp_ns: Optional[int],
        end_timestamp_ns: Optional[int],
    ):

        self.client = m_client
        self.sequence_name = sequence_name
        self.requested_topics = topics
        self.start_timestamp_ns = start_timestamp_ns
        self.end_timestamp_ns = end_timestamp_ns

        self.seq_handler = None
        self.streamer = None

    def validate_sequence(self):
        if self.seq_handler is None:
            raise (
                ValueError(
                    f"Your requested sequence '{self.sequence_name}' could not be found!"
                )
            )

    def _resolve_sequence(self):
        """
        Lazily initializes the sequence handler, resolved topic list, and streamer.

        Called automatically on first access to any property or iterator. Performs
        the following steps:

        1. Fetches the :class:`SequenceHandler` for the configured sequence name
           and validates it exists.
        2. Applies the topic filter via :func:`_filter_topics_from_list`.
        3. Clips ``start_timestamp_ns`` / ``end_timestamp_ns`` to the sequence bounds,
           logging a warning if clipping occurs.
        4. Creates the :class:`SequenceDataStreamer` that will be returned by
           :meth:`__iter__`.

        """
        if self.seq_handler is not None:
            return

        # Get requested sequence + validation

        self.seq_handler = self.client.sequence_handler(
            sequence_name=self.sequence_name
        )

        self.validate_sequence()

        # Filter topics
        self.resolved_topics = _filter_topics_from_list(
            self.seq_handler.topics, self.requested_topics
        )

        if not self.resolved_topics:
            raise RuntimeError(
                "Unable to initialize MosaicoLoader: No topic matched criteria. Try checking the topics filter, if any."
            )

        # Clipping requested start/end timestamp to start/end sequence timestamp if existing
        if (
            self.start_timestamp_ns is not None
            and self.start_timestamp_ns < self.seq_handler.timestamp_ns_min
        ):
            logger.warning(
                f"Provided start_timestamp_ns is lower than sequence timestamp_ns_min: {self.start_timestamp_ns} < {self.seq_handler.timestamp_ns_min}. Clipping start_timestamp_ns to sequence timestamp_ns_min"
            )
            self.start_timestamp_ns = max(
                self.start_timestamp_ns, self.seq_handler.timestamp_ns_min
            )

        if (
            self.end_timestamp_ns is not None
            and self.end_timestamp_ns > self.seq_handler.timestamp_ns_max
        ):
            logger.warning(
                f"Provided end_timestamp_ns is higher than sequence timestamp_ns_max: {self.end_timestamp_ns} > {self.seq_handler.timestamp_ns_max}. Clipping end_timestamp_ns to sequence timestamp_ns_max"
            )
            self.end_timestamp_ns = min(
                self.end_timestamp_ns, self.seq_handler.timestamp_ns_max
            )

        # Resolving streamer
        self.streamer = self.seq_handler.get_data_streamer(
            topics=self.resolved_topics,
            start_timestamp_ns=self.start_timestamp_ns,
            end_timestamp_ns=self.end_timestamp_ns,
        )

    def msg_count(self, topic: Optional[str] = None) -> int:
        """
        Returns the total number of messages for the given topic, or for all
        resolved topics combined.

        Note:
            This performs a full traversal of each topic's data streamer to produce
            the count, which may be slow for large sequences.

        Args:
            topic: If provided, count messages for that specific topic only.
                If ``None``, sum across all resolved topics.

        Returns:
            The total message count.
        """
        self._resolve_sequence()

        topics_to_count = [topic] if topic else self.resolved_topics

        total_msg_count = 0
        for t in topics_to_count:
            t_handler = self.seq_handler.get_topic_handler(t)

            total_msg_count += sum(1 for _ in t_handler.get_data_streamer())

        return total_msg_count

    @property
    def duration(self) -> int:
        """
        Returns the duration of the sequence in nanoseconds.

        Returns:
            int: The duration of the sequence in nanoseconds. Returns 0 if sequence is not valid
        """
        self._resolve_sequence()

        if (
            self.seq_handler.timestamp_ns_max is not None
            and self.seq_handler.timestamp_ns_min is not None
        ):
            return self.seq_handler.timestamp_ns_max - self.seq_handler.timestamp_ns_min

        return 0

    @property
    def topics(self) -> List[str]:
        """
        Returns the list of topic names resolved after applying the topic filter.

        Triggers lazy initialization on first access.

        Returns:
            List[str]: Filtered topic names available in the sequence.
        """
        self._resolve_sequence()

        return self.resolved_topics

    @property
    def msg_types(self) -> List[str | None]:
        """
        Returns the Mosaico ontology type tags for each resolved topic.

        Entries appear in the same order as :attr:`topics`. A ``None`` entry
        indicates that the topic handler could not be found.

        Triggers lazy initialization on first access.

        Returns:
            List[str | None]: Ontology tag strings (e.g. ``"imu"``, ``"image"``)
            or ``None`` for unresolvable topics.
        """
        self._resolve_sequence()

        return [
            t_handler.ontology_tag
            if (t_handler := self.seq_handler.get_topic_handler(topic)) is not None
            else None
            for topic in self.resolved_topics
        ]

    def __iter__(self) -> SequenceDataStreamer:

        self._resolve_sequence()

        return self.streamer
