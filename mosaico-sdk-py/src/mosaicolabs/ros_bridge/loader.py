import fnmatch
from enum import Enum
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple, Union

from rosbags.highlevel import AnyReader
from rosbags.interfaces import Connection, TopicInfo
from rosbags.typesys import Stores, get_typestore

from mosaicolabs.logging_config import get_logger

from .helpers import _to_dict
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


class ROSLoader:
    """
    Unified loader for reading and deserializing ROS 1 (.bag) and ROS 2 (.mcap, .db3) data.

    The `ROSLoader` acts as a resource manager that abstracts the underlying `rosbags` library.
    It provides a standardized Pythonic interface for filtering topics, managing custom message
    registries, and streaming data into the Mosaico adaptation pipeline.


    ### Key Features
    * **Multi-Format Support**: Automatically detects and handles ROS 1 and ROS 2 bag containers.
    * **Semantic Filtering**: Supports glob-style patterns (e.g., `/sensors/*`, `*camera_info`) to include relevant data channels,
        with `!`-prefixed patterns for exclusion (e.g., `!/sensors/debug*`).
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
            topics: A single topic name, a list of names, or glob patterns.
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

    def _resolve_topics(
        self, available_topics: Dict[str, TopicInfo]
    ) -> Dict[str, TopicInfo]:
        """
        Resolve the set of topics to be processed based on user-provided glob patterns.

        This method filters `available_topics` according to the patterns defined in
        `self._requested_topics`, supporting both inclusion and exclusion rules.

        Pattern semantics:
            - Patterns use standard shell-style wildcards (via `fnmatch`):
                * "*" matches any sequence of characters
                * "?" matches any single character
            - Patterns NOT starting with "!" are treated as inclusion patterns.
            - Patterns starting with "!" are treated as exclusion patterns.

        Resolution rules:
            1. If `self._requested_topics` is empty or None:
               → all available topics are selected.
            2. If at least one inclusion pattern is present:
               → only topics matching ANY inclusion pattern are selected.
            3. If no inclusion patterns are present (only exclusions):
               → all topics are initially selected.
            4. Exclusion patterns are applied last and always take precedence:
               → any topic matching an exclusion pattern is removed.

        Examples:
            ["foo*"]
                → include only topics starting with "foo"

            ["!foo*"]
                → include all topics except those starting with "foo"

            ["foo*", "!foo_bad*"]
                → include topics matching "foo*" but exclude "foo_bad*"

            []
                → include all available topics

        Warnings:
            - A warning is logged if an inclusion or exclusion pattern matches no topics.

        Args:
            available_topics (Dict[str, TopicInfo]):
                Mapping of topic names to their associated metadata.

        Side Effects:
            - Sets `self._resolved_topics` to the filtered dictionary of topics.
        """
        if not self._requested_topics:
            return available_topics

        include_patterns = []
        exclude_patterns = []

        # Split patterns
        for pattern in self._requested_topics:
            if pattern.startswith("!"):
                exclude_patterns.append(pattern[1:])
            else:
                include_patterns.append(pattern)

        # Collect inclusions. If no include patterns->include everything by default
        if include_patterns:
            included_keys = set()
            for pattern in include_patterns:
                # fnmatch allows using * and ? wildcards
                matches = fnmatch.filter(available_topics.keys(), pattern)
                if not matches:
                    logger.warning(
                        f"Topic pattern '{pattern}' matched nothing in this bag."
                    )
                    continue
                included_keys.update(matches)
        else:
            included_keys = set(available_topics.keys())

        # Collect exclusions
        excluded_keys = set()
        for pattern in exclude_patterns:
            # fnmatch allows using * and ? wildcards
            matches = fnmatch.filter(available_topics.keys(), pattern)
            if not matches:
                logger.warning(
                    f"Exclude pattern '{pattern}' matched nothing in this bag."
                )
                continue
            excluded_keys.update(matches)

        # Final result
        final_keys = included_keys - excluded_keys

        target_topics = {
            key: val for key, val in available_topics.items() if key in final_keys
        }

        return target_topics

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
        self._resolved_topics = self._resolve_topics(self._reader.topics)

        # Filter connections
        for conn in self._reader.connections:
            if conn.topic in self._resolved_topics:
                self._connections.append(conn)

        if not self._connections:
            raise RuntimeError(
                "Unanble to initialize ROSLoader: No connections matched criteria. Try checking the topics filter, if any."
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
