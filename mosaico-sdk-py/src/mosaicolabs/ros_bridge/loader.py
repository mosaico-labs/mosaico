import fnmatch
from pathlib import Path
from mosaicolabs.logging import get_logger
from rosbags.highlevel import AnyReader
from rosbags.interfaces import Connection, TopicInfo
from typing import Dict, Generator, List, Optional, Tuple, Union
from enum import Enum
from rosbags.typesys import Stores, get_typestore

from .helpers import _to_dict
from .ros_bridge import ROSMessage
from .registry import ROSTypeRegistry

# Set the hierarchical logger
logger = get_logger(__name__)


class LoaderErrorPolicy(Enum):
    """Defines how the loader handles deserialization errors."""

    IGNORE = "ignore"  # Skip bad messages silently
    LOG_WARN = "log_warn"  # Log a warning but continue (Default)
    RAISE = "raise"  # Raise the exception and stop the pipeline


class ROSLoader:
    """
    Concrete loader implementation for reading ROS 1 (.bag) and ROS 2 (.db3, .mcap)
    bag files using the rosbags library.

    It is responsible for resource management (opening/closing the bag file) and message
    deserialization. It can handle custom ROS message types using the definitions embedded
    in the bag file.

    Features:
      - Dynamic Type Registry support.
      - Glob pattern matching for topics (e.g. '/cam/*').
      - Configurable error handling policies.
      - Decoupled progress reporting hooks.
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
        Opens the reader (lazy) and resolves glob patterns to actual connections.
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

        available_topics = self._reader.topics
        self._connections = []
        self._resolved_topics = {}

        # If no specific topics requested, take all
        if not self._requested_topics:
            self._connections = self._reader.connections
            self._resolved_topics = available_topics
            return

        # Smart Filtering: Handle Glob patterns (e.g. "/cam_*/image")
        target_topics = {}
        for pattern in self._requested_topics:
            # fnmatch allows using * and ? wildcards
            matches = fnmatch.filter(available_topics.keys(), pattern)
            if not matches:
                logger.warning(
                    f"Topic pattern '{pattern}' matched nothing in this bag."
                )
            target_topics.update(
                {key: val for key, val in available_topics.items() if key in matches}
            )

        self._resolved_topics = target_topics

        # Filter connections
        for conn in self._reader.connections:
            if conn.topic in target_topics:
                self._connections.append(conn)

        if not self._connections:
            logger.warning("Loader initialized, but no connections matched criteria.")

    # --- Properties ---
    def msg_count(self, topic: Optional[str] = None) -> int:
        """Total messages to be processed based on filters."""
        self._resolve_connections()
        if not topic:
            return sum(c.msgcount for c in self._connections)
        try:
            return next(c.msgcount for c in self._connections if c.topic == topic)
        except StopIteration:
            logger.error(f"Topic '{topic}' not found in the loaded connections.")
            return 0

    @property
    def topics(self) -> List[str]:
        """List of topics that will be loaded."""
        self._resolve_connections()
        return list(self._resolved_topics.keys())

    @property
    def msg_types(self) -> List[str | None]:
        """List of topics that will be loaded."""
        self._resolve_connections()
        return [val.msgtype for val in self._resolved_topics.values()]

    # --- Core Logic ---

    def __iter__(self) -> Generator[Tuple[ROSMessage, Optional[Exception]], None, None]:
        self._resolve_connections()

        if (
            not self._connections or not self._reader
        ):  # just for remove IDE errors on reader usage
            return

        # We allow an external observer hook for progress bars
        # This removes `rich` dependency from the core class

        for connection, timestamp, rawdata in self._reader.messages(
            connections=self._connections
        ):
            try:
                msg_obj = self._reader.deserialize(rawdata, connection.msgtype)

                # Yield the standard SDK message
                yield (
                    ROSMessage(
                        timestamp=timestamp,
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
                        timestamp=timestamp,
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
        if self._reader:
            self._reader.close()
            self._reader = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
