from enum import Enum
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Protocol, Tuple, Union

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
from rosbags.typesys import Stores, get_types_from_msg, get_typestore
from rosbags.typesys.store import Typestore

from mosaicolabs import (
    MosaicoClient,
    SequenceDataStreamer,
    SequenceHandler,
    TopicHandler,
)
from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.logging_config import get_logger
from mosaicolabs.models.core.helpers import resolve_ontology_class
from mosaicolabs.ros_bridge.adapters.unmodeled import UnmodeledAdapter

from ..models.core.serializable import _compute_schema_fingerprint
from ..protocols.ros2msg import convert_ros2msg
from .adapter_base import ROSAdapterBase
from .helpers import (
    _class_name_from_ros_msgtype,
    _clip_timestamp,
    _extract_ros_metadata,
    _filter_topics_from_dict,
    _filter_topics_from_list,
    _to_dict,
    _validate_sequence,
)
from .registry import ROSTypeRegistry
from .ros_bridge import ROSBridge, ROSMessage

# Set the hierarchical logger
logger = get_logger(__name__)


class TopicStatus(Enum):
    """
    Defines the possible topic status (ACCEPTED/REJECTED). In case of rejection, a more informative enum if provided.

    Attributes:
        ACCEPTED: Enum specifying the Topic has been accepted.
        FILTERED: Enum specifying the Topic has been rejected since user provided a filter that excludes the topic.
        UNRESOLVED_ADAPTED: Enum specifying the Topic has been rejected since it has no Mosaico adapter.
        NOT_IN_TYPESTORE: Enum specifying the Topic has been rejected since it is not present in ROS typestore.
        MALFORMED_METADATA: Enum specifying the Topic has been rejected since its ``_ros_`` metadata is malformed.
    """

    ACCEPTED = "Accepted"
    """ Status indicating an accepted Topic """

    FILTERED = "Filtered"
    """ Status indicating topic has been rejected by user specified filter """

    UNRESOLVED_ADAPTED = "Unresolved adapted"
    """ Status indicating the Topic has been rejected since no Mosaico adapter could be resolved """

    NOT_IN_TYPESTORE = "Not in typestore"
    """ Status indicating the Topic has been rejected since it is not present in ROS typestore """

    MALFORMED_METADATA = "Malformed metadata"
    """ Status indicating the Topic has been rejected since its '_ros_' metadata is malformed """

    def display_color(self) -> str:
        """Returns the Rich color string used to render this status in the progress UI."""
        _colors = {
            TopicStatus.ACCEPTED: "bright_green",
            TopicStatus.FILTERED: "bright_yellow",
            TopicStatus.UNRESOLVED_ADAPTED: "dark_orange",
            TopicStatus.NOT_IN_TYPESTORE: "orange1",
            TopicStatus.MALFORMED_METADATA: "red1",
        }
        return _colors.get(self, "bright_red")


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
    def topics(self) -> List[str]:
        """This should return the Mosaico compatible topics of the loaded data as strings"""
        ...

    @property
    def resolved_topics(self) -> List[str]:
        """This should return **all** the topics of the loaded data as strings"""
        ...

    @property
    def rejected_topics(self) -> List[Tuple[str, TopicStatus]]:
        """This should return a list of tuples containing all the rejected topic names, and the rejection reason (topic_name, topic_status)"""
        ...

    def msg_count(self, topic: Optional[str] = None) -> int:
        """This should return the total number of messages in the passed
        topic if not None. Otherwise returns all messages in all topics"""
        ...


# --- UI / Progress Helper ---


class ProgressManager:
    """
    Visual management system for loader tracking.

    This class decouples the UI presentation logic from the data processing pipeline.
    It utilizes the `rich` library to provide real-time feedback through progress bars,
    tracking individual topic throughput and aggregate global progress.


    Methods:
        setup(): Initializes the progress tracking tasks by querying message counts from the loader.
        update_status(topic, status, style): Modifies the label of a specific topic bar.
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
        # Create individual progress bars for each topic but count only the accepted ones
        for topic_name in self.loader.resolved_topics:
            if topic_name in self.loader.topics:
                count = self.loader.msg_count(topic_name)
            else:
                count = None

            self.tasks[topic_name] = self.progress.add_task(
                "", total=count, name=topic_name
            )

        # Rejected topics (with rejected reason) are highlighted
        for topic_name, topic_status in self.loader.rejected_topics:
            self.update_status(
                topic_name, topic_status.value, topic_status.display_color()
            )

        # Create a master progress bar for the aggregate total of the accepted topics
        total_msgs = sum(self.loader.msg_count(t) for t in self.loader.topics)
        self.global_task = self.progress.add_task(
            "Total", total=total_msgs, name="Total Upload"
        )

    def update_status(self, topic: str, status: str, style: str = "white"):
        """
        Updates the text description of a specific topic's progress bar.
        Useful for indicating errors or skipped topics (e.g. "[red]Unresolved Adapter").

        Args:
            topic (str): The topic name.
            status (str): The status message to display.
            style (str): The rich style string (e.g., 'red', 'bold yellow').
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
    * **Configurable Serialization**: Non-adapted message types can be assigned a specific
        [`SerializationFormat`][mosaicolabs.enum.serialization_format.SerializationFormat] via `serialization_formats`,
        overriding the `SerializationFormat.Default` used otherwise.
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
        serialization_formats: Optional[Dict[str, SerializationFormat]] = None,
    ):
        """
        Initializes the loader and prepares the type registry.

        Upon initialization, the loader merges the global definitions from the
        [`ROSTypeRegistry`][mosaicolabs.ros_bridge.ROSTypeRegistry]
        with any `custom_types` provided specifically for this session.

        Example:
            ```python
            from rosbags.typesys import Stores
            from mosaicolabs.enum.serialization_format import SerializationFormat
            from mosaicolabs.ros_bridge import ROSLoader, LoaderErrorPolicy

            # Initialize to read only IMU and GPS data from an MCAP file
            with ROSLoader(
                file_path="mission_01.mcap",
                topics=["/imu*", "/gps/fix"],
                typestore_name=Stores.ROS2_HUMBLE,
                error_policy=LoaderErrorPolicy.RAISE,
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
            topics (Optional[Union[str, List[str]]]): A single topic name, a list of names, or glob patterns. Patterns are evaluated in ORDER (gitignore-like semantics).
                If None, all available topics are loaded.
            typestore_name (Stores): The target ROS distribution for default message schemas.
                See [`rosbags.typesys.Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores).
            error_policy (LoaderErrorPolicy): How to handle errors during message iteration.
            custom_types (Optional[Dict[str, Union[str, Path]]]): Local overrides for message definitions (type_name: path/to/msg).
            serialization_formats (Optional[Dict[str, SerializationFormat]]): Maps a ROS message type string (e.g. `sensor_msgs/msg/CustomPointCloud2`)
                to the [`SerializationFormat`][mosaicolabs.enum.serialization_format.SerializationFormat]
                used when synthesizing an [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled]
                ontology for that type. Only applies to topics that have **no** hand-written Mosaico
                adapter. Message types not present in this mapping default to `SerializationFormat.Default`.
        """

        self._file_path = Path(file_path)
        self._validate_file()

        # Configuration
        self._requested_topics = [topics] if isinstance(topics, str) else topics
        self._typestore = get_typestore(typestore_name)
        self._error_policy = error_policy
        self._serialization_formats: Dict[str, SerializationFormat] = (
            serialization_formats or {}
        )

        # State
        self._reader: Optional[AnyReader] = None
        self._connections: List[Connection] = []
        self._resolved_topics: Dict[
            str, TopicInfo
        ] = {}  # The full topics set contained in the rosbag

        self._accepted_topics: Dict[
            str, TopicInfo
        ] = {}  # The actual topics matched after globbing

        self._unresolved_adapter_topics: Dict[
            str, TopicInfo
        ] = {}  # The topics whose ontology type is not resolved (neither adapted nor message defition available)

        self._filtered_topics: Dict[
            str, TopicInfo
        ] = {}  # The topics which message type are filtered by user

        self._topic_cached_adapters: dict[
            str, type[ROSAdapterBase]
        ] = {}  # Dictionary containing a map from moisaco accepted topics to mosaico adapter

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

            # Overriden local typestore since AnyReader one contains the union messages
            # withing user defined ROS version + any message defined within the bag file
            self._typestore = self._reader.typestore
        except Exception as e:
            raise IOError(f"Could not open bag file: '{e}'") from e

        self._connections = []

        self._resolved_topics = {
            tname: tinfo for tname, tinfo in self._reader.topics.items()
        }
        matched_topics = _filter_topics_from_dict(
            self._reader.topics, self._requested_topics
        )

        # Filter connections
        for conn in self._reader.connections:
            topic_info = matched_topics.get(conn.topic)

            # 1) Filter by requested topic
            if topic_info is None:
                logger.info(
                    f"Skipping topic {conn.topic}: not matching the provided filter."
                )

                filtered_topic_info = self._resolved_topics[conn.topic]
                self._filtered_topics.update({conn.topic: filtered_topic_info})
                continue

            # 2) Filter topics that cannot resolve neither a registered Mosaico-adapter nor an Unmodeled one because no PyArrow schema can be derived
            adapter = self._get_or_create_adapter(topic_info)

            if adapter:
                self._accepted_topics.update({conn.topic: topic_info})
            else:
                logger.warning(
                    f"Topic {conn.topic}: unresolved Adapted for msgtype {topic_info.msgtype}. Did you forget to register it?"
                )
                self._unresolved_adapter_topics.update({conn.topic: topic_info})
                continue

            # Adapter found, add it the the cache and add connection
            self._topic_cached_adapters[conn.topic] = adapter
            self._connections.append(conn)

        if not self._connections:
            raise RuntimeError(
                "Unable to initialize ROSLoader: No connections matched criteria. Try checking the topics filter, if any."
            )

    def _get_or_create_adapter(
        self, topic_info: TopicInfo
    ) -> Optional[type[ROSAdapterBase]]:
        """
        Resolves the Mosaico adapter for a topic, creating an ad-hoc one if none exists.

        This is what lets :class:`ROSLoader` accept **any** ROS message type, even
        proprietary ones without a hand-written adapter, instead of rejecting them.
        It proceeds in three steps:

        1. **Bail out early**: if the topic has no ``msgtype`` at all (empty connection
           metadata), no adapter can be resolved, so ``None`` is returned immediately.
        2. **Look up a known adapter**: :meth:`ROSBridge.get_default_adapter` is queried
           for a hand-written adapter registered for this exact ``msgtype`` (e.g.
           `sensor_msgs/msg/Imu` -> `IMUAdapter`). If one is found, it is returned as-is
           and no further work is needed.
        3. **Fall back to an [`UnmodeledAdapter`][mosaicolabs.ros_bridge.adapters.UnmodeledAdapter]**:
           when no hand-written adapter exists, one is synthesized on the fly so the
           topic can still be loaded generically, without a semantic ontology mapping:

            a. The topic's raw ``.msg``/``.idl`` definition (``topic_info.msgdef.data``)
               is converted into an equivalent PyArrow schema via
               [`convert_ros2msg`][mosaicolabs.protocols.ros2msg.convert_ros2msg].
            b. An [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled] ontology
               class is obtained/created for this schema via
               [`resolve_ontology_class`][mosaicolabs.models.core.helpers.resolve_ontology_class],
               tagged with an ontology tag derived from the ROS msgtype's last path
               segment (e.g. `sensor_msgs/msg/Imu` -> `Imu`). The serialization format
               used for this ontology is looked up in ``self._serialization_formats``
               by ``msgtype``, falling back to ``SerializationFormat.Default`` when the
               msgtype has no entry there.
            c. [`UnmodeledAdapter.get_or_create`][mosaicolabs.ros_bridge.adapters.UnmodeledAdapter.get_or_create]
               returns a cached adapter class for that ontology if one was already
               synthesized for an equivalent topic, or builds and registers a new one
               otherwise, so repeated topics of the same unmodeled type reuse a single
               adapter class rather than creating a new one every time.

        Args:
            topic_info (TopicInfo): The connection metadata (``msgtype``, ``msgdef``, ...)
                of the topic for which an adapter must be resolved.

        Returns:
            Optional[Type[ROSAdapterBase]]: The resolved adapter class, or ``None`` if ``topic_info`` carries no
                ``msgtype`` to key the lookup/creation on.
        """

        if not topic_info.msgtype:
            return None

        # Check if adapter already exists. If yes, return immediately
        adapter = ROSBridge.get_default_adapter(topic_info.msgtype)

        if adapter:
            return adapter

        # If adapter does not exist, create a new one through pyarrow schema deduced from msgdef
        msgtype: str = topic_info.msgtype
        msgdef: str = topic_info.msgdef.data
        pyarrow_schema = convert_ros2msg(msgdef, msgtype)

        if not pyarrow_schema:
            logger.warning(
                f"Topic {topic_info.msgtype} does not contain any message definition and cannot be turned as an Unmodeled"
            )
            return None

        logger.info(
            f"Topic {topic_info.msgtype} adapter cannot be found, therefore an UnmodeledAdapter will be created."
        )

        # Create the ontology, honoring any user-configured serialization format for this msgtype
        serialization_format = self._serialization_formats.get(
            msgtype, SerializationFormat.Default
        )
        unmodeled_ontology = resolve_ontology_class(
            ontology_tag=_class_name_from_ros_msgtype(topic_info.msgtype),
            schema=pyarrow_schema,
            serialization_format=serialization_format,
        )

        # Get the unmodeled adapter or create a new one
        adapter = UnmodeledAdapter.get_or_create(
            # This will make a new class or reuse an already registered one
            ontology_type=unmodeled_ontology,
            msgtype=msgtype,
        )

        return adapter

    # --- Properties ---
    def msg_count(self, topic: Optional[str] = None) -> int:
        """
        Returns the total number of messages to be processed based on active filters.

        Args:
            topic (Optional[str]): If provided, returns the count for that specific topic, even if filtered or unresolved adapted.
                If None, returns the aggregate count for all accepted topics.

        Returns:
            int: The total message count.
        """

        self._resolve_connections()
        if not topic:
            return sum(t_info.msgcount for t_info in self._accepted_topics.values())

        topic_info = self._resolved_topics.get(topic)

        if topic_info is None:
            logger.error(f"Topic '{topic}' not found in the connections.")
            return 0

        return topic_info.msgcount

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
        Retrieves the list of accepted topic names that will be processed.

        This property returns the result of the "Smart Filtering" process which consists in:
        1) resolving any glob patterns (e.g., `/camera/*`) provided at initialization and comparing with the ones contained within the rosbags
        2) excluding all remaining topics that do not have an Adapter that allow the translation to a Mosaico Ontology

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
        return list(self._accepted_topics.keys())

    @property
    def resolved_topics(self) -> List[str]:
        """
        Retrieves the list of **all** the canonical topic names **that are in the rosbag**.
        This property does not account for topics filtered out or not-adapted: it returns everything.

        Example:
            ```python
            with ROSLoader(file_path="data.mcap", topics=["/sensors/*"]) as loader:
                # If the bag contains /sensors/imu, /sensors/gps and /base/camera,
                # this property returns all: ['/sensors/imu', '/sensors/gps', '/base/camera']
                print(f"Loading resolved topics: {loader.resolved_topics}")
            ```

        Returns:
            List[str]: A list of all topic names contained within the rosbag.
        """
        self._resolve_connections()
        return list(self._resolved_topics.keys())

    @property
    def unresolved_adapted_topics(self) -> List[str]:
        """
        Retrieves the list of topic names that are **skipped** due to unavailable Mosaico adapter.

        Example:
            ```python
            with ROSLoader(file_path="data.mcap") as loader:
                # If the bag contains /sensors/imu and /sensors/custom_gps and the user
                # did not provide an adapter for /sensors/custom_gps or the bag file
                # does not contain their message definition this property
                # returns ['/sensors/custom_gps']
                print(f"Unresolved adapter for topics: {loader.unresolved_adapted_topics}")
            ```

        Returns:
            List[str]: A list of topic with unresolved adapter to translate them into Mosaico Ontology.
        """
        self._resolve_connections()
        return list(self._unresolved_adapter_topics.keys())

    @property
    def filtered_topics(self) -> List[str]:
        """
        Retrieves the list of topic names that are **skipped** due to the user filter.

        Example:
            ```python
            with ROSLoader(file_path="data.mcap", topics="*imu*") as loader:
                # If the bag contains /sensors/imu and /sensors/custom_gps and the user
                # filtered for the topics containing `imu` this property returns ['/sensors/custom_gps ']
                print(f"Filtered topics: {loader.filtered_topics}")
            ```

        Returns:
            List[str]: The list of topics filtered by the user. Empty if no filter is provided.
        """
        self._resolve_connections()
        return list(self._filtered_topics.keys())

    @property
    def msg_types(self) -> List[str | None]:
        """
        Retrieves the list of ROS message types corresponding to the accepted topics.

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
        return [val.msgtype for val in self._accepted_topics.values()]

    @property
    def rejected_topics(self) -> List[Tuple[str, TopicStatus]]:

        rejected_topics: List[Tuple[str, TopicStatus]] = []
        self._resolve_connections()

        # Filtered
        for t_filtered in self.filtered_topics:
            rejected_topics.append((t_filtered, TopicStatus.FILTERED))

        # Adapter unresolved
        for t_unresolved_adapter in self._unresolved_adapter_topics:
            rejected_topics.append(
                (t_unresolved_adapter, TopicStatus.UNRESOLVED_ADAPTED)
            )

        return rejected_topics

    # --- Core Logic ---

    def resolve_adapter(self, topic_name: str) -> Optional[type[ROSAdapterBase]]:
        """
        Returns the resolve adapter for accepted topic.

        Args:
            topic_name (str): The topic name whose adapter should be resolved.
                Must be one of the accepted topics produced by :meth:`_resolve_connections`.

        Returns:
            Optional[Type[ROSAdapterBase]]: The ROS->Mosaico adapter type obtained during :meth:`_resolve_connections`.
                Adapter is resolved through the rosmsg_type within Mosaico topic metadata
                (if available) or getting the default adapter associated to the topic's ontology.
        """
        self._resolve_connections()

        if topic_name not in self._accepted_topics:
            return None

        return self._topic_cached_adapters.get(topic_name)

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
                field_data, const_data = _to_dict(msg_obj)

                # Yield the standard SDK message
                yield (
                    ROSMessage(
                        bag_timestamp_ns=bag_timestamp_ns,
                        topic=connection.topic,
                        msg_type=connection.msgtype,
                        data=field_data,
                        const_data=const_data,
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
        m_client (MosaicoClient): An open :class:`MosaicoClient` connection.
        typestore (Typestore): The ROS typestore containing the registered ROS messages.
        sequence_name (str): Name of the Mosaico sequence to load.
        topics (Optional[Union[str, List[str]]]): Optional topic-name filter patterns (glob-style, ``!``-prefixed for
            exclusions). ``None`` loads all topics.
        start_timestamp_ns (Optional[int]): Lower bound for the time window (nanoseconds). Clipped
            to the sequence minimum if out of range.
        end_timestamp_ns (Optional[int]): Upper bound for the time window (nanoseconds). Clipped to
            the sequence maximum if out of range.
    """

    def __init__(
        self,
        m_client: MosaicoClient,
        typestore: Typestore,
        sequence_name: str,
        topics: Optional[List[str]] = None,
        start_timestamp_ns: Optional[int] = None,
        end_timestamp_ns: Optional[int] = None,
    ):

        self.client = m_client
        self.typestore: Typestore = typestore
        self.sequence_name = sequence_name
        self.topic_glob_pattern = topics
        self.start_timestamp_ns = start_timestamp_ns
        self.end_timestamp_ns = end_timestamp_ns

        self.seq_handler: Optional[SequenceHandler] = None
        self.streamer: Optional[SequenceDataStreamer] = None

        self._resolved_topics: list[
            str
        ] = []  # The full topics set contained in the sequence

        self._accepted_topics: list[
            str
        ] = []  # The actual topics matched after globbing

        self._unresolved_adapted_topics: list[
            str
        ] = []  # The topics whose ontology type is not resolved (neither adapted nor message defition available)

        self._unregistered_topics: list[
            str
        ] = []  # The topics whose message type is not present within the typestore

        self._malformed_metadata_topics: list[
            str
        ] = []  # The topics whose '_ros_' metadata is malformed

        self._filtered_topics: list[str] = []  # The topics filtered by user

        self._topic_ros_metadata: dict[
            str, Any
        ] = {}  # Dictionary containing a map from moisaco accepted topics to ros specific metadata (extracted from Mosaico topics)

        self._topic_cached_adapters: dict[
            str, type[ROSAdapterBase]
        ] = {}  # Dictionary containing a map from moisaco accepted topics to mosaico adapter

    def _get_or_create_adapter(
        self, t_handler: TopicHandler
    ) -> Tuple[Optional[type[ROSAdapterBase]], Optional[str]]:
        """
        Resolves a topic's Mosaico adapter and the ROS msgtype used to validate it
        against the typestore.

        The adapter is looked up first using the rosmsg_type stored in the topic's
        ``_ros_`` metadata (if present), falling back to the default adapter
        registered for the topic's ontology tag.

        Args:
            t_handler (TopicHandler): The topic handler whose adapter should be resolved.

        Returns:
            Tuple[Optional[type[ROSAdapterBase]], Optional[str]]: A ``(adapter, rosmsg_type)`` pair.

        Raise: TypeError when the topic's ``_ros_`` metadata carries a non-string ``msgtype`` (malformed
            metadata)

        """
        ros_metadata = _extract_ros_metadata(t_handler)

        declared_rosmsg_type = ros_metadata.get("msgtype")

        adapter = None
        resolved_rosmsg_type = None

        # Try looking for adapter using msgtype
        if declared_rosmsg_type is not None:
            adapter = ROSBridge.get_default_adapter(declared_rosmsg_type)
            resolved_rosmsg_type = declared_rosmsg_type

        if not adapter:
            # If here adapter has not been found -> Check adapter associated to
            # the ontology tag and get its default msgtype (if ontology is
            # adapted, otherwise mantain what has already been found).
            adapter = ROSBridge.get_default_mosaico_adapter(t_handler.ontology_tag)

            # Reset adapter in case in case __schema_fingerprint__ of adapter's
            # ontology type is not coherent with the one coming from the server
            if (
                adapter is not None
                and adapter.ontology_data_type().__schema_fingerprint__
                != _compute_schema_fingerprint(t_handler._arrow_schema)
            ):
                adapter = None

            resolved_rosmsg_type = (
                adapter.get_default_ros_msg() if adapter else resolved_rosmsg_type
            )

        if not adapter:
            # In this case you need to get or create a new adapter from the
            # unmodeled class. In case of new adapter, register msgdef to
            # typestore. Note that msgdef needs to be available here otherwise
            # it is not possible to register the new type in the typestore

            try:
                msgtype: str = ros_metadata["msgtype"]
                msgdef: str = ros_metadata["msgdef"]
            except KeyError:
                logger.warning(
                    f"Cannot create Unmodeled Adapter for topic {t_handler.name} since its metadata do not contain msgtype or msgdef"
                )
                return None, resolved_rosmsg_type

            # Create the ontology
            unmodeled_ontology = resolve_ontology_class(
                ontology_tag=t_handler.ontology_tag,
                schema=t_handler._arrow_schema,
                serialization_format=SerializationFormat(
                    t_handler.serialization_format
                ),
            )

            # Get the unmodeled adapter or create a new one
            adapter = UnmodeledAdapter.get_or_create(
                # This will make a new class or reuse an already registered one
                ontology_type=unmodeled_ontology,
                msgtype=msgtype,
            )

            resolved_rosmsg_type = adapter.get_default_ros_msg()

            # Register new msgdef within typestore
            add_types = get_types_from_msg(msgdef, msgtype)
            self.typestore.register(add_types)

        return adapter, resolved_rosmsg_type

    def _resolve_sequence(self) -> SequenceHandler:
        """
        Lazily initializes the sequence handler, resolved topic list, and streamer.

        Called automatically on first access to any property or iterator. Performs
        the following steps:

        1. Fetches the :class:`SequenceHandler` for the configured sequence name
           and validates it exists.
        2. Clips ``start_timestamp_ns`` / ``end_timestamp_ns`` to the sequence bounds,
           logging a warning if clipping occurs.
        3. Applies the topic filter via :func:`_filter_topics_from_list`.
        4. For each matched topic, extracts its Mosaico adapter via
           :meth:`_get_or_create_adapter`. Adapter is first looked up using
           ``_ros_`` metadata, falling back to adapter associated to the ontology
           tag. Afterward, msgtype is extracted from found adapter if metadata did
           not hold this information.
           Topics that pass all checks are accepted, together with their extracted
           ROS metadata and adapter, cached in ``_topic_ros_metadata`` and
           ``_topic_cached_adapters`` respectively. On the other hand, a topic may
           be rejected because:
            - adapter is not found
            - malformed metadata
            - msgtype is not present within ROS typestore
        5. Creates the :class:`SequenceDataStreamer` over the accepted topics, to be
           returned by :meth:`__iter__`.

        """
        if self.seq_handler is not None:
            return self.seq_handler

        # Get requested sequence + validation
        self.seq_handler = self.client.sequence_handler(
            sequence_name=self.sequence_name
        )

        # Check sequence exists
        if not _validate_sequence(self.seq_handler):
            raise (
                ValueError(
                    f"Your requested sequence '{self.sequence_name}' could not be found!"
                )
            )

        # Get all topics from sequence handler
        self._resolved_topics = self.seq_handler.topics

        # Clipping requested start/end timestamp to start/end sequence timestamp if existing
        self.start_timestamp_ns, self.end_timestamp_ns = _clip_timestamp(
            self.start_timestamp_ns,
            self.end_timestamp_ns,
            self.seq_handler.timestamp_ns_min,
            self.seq_handler.timestamp_ns_max,
        )

        matched_topics = _filter_topics_from_list(
            self.seq_handler.topics, self.topic_glob_pattern
        )

        # Filter topics
        for t_name in self.seq_handler.topics:
            # 1) requested topic
            if t_name not in matched_topics:
                self._filtered_topics.append(t_name)
                continue

            t_handler = self.seq_handler.get_topic_handler(t_name)

            # 2) Find Mosaico topic's adapter using:
            #     - rosmsg_type got from topic metadata
            #     - topic ontology (falling back to default adapter)

            # Extract rosmsg_type from topic_handler metadata (if available) and ensure that it is a string
            try:
                adapter, rosmsg_type = self._get_or_create_adapter(t_handler)
            except TypeError:
                self._malformed_metadata_topics.append(t_name)
                logger.warning(
                    f"Skipping topic {t_name}: malformed metadata {t_handler.user_metadata}."
                )
                continue

            if not adapter:
                logger.warning(
                    f"Skipping topic {t_name}: not-adapted ontology {t_handler.ontology_tag}."
                )
                self._unresolved_adapted_topics.append(t_name)
                continue

            # 3) check that rosmsg_type (either from metadata or default adapter) is present within typestore
            if not rosmsg_type:
                rosmsg_type = adapter.get_default_ros_msg()

            if self.typestore.types.get(rosmsg_type) is None:
                logger.warning(
                    f"Skipping topic {t_name}: {rosmsg_type} not present in ROS typestore."
                )
                self._unregistered_topics.append(t_name)
                continue

            # Finally accept the topic and extract its ROS metadata (if any)
            self._accepted_topics.append(t_name)

            self._topic_ros_metadata.update(
                {t_name: t_handler.user_metadata.get("_ros_")}
            )
            self._topic_cached_adapters.update({t_name: adapter})

        if not self._accepted_topics:
            raise RuntimeError(
                "Unable to initialize MosaicoLoader: No topic matched criteria or adapter found. Try checking the topics filter, if any."
            )

        # Resolving streamer only with accepted topics
        self.streamer = self.seq_handler.get_data_streamer(
            topics=self._accepted_topics,
            start_timestamp_ns=self.start_timestamp_ns,
            end_timestamp_ns=self.end_timestamp_ns,
        )

        return self.seq_handler

    # --- Properties ---
    def msg_count(self, topic: Optional[str] = None) -> int:
        """
        Returns the total number of messages for the given topic, or for all
        resolved topics combined.

        Args:
            topic (Optional[str]): If provided, count messages for that specific topic only.
                If ``None``, sum across all resolved topics.

        Returns:
            int: The total message count.
        """
        self._resolve_sequence()

        if not self.streamer:
            raise Exception(
                "Impossible to start streaming: SequenceDataStreamer is not initialised. Did you forget calling _resolve_sequence()?"
            )

        if topic and topic not in self.topics:
            raise ValueError(
                f"Topic {topic} is not among the accepted topics. Accepted topics are: {self._accepted_topics}"
            )

        topics_to_count = [topic] if topic else self._accepted_topics

        total_msg_count = sum(
            filter(
                None,
                (
                    self.streamer._topic_readers[topic].msg_count
                    for topic in topics_to_count
                ),
            )
        )

        return total_msg_count

    @property
    def duration(self) -> int:
        """
        Returns the duration of the sequence in nanoseconds.

        Returns:
            int: The duration of the sequence in nanoseconds. Returns 0 if sequence is not valid
        """
        s_handler = self._resolve_sequence()

        if (
            s_handler.timestamp_ns_max is not None
            and s_handler.timestamp_ns_min is not None
        ):
            return s_handler.timestamp_ns_max - s_handler.timestamp_ns_min

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

        return self._accepted_topics

    @property
    def resolved_topics(self) -> List[str]:
        """
        Retrieves the list of **all** the canonical topic names **that are in the sequence**.

        This property does not account for topics filtered out or not-adapted: it returns everything.

        Returns:
            List[str]: A list of *all* topic names present within the requested sequence.
        """
        self._resolve_sequence()
        return self._resolved_topics

    @property
    def unresolved_adapted_topics(self) -> List[str]:
        """
        Retrieves the list of topic names that are **skipped** due to unresolved Mosaico adapter.

        Returns:
            List[str]: A list of topic with unresolved adapter to translate them into ROS messages.
        """
        self._resolve_sequence()
        return self._unresolved_adapted_topics

    @property
    def filtered_topics(self) -> List[str]:
        """
        Retrieves the list of topic names that are **skipped** due to the user filter.

        Returns:
            List[str]: The list of topics filtered by the user. Empty if no filter is provided.
        """
        self._resolve_sequence()
        return self._filtered_topics

    @property
    def msg_types(self) -> List[str | None]:
        """
        Returns the Mosaico ontology type tags for each accepted topic.

        Entries appear in the same order as :attr:`topics`. A ``None`` entry
        indicates that the topic handler could not be found.

        Triggers lazy initialization on first access.

        Returns:
            List[str | None]: Ontology tag strings (e.g. ``"imu"``, ``"image"``)
                or ``None`` for unresolvable topics.
        """
        s_handler = self._resolve_sequence()

        return [
            t_handler.ontology_tag
            if (t_handler := s_handler.get_topic_handler(topic)) is not None
            else None
            for topic in self._accepted_topics
        ]

    @property
    def rejected_topics(self) -> List[Tuple[str, TopicStatus]]:

        rejected_topics: List[Tuple[str, TopicStatus]] = []
        self._resolve_sequence()

        # Filtered
        for t_filtered in self.filtered_topics:
            rejected_topics.append((t_filtered, TopicStatus.FILTERED))

        # Adapter not found
        for t_unresolved_adapter in self._unresolved_adapted_topics:
            rejected_topics.append(
                (t_unresolved_adapter, TopicStatus.UNRESOLVED_ADAPTED)
            )

        # Not found in typestore
        for t_unregistered in self._unregistered_topics:
            rejected_topics.append((t_unregistered, TopicStatus.NOT_IN_TYPESTORE))

        # Metadata malformed
        for t_unregistered in self._malformed_metadata_topics:
            rejected_topics.append((t_unregistered, TopicStatus.MALFORMED_METADATA))

        return rejected_topics

    # --- Core Logic ---

    def resolve_rosmsg_type(self, topic_name: str) -> Optional[str]:
        """
        Returns the original ROS message type for a topic stored in Mosaico.

        When a ROS bag is ingested into Mosaico, the original ROS message type
        (e.g. ``sensor_msgs/msg/Imu``) is preserved in the topic's user metadata
        under the ``_ros_`` key. This method retrieves that type so callers can
        reconstruct the correct ROS schema when re-exporting or comparing data.

        Args:
            topic_name (str): The topic whose original ROS message type should be resolved.
                Must be one of the accepted topics produced by :meth:`_resolve_sequence`.

        Returns:
            Optional[str]: The ROS message type string (e.g. ``"sensor_msgs/msg/Imu"``) if the
                metadata was stored at ingestion time, or ``None`` if the topic is
                unknown, the ``_ros_`` metadata block is absent, or the ``msgtype``
                key is missing from that block.
        """
        self._resolve_sequence()

        return (self._topic_ros_metadata.get(topic_name) or {}).get("msgtype")

    def resolve_adapter(self, topic_name: str) -> Optional[type[ROSAdapterBase]]:
        """
        Returns the resolve adapter for accepted topic.

        Args:
            topic_name (str): The topic name whose adapter should be resolved.
                Must be one of the accepted topics produced by :meth:`_resolve_sequence`.

        Returns:
            Optional[Type[ROSAdapterBase]]: The Mosaico->ROS adapter type obtained during :meth:`_resolve_sequence`.
                Adapter is resolved through the rosmsg_type within Mosaico topic metadata
                (if available) or getting the default adapter associated to the topic's ontology.
        """
        self._resolve_sequence()

        if topic_name not in self._accepted_topics:
            return None

        return self._topic_cached_adapters.get(topic_name)

    def __iter__(self):
        self._resolve_sequence()

        if not self.streamer:
            raise Exception(
                "Impossible to start streaming: SequenceDataStreamer is not initialised. Did you forget calling _resolve_sequence()?"
            )

        return self.streamer

    def close(self):
        """
        Explicitly closes the sequence handler and releases system resources.
        """

        # This handles also streamer closing
        if self.seq_handler:
            self.seq_handler.close()
            self.seq_handler = None
            self.streamer = None

    def __enter__(self):
        """Context manager support."""
        self._resolve_sequence()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensures resources are released even if an error occurs in the `with` block."""
        self.close()
