from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
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
from mosaicolabs.bridges.ros.adapters.unmodeled import UnmodeledAdapter
from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.logging_config import get_logger
from mosaicolabs.models.core.helpers import resolve_ontology_class

from ...models.core.serializable import _compute_schema_fingerprint
from ..protocols.mcap.converters.ros_converter import RosMsgSchemaConverter
from .adapter_base import ROSAdapterBase, RosSchemaMetadata
from .bridge import ROSBridge
from .helpers import (
    _class_name_from_ros_msgtype,
    _clip_timestamp,
    _extract_ros_metadata,
    _filter_topics_from_dict,
    _filter_topics_from_list,
    _to_dict,
    _validate_sequence,
)
from .ros_message import ROSMessage

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


# --- Shared Topic Resolution/Rejection Bookkeeping ---


class _BaseROSTopicResolver(ABC):
    """
    Shared topic classification and adapter-resolution logic for :class:`ROSLoader`
    (bag file source) and :class:`MosaicoLoader` (Mosaico sequence source).

    Both loaders classify every topic of their underlying data source into one of:
    **accepted** (adapter resolved, passes the user's `topics` filter), **filtered**
    (excluded by the user's `topics` filter), or **unresolved** (no Mosaico adapter
    could be resolved) — plus any source-specific rejection reasons (see
    :meth:`_extra_rejected_topics`). This base class implements the properties that
    only need to read those buckets, so each subclass only has to populate them via
    its own `_ensure_resolved()` (which performs the actual, source-specific
    resolution: opening the bag file, or querying the Mosaico sequence).

    Subclasses are expected to set, during `_ensure_resolved()`:

    * `_resolved_topics`: **all** topic names in the source (dict or list; only
      the keys/elements are read by this base class).
    * `_accepted_topics`: topic names that passed filtering and adapter resolution.
    * `_unresolved_adapter_topics`: topic names with no resolvable Mosaico adapter.
    * `_filtered_topics`: topic names excluded by the user's `topics` filter.
    * `_topic_cached_adapters`: `Dict[str, Type[ROSAdapterBase]]` mapping accepted
      topic names to their resolved adapter.
    """

    _resolved_topics: Any
    _accepted_topics: Any
    _unresolved_adapter_topics: Any
    _filtered_topics: Any
    _topic_cached_adapters: Dict[str, Type[ROSAdapterBase]]

    def __init__(self, container_type: Type[Iterable]):
        self._resolved_topics = container_type()
        """The full set of canonical topic names in the underlying source (dict or list; only the keys/elements are read by this base class)."""
        self._accepted_topics = container_type()
        """The set of topic names that passed filtering and adapter resolution (dict or list; only the keys/elements are read by this base class)."""
        self._unresolved_adapter_topics = container_type()
        """The set of topic names that have no resolvable Mosaico adapter (dict or list; only the keys/elements are read by this base class)."""
        self._filtered_topics = container_type()
        """The set of topic names that are excluded by the user's `topics` filter (dict or list; only the keys/elements are read by this base class)."""
        self._topic_cached_adapters: dict[str, type[ROSAdapterBase]] = {}
        """Dictionary mapping accepted topic names to their resolved Mosaico adapter class."""

    @abstractmethod
    def _ensure_resolved(self) -> None:
        """Lazily triggers the source-specific resolution, populating the topic buckets."""

    def _extra_rejected_topics(self) -> List[Tuple[str, "TopicStatus"]]:
        """
        Hook for source-specific rejection reasons beyond FILTERED/UNRESOLVED_ADAPTED
        (e.g. :class:`MosaicoLoader`'s `NOT_IN_TYPESTORE`/`MALFORMED_METADATA`).

        Returns:
            List[Tuple[str, TopicStatus]]: Additional `(topic_name, status)` rejections.
                Empty by default.
        """
        return []

    @property
    def topics(self) -> List[str]:
        """
        Retrieves the list of accepted topic names that will be processed.

        Returns:
            List[str]: A list of topic names currently matched and scheduled for loading.
        """
        self._ensure_resolved()
        return list(self._accepted_topics)

    @property
    def resolved_topics(self) -> List[str]:
        """
        Retrieves the list of **all** the canonical topic names in the underlying source.
        This property does not account for topics filtered out or not-adapted: it returns everything.

        Returns:
            List[str]: A list of all topic names contained within the source.
        """
        self._ensure_resolved()
        return list(self._resolved_topics)

    @property
    def unresolved_adapted_topics(self) -> List[str]:
        """
        Retrieves the list of topic names that are **skipped** due to unavailable Mosaico adapter.

        Returns:
            List[str]: A list of topics with unresolved adapter to translate them into/from Mosaico Ontology.
        """
        self._ensure_resolved()
        return list(self._unresolved_adapter_topics)

    @property
    def filtered_topics(self) -> List[str]:
        """
        Retrieves the list of topic names that are **skipped** due to the user filter.

        Returns:
            List[str]: The list of topics filtered by the user. Empty if no filter is provided.
        """
        self._ensure_resolved()
        return list(self._filtered_topics)

    @property
    def rejected_topics(self) -> List[Tuple[str, "TopicStatus"]]:
        """
        Retrieves every rejected topic together with the reason it was rejected.

        Returns:
            List[Tuple[str, TopicStatus]]: `(topic_name, status)` pairs for every topic
                excluded from `topics`, combining `filtered_topics`, `unresolved_adapted_topics`,
                and any source-specific rejections from `_extra_rejected_topics()`.
        """
        self._ensure_resolved()

        rejected: List[Tuple[str, TopicStatus]] = [
            (t, TopicStatus.FILTERED) for t in self.filtered_topics
        ]
        rejected += [
            (t, TopicStatus.UNRESOLVED_ADAPTED) for t in self.unresolved_adapted_topics
        ]
        rejected += self._extra_rejected_topics()
        return rejected

    def resolve_adapter(self, topic_name: str) -> Optional[type[ROSAdapterBase]]:
        """
        Returns the resolved adapter for an accepted topic.

        Args:
            topic_name (str): The topic name whose adapter should be resolved.
                Must be one of the accepted topics produced by `_ensure_resolved()`.

        Returns:
            Optional[Type[ROSAdapterBase]]: The resolved adapter type, or `None` if the
                topic is not among the accepted topics.
        """
        self._ensure_resolved()

        if topic_name not in self._accepted_topics:
            return None

        return self._topic_cached_adapters.get(topic_name)


class ROSLoader(_BaseROSTopicResolver):
    """
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

    ACCEPTED_EXTENSIONS = {".bag", ".db3", ".mcap"}

    def __init__(
        self,
        file_path: Union[str, Path],
        typestore_or_distro: Typestore | Stores,
        topics: Optional[Union[str, List[str]]] = None,
        serialization_formats: Optional[Dict[str, SerializationFormat]] = None,
    ):
        """
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
            container_type=dict[str, TopicInfo]
        )  # Initialize the base class to set up topic resolution state
        self._file_path = Path(file_path)
        """The path to the bag file or directory."""

        # Configuration
        self._requested_topics = [topics] if isinstance(topics, str) else topics
        """The user-specified topic filter(s) to apply when resolving topics."""
        self._typestore: Typestore = (
            typestore_or_distro
            if isinstance(typestore_or_distro, Typestore)
            else get_typestore(typestore_or_distro)
        )
        """The typestore used for message type resolution."""
        self._serialization_formats: Dict[str, SerializationFormat] = (
            serialization_formats or {}
        )
        """Mapping of ROS message types to their desired serialization format for Unmodeled ontologies."""

        # State
        self._reader: Optional[AnyReader] = None
        """The underlying `rosbags` reader instance, lazily initialized."""
        self._connections: List[Connection] = []
        """The list of resolved connections (topics) that will be iterated over."""

    def _validate_file(self):
        if not self._file_path.exists():
            raise FileNotFoundError(f"ROS bag not found: {self._file_path}")
        if self._file_path.suffix not in self.ACCEPTED_EXTENSIONS:
            raise ValueError(
                f"Unsupported format '{self._file_path.suffix}'. Supported: {self.ACCEPTED_EXTENSIONS}"
            )

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
            self._validate_file()
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
        3. **Fall back to an [`UnmodeledAdapter`][mosaicolabs.bridges.ros.adapters.UnmodeledAdapter]**:
           when no hand-written adapter exists, one is synthesized on the fly so the
           topic can still be loaded generically, without a semantic ontology mapping:

            a. The topic's raw ``.msg``/``.idl`` definition (``topic_info.msgdef.data``)
               is converted into an equivalent PyArrow schema via
               [`convert_rosmsg`][mosaicolabs.protocols.ros_converter.RosMsgConverter.convert_rosmsg].
            b. An [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled] ontology
               class is obtained/created for this schema via
               [`resolve_ontology_class`][mosaicolabs.models.core.helpers.resolve_ontology_class],
               tagged with an ontology tag derived from the ROS msgtype's last path
               segment (e.g. `sensor_msgs/msg/Imu` -> `Imu`). The serialization format
               used for this ontology is looked up in ``self._serialization_formats``
               by ``msgtype``, falling back to ``SerializationFormat.Default`` when the
               msgtype has no entry there.
            c. [`UnmodeledAdapter.get_or_create`][mosaicolabs.bridges.ros.adapters.UnmodeledAdapter.get_or_create]
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
        pyarrow_schema = RosMsgSchemaConverter.convert_rosmsg(msgdef, msgtype)

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

    def _ensure_resolved(self) -> None:
        """Lazily opens the bag file and resolves topics (see `_resolve_connections`)."""
        self._resolve_connections()

    @property
    def msg_types(self) -> List[str | None]:
        """
        Retrieves the list of ROS message types corresponding to the accepted topics.

        Each entry in this list represents the schema name (e.g., `sensor_msgs/msg/Image`)
        required to correctly deserialize the messages for the topics returned by
        the `.topics` property.

        Example:
            ```python
            with ROSLoader(file_path="data.mcap", typestore_or_distro=get_typestore(Stores.EMPTY)) as loader:
                for topic, msg_type in zip(loader.topics, loader.msg_types):
                    print(f"Topic {topic} requires schema: {msg_type}")
            ```

        Returns:
            List[str]: A list of ROS message type strings in the same order
                as the resolved topics.
        """
        self._resolve_connections()
        return [val.msgtype for val in self._accepted_topics.values()]

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
                yield (
                    ROSMessage(
                        bag_timestamp_ns=bag_timestamp_ns,
                        topic=connection.topic,
                        msg_type=connection.msgtype,
                        data=None,
                    ),
                    e,
                )

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


class MosaicoLoader(_BaseROSTopicResolver):
    """
    Lazy data loader that streams messages from a Mosaico sequence.

    Connects to the Mosaico server on first access, resolves the requested
    sequence and topic filter, clips the time window to valid sequence bounds,
    and exposes a :class:`SequenceDataStreamer` for iteration.

    Conforms to the :class:`Loader` protocol, making it usable with
    :class:`ProgressManager` for live progress reporting.

    Note: `MosaicoLoader` does not itself consult the [`ROSTypeRegistry`][mosaicolabs.bridges.ros.ROSTypeRegistry].
        Resolving `ros_distro`/`custom_msgs` into a concrete `Typestore` (including any custom
        `.msg` registration) is the caller's responsibility — [`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor]
        does this internally before constructing a `MosaicoLoader`.
    """

    def __init__(
        self,
        m_client: MosaicoClient,
        typestore_or_distro: Typestore | Stores,
        sequence_name: str,
        topics: Optional[List[str]] = None,
        start_timestamp_ns: Optional[int] = None,
        end_timestamp_ns: Optional[int] = None,
    ):
        """
        Initializes the loader against a Mosaico sequence, using a caller-supplied
        `Typestore` or ROS distro to resolve adapters and ROS message types.

        `MosaicoLoader` performs no `ROSTypeRegistry` lookups itself — pass in a `Typestore`
        that already has any custom `.msg` definitions registered (e.g. via
        `get_typestore(ros_distro)` plus `Typestore.register(...)`, or the `Typestore`
        that `ROSSequenceExtractor` builds internally from `ROSExtractorConfig.ros_distro`/
        `custom_msgs`), needed when a topic's `_ros_.msgdef` isn't available to auto-register
        the type (e.g. the sequence wasn't ingested from a ROS bag in the first place).

        Example:
            ```python
            from rosbags.typesys import Stores
            from mosaicolabs import MosaicoClient
            from mosaicolabs.bridges.ros import MosaicoLoader

            with MosaicoClient.connect("localhost", 6726) as client:
                # Stream only IMU and GPS topics back out of a sequence
                with MosaicoLoader(
                    m_client=client,
                    typestore_or_distro=Stores.ROS2_HUMBLE,
                    sequence_name="on_track_experiment",
                    topics=["/imu*", "/gps/fix"],
                ) as loader:
                    for topic, ms_msg in loader:
                        print(f"Read {topic} @ {ms_msg.timestamp_ns}")
            ```

        Args:
            m_client (MosaicoClient): An open :class:`MosaicoClient` connection.
            typestore_or_distro (Typestore | Stores): A pre-built `Typestore` (e.g. one
                already carrying custom `.msg` registrations), or a `Stores` distro to
                resolve a fresh, empty typestore for via `get_typestore()`.
            sequence_name (str): Name of the Mosaico sequence to load.
            topics (Optional[Union[str, List[str]]]): Optional topic-name filter patterns (glob-style, ``!``-prefixed for
                exclusions). ``None`` loads all topics.
            start_timestamp_ns (Optional[int]): Lower bound for the time window (nanoseconds). Clipped
                to the sequence minimum if out of range.
            end_timestamp_ns (Optional[int]): Upper bound for the time window (nanoseconds). Clipped to
                the sequence maximum if out of range.
        """

        super().__init__(
            container_type=list[str]
        )  # Initialize the base class to set up topic resolution state

        self._client = m_client
        """The MosaicoClient used to fetch sequence data and metadata."""
        self._typestore: Typestore = (
            typestore_or_distro
            if isinstance(typestore_or_distro, Typestore)
            else get_typestore(typestore_or_distro)
        )
        """The ROS typestore containing the registered ROS messages. Used for adapter resolution."""
        self._sequence_name = sequence_name
        """The name of the Mosaico sequence to load."""
        self._topic_glob_pattern = topics
        """Optional list of topic-name filter patterns (glob-style, ``!``-prefixed for exclusions)."""
        self._start_timestamp_ns = start_timestamp_ns
        """Lower bound for the time window (nanoseconds). Clipped to the sequence minimum if out of range."""
        self._end_timestamp_ns = end_timestamp_ns
        """Upper bound for the time window (nanoseconds). Clipped to the sequence maximum if out of range."""
        self._seq_handler: Optional[SequenceHandler] = None
        """The mosaico sequence handler, lazily initialized on first access."""
        self._streamer: Optional[SequenceDataStreamer] = None
        """The mosaico sequence streamer, lazily initialized on first access. Provides an iterator over the sequence messages."""

        # Additional rejection buckets for Mosaico-specific reasons
        self._unregistered_topics: list[str] = []
        """
        The topics whose message type is not present within the typestore.
        These topics are rejected because their ROS message type cannot be deserialized without a registered schema.
        """
        self._malformed_metadata_topics: list[str] = []
        """
        The topics whose '_ros_' metadata is malformed.
        These topics are rejected because their metadata does not contain the required 'msgtype' or 'msgdef' fields.
        """
        self._topic_ros_metadata: dict[str, Any] = {}
        """Dictionary containing a map from Mosaico accepted topics to their extracted ROS metadata (from '_ros_' field)."""
        # self._topic_cached_adapters: dict[str, type[ROSAdapterBase]] = {}
        # """Dictionary containing a map from Mosaico accepted topics to their resolved Mosaico adapter class."""

    def _adapter_from_metadata_msgtype(
        self, t_handler: TopicHandler
    ) -> Optional[Tuple[type[ROSAdapterBase], str]]:
        """
        Strategy 1: look up a hand-written adapter using the ``msgtype`` recorded
        in the topic's ``_ros_`` metadata.

        Args:
            t_handler (TopicHandler): The topic handler whose ``_ros_`` metadata is read for a ``msgtype``.

        Returns:
            Optional[Tuple[type[ROSAdapterBase], str]]: The ``(adapter, msgtype)`` pair if a hand-written
                adapter is registered for ``msgtype``, otherwise ``None``.
        """

        ros_metadata = _extract_ros_metadata(t_handler)
        msgtype: Optional[str] = ros_metadata.get("msgtype")

        if msgtype is None:
            return None

        adapter = ROSBridge.get_default_adapter(msgtype)

        if adapter is None:
            return None

        return adapter, msgtype

    def _adapter_from_ontology_tag(
        self, t_handler: TopicHandler
    ) -> Optional[Tuple[type[ROSAdapterBase], str]]:
        """
        Strategy 2: look up the default hand-written adapter registered for the
        topic's ontology tag, honored only if its ontology's schema fingerprint
        still matches the schema coming from the server (otherwise the topic's
        data no longer matches what that adapter expects).

        Args:
            t_handler (TopicHandler): The topic handler whose ontology tag and Arrow schema are checked.

        Returns:
            Optional[Tuple[type[ROSAdapterBase], str]]: The ``(adapter, default_rosmsg_type)`` pair if a
                matching, fingerprint-compatible adapter is found, otherwise ``None``.
        """
        adapter = ROSBridge.get_default_mosaico_adapter(t_handler.ontology_tag)

        if adapter is None:
            return None

        if (
            adapter.ontology_data_type().__schema_fingerprint__
            != _compute_schema_fingerprint(t_handler._arrow_schema)
        ):
            return None

        return adapter, adapter.get_default_ros_msg()

    def _create_unmodeled_adapter(
        self, t_handler: TopicHandler
    ) -> Optional[Tuple[type[UnmodeledAdapter], str]]:
        """
        Strategy 3 (fallback): synthesize an ``UnmodeledAdapter`` for the topic's
        ontology when no hand-written adapter could be resolved. Always succeeds,
        provided ``msgtype`` is known.

        Args:
            t_handler (TopicHandler): The topic handler used to derive the ``msgtype`` (from its
                ``_ros_`` metadata) and to build the unmodeled ontology (from its ontology tag,
                Arrow schema, and serialization format).

        Returns:
            Optional[Tuple[type[UnmodeledAdapter], str]]: The ``(adapter, msgtype)`` pair,
                otherwise ``None``.
        """

        ros_metadata = _extract_ros_metadata(t_handler)
        msgtype: Optional[str] = ros_metadata.get("msgtype")

        if msgtype is None:
            return None

        unmodeled_ontology = resolve_ontology_class(
            ontology_tag=t_handler.ontology_tag,
            schema=t_handler._arrow_schema,
            serialization_format=SerializationFormat(t_handler.serialization_format),
        )

        # This will make a new class or reuse an already registered one
        adapter = UnmodeledAdapter.get_or_create(
            ontology_type=unmodeled_ontology,
            msgtype=msgtype,
        )

        return adapter, msgtype

    def _register_msgtype(self, msgtype: str, msgdef: Optional[str]):
        """Registers ``msgtype`` in the typestore using ``msgdef``, unless already present."""
        if msgtype in self._typestore.types:
            return

        if msgdef is None:
            logger.warning(f"Failed registering {msgtype}: missing msgdef.")
            return

        add_types = get_types_from_msg(msgdef, msgtype)
        self._typestore.register(add_types)

    def _get_or_create_adapter(
        self, t_handler: TopicHandler
    ) -> Tuple[type[ROSAdapterBase] | type[UnmodeledAdapter], str]:
        """
        Resolves a topic's Mosaico adapter and the ROS msgtype used to validate it
        against the typestore.

        Three resolution strategies are tried in order, the first to succeed wins:

        1. :meth:`_adapter_from_metadata_msgtype` - hand-written adapter keyed by the
           ``msgtype`` recorded in the topic's ``_ros_`` metadata.
        2. :meth:`_adapter_from_ontology_tag` - hand-written adapter registered as the
           default for the topic's ontology tag (schema-fingerprint checked).
        3. :meth:`_create_unmodeled_adapter` - fallback that synthesizes an
           ``UnmodeledAdapter``. This always succeeds unless ``msgtype`` is unknown,
           in which case it raises.

        Once resolved, the ``rosmsg_type`` is registered in the typestore if it
        isn't already present.

        Args:
            t_handler (TopicHandler): The topic handler whose adapter should be resolved.

        Returns:
            Tuple[type[ROSAdapterBase] | type[UnmodeledAdapter], str]: A ``(adapter, rosmsg_type)`` pair.
                Both are always populated: either an earlier strategy resolves both together,
                or the final fallback does.

        Raises:
            TypeError: when the topic's ``_ros_`` metadata carries a non-string ``msgtype`` (malformed
                metadata).
            RuntimeError: when every hand-written-adapter strategy fails and ``msgtype`` is unknown, so
                even the unmodeled fallback cannot be created.
        """

        factory_result = (
            self._adapter_from_metadata_msgtype(t_handler)
            or self._adapter_from_ontology_tag(t_handler)
            or self._create_unmodeled_adapter(t_handler)
        )

        if factory_result is None:
            raise RuntimeError(f"Unable to infer an adapter for {t_handler.name} topic")
        else:
            adapter, resolved_rosmsg_type = factory_result

        # Register type within typestore (no-op if already registered)
        msgdef = _extract_ros_metadata(t_handler).get("msgdef")
        self._register_msgtype(resolved_rosmsg_type, msgdef)

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
        if self._seq_handler is not None:
            return self._seq_handler

        # Get requested sequence + validation
        self._seq_handler = self._client.sequence_handler(
            sequence_name=self._sequence_name
        )

        # Check sequence exists
        if not _validate_sequence(self._seq_handler):
            raise (
                ValueError(
                    f"Your requested sequence '{self._sequence_name}' could not be found!"
                )
            )

        # Get all topics from sequence handler
        self._resolved_topics = self._seq_handler.topics

        # Clipping requested start/end timestamp to start/end sequence timestamp if existing
        self._start_timestamp_ns, self._end_timestamp_ns = _clip_timestamp(
            self._start_timestamp_ns,
            self._end_timestamp_ns,
            self._seq_handler.timestamp_ns_min,
            self._seq_handler.timestamp_ns_max,
        )

        matched_topics = _filter_topics_from_list(
            self._seq_handler.topics, self._topic_glob_pattern
        )

        # Filter topics
        for t_name in self._seq_handler.topics:
            # 1) Filter if topic has not been requested
            if t_name not in matched_topics:
                self._filtered_topics.append(t_name)
                continue

            t_handler = self._seq_handler.get_topic_handler(t_name)

            # 2) Filter if Mosaico adapter cannot be deduced topic's adapter
            try:
                adapter, rosmsg_type = self._get_or_create_adapter(t_handler)
            except TypeError:
                self._malformed_metadata_topics.append(t_name)
                logger.warning(
                    f"Skipping topic '{t_name}': malformed metadata {t_handler.user_metadata}."
                )
                continue
            except RuntimeError:
                logger.warning(
                    f"Skipping topic '{t_name}': not-adapted ontology '{t_handler.ontology_tag}'."
                )
                self._unresolved_adapter_topics.append(t_name)
                continue

            # 3) check that rosmsg_type (either from metadata or default adapter) is present within typestore
            if self._typestore.types.get(rosmsg_type) is None:
                logger.warning(
                    f"Skipping topic '{t_name}': '{rosmsg_type}' not present in ROS typestore."
                )
                self._unregistered_topics.append(t_name)
                continue

            # Finally accept the topic and extract its ROS metadata (if any)
            self._accepted_topics.append(t_name)

            self._topic_ros_metadata.update(
                {t_name: RosSchemaMetadata.extract(t_handler.user_metadata)}
            )
            self._topic_cached_adapters.update({t_name: adapter})

        if not self._accepted_topics:
            raise RuntimeError(
                "Unable to initialize MosaicoLoader: No topic matched criteria or adapter found. Try checking the topics filter, if any."
            )

        # Resolving streamer only with accepted topics
        self._streamer = self._seq_handler.get_data_streamer(
            topics=self._accepted_topics,
            start_timestamp_ns=self._start_timestamp_ns,
            end_timestamp_ns=self._end_timestamp_ns,
        )

        return self._seq_handler

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

        if not self._streamer:
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
                    self._streamer._topic_readers[topic].msg_count
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

    def _ensure_resolved(self) -> None:
        """Lazily resolves the sequence, its topics, and their adapters (see `_resolve_sequence`)."""
        self._resolve_sequence()

    def _extra_rejected_topics(self) -> List[Tuple[str, TopicStatus]]:
        """Adds the Mosaico-specific rejection reasons on top of FILTERED/UNRESOLVED_ADAPTED."""
        rejected: List[Tuple[str, TopicStatus]] = [
            (t, TopicStatus.NOT_IN_TYPESTORE) for t in self._unregistered_topics
        ]
        rejected += [
            (t, TopicStatus.MALFORMED_METADATA) for t in self._malformed_metadata_topics
        ]
        return rejected

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

    def __iter__(self):
        self._resolve_sequence()

        if not self._streamer:
            raise Exception(
                "Impossible to start streaming: SequenceDataStreamer is not initialised. Did you forget calling _resolve_sequence()?"
            )

        return self._streamer

    def close(self):
        """
        Explicitly closes the sequence handler and releases system resources.
        """

        # This handles also streamer closing
        if self._seq_handler:
            self._seq_handler.close()
            self._seq_handler = None
            self._streamer = None

    def __enter__(self):
        """Context manager support."""
        self._resolve_sequence()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensures resources are released even if an error occurs in the `with` block."""
        self.close()
