from pathlib import Path
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Union,
)

from rosbags.highlevel import AnyReader
from rosbags.interfaces import Connection, TopicInfo
from rosbags.typesys import Stores, get_types_from_msg, get_typestore
from rosbags.typesys.store import Typestore

from mosaicolabs import (
    MosaicoClient,
    TopicHandler,
)
from mosaicolabs.bridges.ros.adapters.unmodeled import UnmodeledAdapter
from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.logging_config import get_logger
from mosaicolabs.models.core.helpers import resolve_ontology_class

from ...models.core.serializable import _compute_schema_fingerprint
from ..loader_base import BaseLoader, MosaicoLoader, TopicResolution
from ..protocols.mcap.converters.ros_converter import RosMsgSchemaConverter
from ..topic_status import CommonTopicStatus, ROSTopicStatus
from .adapter_base import ROSAdapterBase, RosSchemaMetadata
from .bridge import ROSBridge
from .helpers import (
    _class_name_from_ros_msgtype,
    _extract_ros_metadata,
    _filter_topics_from_dict,
    _to_dict,
)
from .ros_message import ROSMessage

# Set the hierarchical logger
logger = get_logger(__name__)


class ROSLoader(BaseLoader[ROSAdapterBase]):
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

                self._reject(conn.topic, CommonTopicStatus.FILTERED)
                continue

            # 2) Filter topics that cannot resolve neither a registered Mosaico-adapter nor an Unmodeled one because no PyArrow schema can be derived
            adapter = self._get_or_create_adapter(topic_info)

            if adapter:
                self._accepted_topics.update({conn.topic: topic_info})
            else:
                logger.warning(
                    f"Topic {conn.topic}: unresolved Adapted for msgtype {topic_info.msgtype}. Did you forget to register it?"
                )
                self._reject(conn.topic, CommonTopicStatus.UNRESOLVED_ADAPTER)
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


class MosaicoToROSLoader(MosaicoLoader[ROSAdapterBase]):
    """
    Streams messages out of a Mosaico sequence and adapts them back into ROS messages.

    This is the ROS specialization of [`MosaicoLoader`][mosaicolabs.bridges.loader_base.MosaicoLoader]
    and the read end of the extraction pipeline: [`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor]
    iterates it and writes the results into a bag. It is the mirror image of
    [`ROSLoader`][mosaicolabs.bridges.ros.ROSLoader], which reads a bag *into* Mosaico.

    On top of the generic sequence handling it adds the two things that are specific to
    targeting ROS:

    * **Typestore registration.** A topic's `_ros_.msgdef`, recorded at ingestion time, is
      registered into the typestore as topics are resolved, so custom messages can be
      reconstructed without the caller having to supply their `.msg` files again.
    * **Adapter resolution against the `_ros_` namespace**, falling back to the ontology
      tag and finally to a synthesized `UnmodeledAdapter` — see :meth:`_resolve_topic`.

    Note: `MosaicoToROSLoader` does not itself consult the [`ROSTypeRegistry`][mosaicolabs.bridges.ros.ROSTypeRegistry].
        Resolving `ros_distro`/`custom_msgs` into a concrete `Typestore` (including any custom
        `.msg` registration) is the caller's responsibility — [`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor]
        does this internally before constructing a `MosaicoToROSLoader`.
    """

    SCHEMA_METADATA = RosSchemaMetadata
    """Topics ingested by the ROS bridge carry their bookkeeping under the `_ros_` key."""

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

        `MosaicoToROSLoader` performs no `ROSTypeRegistry` lookups itself — pass in a `Typestore`
        that already has any custom `.msg` definitions registered (e.g. via
        `get_typestore(ros_distro)` plus `Typestore.register(...)`, or the `Typestore`
        that `ROSSequenceExtractor` builds internally from `ROSExtractorConfig.ros_distro`/
        `custom_msgs`), needed when a topic's `_ros_.msgdef` isn't available to auto-register
        the type (e.g. the sequence wasn't ingested from a ROS bag in the first place).

        Example:
            ```python
            from rosbags.typesys import Stores
            from mosaicolabs import MosaicoClient
            from mosaicolabs.bridges.ros import MosaicoToROSLoader

            with MosaicoClient.connect("localhost", 6726) as client:
                # Stream only IMU and GPS topics back out of a sequence
                with MosaicoToROSLoader(
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
            topics (Optional[List[str]]): Optional topic-name filter patterns (glob-style, ``!``-prefixed for
                exclusions). ``None`` loads all topics.
            start_timestamp_ns (Optional[int]): Lower bound for the time window (nanoseconds). Clipped
                to the sequence minimum if out of range.
            end_timestamp_ns (Optional[int]): Upper bound for the time window (nanoseconds). Clipped to
                the sequence maximum if out of range.
        """

        super().__init__(
            m_client, sequence_name, topics, start_timestamp_ns, end_timestamp_ns
        )

        self._typestore: Typestore = (
            typestore_or_distro
            if isinstance(typestore_or_distro, Typestore)
            else get_typestore(typestore_or_distro)
        )
        """The ROS typestore containing the registered ROS messages. Used for adapter resolution."""

    def _register_msgtype(self, msgtype: str, msgdef: Optional[str]):
        """Registers ``msgtype`` in the typestore using ``msgdef``, unless already present."""
        if msgtype in self._typestore.types:
            return

        if msgdef is None:
            logger.warning(f"Failed registering {msgtype}: missing msgdef.")
            return

        add_types = get_types_from_msg(msgdef, msgtype)
        self._typestore.register(add_types)

    def _resolve_topic(
        self, t_handler: TopicHandler
    ) -> TopicResolution[ROSAdapterBase]:
        """
        Resolves a topic's Mosaico adapter and the ROS msgtype to write it back out as.

        Three resolution strategies are tried in order, the first to succeed wins:

        1. :meth:`_adapter_from_metadata_msgtype` - hand-written adapter keyed by the
           ``msgtype`` recorded in the topic's ``_ros_`` metadata.
        2. :meth:`_adapter_from_ontology_tag` - hand-written adapter registered as the
           default for the topic's ontology tag (schema-fingerprint checked).
        3. :meth:`_create_unmodeled_adapter` - fallback that synthesizes an
           ``UnmodeledAdapter``. This always succeeds, provided ``msgtype`` is known.

        Once resolved, the msgtype is registered in the typestore if it isn't already
        present, and then required to be there — a type that can be neither recovered
        from ``_ros_.msgdef`` nor supplied by the caller cannot be serialized, so the
        topic is rejected rather than silently mistranslated.

        Args:
            t_handler (TopicHandler): The topic handler whose adapter should be resolved.

        Returns:
            TopicResolution[ROSAdapterBase]: The resolved ``(adapter, msgtype)`` pair, or a
                rejection carrying one of:

                * ``ROSTopicStatus.MALFORMED_METADATA`` - the ``_ros_`` block holds a field
                  of an unexpected type.
                * ``CommonTopicStatus.UNRESOLVED_ADAPTER`` - no strategy produced an adapter,
                  because ``msgtype`` is unknown and the ontology is not adapted.
                * ``ROSTopicStatus.NOT_IN_TYPESTORE`` - the msgtype is still absent from the
                  typestore after registration was attempted.
        """

        # Read and validate the `_ros_` block once, here, rather than in each strategy.
        try:
            ros_metadata = _extract_ros_metadata(t_handler)
        except TypeError as e:
            return TopicResolution.rejected(ROSTopicStatus.MALFORMED_METADATA, str(e))

        factory_result = (
            self._adapter_from_metadata_msgtype(ros_metadata)
            or self._adapter_from_ontology_tag(t_handler)
            or self._create_unmodeled_adapter(t_handler, ros_metadata)
        )

        if factory_result is None:
            return TopicResolution.rejected(
                CommonTopicStatus.UNRESOLVED_ADAPTER,
                f"Unable to infer an adapter for ontology '{t_handler.ontology_tag}'.",
            )

        adapter, resolved_rosmsg_type = factory_result

        # Register type within typestore (no-op if already registered)
        self._register_msgtype(resolved_rosmsg_type, ros_metadata.get("msgdef"))

        if self._typestore.types.get(resolved_rosmsg_type) is None:
            return TopicResolution.rejected(
                ROSTopicStatus.NOT_IN_TYPESTORE,
                f"'{resolved_rosmsg_type}' is not present in the ROS typestore.",
            )

        return TopicResolution.accepted(adapter, resolved_rosmsg_type)

    def _adapter_from_metadata_msgtype(
        self, ros_metadata: Dict[str, Any]
    ) -> Optional[Tuple[type[ROSAdapterBase], str]]:
        """
        Strategy 1: look up a hand-written adapter using the ``msgtype`` recorded
        in the topic's ``_ros_`` metadata.

        Args:
            ros_metadata (Dict[str, Any]): The topic's already-extracted ``_ros_`` block.

        Returns:
            Optional[Tuple[type[ROSAdapterBase], str]]: The ``(adapter, msgtype)`` pair if a hand-written
                adapter is registered for ``msgtype``, otherwise ``None``.
        """

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
        self, t_handler: TopicHandler, ros_metadata: Dict[str, Any]
    ) -> Optional[Tuple[type[UnmodeledAdapter], str]]:
        """
        Strategy 3 (fallback): synthesize an ``UnmodeledAdapter`` for the topic's
        ontology when no hand-written adapter could be resolved. Always succeeds,
        provided ``msgtype`` is known.

        Args:
            t_handler (TopicHandler): The topic handler used to build the unmodeled ontology
                (from its ontology tag, Arrow schema, and serialization format).
            ros_metadata (Dict[str, Any]): The topic's already-extracted ``_ros_`` block,
                read for the ``msgtype`` to key the adapter on.

        Returns:
            Optional[Tuple[type[UnmodeledAdapter], str]]: The ``(adapter, msgtype)`` pair,
                or ``None`` if the ``_ros_`` block carries no ``msgtype``.
        """

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
