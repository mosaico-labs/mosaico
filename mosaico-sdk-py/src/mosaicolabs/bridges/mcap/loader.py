import json
from enum import Enum
from pathlib import Path
from typing import ClassVar, Dict, Generator, List, Optional, Tuple, Union

from google.protobuf.descriptor_pb2 import (
    FileDescriptorSet,
)
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.json_format import MessageToDict
from mcap.reader import McapReader
from mcap.records import Channel, Schema
from mcap.summary import Summary
from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory

from mosaicolabs.bridges.mcap.adapters.unmodeled import UnmodeledAdapter
from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.logging_config import get_logger
from mosaicolabs.models.core.helpers import resolve_ontology_class

from ..loader_base import BaseLoader
from ..protocols.mcap.registry import McapSchemaRegistry
from .adapter_base import MCAPAdapterBase
from .bridge import MCAPBridge

# from mcap_ros1.decoder import DecoderFactory as Ros1DecoderFactory
# from mcap_ros2.decoder import DecoderFactory as Ros2DecoderFactory
from .helpers import _class_name_from_mcap_channel, _filter_channels_from_dict
from .mcap_file import MCAPFile
from .mcap_message import MCAPMessage

# Set the hierarchical logger
logger = get_logger(__name__)


class TopicStatus(Enum):
    """
    Defines the possible topic status (ACCEPTED/REJECTED). In case of rejection, a more informative enum if provided.

    Attributes:
        ACCEPTED: Enum specifying the Topic has been accepted.
        FILTERED: Enum specifying the Topic has been rejected since user provided a filter that excludes the topic.
        UNRESOLVED_ADAPTED: Enum specifying the Topic has been rejected since it has no Mosaico adapter.
        MALFORMED_METADATA: Enum specifying the Topic has been rejected since its ``_ros_`` metadata is malformed.
    """

    ACCEPTED = "Accepted"
    """ Status indicating an accepted Topic """

    FILTERED = "Filtered"
    """ Status indicating topic has been rejected by user specified filter """

    UNRESOLVED_ADAPTED = "Unresolved adapted"
    """ Status indicating the Topic has been rejected since no Mosaico adapter could be resolved """

    MALFORMED_METADATA = "Malformed metadata"
    """ Status indicating the Topic has been rejected since its '_ros_' metadata is malformed """

    def display_color(self) -> str:
        """Returns the Rich color string used to render this status in the progress UI."""
        _colors = {
            TopicStatus.ACCEPTED: "bright_green",
            TopicStatus.FILTERED: "bright_yellow",
            TopicStatus.UNRESOLVED_ADAPTED: "dark_orange",
            TopicStatus.MALFORMED_METADATA: "red1",
        }
        return _colors.get(self, "bright_red")


class _MCAPLoader(BaseLoader[MCAPAdapterBase]):
    """
    Base MCAP loader for reading and deserializing MCAP files.

    This is the MCAP equivalent of `ROSLoader`. It is declared as an internal, non-instantiable
    middle class rather than a concrete loader because how a raw record is turned into an
    `MCAPMessage.data_field` dictionary depends on the **encoding of the MCAP Channel** being
    read, not just on the file itself: a single mcap file can (in principle) mix channels
    encoded as `protobuf`, `jsonschema`, or other encodings, each requiring a different
    decoding path (e.g. protobuf needs a populated `DescriptorPool` and `MessageToDict`,
    while jsonschema only needs `json.loads`). `_MCAPLoader` therefore implements everything
    that is encoding-agnostic (channel resolution/filtering, adapter resolution, message
    counting, duration, resource lifecycle), while each concrete subclass —
    `MCAPLoaderProtobuf` and `MCAPLoaderJsonschema` — only implements the encoding-specific
    `__iter__` streaming loop (and, for protobuf, the extra descriptor-pool bookkeeping it needs).

    The `_MCAPLoader` acts as a resource manager that abstracts the underlying `mcap` library.
    It provides a standardized Pythonic interface for filtering topics and streaming data
    into the Mosaico adaptation pipeline.

    ### Key Features
    * **Multi-Format Support**: Automatically detects and handles different encoded messages (protobuf, jsonschema, ...).
    * **Semantic Filtering**: Supports glob-style patterns (e.g., `/sensors/*`, `*camera_info`) to include relevant data channels,
        with `!`-prefixed patterns for exclusion (e.g., `!sensors.debug*`). Patterns are evaluated in ORDER (gitignore-like semantics).
    * **Configurable Serialization**: Non-adapted message types can be assigned a specific
        [`SerializationFormat`][mosaicolabs.enum.serialization_format.SerializationFormat] via `serialization_formats`,
        overriding the `SerializationFormat.Default` used otherwise.
    * **Memory Efficient**: Implements a generator-based iteration pattern to process large bags without loading them into RAM.
    """

    def __init__(
        self,
        file_path: Union[str, Path],
        channels: Optional[Union[str, List[str]]] = None,
        serialization_formats: Optional[Dict[str, SerializationFormat]] = None,
    ):
        """
        Initializes the _MCAPLoader.

        Example:
            ```python
            from mosaicolabs.enum.serialization_format import SerializationFormat
            from mosaicolabs.bridges.mcap import MCAPLoaderProtobuf

            # Initialize to read only IMU and GPS data from an MCAP file
            with MCAPLoaderProtobuf(
                file_path="mission_01.mcap",
                channels=["/imu*", "/gps/fix"],
                # Non-adapted (Unmodeled) messages of this type will be
                # serialized as Ragged instead of the Default format
                serialization_formats={
                    "/sensors/custom_point_cloud": SerializationFormat.Ragged,
                },
            ) as loader:
                for msg, exc in loader:
                    if not exc:
                        print(f"Read {msg.schema_name} from {msg.channel_name}")
            ```

        Args:
            file_path (Union[str, Path]): Path to the mcap file or directory.
            channels (Optional[Union[str, List[str]]]): A single channel name, a list of names, or glob patterns. Patterns are evaluated in ORDER (gitignore-like semantics).
                If None, all available topics are loaded.
            serialization_formats (Optional[Dict[str, SerializationFormat]]): Maps a MCAP message channel name
                (e.g. `sensor_msgs.CustomPointCloud2`) to the [`SerializationFormat`][mosaicolabs.enum.serialization_format.SerializationFormat]
                used when synthesizing an [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled]
                ontology for that type. Only applies to topics that have **no** hand-written Mosaico
                adapter. Message types not present in this mapping default to `SerializationFormat.Default`.
        """

        super().__init__(
            container_type=dict[str, Channel]
        )  # Initialize the base class to set up topic resolution state
        self._file_path = Path(file_path)
        """The path to the mcap file or directory."""

        # Configuration
        self._requested_channels = [channels] if isinstance(channels, str) else channels
        """The user-specified channel filter(s) to apply when resolving channels."""

        self._serialization_formats: Dict[str, SerializationFormat] = (
            serialization_formats or {}
        )
        """Mapping of MCAP message types to their desired serialization format for Unmodeled ontologies."""

        # State
        self._reader: Optional[McapReader] = None
        """The underlying `mcap` reader instance, lazily initialized."""
        self._mcap_file: Optional[MCAPFile] = None

    def _resolve_channels(self):
        """
        Lazily opens the bag file and resolves requested channel patterns.

        This method performs "Smart Filtering" by matching requested glob patterns against
        the actual channels available in the mcap file. It populates the
        internal `_connections` list used for optimized iteration.
        """
        if self._reader is not None:
            return None

        self._mcap_file = MCAPFile(self._file_path, [ProtobufDecoderFactory()])
        self._reader = self._mcap_file.reader

        mcap_summary = self._get_mcap_summary(self._mcap_file)

        self._resolved_topics = {
            channel.topic: channel
            for channel_id, channel in mcap_summary.channels.items()
        }

        matched_channels = _filter_channels_from_dict(
            self._resolved_topics, self._requested_channels
        )

        # Filter channels
        for channel_id, channel in self._resolved_topics.items():
            matched_channel = matched_channels.get(channel.topic)

            # 1) Filter by requested topic
            if matched_channel is None:
                logger.info(
                    f"Skipping channel {channel.topic}: not matching the provided filter."
                )

                self._filtered_topics.update({channel.topic: channel})
                continue

            # 2) Filter topics that cannot resolve neither a registered Mosaico-adapter nor an Unmodeled one because no PyArrow schema can be derived
            schema = mcap_summary.schemas.get(channel.schema_id)

            if schema is None:
                logger.warning(
                    f"{channel.topic} channel with {channel.schema_id} schema_id cannot be found among all schema ids.\
                      Available schema ids are {[id for id in mcap_summary.schemas.keys()]}"
                )
                continue

            adapter = self._get_or_create_adapter(schema, channel)

            if adapter:
                self._accepted_topics.update({channel.topic: channel})
            else:
                logger.warning(
                    f"Channel {channel.topic}: unresolved Adapted for mcap type {(schema.name, schema.encoding)}. Did you forget to register it?"
                )
                self._unresolved_adapter_topics.update({channel.topic: channel})
                continue

            # Adapter found, add it the the cache and add connection
            self._topic_cached_adapters[channel.topic] = adapter

        if not self._accepted_topics:
            raise RuntimeError(
                "Unable to initialize _MCAPLoader: No connections matched criteria. Try checking the channel filter, if any."
            )

        return self._reader

    def _get_or_create_adapter(
        self, schema: Schema, channel: Channel
    ) -> Optional[type[MCAPAdapterBase]]:
        """
        Resolves the Mosaico adapter for a channel, creating an ad-hoc one if none exists.

        It proceeds in three steps:

        1. **Bail out early**: if ``channel.schema_id`` does not match ``schema.id``
           (i.e. the caller passed a mismatched schema/channel pair), no adapter can
           be safely resolved, so ``None`` is returned immediately.
        2. **Look up a known adapter**: :meth:`MCAPBridge.get_default_adapter` is queried
           for a hand-written adapter registered for this exact ``(schema.name, schema.encoding)``
           pair (e.g. `sensor_msgs.Imu` + `protobuf` -> `IMUAdapter`). If one is found,
           it is returned as-is and no further work is needed.
        3. **Fall back to an [`UnmodeledAdapter`][mosaicolabs.bridges.mcap.adapters.unmodeled.UnmodeledAdapter]**:
           when no hand-written adapter exists, one is synthesized on the fly so the
           channel can still be loaded generically, without a semantic ontology mapping:

            a. The encoding-specific converter is looked up via
               [`McapSchemaRegistry.get_converter`][mosaicolabs.bridges.protocols.mcap.registry.McapSchemaRegistry.get_converter]
               using ``schema.encoding``. ``None`` is returned if the encoding has no
               registered converter (e.g. an encoding the SDK does not yet support).
            b. The raw schema definition (``schema.data``) is converted into an
               equivalent PyArrow schema via the converter's ``to_pyarrow``. ``None`` is
               returned if no PyArrow schema could be derived (e.g. an empty/malformed
               schema definition).
            c. An [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled] ontology
               class is obtained/created for this schema via
               [`resolve_ontology_class`][mosaicolabs.models.core.helpers.resolve_ontology_class],
               tagged with an ontology tag derived from the channel's topic (its last
               `.`-separated segment, e.g. `sensors.imu` -> `imu`). The serialization
               format used for this ontology is looked up in ``self._serialization_formats``
               by ``channel.topic``, falling back to ``SerializationFormat.Default``
               when the ``channel.topic`` has no entry there.
            d. [`UnmodeledAdapter.get_or_create`][mosaicolabs.bridges.mcap.adapters.unmodeled.UnmodeledAdapter.get_or_create]
               returns a cached adapter class for that ontology if one was already
               synthesized for an equivalent channel, or builds and registers a new one
               otherwise, so repeated channels of the same unmodeled type reuse a single
               adapter class rather than creating a new one every time.

        Args:
            schema (Schema): The MCAP schema record (name, encoding, raw definition) for
                the channel an adapter must be resolved for.
            channel (Channel): The MCAP channel record (topic, schema_id, ...) for which
                an adapter must be resolved.

        Returns:
            Optional[Type[MCAPAdapterBase]]: The resolved adapter class, or ``None`` if
                ``channel.schema_id`` doesn't match ``schema.id``, the schema's encoding
                has no registered converter, or no PyArrow schema could be derived from it.
        """

        if channel.schema_id != schema.id:
            logger.warning(
                f"Schema id mismatch error: {channel.topic} with id {channel.schema_id} \
                  {schema.name} name cannot be used together with {schema.name} schema \
                  with {schema.id} id"
            )
            return None

        # Check if adapter already exists. If yes, return immediately
        adapter = MCAPBridge.get_default_adapter(schema.name, schema.encoding)

        if adapter:
            return adapter

        # If adapter does not exist, create a new one through pyarrow schema deduced from msgdef
        schema_converter = McapSchemaRegistry.get_converter(schema.encoding)

        if schema_converter is None:
            logger.warning(
                f"{channel.topic} contains a message with {schema.name} name \
                  and {schema.encoding} encoding schema that is not supported. \
                  Supported encodings are {[supp_enc for supp_enc in McapSchemaRegistry._registry.keys()]}"
            )
            return None

        pyarrow_schema = (
            schema_converter.to_pyarrow(schema) if schema_converter else None
        )

        if not pyarrow_schema:
            logger.warning(
                f"Topic {channel.topic} does not contain any message \
                  definition and cannot be turned as an Unmodeled"
            )
            return None

        logger.info(
            f"Channel {channel.topic} adapter cannot be found, therefore an UnmodeledAdapter will be created."
        )

        # Create the ontology, honoring any user-configured serialization format for this msgtype
        serialization_format = self._serialization_formats.get(
            channel.topic, SerializationFormat.Default
        )
        unmodeled_ontology = resolve_ontology_class(
            ontology_tag=_class_name_from_mcap_channel(channel),
            schema=pyarrow_schema,
            serialization_format=serialization_format,
        )

        # Get the unmodeled adapter or create a new one
        adapter = UnmodeledAdapter.get_or_create(
            # This will make a new class or reuse an already registered one
            ontology_type=unmodeled_ontology,
            schema_name=schema.name,
            schema_encoding=schema.encoding,
        )

        return adapter

    def _get_mcap_summary(self, mcap_file: MCAPFile) -> Summary:
        """
        Reads and returns the mcap file's summary section (schemas, channels, statistics).

        Args:
            mcap_file (MCAPFile): The already-opened mcap file to read the summary from.

        Returns:
            Summary: The mcap file's summary, containing its `schemas`, `channels`, and
                `statistics`.

        Raises:
            RuntimeError: If the mcap file has no summary section (e.g. it was written by
                a non-seeking/streaming writer that omitted one).
        """

        mcap_summary = mcap_file.reader.get_summary()

        if mcap_summary is None:
            raise RuntimeError(
                f"{self._file_path} file does not contain Summary information. \
                                Failed to resolve channels"
            )

        return mcap_summary

    def _ensure_resolved(self) -> None:
        """Lazily opens the mcap file and resolves topics (see `_resolve_channels`)."""
        self._resolve_channels()

    # --- Properties ---

    def msg_count(self, channel_name: Optional[str] = None) -> int:
        """
        Returns the total number of messages to be processed based on active filters.

        Args:
            channel (Optional[str]): If provided, returns the count for that specific channel, even if filtered or unresolved adapted.
                If None, returns the aggregate count for all accepted channels.

        Returns:
            int: The total message count.
        """

        self._resolve_channels()

        if self._mcap_file is None:
            logger.error(
                f"MCAP at {self._file_path} has not been initialised. Impossibile to compute the message count"
            )
            return 0

        mcap_statistics = self._get_mcap_summary(self._mcap_file).statistics

        if mcap_statistics is None:
            logger.error(
                f"Cannot compute message count for MCAP at {self._file_path}. \
                  Statistics are not present"
            )
            return 0

        if not channel_name:  # returns the sum of all accepted channels
            acccepted_channel_ids: List[int] = [
                channel.id for channel in self._accepted_topics.values()
            ]

            return sum(
                mcap_statistics.channel_message_counts.get(channel_id) or 0
                for channel_id in acccepted_channel_ids
            )

        channel: Optional[Channel] = self._resolved_topics.get(channel_name)

        if channel is None:
            logger.error(
                f"Channel '{channel_name}' not found. \
                  Accepted channels are: {[channel_name for channel_name in self._accepted_topics.keys()]}."
            )
            return 0

        return mcap_statistics.channel_message_counts[channel.id]

    @property
    def duration(self) -> int:
        """
        Returns the duration of the mcap file in nanoseconds.

        Returns:
            int: The duration of the mcap file in nanoseconds.
        """
        self._resolve_channels()

        if self._mcap_file is None:
            raise ValueError(
                f"MCAP at {self._file_path} has not been initialised. \
                  Impossibile to compute the message count"
            )

        mcap_statistics = self._get_mcap_summary(self._mcap_file).statistics

        if mcap_statistics is None:
            raise ValueError(
                f"Cannot compute file duration for MCAP at {self._file_path}. \
                                  Statistics are not present"
            )

        return mcap_statistics.message_end_time - mcap_statistics.message_start_time

    @property
    def channel_types(self) -> List[Tuple[str, str]]:
        """
        Retrieves the list of MCAP channel types corresponding to the accepted topics.

        Each entry in this list represents the channel name and channel encoding
        (sensor_msgs.Image, protobuf) required to correctly deserialize the messages
        for the channels returned by the `.topics` property.

        Returns:
            List[str]: A list of MCAP channel type strings in the same order
                as the resolved channels.
        """
        self._resolve_channels()
        return [
            (channel.topic, channel.message_encoding)
            for channel in self._accepted_topics.values()
        ]

    # --- Core Logic ---

    def close(self):
        """
        Explicitly closes the mcap file and releases system resources.
        """
        if self._mcap_file:
            self._mcap_file.close()
            self._reader = None

    def __enter__(self):
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensures resources are released even if an error occurs in the `with` block."""
        self.close()


class MCAPLoaderProtobuf(_MCAPLoader):
    SUPPORTED_ENCODINGS: ClassVar[Tuple[str, ...]] = ("protobuf",)

    def __init__(
        self,
        file_path: Union[str, Path],
        channels: Optional[Union[str, List[str]]] = None,
        serialization_formats: Optional[Dict[str, SerializationFormat]] = None,
    ):
        super().__init__(file_path, channels, serialization_formats)
        self.descriptor_pool: DescriptorPool = DescriptorPool()

    def _resolve_channels(self):
        """Resolves channels via the base class, then registers every schema's protobuf
        `FileDescriptorSet` into `self.descriptor_pool` so messages can be decoded during iteration."""
        super()._resolve_channels()

        if (
            self._mcap_file is None
        ):  # Here _mcap_file cannot be None. Used just to silence IDE
            raise RuntimeError(
                f"MCAP at {self._file_path} has not been initialised! \
                  Impossible to resolve channels through {MCAPLoaderProtobuf.__name__}"
            )

        mcap_summary = self._get_mcap_summary(self._mcap_file)

        for schema in mcap_summary.schemas.values():
            self.register_schemas_to_pool(schema)

    def register_schemas_to_pool(self, schema: Schema):
        """Registers MCAP file contained schemas to DescriptorPool used to turn Protobuf classes to Dictionaries"""
        try:
            msgtype = schema.name
            self.descriptor_pool.FindMessageTypeByName(msgtype)
        except KeyError:
            fds_bytes = schema.data
            file_proto = FileDescriptorSet.FromString(fds_bytes).file
            for file_proto in FileDescriptorSet.FromString(fds_bytes).file:
                self.descriptor_pool.Add(file_proto)

    # --- Core Logic ---

    def __iter__(
        self,
    ) -> Generator[Tuple[MCAPMessage, Optional[Exception]], None, None]:
        """
        The primary data streaming loop for protobuf messages.

        Yields:
            A tuple of (MCAPMessage, Exception). If deserialization succeeds, Exception is None.
        """

        self._resolve_channels()

        if (
            not self._accepted_topics or not self._reader
        ):  # just for remove IDE errors on reader usage
            return

        for decoded_message in self._reader.iter_decoded_messages(
            topics=[topic for topic in self._accepted_topics.keys()],
            start_time=None,
            end_time=None,
        ):
            channel_name = decoded_message.channel.topic
            log_time_ns = decoded_message.message.log_time
            publish_time = decoded_message.message.publish_time

            try:
                if decoded_message.schema is None:
                    raise RuntimeError(
                        f"Impossible to read schema from channel: {channel_name} since it is not present"
                    )
                schema_name = decoded_message.schema.name
                schema_encoding = decoded_message.schema.encoding

                if schema_encoding not in self.SUPPORTED_ENCODINGS:
                    raise ValueError(
                        f"{MCAPLoaderProtobuf.__name__} cannot decode a message with `{schema_encoding}` encoding. \
                          Supported encodings: {[enc for enc in self.SUPPORTED_ENCODINGS]}"
                    )

                # Create dictionary from decoded message Python Object
                data_dict = MessageToDict(
                    decoded_message.decoded_message,
                    always_print_fields_with_no_presence=True,
                    preserving_proto_field_name=True,
                    use_integers_for_enums=True,
                    descriptor_pool=self.descriptor_pool,
                )

                # Yield the standard SDK message
                yield (
                    MCAPMessage(
                        channel_name=channel_name,
                        schema_name=schema_name,
                        schema_encoding=schema_encoding,
                        data=data_dict,
                        log_time_ns=log_time_ns,
                        publish_time_ns=publish_time,
                    ),
                    None,
                )

            except Exception as e:
                yield (
                    MCAPMessage(
                        channel_name=channel_name,
                        schema_name=None,
                        schema_encoding=None,
                        data=None,
                        log_time_ns=log_time_ns,
                        publish_time_ns=publish_time,
                    ),
                    e,
                )


class MCAPLoaderJsonschema(_MCAPLoader):
    SUPPORTED_ENCODINGS: ClassVar[Tuple[str, ...]] = ("jsonschema", "json")

    # --- Core Logic ---

    def __iter__(
        self,
    ) -> Generator[Tuple[MCAPMessage, Optional[Exception]], None, None]:
        """
        The primary data streaming loop for jsonschema encoding.

        Yields:
            A tuple of (MCAPMessage, Exception). If deserialization succeeds, Exception is None.
        """

        self._resolve_channels()

        if (
            not self._accepted_topics or not self._reader
        ):  # just for remove IDE errors on reader usage
            return

        for schema, channel, message in self._reader.iter_messages(
            topics=[topic for topic in self._accepted_topics.keys()],
            start_time=None,
            end_time=None,
        ):
            try:
                if schema is None:
                    raise RuntimeError(
                        f"Impossible to read schema from channel: {channel.topic} since it is not present"
                    )
                schema_name = schema.name
                schema_encoding = schema.encoding

                if schema_encoding not in self.SUPPORTED_ENCODINGS:
                    raise ValueError(
                        f"{MCAPLoaderJsonschema.__name__} cannot decode a message with `{schema_encoding}` encoding. \
                          Supported encodings: {[enc for enc in self.SUPPORTED_ENCODINGS]}"
                    )

                # Create dictionary from decoded message Python Object
                data_dict = json.loads(message.data)

                # Yield the standard SDK message
                yield (
                    MCAPMessage(
                        channel_name=channel.topic,
                        schema_name=schema_name,
                        schema_encoding=schema_encoding,
                        data=data_dict,
                        log_time_ns=message.log_time,
                        publish_time_ns=message.publish_time,
                    ),
                    None,
                )

            except Exception as e:
                yield (
                    MCAPMessage(
                        channel_name=channel.topic,
                        schema_name=None,
                        schema_encoding=None,
                        data=None,
                        log_time_ns=message.log_time,
                        publish_time_ns=message.publish_time,
                    ),
                    e,
                )
