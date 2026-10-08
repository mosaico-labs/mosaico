from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple, Union

from mcap.reader import McapReader
from mcap.records import Channel, Schema, Statistics
from mcap.summary import Summary

from mosaicolabs import MosaicoClient, TopicHandler
from mosaicolabs.bridges.mcap.adapters.unmodeled import UnmodeledAdapter
from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.logging_config import get_logger
from mosaicolabs.models.core.helpers import resolve_ontology_class

from ..loader_base import BaseLoader, MosaicoLoader, TopicResolution
from ..protocols.mcap.registry import McapSchemaRegistry
from ..topic_status import TopicStatus
from .adapter_base import MCAPAdapterBase, MCAPSchemaMetadata
from .bridge import MCAPBridge
from .decoders.decoder_base import MCAPMsgDecoderBase
from .decoders.registry import DecoderRegistry
from .helpers import (
    _class_name_from_mcap_schema,
    _extract_mcap_metadata,
    _filter_channels_from_dict,
)
from .mcap_file import MCAPFileReader
from .mcap_message import MCAPMessage

# Set the hierarchical logger
logger = get_logger(__name__)


class MCAPLoader(BaseLoader[MCAPAdapterBase]):
    """
    MCAP loader for reading and deserializing MCAP files.

    This is the MCAP equivalent of `ROSLoader`. It acts as a resource manager that abstracts
    the underlying `mcap` library, providing a standardized Pythonic interface for filtering
    topics and streaming data into the Mosaico adaptation pipeline.

    A single mcap file can (in principle) mix channels encoded as `protobuf`, `json`, or other
    encodings, each requiring a different decoding path (e.g. protobuf needs a populated
    `DescriptorPool` and `MessageToDict`, while json only needs `json.loads`). This
    encoding-specific behavior is delegated to `MCAPMsgDecoderBase` instances: the class for a
    given encoding is looked up via `DecoderRegistry.get_decoder(channel.message_encoding)`,
    and the resulting instance is then cached per `channel.topic` (see `_get_decoder`), so
    each channel keeps its own stateful decoder (e.g. `MCAPProtobufMsgDecoder`'s
    `DescriptorPool` and field rules) from `_resolve_channels()` to `__iter__()`.
    `MCAPLoader` itself implements everything that is encoding-agnostic
    (channel resolution/filtering, adapter resolution, message counting, duration, resource
    lifecycle, and the single streaming loop that dispatches each message to its decoder).
    Support for a new encoding is added entirely within the `decoders/` package (a new
    `MCAPMsgDecoderBase` subclass decorated with `@register_decoder`, following the existing
    `decoders/protobuf/` and `decoders/jsonschema/` subpackages), with no changes needed here.

    ### Key Features
    * **Multi-Format Support**: Automatically detects and handles different encoded messages (protobuf, json, ...).
    * **Semantic Filtering**: Supports glob-style patterns (e.g., `/sensors/*`, `*camera_info`) to include relevant data channels,
        with `!`-prefixed patterns for exclusion (e.g., `!sensors.debug*`). Patterns are evaluated in ORDER (gitignore-like semantics).
    * **Configurable Serialization**: Non-adapted channels can be assigned a specific
        [`SerializationFormat`][mosaicolabs.enum.serialization_format.SerializationFormat] via `serialization_formats`,
        overriding the `SerializationFormat.Default` used otherwise.
    * **Memory Efficient**: Implements a generator-based iteration pattern to process large MCAPs without loading them into RAM.
    """

    def __init__(
        self,
        file_path: Union[str, Path],
        channels: Optional[Union[str, List[str]]] = None,
        serialization_formats: Optional[Dict[str, SerializationFormat]] = None,
    ):
        """
        Initializes the MCAPLoader.

        Example:
            ```python
            from mosaicolabs.enum.serialization_format import SerializationFormat
            from mosaicolabs.bridges.mcap import MCAPLoader

            # Initialize to read only IMU and GPS data from an MCAP file
            with MCAPLoader(
                file_path="mission_01.mcap",
                channels=["/imu*", "/gps/fix"],
                # Non-adapted (Unmodeled) messages of this channel will be
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
            serialization_formats (Optional[Dict[str, SerializationFormat]]): Maps an original MCAP channel name
                (e.g. `sensor_msgs.CustomPointCloud2`, as found in the mcap file) to the [`SerializationFormat`][mosaicolabs.enum.serialization_format.SerializationFormat]
                used when synthesizing an [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled]
                ontology for that channel. Only applies to channels that have **no** hand-written Mosaico
                adapter. Channels not present in this mapping default to `SerializationFormat.Default`.
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
        """Mapping of original MCAP channel names to their desired serialization format for Unmodeled ontologies."""

        # State
        self._reader: Optional[McapReader] = None
        """The underlying `mcap` reader instance, lazily initialized."""
        self._mcap_file: Optional[MCAPFileReader] = None
        """Handler for the mcap file, lazily initialized."""
        self._mcap_summary: Optional[Summary] = None
        """Summary of the mcap file, lazily initialized."""
        self._mcap_statistics: Optional[Statistics] = None
        """Statistics of the mcap file, lazily initialized."""

        self._decoder_cache: Dict[str, MCAPMsgDecoderBase] = {}
        """`MCAPMsgDecoderBase` instances resolved so far, keyed by `channel.topic` and
        scoped to this loader. Lazily populated by `_get_decoder()`."""

        self._schema_def_str_cache: Dict[int, str] = {}
        """Stringified schema definitions resolved so far, keyed by `schema.id`. Lazily
        populated by `get_or_create_schema_def_str()`."""

    def _get_decoder(self, channel: Channel) -> Optional[MCAPMsgDecoderBase]:
        """
        Returns the `MCAPMsgDecoderBase` associated to `channel.topic` if available, or instantiating it from
        `DecoderRegistry` and caching it on first use.

        Caching matters because a decoder is stateful (e.g. `MCAPProtobufMsgDecoder`'s
        `DescriptorPool` and field rules, built when the channel's schema is registered during
        `_resolve_channels()`): the same instance must still be the one `__iter__()` later
        calls `decode()` on, or that state would be lost.

        Args:
            channel (Channel): A channel coming from opening a MCAP file using the mcap library.

        Returns:
            Optional[MCAPMsgDecoderBase]: This loader's decoder instance for `channel.topic`, or `None`
                if no decoder is registered for its encoding.
        """
        decoder = self._decoder_cache.get(channel.topic)
        if decoder is None:
            decoder_cls = DecoderRegistry.get_decoder(channel.message_encoding)
            if decoder_cls is None:
                return None
            decoder = decoder_cls()
            self._decoder_cache[channel.topic] = decoder
        return decoder

    def get_or_create_schema_def_str(self, schema: Schema) -> str:
        """
        Returns the stringified definition of `schema` (as produced by the resolved
        `McapSchemaConverter.stringify_schema_def()`) if already cached, or creating it and
        caching it by `schema.id` on first use.

        Caching matters because the definition is the same for every message of a schema:
        stringifying it (e.g. base64-encoding a protobuf `FileDescriptorSet`) once per schema
        avoids repeating that work for every message `__iter__()` yields.

        Args:
            schema (Schema): The MCAP schema record whose definition should be stringified.

        Returns:
            str: The stringified schema definition.

        Raises:
            ValueError: If no `McapSchemaConverter` is registered for `schema.encoding`.
        """
        schema_def_str = self._schema_def_str_cache.get(schema.id)

        if schema_def_str is None:
            schema_converter = McapSchemaRegistry.get_converter(schema.encoding)
            if schema_converter is None:
                supported = [
                    enc for enc in McapSchemaRegistry.all_supported_encodings()
                ]
                raise ValueError(
                    f"{type(self).__name__} cannot stringify a message with `{schema.encoding}` encoding. \
                        Supported encodings: {supported}"
                )

            schema_def_str = schema_converter.stringify_schema_def(schema.data)
            self._schema_def_str_cache[schema.id] = schema_def_str

        return schema_def_str

    def _resolve_channels(self) -> McapReader:
        """
        Lazily resolves requested channel patterns against the mcap file's channels.

        This method performs "Smart Filtering" by matching requested glob patterns against
        the actual channels available in the mcap file (read from its summary, see
        `_resolve_summary`).
        It populates the internal `_accepted_topics` dict used for optimized iteration.

        Returns:
            McapReader: The reader of the underlying mcap file, used to stream the
                accepted channels' messages.

        Raises:
            RuntimeError: If the mcap file has no summary section, or if no channel
                matched the filter and resolved a decoder and an adapter.
        """
        if self._reader is not None:
            return self._reader

        self._mcap_file = self._resolve_mcap_file()
        mcap_summary = self._resolve_summary()
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

                self._reject(channel.topic, TopicStatus.FILTERED)
                continue

            # 2) Reject channels that do not hold schema information
            schema = mcap_summary.schemas.get(channel.schema_id)

            if schema is None:
                logger.warning(
                    f"{channel.topic} channel with {channel.schema_id} schema_id cannot be found among all schema ids. "
                    f"Available schema ids are {[id for id in mcap_summary.schemas.keys()]}"
                )
                self._reject(channel.topic, TopicStatus.UNAVAILABLE_SCHEMA)
                continue

            # 3) Filter topics whose message encoding has no registered decoder.
            decoder = self._get_decoder(channel)
            if decoder is None:
                supported = [
                    d.supported_encoding() for d in DecoderRegistry.list_decoders()
                ]
                logger.warning(
                    f"Channel {channel.topic}: message encoding '{channel.message_encoding}' has no "
                    f"registered decoder on {type(self).__name__}. Supported: {supported}"
                )
                self._reject(channel.topic, TopicStatus.UNRESOLVED_DECODER)
                continue
            decoder.register_schema(schema)

            # 4) Filter topics that cannot resolve neither a registered Mosaico-adapter nor an Unmodeled one because no PyArrow schema can be derived.
            adapter = self._get_or_create_adapter(schema, channel)

            if adapter is None:
                logger.warning(
                    f"Channel {channel.topic}: unresolved Adapted for mcap type {(schema.name, schema.encoding)}. Did you forget to register it?"
                )
                self._reject(channel.topic, TopicStatus.UNRESOLVED_ADAPTER)
                continue

            # Adapter found, add it the the cache and add to accepted topics
            self._accepted_topics.update({channel.topic: channel})
            self._topic_cached_adapters[channel.topic] = adapter

        if not self._accepted_topics:
            raise RuntimeError(
                "Unable to initialize MCAPLoader: No connections matched criteria. Try checking the channel filter, if any."
            )

        self._reader = self._resolve_mcap_file().reader
        return self._reader

    def _resolve_mcap_file(self) -> MCAPFileReader:
        """
        Lazily creates the `MCAPFileReader` wrapper for `_file_path`, with a decoder factory for
        every decoder registered in `DecoderRegistry`.

        The file itself is not opened here: `MCAPFileReader` validates and opens it on first
        access to its `.reader`.

        Returns:
            MCAPFileReader: The wrapper around the mcap file.
        """
        if self._mcap_file is not None:
            return self._mcap_file

        self._mcap_file = MCAPFileReader(
            self._file_path,
            [
                decoder_cls().decoder_factory()
                for decoder_cls in DecoderRegistry.list_decoders()
            ],
        )

        return self._mcap_file

    def _resolve_summary(self) -> Summary:
        """
        Lazily reads and returns the mcap file's summary section (schemas, channels, statistics).

        Returns:
            Summary: The mcap file's summary, containing its `schemas`, `channels`, and
                `statistics`.

        Raises:
            RuntimeError: If the mcap file has no summary section (e.g. it was written by
                a non-seeking/streaming writer that omitted one).
        """
        if self._mcap_summary is not None:
            return self._mcap_summary

        mcap_file = self._resolve_mcap_file()

        self._mcap_summary = mcap_file.reader.get_summary()

        if self._mcap_summary is None:
            raise RuntimeError("MCAP file does not contain any summary")

        return self._mcap_summary

    def _resolve_statistics(self) -> Statistics:
        """
        Lazily reads and returns the statistics record from the mcap file's summary section.

        Returns:
            Statistics: The mcap file's statistics (per-channel message counts, message
                start/end times, ...).

        Raises:
            RuntimeError: If the mcap file has no summary section, or its summary has no
                statistics record.
        """
        if self._mcap_statistics is not None:
            return self._mcap_statistics

        mcap_summary = self._resolve_summary()

        self._mcap_statistics = mcap_summary.statistics

        if self._mcap_statistics is None:
            raise RuntimeError("MCAP file does not contain any statistics")

        return self._mcap_statistics

    def _get_or_create_adapter(
        self, schema: Schema, channel: Channel
    ) -> Optional[type[MCAPAdapterBase]]:
        """
        Resolves the Mosaico adapter for a channel, creating an ad-hoc one if none exists.

        It proceeds in three steps:

        1. **Bail out early**: if ``channel.schema_id`` does not match ``schema.id``
           (i.e. the caller passed a mismatched schema/channel pair), no adapter can
           be safely resolved, so ``None`` is returned immediately.
        2. **Look up a known adapter**: [`MCAPBridge.get_default_adapter`][mosaicolabs.bridges.mcap.bridge.MCAPBridge.get_default_adapter] is queried
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
               tagged with the last component of the schema name, without package or
               encoding (e.g. `sensor_msgs.Imu` -> `Imu`, see `_class_name_from_mcap_schema`).
               Schemas sharing that short name share the tag, and `resolve_ontology_class`
               keeps them apart as schema variants. The serialization format used for this
               ontology is looked up in ``self._serialization_formats`` by ``channel.topic``,
               falling back to ``SerializationFormat.Default`` when the ``channel.topic`` has no entry there.
            d. ``schema_converter.get_schema_class(schema.name, schema.data)`` resolves the native
               Python type the decoded message data will be wrapped in (e.g. the generated
               protobuf message class, or ``dict`` for jsonschema).
            e. [`UnmodeledAdapter.get_or_create`][mosaicolabs.bridges.mcap.adapters.unmodeled.UnmodeledAdapter.get_or_create]
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
                f"Schema id mismatch error: {channel.topic} with id {channel.schema_id} "
                f"{schema.name} name cannot be used together with {schema.name} schema "
                f"with {schema.id} id"
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
                f"{channel.topic} contains a message with {schema.name} name "
                f"and {schema.encoding} encoding schema that is not supported. "
                f"Supported encodings are {[supp_enc for supp_enc in McapSchemaRegistry._registry.keys()]}"
            )
            return None

        pyarrow_schema = (
            schema_converter.to_pyarrow(schema) if schema_converter else None
        )

        if not pyarrow_schema:
            logger.warning(
                f"Topic {channel.topic} does not contain any message "
                f"definition and cannot be turned as an Unmodeled"
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
            ontology_tag=_class_name_from_mcap_schema(schema),
            schema=pyarrow_schema,
            serialization_format=serialization_format,
        )

        mcap_class = schema_converter.get_schema_class(schema.name, schema.data)

        # Get the unmodeled adapter or create a new one
        adapter = UnmodeledAdapter.get_or_create(
            # This will make a new class or reuse an already registered one
            ontology_type=unmodeled_ontology,
            mcap_class=mcap_class,
            schema_name=schema.name,
            schema_encoding=schema.encoding,
        )

        return adapter

    def _ensure_resolved(self) -> None:
        """Lazily opens the mcap file and resolves topics (see `_resolve_channels`)."""
        self._resolve_channels()

    # --- Properties ---

    def msg_count(self, topic: Optional[str] = None) -> int:
        """
        Returns the total number of messages to be processed based on accepted channels.

        Args:
            topic (Optional[str]): If provided, returns the count for that specific channel,
                even if filtered or unresolved adapter. If None, returns the aggregate count
                for all accepted channels.

        Returns:
            int: The total message count. Returns 0 (and logs an error) if the mcap file has
                no summary or statistics, if no channel is accepted, or if `topic` is not
                a channel of the mcap file.
        """
        try:
            mcap_statistics = self._resolve_statistics()
            self._resolve_channels()
        except RuntimeError as ex:
            logger.error(
                f"Cannot compute message count for MCAP at {self._file_path} because: {ex}"
            )
            return 0

        if not topic:  # returns the sum of all accepted channels
            accepted_channel_ids: List[int] = [
                channel.id for channel in self._accepted_topics.values()
            ]

            return sum(
                mcap_statistics.channel_message_counts.get(channel_id) or 0
                for channel_id in accepted_channel_ids
            )

        channel: Optional[Channel] = self._resolved_topics.get(topic)

        if channel is None:
            logger.error(
                f"Channel '{topic}' not found. "
                f"Accepted channels are: {[topic for topic in self._accepted_topics.keys()]}."
            )
            return 0

        return mcap_statistics.channel_message_counts[channel.id]

    @property
    def duration(self) -> int:
        """
        Returns the duration of the mcap file in nanoseconds.

        The duration spans all the messages in the file, regardless of the channel filter.

        Returns:
            int: The duration of the mcap file in nanoseconds. Returns 0 (and logs an error)
                if the mcap file has no summary or statistics.
        """
        try:
            mcap_statistics = self._resolve_statistics()
        except RuntimeError as ex:
            logger.error(
                f"Cannot compute duration for MCAP at {self._file_path} because: {ex}"
            )
            return 0

        return mcap_statistics.message_end_time - mcap_statistics.message_start_time

    @property
    def channel_types(self) -> List[Tuple[str, str]]:
        """
        Retrieves the list of MCAP channel types corresponding to the accepted topics.

        Each entry in this list represents the channel name and channel encoding
        (sensor_msgs.Image, protobuf) required to correctly deserialize the messages
        for the channels returned by the `.topics` property.

        Returns:
            List[Tuple[str, str]]: A list of tuples containing each accepted MCAP channel
                name and encoding in the same order as the resolved channels. Returns an
                empty list (and logs an error) if the mcap file has no summary or no
                channel is accepted.
        """

        try:
            self._resolve_channels()
        except RuntimeError as ex:
            logger.error(
                f"Cannot deduce channel types for MCAP at {self._file_path} because: {ex}"
            )
            return []

        return [
            (channel.topic, channel.message_encoding)
            for channel in self._accepted_topics.values()
        ]

    # --- Core Logic ---

    def __iter__(
        self,
    ) -> Generator[Tuple[MCAPMessage, Optional[Exception]], None, None]:
        """
        The primary data streaming loop, dispatching each message to the `MCAPMsgDecoderBase`
        registered for its channel's message encoding.

        Yields:
            A tuple of (MCAPMessage, Exception). If deserialization succeeds, Exception is None.
        """

        reader = self._resolve_channels()

        if not self._accepted_topics:
            return

        for decoded_message in reader.iter_decoded_messages(
            topics=[topic for topic in self._accepted_topics.keys()],
            start_time=None,
            end_time=None,
        ):
            channel_name = decoded_message.channel.topic
            channel_encoding = decoded_message.channel.message_encoding
            log_time_ns = decoded_message.message.log_time
            publish_time = decoded_message.message.publish_time
            sequence_id = decoded_message.message.sequence

            try:
                if decoded_message.schema is None:
                    raise RuntimeError(
                        f"Impossible to read schema from channel: {channel_name} since it is not present"
                    )
                schema_name = decoded_message.schema.name
                schema_encoding = decoded_message.schema.encoding

                decoder = self._get_decoder(decoded_message.channel)
                if decoder is None:
                    supported = [
                        d.supported_encoding() for d in DecoderRegistry.list_decoders()
                    ]
                    raise ValueError(
                        f"{type(self).__name__} cannot decode a message with `{channel_encoding}` encoding. \
                          Supported encodings: {supported}"
                    )

                # Create dictionary from decoded message Python Object
                data_dict = decoder.decode(decoded_message)

                schema_def_str = self.get_or_create_schema_def_str(
                    decoded_message.schema
                )

                # Yield the standard SDK message
                yield (
                    MCAPMessage(
                        channel_name=channel_name,
                        channel_encoding=channel_encoding,
                        schema_name=schema_name,
                        schema_encoding=schema_encoding,
                        schema_def=schema_def_str,
                        data_field=data_dict,
                        log_time_ns=log_time_ns,
                        publish_time_ns=publish_time,
                        sequence_id=sequence_id,
                    ),
                    None,
                )

            except Exception as e:
                yield (
                    MCAPMessage(
                        channel_name=channel_name,
                        channel_encoding=channel_encoding,
                        schema_name=None,
                        schema_encoding=None,
                        schema_def=None,
                        data_field=None,
                        log_time_ns=log_time_ns,
                        publish_time_ns=publish_time,
                        sequence_id=0,
                    ),
                    e,
                )

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


class MosaicoToMCAPLoader(MosaicoLoader[MCAPAdapterBase]):
    """
    Streams messages out of a Mosaico sequence and adapts them back into MCAP messages.

    This is the MCAP specialization of [`MosaicoLoader`][mosaicolabs.bridges.loader_base.MosaicoLoader]
    and the read end of the extraction pipeline: [`MCAPSequenceExtractor`][mosaicolabs.bridges.mcap.MCAPSequenceExtractor]
    iterates it and writes the results into a mcap file. It is the mirror image of
    [`MCAPLoader`][mosaicolabs.bridges.mcap.loader.MCAPLoader], which reads a mcap *into* Mosaico.

    On top of the generic sequence handling it adds the one thing that is specific to
    targeting MCAP:

    * **Adapter resolution against the `_mcap_` namespace**, falling back to a synthesized
      `UnmodeledAdapter` when no hand-written adapter is registered for the topic's recorded
      schema — see `_resolve_topic`.
    """

    SCHEMA_METADATA = MCAPSchemaMetadata
    """Topics ingested by the MCAP bridge carry their bookkeeping under the `_mcap_` key."""

    def __init__(
        self,
        m_client: MosaicoClient,
        sequence_name: str,
        topics: Optional[List[str]] = None,
        start_timestamp_ns: Optional[int] = None,
        end_timestamp_ns: Optional[int] = None,
    ):
        """
        Initializes the loader against a Mosaico sequence.

        A thin pass-through to
        [`MosaicoLoader.__init__`][mosaicolabs.bridges.loader_base.MosaicoLoader]:
        MCAP has no per-loader external state to set up (unlike ROS's `Typestore`), so
        nothing MCAP-specific happens here.

        Example:
            ```python
            from mosaicolabs import MosaicoClient
            from mosaicolabs.bridges.mcap import MosaicoToMCAPLoader

            with MosaicoClient.connect("localhost", 6726) as client:
                # Stream only IMU and GPS topics back out of a sequence
                with MosaicoToMCAPLoader(
                    m_client=client,
                    sequence_name="on_track_experiment",
                    topics=["/imu*", "/gps/fix"],
                ) as loader:
                    for topic, ms_msg in loader:
                        print(f"Read {topic} @ {ms_msg.timestamp_ns}")
            ```

        Args:
            m_client (MosaicoClient): An open [`MosaicoClient`][mosaicolabs.MosaicoClient] connection.
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

    def _resolve_topic(
        self, t_handler: TopicHandler
    ) -> TopicResolution[MCAPAdapterBase]:
        """
        Resolves a topic's Mosaico adapter and the native MCAP type to write it back out as.

        The topic's ``_mcap_`` metadata is read and validated first (see
        `_extract_mcap_metadata`). Then two resolution strategies are tried in order, the
        first to succeed wins:

        1. `_adapter_from_metadata_schema` - hand-written adapter keyed by the
           ``schema_name``/``schema_encoding`` recorded in the topic's ``_mcap_`` metadata.
        2. `_create_unmodeled_adapter` - fallback that synthesizes an ``UnmodeledAdapter``.
           Succeeds as long as a `McapSchemaConverter` is registered for ``schema_encoding``.

        Args:
            t_handler (TopicHandler): The topic handler whose adapter should be resolved.

        Returns:
            TopicResolution[MCAPAdapterBase]: The resolved ``(adapter, schema_name)`` pair,
                or a rejection carrying one of:

                * ``TopicStatus.MALFORMED_METADATA`` - the ``_mcap_`` block is missing, lacks
                  a required field, or holds a field of an unexpected type
                  (`_extract_mcap_metadata` raised `ValueError` or `TypeError`).
                * ``TopicStatus.UNRESOLVED_ADAPTER`` - neither strategy produced an
                  adapter: no hand-written adapter is registered for the schema, and no
                  `McapSchemaConverter` is registered for its encoding.
        """

        # Read and validate the `_mcap_` block once, here, rather than in each strategy.
        try:
            mcap_metadata = _extract_mcap_metadata(t_handler)
        except (TypeError, ValueError) as e:
            return TopicResolution.rejected(TopicStatus.MALFORMED_METADATA, str(e))

        factory_result = self._adapter_from_metadata_schema(
            mcap_metadata
        ) or self._create_unmodeled_adapter(t_handler, mcap_metadata)

        if factory_result is None:
            return TopicResolution.rejected(
                TopicStatus.UNRESOLVED_ADAPTER,
                f"Unable to infer an adapter for ontology '{t_handler.ontology_tag}'.",
            )

        adapter, resolved_mcap_type = factory_result

        return TopicResolution.accepted(adapter, resolved_mcap_type)

    def _adapter_from_metadata_schema(
        self, mcap_metadata: MCAPSchemaMetadata
    ) -> Optional[Tuple[type[MCAPAdapterBase], str]]:
        """
        Strategy 1: look up a hand-written adapter using the ``schema_name``
        and ``schema_encoding`` recorded in the topic's ``_mcap_`` metadata.

        Args:
            mcap_metadata (MCAPSchemaMetadata): The topic's already-extracted ``_mcap_`` block.

        Returns:
            Optional[Tuple[type[MCAPAdapterBase], str]]: The ``(adapter, schema_name)`` pair if a hand-written
                adapter is registered for (``schema_name``, ``schema_encoding``) pair, otherwise ``None``.
        """

        schema_name: str = mcap_metadata.get_schema_name()
        schema_encoding: str = mcap_metadata.get_schema_encoding()

        adapter = MCAPBridge.get_default_adapter(schema_name, schema_encoding)

        if adapter is None:
            return None

        return adapter, schema_name

    def _create_unmodeled_adapter(
        self, t_handler: TopicHandler, mcap_metadata: MCAPSchemaMetadata
    ) -> Optional[Tuple[type[UnmodeledAdapter], str]]:
        """
        Strategy 2 (fallback): synthesize an ``UnmodeledAdapter`` for the topic's
        ontology when no hand-written adapter could be resolved. Succeeds as long as a
        `McapSchemaConverter` is registered for the ``schema_encoding`` recorded in the
        topic's ``_mcap_`` metadata; returns ``None`` otherwise.

        Args:
            t_handler (TopicHandler): The topic handler used to build the unmodeled ontology
                (from its ontology tag, Arrow schema, and serialization format).
            mcap_metadata (MCAPSchemaMetadata): The topic's already-extracted ``_mcap_`` block,
                read for the ``schema_name``, ``schema_encoding`` and ``schema_def`` needed to
                key and decode the adapter.

        Returns:
            Optional[Tuple[type[UnmodeledAdapter], str]]: The ``(adapter, schema_name)``
                pair, or ``None`` if no `McapSchemaConverter` is registered for the schema
                encoding.
        """

        schema_encoding: str = mcap_metadata.get_schema_encoding()
        schema_name: str = mcap_metadata.get_schema_name()

        # schema_def are bytes that should be interpreted differently depending on the
        # original encoding (`protobuf`, `jsonschema`, ...). These can be interpreted
        # using the McapSchemaConverter registered for that encoding.
        schema_converter = McapSchemaRegistry.get_converter(schema_encoding)

        if schema_converter is None:
            return None

        unmodeled_ontology = resolve_ontology_class(
            ontology_tag=t_handler.ontology_tag,
            schema=t_handler._arrow_schema,
            serialization_format=SerializationFormat(t_handler.serialization_format),
        )

        schema_def_bytes = schema_converter.destringify_schema_def(
            mcap_metadata.get_schema_def()
        )
        mcap_class = schema_converter.get_schema_class(schema_name, schema_def_bytes)

        # This will make a new class or reuse an already registered one
        adapter = UnmodeledAdapter.get_or_create(
            ontology_type=unmodeled_ontology,
            mcap_class=mcap_class,
            schema_name=schema_name,
            schema_encoding=schema_encoding,
        )

        return adapter, schema_name
