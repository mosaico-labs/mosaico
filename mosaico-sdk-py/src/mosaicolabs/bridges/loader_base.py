from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from mosaicolabs import (
    MosaicoClient,
    SequenceDataStreamer,
    SequenceHandler,
    TopicHandler,
)
from mosaicolabs.logging_config import get_logger

from .base_schema_metadata import BaseSchemaMetadata
from .bridge_adapter_base import BridgeAdapterBase
from .helpers import _clip_timestamp, _filter_from_list, _validate_sequence
from .topic_status import TopicStatus

# Set the hierarchical logger
logger = get_logger(__name__)

# --- Shared Topic Resolution/Rejection Bookkeeping ---

AdapterT = TypeVar("AdapterT", bound=BridgeAdapterBase)


@dataclass(frozen=True)
class TopicResolution(Generic[AdapterT]):
    """
    The outcome of resolving a single topic: either an adapter to translate it with,
    or the reason it was rejected.

    Build these with `accepted` / `rejected` rather than by hand — the
    constructor enforces that exactly the accepted case carries an adapter.

    Attributes:
        status (TopicStatus): `TopicStatus.ACCEPTED`, or the reason for rejection.
        adapter (Optional[Type[AdapterT]]): The resolved adapter class. Set if and only
            if `status` is `ACCEPTED`.
        native_msg_type (Optional[str]): The source-format message type the topic
            originally came from (e.g. `"sensor_msgs/msg/Imu"` for ROS, or the schema name,
            e.g. `"sensor_msgs.Imu"`, for MCAP). Set only on acceptance.
        detail (str): Human-readable context for a rejection, used for logging.
    """

    status: TopicStatus
    adapter: Optional[Type[AdapterT]] = None
    native_msg_type: Optional[str] = None
    detail: str = ""

    @classmethod
    def accepted(
        cls, adapter: Type[AdapterT], native_msg_type: str
    ) -> "TopicResolution[AdapterT]":
        """
        Builds the accepted outcome.

        Args:
            adapter (Type[AdapterT]): The adapter class resolved for this topic.
            native_msg_type (str): The topic's original source-format message type.

        Returns:
            TopicResolution[AdapterT]: An `ACCEPTED` resolution carrying both values.
        """
        return cls(TopicStatus.ACCEPTED, adapter, native_msg_type)

    @classmethod
    def rejected(
        cls, status: TopicStatus, detail: str = ""
    ) -> "TopicResolution[AdapterT]":
        """
        Builds a rejected outcome.

        Args:
            status (TopicStatus): Why the topic was rejected. Must not be `ACCEPTED`.
            detail (str): Optional human-readable context, surfaced in the loader's log line.

        Returns:
            TopicResolution[AdapterT]: A rejection carrying no adapter.
        """
        return cls(status, detail=detail)

    @property
    def is_accepted(self) -> bool:
        """Whether this topic was accepted and therefore carries an adapter."""
        return self.status is TopicStatus.ACCEPTED

    def __post_init__(self):
        # Keeps the optional fields honest: an ACCEPTED resolution without an adapter
        # (or a rejection carrying one) fails here, inside the subclass that built it,
        # instead of surfacing later as a mystery `None` out of `resolve_adapter()`.
        if self.is_accepted != (self.adapter is not None):
            raise ValueError(
                f"TopicResolution: status '{self.status}' is inconsistent with "
                f"adapter '{self.adapter}'. ACCEPTED requires an adapter; a rejection "
                f"must not carry one."
            )


class BaseLoader(ABC, Generic[AdapterT]):
    """
    Shared topic classification and adapter-resolution logic for
    - `ROSLoader` (bag file source)
    - `MCAPLoader` (mcap file source)
    - `MosaicoLoader` (Mosaico sequence source).

    All loaders classify every topic of their underlying data source as either
    **accepted** (adapter resolved, passes the user's `topics` filter) or **rejected**,
    the latter carrying a `TopicStatus` saying why — excluded by the user's
    filter, no adapter resolvable, or a source-specific reason such as
    `TopicStatus.NOT_IN_TYPESTORE` or `TopicStatus.UNAVAILABLE_SCHEMA`. This base
    class implements the properties that only need to read that bookkeeping, so each
    subclass only has to populate it via its own `_ensure_resolved()` (which performs the
    actual, source-specific resolution: opening the bag file, or querying the Mosaico
    sequence).

    Subclasses are expected to set, during `_ensure_resolved()`:

    * `_resolved_topics`: **all** topic names in the source (dict or list; only
      the keys/elements are read by this base class).
    * `_accepted_topics`: topic names that passed filtering and adapter resolution.
    * `_topic_cached_adapters`: `Dict[str, Type[AdapterT]]` mapping accepted
      topic names to their resolved adapter.

    and record every rejection through `_reject`, which is the single place
    rejection reasons live — there are no per-reason buckets to keep in sync.
    """

    _resolved_topics: Any
    _accepted_topics: Any
    _rejections: Dict[str, TopicStatus]
    _topic_cached_adapters: Dict[str, Type[AdapterT]]

    def __init__(self, container_type: Type[Iterable]):
        self._resolved_topics = container_type()
        """The full set of canonical topic names in the underlying source (dict or list; only the keys/elements are read by this base class)."""
        self._accepted_topics = container_type()
        """The set of topic names that passed filtering and adapter resolution (dict or list; only the keys/elements are read by this base class)."""
        self._rejections: Dict[str, TopicStatus] = {}
        """Every rejected topic, mapped to the reason it was rejected."""
        self._topic_cached_adapters: dict[str, type[AdapterT]] = {}
        """Dictionary mapping accepted topic names to their resolved Mosaico adapter class."""

    @abstractmethod
    def _ensure_resolved(self) -> None:
        """Lazily triggers the source-specific resolution, classifying every topic as
        accepted or rejected."""

    def _reject(self, topic_name: str, status: TopicStatus) -> None:
        """
        Records a topic as rejected, together with the reason.

        This is the only way a subclass reports a rejection; `rejected_topics` and the
        per-reason properties all derive from what is recorded here.

        Args:
            topic_name (str): The rejected topic's canonical name.
            status (TopicStatus): Why it was rejected.
        """
        self._rejections[topic_name] = status

    def _topics_with_status(self, status: TopicStatus) -> List[str]:
        """
        Returns the rejected topics whose reason is `status`.

        Args:
            status (TopicStatus): The rejection reason to filter on.

        Returns:
            List[str]: Matching topic names, in rejection order.
        """
        self._ensure_resolved()
        return [t for t, s in self._rejections.items() if s is status]

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
    def unresolved_adapter_topics(self) -> List[str]:
        """
        Retrieves the list of topic names that are **skipped** due to unavailable Mosaico adapter.

        Returns:
            List[str]: A list of topics with unresolved adapter to translate them into/from Mosaico Ontology.
        """
        return self._topics_with_status(TopicStatus.UNRESOLVED_ADAPTER)

    @property
    def filtered_topics(self) -> List[str]:
        """
        Retrieves the list of topic names that are **skipped** due to the user filter.

        Returns:
            List[str]: The list of topics filtered by the user. Empty if no filter is provided.
        """
        return self._topics_with_status(TopicStatus.FILTERED)

    @property
    def rejected_topics(self) -> List[Tuple[str, TopicStatus]]:
        """
        Retrieves every rejected topic together with the reason it was rejected.

        Returns:
            List[Tuple[str, TopicStatus]]: `(topic_name, status)` pairs for every topic
                excluded from `topics`, whatever the reason — the user's filter, an
                unresolvable adapter, or any source-specific rejection reason.
        """
        self._ensure_resolved()
        return list(self._rejections.items())

    def resolve_adapter(self, topic_name: str) -> Optional[type[AdapterT]]:
        """
        Returns the resolved adapter for an accepted topic.

        Args:
            topic_name (str): The topic name whose adapter should be resolved.
                Must be one of the accepted topics produced by `_ensure_resolved()`.

        Returns:
            Optional[Type[AdapterT]]: The resolved adapter type, or `None` if the
                topic is not among the accepted topics.
        """
        self._ensure_resolved()

        if topic_name not in self._accepted_topics:
            return None

        return self._topic_cached_adapters.get(topic_name)


class MosaicoLoader(BaseLoader[AdapterT], Generic[AdapterT]):
    """
    Streams messages back **out of** a Mosaico sequence, adapting each one into a native
    format on the way.

    This is the source-agnostic half of the extraction pipeline: connecting to the
    server, resolving the sequence and its topic filter, clipping the time window to the
    sequence bounds, deciding which topics can be adapted, and exposing a
    [`SequenceDataStreamer`][mosaicolabs.handlers.SequenceDataStreamer] for iteration.
    Everything that depends on the *target* format lives in a subclass — see
    `MosaicoToROSLoader` and `MosaicoToMCAPLoader`.

    A subclass supplies exactly two things:

    * `SCHEMA_METADATA` — the reserved metadata namespace its bridge writes at
      ingestion time (`_ros_`, `_mcap_`, ...), used to recover per-topic bookkeeping.
    * `_resolve_topic` — given a topic, the adapter and native message type to use,
      or the reason the topic is rejected.

    Conforms to the `LoaderUIAPI` protocol, making it usable with
    `ProgressManager` for live progress reporting.
    """

    SCHEMA_METADATA: ClassVar[Type[BaseSchemaMetadata]]
    """The reserved topic-metadata namespace this loader's bridge writes (e.g.
    [`RosSchemaMetadata`][mosaicolabs.bridges.ros.RosSchemaMetadata] for `_ros_`). Every
    subclass must set it; the base class cannot, since `BaseSchemaMetadata` has an empty
    `KEY` and would silently extract nothing."""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Mirrors BaseSchemaMetadata's own guard: fail at import time rather than
        # silently extracting `{}` for every topic.
        if not getattr(cls, "SCHEMA_METADATA", None):
            raise TypeError(
                f"{cls.__name__} must define SCHEMA_METADATA (the BaseSchemaMetadata "
                f"subclass owning this bridge's reserved metadata namespace)."
            )

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

        Nothing is fetched here — the sequence is resolved lazily on first access to any
        property or on iteration (see `_resolve_sequence`).

        Args:
            m_client (MosaicoClient): An open
                [`MosaicoClient`][mosaicolabs.comm.MosaicoClient] connection.
            sequence_name (str): Name of the Mosaico sequence to load.
            topics (Optional[List[str]]): Optional topic-name filter patterns (glob-style,
                ``!``-prefixed for exclusions, evaluated in order with gitignore-like
                semantics). ``None`` loads all topics.
            start_timestamp_ns (Optional[int]): Inclusive lower bound for the time window (nanoseconds). Clipped
                to the sequence minimum if out of range.
            end_timestamp_ns (Optional[int]): Exclusive upper bound for the time window (nanoseconds). Clipped to
                `None` if out of range.
        """

        super().__init__(
            container_type=list[str]
        )  # Initialize the base class to set up topic resolution state

        self._client = m_client
        """The MosaicoClient used to fetch sequence data and metadata."""
        self._sequence_name = sequence_name
        """The name of the Mosaico sequence to load."""
        self._topic_glob_pattern = topics
        """Optional list of topic-name filter patterns (glob-style, ``!``-prefixed for exclusions)."""
        self._start_timestamp_ns = start_timestamp_ns
        """Inclusive lower bound for the time window (nanoseconds). Clipped to the sequence minimum if out of range."""
        self._end_timestamp_ns = end_timestamp_ns
        """Exclusive upper bound for the time window (nanoseconds). Clipped to the sequence maximum if out of range."""
        self._seq_handler: Optional[SequenceHandler] = None
        """The mosaico sequence handler, lazily initialized on first access."""
        self._streamer: Optional[SequenceDataStreamer] = None
        """The mosaico sequence streamer, lazily initialized on first access. Provides an iterator over the sequence messages."""

        self._topic_metadata: dict[str, BaseSchemaMetadata] = {}
        """Maps each accepted topic to its extracted `SCHEMA_METADATA` block."""
        self._topic_msgtype: dict[str, str] = {}
        """Maps each accepted topic to the native message type it was ingested as."""

    @abstractmethod
    def _resolve_topic(self, t_handler: TopicHandler) -> TopicResolution[AdapterT]:
        """
        Resolves a topic's adapter and native message type, or reports why it cannot be
        adapted.

        This is the single extension point of `MosaicoLoader`. Implementations
        should return `TopicResolution.rejected` rather than raising — the caller
        records the returned status verbatim, so any rejection reason a subclass invents
        reaches the UI without further plumbing.

        Args:
            t_handler (TopicHandler): The topic handler to resolve.

        Returns:
            TopicResolution[AdapterT]: `TopicResolution.accepted` carrying the
                adapter and the native message type, or `TopicResolution.rejected`
                carrying a `TopicStatus` explaining the rejection.
        """

    def _resolve_sequence(self) -> SequenceHandler:
        """
        Lazily initializes the sequence handler, resolved topic list, and streamer.

        Called automatically on first access to any property or iterator. Performs
        the following steps:

        1. Fetches the `SequenceHandler` for the configured sequence name
           and validates it exists.
        2. Clips ``start_timestamp_ns`` / ``end_timestamp_ns`` to the sequence bounds,
           logging a warning if clipping occurs.
        3. Applies the topic filter via `_filter_from_list`; topics excluded by it
           are rejected as ``FILTERED``.
        4. Asks `_resolve_topic` to resolve each surviving topic. Accepted topics
           are recorded together with their adapter, their native message type, and their
           `SCHEMA_METADATA` block; rejected ones are recorded with the status the
           subclass returned.
        5. Creates the `SequenceDataStreamer` over the accepted topics, to be
           returned by `__iter__`.

        Returns:
            SequenceHandler: The resolved handler for the configured sequence.

        Raises:
            ValueError: when the configured sequence does not exist.
            RuntimeError: when no topic survived filtering and adapter resolution.
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

        matched_topics = _filter_from_list(
            self._seq_handler.topics, self._topic_glob_pattern
        )

        # Filter topics
        for t_name in self._seq_handler.topics:
            # 1) Filter if topic has not been requested
            if t_name not in matched_topics:
                self._reject(t_name, TopicStatus.FILTERED)
                continue

            t_handler = self._seq_handler.get_topic_handler(t_name)

            # 2) Ask the specialization for an adapter, or the reason there isn't one
            resolution = self._resolve_topic(t_handler)

            if not resolution.is_accepted:
                logger.warning(
                    f"Skipping topic '{t_name}': {resolution.status.value}. {resolution.detail}"
                )
                self._reject(t_name, resolution.status)
                continue

            # Finally accept the topic, caching what the extraction pipeline will need
            self._accepted_topics.append(t_name)

            assert (
                resolution.adapter is not None
            )  # here cannot be None since it has been accepted
            assert (
                resolution.native_msg_type is not None
            )  # here cannot be None since it has been accepted

            self._topic_cached_adapters[t_name] = resolution.adapter
            self._topic_msgtype[t_name] = resolution.native_msg_type
            self._topic_metadata[t_name] = self.SCHEMA_METADATA.from_dict(
                t_handler.user_metadata
            )

        if not self._accepted_topics:
            raise RuntimeError(
                f"Unable to initialize {type(self).__name__}: No topic matched criteria or adapter found. Try checking the topics filter, if any."
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
        accepted topics combined.

        Args:
            topic (Optional[str]): If provided, count messages for that specific topic only.
                If ``None``, sum across all accepted topics.

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

    @property
    def msg_types(self) -> List[str | None]:
        """
        Returns the Mosaico ontology type tags for each accepted topic.

        Entries appear in the same order as `topics`. A ``None`` entry
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
    def resolve_native_msg_type(self, topic_name: str) -> Optional[str]:
        """
        Returns the native message type a topic was originally ingested as.

        When a bag or mcap file is ingested into Mosaico, the source message type
        (e.g. ``sensor_msgs/msg/Imu``) is preserved in the topic's user metadata under
        this bridge's reserved namespace. Recovering it lets the extraction pipeline
        write the topic back out under its original type instead of falling back to
        whatever type the adapter defaults to.

        Args:
            topic_name (str): The topic whose original message type should be resolved.
                Must be one of the accepted topics produced by `_resolve_sequence`.

        Returns:
            Optional[str]: The message type string (e.g. ``"sensor_msgs/msg/Imu"``),
            or ``None`` if the topic is unknown or was not accepted.
        """
        self._resolve_sequence()

        return self._topic_msgtype.get(topic_name)

    def resolve_metadata(self, topic_name: str) -> Optional[BaseSchemaMetadata]:
        """
        Returns the metadata required for the topic reconstruction to the native message.

        When a bag or mcap file is ingested into Mosaico, the source message metadata
        (e.g. message definition, original topic name, ...) are saved in the topic's user
        metadata under this bridge's reserved namespace. Recovering it lets the extraction
        pipeline write the topic back out under its original form.

        Args:
            topic_name (str): The topic whose reconstruction metadata should be resolved.
                Must be one of the accepted topics produced by `_resolve_sequence`.

        Returns:
            Optional[BaseSchemaMetadata]: All the message metadata required for the
                reconstruction, or ``None`` if the topic is unknown or was not accepted.
        """

        self._resolve_sequence()

        return self._topic_metadata.get(topic_name)

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
