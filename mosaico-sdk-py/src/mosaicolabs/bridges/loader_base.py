from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Generic, Iterable, List, Optional, Tuple, Type, TypeVar

from .bridge_adapter_base import BridgeAdapterBase


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

    NOT_IN_TYPESTORE = (
        "Not in typestore"  # TODO: should this be moved to RosTopicStatus?
    )
    """ Status indicating the Topic has been rejected since it is not present in ROS typestore """

    MALFORMED_METADATA = (
        "Malformed metadata"  # TODO: should this be moved to RosTopicStatus?
    )
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

AdapterT = TypeVar("AdapterT", bound=BridgeAdapterBase)


class BaseLoader(ABC, Generic[AdapterT]):
    """
    Shared topic classification and adapter-resolution logic for
    - :class:`ROSLoader` (bag file source)
    - :class:`MCAPLoader` (mcap file source)
    - :class:`MosaicoLoader` (Mosaico sequence source).

    All loaders classify every topic of their underlying data source into one of:
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
    * `_topic_cached_adapters`: `Dict[str, Type[AdapterT]]` mapping accepted
      topic names to their resolved adapter.
    """

    _resolved_topics: Any
    _accepted_topics: Any
    _unresolved_adapter_topics: Any
    _filtered_topics: Any
    _topic_cached_adapters: Dict[str, Type[AdapterT]]

    def __init__(self, container_type: Type[Iterable]):
        self._resolved_topics = container_type()
        """The full set of canonical topic names in the underlying source (dict or list; only the keys/elements are read by this base class)."""
        self._accepted_topics = container_type()
        """The set of topic names that passed filtering and adapter resolution (dict or list; only the keys/elements are read by this base class)."""
        self._unresolved_adapter_topics = container_type()
        """The set of topic names that have no resolvable Mosaico adapter (dict or list; only the keys/elements are read by this base class)."""
        self._filtered_topics = container_type()
        """The set of topic names that are excluded by the user's `topics` filter (dict or list; only the keys/elements are read by this base class)."""
        self._topic_cached_adapters: dict[str, type[AdapterT]] = {}
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
