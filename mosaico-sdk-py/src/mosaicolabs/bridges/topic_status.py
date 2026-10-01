from enum import Enum


class TopicStatus(Enum):
    """
    Defines the possible topic status (ACCEPTED/REJECTED) for all Loaders.

    Rejection statuses are reported by the loaders' `rejected_topics` property:

    * **All loaders** (`ROSLoader`, `MosaicoLoader`, `MCAPLoader`, via `BaseLoader`):
      `FILTERED`, `UNRESOLVED_ADAPTER`.
    * **`MosaicoLoader`** only (`MosaicoToROSLoader`, `MosaicoToMCAPLoader`):
      `MALFORMED_METADATA`, plus `NOT_IN_TYPESTORE` for `MosaicoToROSLoader`.
    * **`MCAPLoader`** only: `UNRESOLVED_DECODER`, `UNAVAILABLE_SCHEMA`.

    `ACCEPTED` is never reported as a rejection reason.

    Attributes:
        ACCEPTED: Enum specifying the Topic has been accepted.
        FILTERED: Enum specifying the Topic has been rejected since user provided a filter that excludes the topic.
        UNRESOLVED_ADAPTER: Enum specifying the Topic has been rejected since it has no Mosaico adapter.
        NOT_IN_TYPESTORE: Enum specifying the Topic has been rejected since it is not present in ROS typestore.
        MALFORMED_METADATA: Enum specifying the Topic has been rejected since its bridge metadata (``_ros_`` or ``_mcap_``) is malformed.
        UNRESOLVED_DECODER: Enum specifying the Channel has been rejected since its encoding does not have an implemented decoder.
        UNAVAILABLE_SCHEMA: Enum specifying the Channel has been rejected since it does not contain any schema information.
    """

    ACCEPTED = "Accepted"
    """ Status indicating an accepted Topic """

    FILTERED = "Filtered"
    """ Status indicating topic has been rejected by user specified filter """

    UNRESOLVED_ADAPTER = "Unresolved adapter"
    """ Status indicating the Topic has been rejected since no Mosaico adapter could be resolved """

    NOT_IN_TYPESTORE = "Not in typestore"
    """ Status indicating the Topic has been rejected since it is not present in ROS typestore """

    MALFORMED_METADATA = "Malformed metadata"
    """ Status indicating the Topic has been rejected since its bridge metadata ('_ros_' or '_mcap_') is malformed """

    UNRESOLVED_DECODER = "Unresolved decoder"
    """ Status indicating the Channel has been rejected since its encoding does not have an implemented decoder """

    UNAVAILABLE_SCHEMA = "Unavailable schema"
    """ Status indicating the Channel has been rejected since it does not contain any schema information """


def to_color(topic_status: TopicStatus) -> str:
    """Returns the Rich color string used to render this status in the progress UI."""
    _colors = {
        TopicStatus.ACCEPTED: "bright_green",
        TopicStatus.FILTERED: "bright_yellow",
        TopicStatus.UNRESOLVED_ADAPTER: "dark_orange",
        TopicStatus.NOT_IN_TYPESTORE: "orange1",
        TopicStatus.MALFORMED_METADATA: "red1",
        TopicStatus.UNRESOLVED_DECODER: "orange1",
        TopicStatus.UNAVAILABLE_SCHEMA: "dark_orange",
    }
    return _colors.get(topic_status, "bright_red")
