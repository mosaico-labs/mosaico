from enum import Enum


class TopicStatus(Enum):
    pass


class CommonTopicStatus(TopicStatus):
    """
    Defines the possible topic status (ACCEPTED/REJECTED) for all Loaders. In case of rejection, a more informative enum if provided.

    Attributes:
        ACCEPTED: Enum specifying the Topic has been accepted.
        FILTERED: Enum specifying the Topic has been rejected since user provided a filter that excludes the topic.
        UNRESOLVED_ADAPTER: Enum specifying the Topic has been rejected since it has no Mosaico adapter.
    """

    ACCEPTED = "Accepted"
    """ Status indicating an accepted Topic """

    FILTERED = "Filtered"
    """ Status indicating topic has been rejected by user specified filter """

    UNRESOLVED_ADAPTER = "Unresolved adapter"
    """ Status indicating the Topic has been rejected since no Mosaico adapter could be resolved """


class ROSTopicStatus(TopicStatus):
    """
    Defines the possible topic status (ACCEPTED/REJECTED) for ROSLoader. In case of rejection, a more informative enum if provided.

    Attributes:
        NOT_IN_TYPESTORE: Enum specifying the Topic has been rejected since it is not present in ROS typestore.
        MALFORMED_METADATA: Enum specifying the Topic has been rejected since its ``_ros_`` metadata is malformed.
    """

    NOT_IN_TYPESTORE = "Not in typestore"
    """ Status indicating the Topic has been rejected since it is not present in ROS typestore """

    MALFORMED_METADATA = "Malformed metadata"
    """ Status indicating the Topic has been rejected since its '_ros_' metadata is malformed """


class MCAPTopicStatus(TopicStatus):
    """
    Defines the possible topic status (ACCEPTED/REJECTED) for MCAPLoader. In case of rejection, a more informative enum if provided.

    Attributes:
        UNRESOLVED_DECODER: Enum specifying the Channel has been rejected since its encoding does not have an implemented decoder.
        UNAVAILABLE_SCHEMA: Enum specifying the Channel has been rejected since it does not contain any schema information.
    """

    UNRESOLVED_DECODER = "Unresolved decoder"
    """ Status indicating the Channel has been rejected since its encoding does not have an implemented decoder """

    UNAVAILABLE_SCHEMA = "Unavailable schema"
    """ Status indicating the Channel has been rejected since it does not contain any schema information """


def to_color(topic_status: TopicStatus) -> str:
    """Returns the Rich color string used to render this status in the progress UI."""
    _colors = {
        CommonTopicStatus.ACCEPTED: "bright_green",
        CommonTopicStatus.FILTERED: "bright_yellow",
        CommonTopicStatus.UNRESOLVED_ADAPTER: "dark_orange",
        ROSTopicStatus.NOT_IN_TYPESTORE: "orange1",
        ROSTopicStatus.MALFORMED_METADATA: "red1",
        MCAPTopicStatus.UNRESOLVED_DECODER: "orange1",
        MCAPTopicStatus.UNAVAILABLE_SCHEMA: "dark_orange",
    }
    return _colors.get(topic_status, "bright_red")
