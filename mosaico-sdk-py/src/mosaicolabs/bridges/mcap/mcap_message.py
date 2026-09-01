from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class MCAPMessage:
    """
    TODO: intructions for the LLM: update this documentation accordingly to the newly defined MCAPMessage. Try to mantain the same structure
    The standardized container for a single ROS message record yielded by the loader.

    This object serves as the primary "unit of work" within the ROS Bridge pipeline.
    It encapsulates the raw deserialized payload along with essential storage-level metadata
    needed for accurate platform ingestion.

    ### Life Cycle
    1. **Produced** by `ROSLoader` during bag iteration.
    2. **Consumed** by `ROSBridge` to identify the correct adapter.
    3. **Translated** by a `ROSAdapter` into a Mosaico `Message`.


    Example:
        ```python
        # Manual construction (usually handled by the loader)
        msg = ROSMessage(
            bag_timestamp_ns=1625000000000000000,
            topic="/odom",
            msg_type="nav_msgs/msg/Odometry",
            data={"header": {...}, "pose": {...}}
        )

        print(f"Processing {msg.msg_type} from {msg.topic}")
        if msg.header:
            print(f"Frame: {msg.header.frame_id}")
        ```

    Attributes:
        channel_name (str): The channel name of the message source.
        schema_name (str): The schema name of the message source.
        schema_encoding (str): The schema_encoding of the message source.
        data: Dict[str, Any]: The message payload, converted into a standard nested Python dictionary.
        log_time_ns (int): Timestamp (nanoseconds) at which the message was recorded.
        publish_time_ns (Optional[int]): Timestamp (nanoseconds) at which the message was published. If not available, must be set to the log time.
    """

    def __init__(
        self,
        channel_name: str,
        schema_name: Optional[str],
        schema_encoding: Optional[str],
        data: Optional[Dict[str, Any]],
        log_time_ns: int,
        publish_time_ns: Optional[int] = None,
    ):
        """
        Initializes a new ROSMessage instance.

        Args:
            channel_name (str): The channel name of the message source.
            schema_name (Optional[str]): The schema name of the message source.
            schema_encoding (Optional[str]): The encoding of the message source.
            data (Optional[Dict[str, Any]]): The message payload, converted into a standard nested Python dictionary.
            log_time_ns (int): Timestamp (nanoseconds) at which the message was recorded.
            publish_time_ns (Optional[int]): Timestamp (nanoseconds) at which the message was published. If not available, must be set to the log time.
        """
        self.channel_name = channel_name
        self.schema_name = schema_name
        self.schema_encoding = schema_encoding
        self.data_field = data
        self.log_time_ns = log_time_ns
        self.publish_time_ns = publish_time_ns if publish_time_ns else log_time_ns

    channel_name: str
    """The channel name of the message source."""
    schema_name: Optional[str]
    """The schema name of the message source."""
    schema_encoding: Optional[str]
    """The encoding of the message source."""
    data_field: Optional[Dict[str, Any]]
    """The message payload, converted into a standard nested Python dictionary."""
    log_time_ns: int
    """
    Timestamp (nanoseconds) at which the message was recorded.
    """
    publish_time_ns: int
    """
    Timestamp (nanoseconds) at which the message was published. If not available, must be set to the log time.
    """
