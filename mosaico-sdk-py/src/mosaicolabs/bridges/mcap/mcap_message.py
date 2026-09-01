from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class MCAPMessage:
    """
    The standardized container for a single MCAP message record yielded by the loader.

    This object serves as the primary "unit of work" within the MCAP Bridge pipeline.
    It encapsulates the raw deserialized payload along with essential channel/schema-level
    metadata needed for accurate platform ingestion. Deserialization itself (protobuf via
    `MessageToDict`, jsonschema via `json.loads`) is performed by the loader before this
    container is built, so `data_field` is always a plain nested Python dictionary regardless
    of the source encoding.

    ### Life Cycle
    1. **Produced** by `MCAPLoaderProtobuf`/`MCAPLoaderJsonschema` during mcap iteration.
    2. **Consumed** by `MCAPBridge` to identify the correct adapter, based on `schema_name`
       and `schema_encoding`.
    3. **Translated** by an `MCAPAdapterBase` into a Mosaico `Message`.

    Example:
        ```python
        # Manual construction (usually handled by the loader)
        msg = MCAPMessage(
            channel_name="/imu",
            schema_name="sensor_msgs.Imu",
            schema_encoding="protobuf",
            data={"header": {...}, "linear_acceleration": {...}},
            log_time_ns=1625000000000000000,
        )

        print(f"Processing {msg.schema_name} from {msg.channel_name}")
        ```

    Attributes:
        channel_name (str): The channel name of the message source.
        schema_name (Optional[str]): The schema name of the message source. `None` if the
            schema could not be resolved or decoding failed.
        schema_encoding (Optional[str]): The schema encoding of the message source (e.g.
            `protobuf`, `jsonschema`). `None` if the schema could not be resolved or decoding failed.
        data_field (Optional[Dict[str, Any]]): The message payload, converted into a standard
            nested Python dictionary. `None` if decoding failed.
        log_time_ns (int): Timestamp (nanoseconds) at which the message was recorded.
        publish_time_ns (int): Timestamp (nanoseconds) at which the message was published. If not available, it is set to the log time.
    """

    def __init__(
        self,
        channel_name: str,
        channel_encoding: str,
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
            channel_encoding (str): The encoding of the message source.
            schema_name (Optional[str]): The schema name of the message source.
            schema_encoding (Optional[str]): The encoding of the schema source.
            data (Optional[Dict[str, Any]]): The message payload, converted into a standard nested Python dictionary.
            log_time_ns (int): Timestamp (nanoseconds) at which the message was recorded.
            publish_time_ns (Optional[int]): Timestamp (nanoseconds) at which the message was published. If not available, must be set to the log time.
        """
        self.channel_name = channel_name
        self.channel_encoding = channel_encoding
        self.schema_name = schema_name
        self.schema_encoding = schema_encoding
        self.data_field = data
        self.log_time_ns = log_time_ns
        self.publish_time_ns = publish_time_ns if publish_time_ns else log_time_ns

    channel_name: str
    """The channel name of the message source."""
    channel_encoding: str
    """The encoding of the message source."""
    schema_name: Optional[str]
    """The schema name of the message source."""
    schema_encoding: Optional[str]
    """The encoding of the schema source."""
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
