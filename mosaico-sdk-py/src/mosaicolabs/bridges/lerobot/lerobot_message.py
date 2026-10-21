from dataclasses import dataclass
from typing import List, Optional, Type

import pyarrow as pa

from mosaicolabs import Serializable


@dataclass
class LeRobotMessage:
    """
    FIXME
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
        bag_timestamp_ns (int): Timestamp (nanoseconds) when the message was recorded
            to the bag file. This is the "storage time".
        topic (str): The specific topic string source (e.g., "/camera/left/image_raw").
        msg_type (str): The canonical ROS type string (e.g., "sensor_msgs/msg/Image").
        data_field (Optional[Dict[str, Any]]): The message payload converted into a
            standard nested Python dictionary.
        const_data (Dict[str, Any]): The message constants (e.g. `uint8 STATUS_FIX=0`)
            declared at the top level of the message, keyed by their `UPPER_CASE` name.
        header (Optional[ROSHeader]): The message payload header, if present.
    """

    msco_topic_name: str
    """The topic string of the message source."""

    column_names: List[str]
    """The column name from the native parquet file considered for this message."""

    timestamp_ns: Optional[pa.Array]
    """The message timestamps to be associated with the main payload."""

    data: Optional[pa.RecordBatch]
    """The message payload as a PyArrow RecordBatch."""

    msco_ontology: Type[Serializable]
    """TODO"""

    n_rows: Optional[int]
