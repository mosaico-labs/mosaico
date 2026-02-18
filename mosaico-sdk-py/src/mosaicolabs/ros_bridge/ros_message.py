from dataclasses import dataclass
from typing import Any, Dict, Optional

from mosaicolabs.models import Header, Time


def _validate_header_fields(ros_hdata: dict):
    if (
        "frame_id" not in ros_hdata.keys()
        or "stamp" not in ros_hdata.keys()
        or (
            "sec" not in ros_hdata["stamp"].keys()
            or "nanosec" not in ros_hdata["stamp"].keys()
        )
    ):
        raise ValueError(
            f"Malformed ROS message header: missing required keys 'frame_id', 'stamp' or 'sec', 'nanosec' in 'stamp'. "
            f"Available keys: {list(ros_hdata.keys())}"
        )


@dataclass
class ROSHeader:
    """
    Standardized internal representation of a ROS message header.

    This class extracts and normalizes common metadata found in nearly all ROS sensor
    messages (e.g., `std_msgs/Header`). It provides utility methods to validate raw
    dictionary structures and convert them into Mosaico-compatible `Header` objects.


    Attributes:
        seq (Optional[int]): Consecutively increasing ID used to identify data order
            and drops at the hardware/driver level.
        frame_id (str): The coordinate frame name associated with this data (e.g., "base_link", "velodyne").
        stamp (Time): The specific point in time (seconds and nanoseconds) when the data
            was physically sampled by the sensor.
    """

    seq: Optional[int]
    """sequence ID: consecutively increasing ID """
    frame_id: str
    """Frame this data is associated with"""
    stamp: Time
    """seconds (stamp_secs) since epoch (in Python the variable is called 'secs')"""

    _REQUIRED_KEYS = ("frame_id", "stamp")

    def translate(
        self,
        **kwargs: Any,
    ) -> Header:
        """
        Converts the internal ROS representation into a Mosaico Ontology Header.

        This is typically called by a `ROSAdapter` during the final phase of message translation.

        Returns:
            Header: A validated Mosaico Header object ready for serialization.
        """
        return Header(
            frame_id=self.frame_id,
            seq=self.seq,
            stamp=self.stamp,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ROSHeader":
        """
        Factory method to construct a ROSHeader from a raw ROS dictionary.

        This method performs strict structural validation to ensure the input dictionary
        contains the required ROS header fields (`frame_id` and `stamp` with `sec`/`nanosec`).

        Example:
            ```python
            header_data = {
                "seq": 101,
                "frame_id": "camera_optical_frame",
                "stamp": {"sec": 1625000000, "nanosec": 500}
            }
            header_obj = ROSHeader.from_dict(header_data)
            ```

        Args:
            data: The 'header' sub-dictionary from a deserialized ROS message.

        Raises:
            ValueError: If the input dictionary is missing mandatory ROS keys.
        """
        _validate_header_fields(data)
        return ROSHeader(
            seq=data.get("seq"),
            frame_id=data["frame_id"],
            stamp=Time(sec=data["stamp"]["sec"], nanosec=data["stamp"]["nanosec"]),
        )


@dataclass
class ROSMessage:
    """
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
        data (Optional[Dict[str, Any]]): The message payload converted into a standard
            nested Python dictionary.
        header (Optional[ROSHeader]): An automatically parsed `ROSHeader` if the
            `data` payload contains a valid header field.
    """

    def __init__(
        self,
        bag_timestamp_ns: int,
        topic: str,
        msg_type: str,
        data: Optional[Dict[str, Any]],
    ):
        self.bag_timestamp_ns = bag_timestamp_ns
        self.topic = topic
        self.msg_type = msg_type
        self.data = data
        if data:
            header_dict = data.get("header")
            if header_dict:
                self.header = ROSHeader.from_dict(header_dict)

    bag_timestamp_ns: int
    """
    Timestamp (nanoseconds) when the message was written to the rosbag.
    This corresponds to the rosbag storage time and may differ from
    the time the data was originally generated.
    """
    topic: str
    """The topic string of the message source."""
    msg_type: str
    """The message ros type string."""
    data: Optional[Dict[str, Any]]
    """The message payload, converted into a standard nested Python dictionary."""
    header: Optional[ROSHeader] = None
    """The message payload header"""
