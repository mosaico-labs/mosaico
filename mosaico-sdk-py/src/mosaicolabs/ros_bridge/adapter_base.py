from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Optional, Tuple, Type, TypeVar, Union

from rosbags.typesys.store import Typestore

if TYPE_CHECKING:
    from rosbags.typesys.store import MsgType

from mosaicolabs.models.message import Message

from ..models import Serializable
from .ros_message import ROSHeader, ROSMessage, Time

T = TypeVar("T", bound=Serializable)


class ROSAdapterBase(ABC, Generic[T]):
    """
    Abstract Base Class for converting ROS messages to Mosaico Ontology types.

    The Adaptation Layer is the semantic core of the ROS Bridge. Rather than
    performing simple parsing, adapters actively translate raw ROS data into standardized,
    strongly-typed Mosaico Ontology objects.

    Attributes:
        ros_msgtype: The ROS message type string (e.g., 'sensor_msgs/msg/Imu') or a tuple
            of supported types.
        is_default_adapter: whether this adapter is the default one for __mosaico_ontology_type__
        __mosaico_ontology_type__: The target Mosaico class (e.g., IMU).
        _REQUIRED_KEYS: Internal validation list for mandatory ROS message fields.
    """

    ros_msgtype: str | Tuple[str, ...]
    __is_default_adapter__: bool = False
    __mosaico_ontology_type__: Type[T]
    _REQUIRED_KEYS: Tuple[str, ...]
    _REQUIRED_KEYS_CASE_INSENSITIVE: Tuple[str, ...] = ()

    @classmethod
    @abstractmethod
    def ros_msg_type(cls) -> str | Tuple[str, ...]:
        """Returns the specific ROS message type handled by this adapter."""
        return cls.ros_msgtype

    @classmethod
    def get_default_ros_msg(cls) -> Optional[str]:

        if isinstance(cls.ros_msgtype, str):
            return cls.ros_msg_type()

        elif isinstance(
            cls.ros_msgtype, Tuple
        ):  # In case of a tuple, default ros message is the first tuple element
            return cls.ros_msg_type()[0]

        return None

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message instance into a Mosaico Message.

        Implementation should handle recursive unwrapping, unit conversion, and
        validation.

        Args:
            ros_msg: The source container yielded by the ROSLoader.
            **kwargs: Contextual data such as calibration parameters or frame overrides.

        Returns:
            A Mosaico Message object containing the instantiated ontology data.
        """
        if ros_msg.data is None:
            raise Exception(f"'data' attribute is None for topic {ros_msg.topic}")

        try:
            return Message(
                timestamp_ns=ros_msg.header.stamp.to_nanoseconds()
                if ros_msg.header
                else ros_msg.bag_timestamp_ns,
                data=cls.from_dict(ros_msg.data),
                recording_timestamp_ns=ros_msg.bag_timestamp_ns,
                frame_id=ros_msg.header.frame_id if ros_msg.header else None,
                sequence_id=ros_msg.header.seq if ros_msg.header else None,
            )
        except Exception as e:
            raise Exception(f"Translation failed for {ros_msg.topic}: {e}")

    @classmethod
    @abstractmethod
    def from_dict(cls, ros_data: dict) -> T:
        """
        Maps the raw ROS dictionary to the Pydantic model.

        This method performs field validation and reconstruction.
        """
        pass

    @classmethod
    def is_rosmsg_type_valid(cls, type_to_validate: str) -> bool:
        """
        Checks whether a given ROS message type string is handled by this adapter.

        Args:
            type_to_validate: The full ROS message type string to check
                (e.g., ``"sensor_msgs/msg/Imu"``).

        Returns:
            ``True`` if the adapter supports this type, ``False`` otherwise.
        """
        if isinstance(cls.ros_msgtype, str):
            return type_to_validate == cls.ros_msgtype
        elif isinstance(cls.ros_msgtype, Tuple):
            return type_to_validate in cls.ros_msgtype

        return False

    @classmethod
    def unpack_mosaico_msg(cls, mosaico_msg: Union[Message, T]) -> tuple[T, ROSHeader]:
        """
        Extracts the typed Mosaico payload and a ``ROSHeader`` from a wrapped or bare message.

        Handles two input cases:

        - **``Message`` wrapper**: the typed data is extracted via ``get_data()``;
          a ``ROSHeader`` is reconstructed from the message's ``timestamp_ns``,
          ``frame_id``, and ``sequence_id`` metadata.
        - **Raw ontology instance**: returned as-is with a zeroed ``ROSHeader``
          (seq=0, frame_id="", stamp=0).

        Args:
            mosaico_msg: Either a ``Message`` envelope or a raw instance of
                ``cls.__mosaico_ontology_type__``.

        Returns:
            A ``(data, header)`` tuple where *data* is the typed ontology object and
            *header* is the corresponding ``ROSHeader``.

        Raises:
            TypeError: If *mosaico_msg* is neither a ``Message`` nor an instance of
                the expected ontology type.
        """
        if isinstance(mosaico_msg, Message):
            data: T = mosaico_msg.get_data(cls.__mosaico_ontology_type__)
            if data is None:
                raise TypeError(
                    f"Adapter {cls.__name__} cannot handle {mosaico_msg.ontology_tag()} Mosaico type"
                )

            mosaico_time = Time.from_nanoseconds(mosaico_msg.timestamp_ns)
            header = ROSHeader.from_dict(
                {
                    "seq": mosaico_msg.sequence_id or 0,
                    "frame_id": mosaico_msg.frame_id or "",
                    "stamp": {
                        "sec": mosaico_time.seconds,
                        "nanosec": mosaico_time.nanoseconds,
                    },
                }
            )

        elif isinstance(mosaico_msg, cls.__mosaico_ontology_type__):
            data = mosaico_msg
            header = ROSHeader.from_dict(
                {"seq": 0, "frame_id": "", "stamp": {"sec": 0, "nanosec": 0}}
            )

        else:
            raise TypeError(
                f"Mosaico data passed to {cls.__name__} Adapter has type {type(mosaico_msg)} and it is neither a Message nor a {cls.__mosaico_ontology_type__.ontology_tag()}"
            )

        return data, header

    @classmethod
    @abstractmethod
    def to_ros(
        cls,
        mosaico_msg: Union[Message, Serializable],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> Optional["MsgType"]:
        """
        Converts a Mosaico message or ontology object back into a native ROS message.

        Args:
            mosaico_msg: A ``Message`` wrapper or a raw ``Serializable`` ontology instance.
            typestore: The rosbags typestore used to resolve and construct target ROS types.
            ros_msg_type: Override for the output ROS type string. If ``None``, the adapter
                defaults to ``cls.get_default_ros_msg()``.

        Returns:
            The constructed ROS message instance, or ``None`` if the type is unsupported.
        """
        pass

    @classmethod
    @abstractmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extracts ROS-specific schema metadata for the Mosaico platform.

        This allows preserving original ROS attributes that may not fit directly
        into the physical ontology fields.
        """
        pass

    @classmethod
    def ontology_data_type(cls) -> Type[T]:
        """Returns the Ontology class type associated with this adapter."""
        return cls.__mosaico_ontology_type__
