from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Optional, Tuple, Type, TypeVar, Union

from rosbags.typesys.store import Typestore

if TYPE_CHECKING:
    from rosbags.typesys.store import MsgType

from mosaicolabs import Header, Time
from mosaicolabs.models.core import Message, Serializable

from .ros_message import ROSMessage

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
    def get_default_ros_msg(cls) -> str:

        adapter_rosmsg_type = cls.ros_msg_type()

        if isinstance(adapter_rosmsg_type, str):
            return adapter_rosmsg_type

        elif isinstance(
            adapter_rosmsg_type, Tuple
        ):  # In case of a tuple, default ros message is the first tuple element
            return adapter_rosmsg_type[0]

        raise Exception(
            f"Adapter {cls.__name__} has ros_msgtype that is neither a {str.__name__} nor a {tuple.__name__} "
        )

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
                timestamp_ns=ros_msg.bag_timestamp_ns,
                data=cls.from_dict(ros_msg.data),
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
    def unpack_mosaico_msg(cls, mosaico_msg: Union[Message, T]) -> tuple[T, Header]:
        """
        Extracts the typed Mosaico payload and its ``Header`` (if present) from a wrapped or bare message.

        Handles two input cases:

        - **``Message`` wrapper**: the typed data is extracted via ``get_data()``.
        - **Raw ontology instance**: returned as-is with

        the ``Header`` is extracted from the ontology (if supported), otherwise an default Header (empty `frame_id` and zero `Time`) is returned.

        Args:
            mosaico_msg: Either a ``Message`` envelope or a raw instance of
                ``cls.__mosaico_ontology_type__``.

        Returns:
            A ``(data, header)`` tuple where *data* is the typed ontology object and
            *header* is the corresponding ``Header`` or None if not present.

        Raises:
            TypeError: If *mosaico_msg* is neither a ``Message`` nor an instance of
                the expected ontology type.
        """
        if isinstance(mosaico_msg, Message):
            data: Optional[T] = mosaico_msg.get_data(cls.__mosaico_ontology_type__)
            if data is None:
                raise TypeError(
                    f"Adapter {cls.__name__} cannot handle {mosaico_msg.ontology_tag()} Mosaico type"
                )

        elif isinstance(mosaico_msg, cls.__mosaico_ontology_type__):
            data = mosaico_msg

        else:
            raise TypeError(
                f"Mosaico data passed to {cls.__name__} Adapter has type {type(mosaico_msg)} and it is neither a Message nor a {cls.__mosaico_ontology_type__.ontology_tag()}"
            )

        header = Header(frame_id="", timestamp=Time(seconds=0, nanoseconds=0))

        tmp = getattr(data, "header", None)

        if tmp:
            if isinstance(tmp, Header):
                header.frame_id = tmp.frame_id
                header.timestamp = tmp.timestamp

            else:
                raise TypeError(
                    f"Message {mosaico_msg.ontology_tag()} has a field called `header` that is not of type {Header.__class__.__name__}. Please rename it!"
                )

        return data, header

    @classmethod
    @abstractmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, T],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico message or ontology object back into a native ROS message.

        Args:
            mosaico_data: A ``Message`` wrapper or a raw ``Serializable`` ontology instance.
            typestore: The rosbags typestore used to resolve and construct target ROS types.
            ros_msg_type: Override for the output ROS type string. If ``None``, the adapter
                defaults to ``cls.get_default_ros_msg()``.

        Returns:
            The constructed ROS message instance, or raises an error if:
                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or resolved_rosmsg_type are unsupported by typestore (TypeError)
                - the ros_msg_type or resolved_rosmsg_type are supported but translation is not implemented (NotImplementedError)
        """
        pass

    @classmethod
    def schema_metadata(cls, typestore: Typestore, ros_msg_type: str) -> Optional[dict]:
        """
        Extracts ROS-specific schema metadata for the Mosaico platform.

        This allows preserving original ROS attributes that may not fit directly
        into the physical ontology fields like the original ros type or the constants.

        Args:
            typestore: The rosbags typestore used to resolve and construct target ROS types.
            ros_msg_type: The ros message type whose metadata should be extracted compatible with the adapter.

        Returns:
            The constructed dictionary compatible for Mosaico topic metadata. It contains:
            1) ros_msg_type constants (enums)
            2) the original message type in string format

            Returns ``None`` if the passed ros message type is unsupported by the adapter.

        For the BatteryStateAdapter the expected output is
        {
            "_ros_":
            {
                "enums":
                {
                    "POWER_SUPPLY_STATUS_UNKNOWN": 0,
                    "POWER_SUPPLY_STATUS_CHARGING": 1,
                    "POWER_SUPPLY_STATUS_DISCHARGING": 2,
                    "POWER_SUPPLY_STATUS_NOT_CHARGING": 3,
                    "POWER_SUPPLY_STATUS_FULL": 4,
                    "POWER_SUPPLY_HEALTH_UNKNOWN": 0,
                    "POWER_SUPPLY_HEALTH_GOOD": 1,
                    "POWER_SUPPLY_HEALTH_OVERHEAT": 2,
                    "POWER_SUPPLY_HEALTH_DEAD": 3,
                    "POWER_SUPPLY_HEALTH_OVERVOLTAGE": 4,
                    "POWER_SUPPLY_HEALTH_UNSPEC_FAILURE": 5,
                    "POWER_SUPPLY_HEALTH_COLD": 6,
                    "POWER_SUPPLY_HEALTH_WATCHDOG_TIMER_EXPIRE": 7,
                    "POWER_SUPPLY_HEALTH_SAFETY_TIMER_EXPIRE": 8,
                    "POWER_SUPPLY_TECHNOLOGY_UNKNOWN": 0,
                    "POWER_SUPPLY_TECHNOLOGY_NIMH": 1,
                    "POWER_SUPPLY_TECHNOLOGY_LION": 2,
                    "POWER_SUPPLY_TECHNOLOGY_LIPO": 3,
                    "POWER_SUPPLY_TECHNOLOGY_LIFE": 4,
                    "POWER_SUPPLY_TECHNOLOGY_NICD": 5,
                    "POWER_SUPPLY_TECHNOLOGY_LIMN": 6,
                },
                "msgtype": "sensor_msgs/msg/BatteryState"
            }
        }


        """
        # Check that ros_msg_type is handled by adapter
        if not cls.is_rosmsg_type_valid(ros_msg_type):
            return None

        # Check that ros_msg_type exists in typestore
        msg_def = typestore.fielddefs.get(ros_msg_type)

        if msg_def is None:
            return None

        # Extract ENUM associated to ros_msg_type and adding it to the out dict with the ros_msg_type
        enum_list, _ = msg_def
        out_dict = {"enums": {name: val for name, _, val in enum_list}}
        out_dict.update({"msgtype": ros_msg_type})

        ms_metadata = {"_ros_": out_dict}

        return ms_metadata

    @classmethod
    def ontology_data_type(cls) -> Type[T]:
        """Returns the Ontology class type associated with this adapter."""
        return cls.__mosaico_ontology_type__
