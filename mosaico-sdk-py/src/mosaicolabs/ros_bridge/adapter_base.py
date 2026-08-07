from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from rosbags.typesys.store import Typestore

if TYPE_CHECKING:
    from rosbags.typesys.store import MsgType

from mosaicolabs import Header, Time
from mosaicolabs.models.core import Message, Serializable

from .ros_message import ROSMessage

T = TypeVar("T", bound=Serializable)


class RosSchemaMetadata:
    """
    Encapsulates Mosaico's reserved ``_ros_`` topic-metadata namespace in a single place.

    Every topic ingested by the ROS bridge carries ROS-specific bookkeeping (original
    ``msgtype``, raw ``msgdef``, extracted ``enums``) plus bridge-internal fields (e.g. the
    source bag file) under one reserved key, so that:

    * The literal string ``"_ros_"`` exists in exactly one place (:attr:`KEY`), instead of
      being duplicated across adapters, loaders, and the injector.
    * Callers build up this namespace incrementally via :meth:`update` without ever touching
      the wrapping dict shape by hand, which is what previously caused bugs (e.g. accidentally
      overwriting the whole ``_ros_`` block instead of merging into it).

    Example:
        ```python
        meta = RosSchemaMetadata(msgtype="sensor_msgs/msg/Imu").update(source_file="a.mcap")
        topic_metadata = meta.merge_into(user_supplied_metadata)
        # topic_metadata == {..., "_ros_": {"msgtype": "sensor_msgs/msg/Imu", "source_file": "a.mcap"}}
        ```
    """

    KEY: ClassVar[str] = "_ros_"
    """The reserved metadata key. Adapters/loaders/the injector should reference this
    constant rather than the literal string, so the namespace can be renamed in one place."""

    def __init__(self, **fields: Any):
        self.fields: dict = dict(fields)

    def update(self, **fields: Any) -> "RosSchemaMetadata":
        """
        Merges additional fields into this block, in place. Returns `self` for chaining.

        Args:
            **fields (Any): Additional fields to merge.

        Returns:
            RosSchemaMetadata: The updated metadata instance.
        """
        self.fields.update(fields)
        return self

    def to_dict(self) -> dict:
        """
        Wraps the current fields under the reserved key, e.g. `{"_ros_": {...}}`.

        Returns:
            dict: A dictionary containing the `_ros_` block with the current fields.
        """
        return {self.KEY: dict(self.fields)}

    def merge_into(self, metadata: dict) -> dict:
        """
        Merges this block into an existing metadata dict's `_ros_` namespace, creating it
        if absent. Mutates and returns `metadata`.

        Args:
            metadata (dict): The existing metadata dict to merge into.

        Returns:
            dict: The updated metadata dict with the `_ros_` block merged in.
        """
        metadata.setdefault(self.KEY, {}).update(self.fields)
        return metadata

    @classmethod
    def extract(cls, metadata: Optional[dict]) -> dict:
        """
        Reads the `_ros_` block out of a metadata dict (e.g. a topic's `user_metadata`),
        or `{}` if absent.

        Args:
            metadata (Optional[dict]): A metadata dict, typically `{"_ros_": {...}}` or `None`.

        Returns:
            dict: The extracted `_ros_` block, or an empty dict if not present.
        """
        return dict((metadata or {}).get(cls.KEY) or {})

    @classmethod
    def from_dict(cls, metadata: Optional[dict]) -> "RosSchemaMetadata":
        """
        Creates a `RosSchemaMetadata` from a plain metadata dict, e.g. the return value of
        `ROSAdapterBase.schema_metadata()`. Any keys outside the `_ros_` namespace are ignored.

        Args:
            metadata (Optional[dict]): A metadata dict, typically `{"_ros_": {...}}` or `None`.

        Returns:
            RosSchemaMetadata: A new instance seeded with the extracted `_ros_` fields
                (empty if `metadata` is `None` or carries no `_ros_` block).
        """
        return cls(**cls.extract(metadata))


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
            ros_msg (ROSMessage): The source container yielded by the ROSLoader.
            **kwargs (Any): Contextual data such as calibration parameters or frame overrides.

        Returns:
            Message: A Mosaico Message object containing the instantiated ontology data.

        Raises:
            Exception: If translation fails due to missing fields, type mismatches, or other errors.
        """
        if ros_msg.data_field is None:
            raise Exception(f"'data' payload is `None` for topic {ros_msg.topic}.")

        try:
            return Message(
                timestamp_ns=ros_msg.bag_timestamp_ns,
                data=cls.from_dict(ros_msg.data_field),
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
            type_to_validate (str): The full ROS message type string to check
                (e.g., ``"sensor_msgs/msg/Imu"``).

        Returns:
            bool: `True` if the adapter supports this type, ``False`` otherwise.
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
            mosaico_msg (Union[Message, T]): Either a ``Message`` envelope or a raw instance of
                ``cls.__mosaico_ontology_type__``.

        Returns:
            tuple[T, Header]: A ``(data, header)`` tuple where *data* is the typed ontology object and
            *header* is the corresponding ``Header``, or a default ``Header`` (empty ``frame_id`` and
            zero ``Time``) if not present.

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
            mosaico_data (Union[Message, T]): A ``Message`` wrapper or a raw ``Serializable`` ontology instance.
            typestore (Typestore): The rosbags typestore used to resolve and construct target ROS types.
            ros_msg_type (Optional[str]): Override for the output ROS type string. If ``None``, the adapter
                defaults to ``cls.get_default_ros_msg()``.

        Returns:
            MsgType: The constructed ROS message instance, or raises an error if:

                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or resolved_rosmsg_type are unsupported by typestore (TypeError)
                - the ros_msg_type or resolved_rosmsg_type are supported but translation is not implemented (NotImplementedError)
        """
        raise NotImplementedError(
            f"{cls.__name__} does not implement to_ros(). Unable to encode the Mosaico Message to ROS"
        )

    @classmethod
    def schema_metadata(
        cls, typestore: Typestore, ros_msg_type: str, ros_version: int
    ) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.

        Args:
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (str): The ROS message type to extract metadata for.
            ros_version (int): The ROS version (1 or 2) to consider for metadata extraction.

        Returns:
            Optional[dict]: A dictionary containing the schema metadata, or None if not applicable.

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
                "msgdef": "..."
            }
        }


        """
        # Check that ros_msg_type is handled by adapter
        if not cls.is_rosmsg_type_valid(ros_msg_type):
            return None

        ros_meta = RosSchemaMetadata(msgtype=ros_msg_type)

        # Check that ros_msg_type exists in typestore
        msg_def = typestore.fielddefs.get(ros_msg_type)

        # Extract ENUM associated to ros_msg_type and adding it to the out dict (if available in typestore)
        if msg_def:
            enum_list, _ = msg_def
            msgdef, _ = typestore.generate_msgdef(ros_msg_type, ros_version=ros_version)
            ros_meta.update(
                enums={name: val for name, _, val in enum_list},
                msgdef=msgdef,
            )

        return ros_meta.to_dict()

    @classmethod
    def ontology_data_type(cls) -> Type[T]:
        """Returns the Ontology class type associated with this adapter."""
        return cls.__mosaico_ontology_type__
