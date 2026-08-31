from abc import abstractmethod
from collections.abc import Hashable
from typing import Any, ClassVar, Optional, Tuple, Type, TypeVar, Union

from mcap.records import Message as MCAPRecordMessage
from mcap.well_known import SchemaEncoding

from mosaicolabs import Header, Time
from mosaicolabs.models.core import Message as Message, Serializable

from ..bridge_adapter_base import BridgeAdapterBase
from .mcap_message import MCAPMessage

T = TypeVar("T", bound=Serializable)


def compute_mcap_msg_type(schema_name: str, schema_encoding: str) -> str:
    """
    Returns the specific combination of the schema name + schema encoding,
    defining the MCAP message type handled by this adapter.

    Args:
        schema_name (str): the schema name of the MCAP message.
        schema_encoding (str): the schema encoding of the MCAP message.

    Returns:
        str: The unique combination for passed schema_name and schema_encoding.

    """
    return f"{schema_name}__{schema_encoding}"


class MCAPAdapterBase(BridgeAdapterBase[T, MCAPRecordMessage]):
    """
    Abstract Base Class for converting MCAP messages to Mosaico Ontology types.

    The Adaptation Layer is the semantic core of the MCAP Bridge. Rather than
    performing simple parsing, adapters actively translate MCAP data into standardized,
    strongly-typed Mosaico Ontology objects.

    Attributes:
        schema_name: TODO instructions for the LLM: give a short explanation for the parameter
        schema_encoding: TODO instructions for the LLM: give a short explanation for the parameter
        _REQUIRED_KEYS: TODO: understand whether this is useful or not
        __mosaico_supported_schema_encodings__: TODO instructions for the LLM: give a short explanation for the parameter
        __mosaico_ontology_type__: The target Mosaico class (e.g., IMU).
    """

    schema_name: ClassVar[str]
    schema_encoding: ClassVar[str]
    skip_encoding_check: ClassVar[bool] = False
    _REQUIRED_KEYS: Tuple[str, ...]

    # TODO: information for the LLM: should this information be placed within a different file since
    # it could also be used to check that the created MCAPMessage has an encoding among the supported one?
    # or should the user be able to create whatever MCAPMessage even if it is not handled by the system and be stopped
    # somewhere else in the code?
    __mosaico_supported_schema_encodings__: ClassVar[Tuple[str, ...]] = (
        SchemaEncoding.Protobuf,
        SchemaEncoding.JSONSchema,
        SchemaEncoding.ROS2,
        SchemaEncoding.ROS2IDL,
        SchemaEncoding.ROS1,
    )
    __mosaico_ontology_type__: Type[T]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Check that class encoding exists and is contained within MSCO_SUPPORTED_ENCODINGS

        if not cls.skip_encoding_check:
            if (
                getattr(cls, "schema_encoding", None) is None
                or cls.schema_encoding not in cls.__mosaico_supported_schema_encodings__
            ):
                raise TypeError(
                    f"{cls.__name__} must defined a non-empty SUPPORTED_ENCODINGS"
                )

    # --- API to be compliant with BridgeAdapterBase

    @classmethod
    def adapter_key(cls) -> Hashable:
        return hash(compute_mcap_msg_type(cls.schema_name, cls.schema_encoding))

    @classmethod
    @abstractmethod
    def from_dict(cls, mcap_data: dict) -> T:
        """
        Maps the raw MCAP dictionary to the Mosaico model.

        This method performs field validation and reconstruction.
        """
        pass

    @classmethod
    @abstractmethod
    def to_native(cls, mosaico_data: Union[Message, T], **kwargs) -> MCAPRecordMessage:
        return cls.to_mcap(mosaico_data, **kwargs)

    @classmethod
    def translate(cls, msg: MCAPMessage, **kwargs: Any) -> Message:
        """
        Translates a MCAP message instance into a Mosaico Message.

        Args:
            msg (MCAPMessage): The source container yielded by the MCAPLoader.
            **kwargs (Any): Contextual data such as calibration parameters or frame overrides.

        Returns:
            Message: A Mosaico Message object containing the instantiated ontology data.

        Raises:
            Exception: If translation fails due to missing fields, type mismatches, or other errors.
        """
        if msg.data_field is None:
            raise Exception(f"'data' payload is `None` for schema {msg.schema_name}.")

        try:
            return Message(
                timestamp_ns=msg.publish_time_ns,
                data=cls.from_dict(msg.data_field),
            )
        except Exception as e:
            raise Exception(f"Translation failed for {msg.schema_name}: {e}")

    # --- Custom API specific for MCAP adapter

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
    def to_mcap(
        cls,
        mosaico_data: Union[Message, T],
    ) -> MCAPRecordMessage:
        """
        Converts a Mosaico message or ontology object back into a native MCAP message.

        Args:
            mosaico_data (Union[Message, T]): A ``Message`` wrapper or a raw ``Serializable`` ontology instance.

        Returns:
            MCAPRecordMessage: The constructed ROS message instance, or raises an error if:
        """
        raise NotImplementedError(
            f"{cls.__name__} does not implement to_mcap(). Unable to encode the Mosaico Message to MCAP with {cls.schema_encoding} schema encoding"
        )

    @classmethod
    def is_mcap_type_valid(
        cls, encoding_to_validate: str, schema_name_to_validate: str
    ) -> bool:
        """
        Checks whether a given MCAP message type (schema_name, schema_encoding) is
        handled by this adapter.

        Args:
            schema_name_to_validate (str): The full MCAP schema name string to check
                                        (e.g., ``"Foxglove.Imu``).
            encoding_to_validate (str): The full MCAP encoding string to check
                (e.g., ``"protobuf``).

        Returns:
            bool: `True` if the adapter supports this type, ``False`` otherwise.
        """

        return (
            schema_name_to_validate == cls.schema_name
            and encoding_to_validate == cls.schema_encoding
        )

    # @classmethod
    # def schema_metadata(
    #     cls, typestore: Typestore, ros_msg_type: str, ros_version: int
    # ) -> Optional[dict]:
    #     """
    #     Extract the ROS message specific schema metadata, if any.

    #     Args:
    #         typestore (Typestore): The rosbags typestore for target type resolution.
    #         ros_msg_type (str): The ROS message type to extract metadata for.
    #         ros_version (int): The ROS version (1 or 2) to consider for metadata extraction.

    #     Returns:
    #         Optional[dict]: A dictionary containing the schema metadata, or None if not applicable.

    #     For the BatteryStateAdapter the expected output is
    #     {
    #         "_ros_":
    #         {
    #             "enums":
    #             {
    #                 "POWER_SUPPLY_STATUS_UNKNOWN": 0,
    #                 "POWER_SUPPLY_STATUS_CHARGING": 1,
    #                 "POWER_SUPPLY_STATUS_DISCHARGING": 2,
    #                 "POWER_SUPPLY_STATUS_NOT_CHARGING": 3,
    #                 "POWER_SUPPLY_STATUS_FULL": 4,
    #                 "POWER_SUPPLY_HEALTH_UNKNOWN": 0,
    #                 "POWER_SUPPLY_HEALTH_GOOD": 1,
    #                 "POWER_SUPPLY_HEALTH_OVERHEAT": 2,
    #                 "POWER_SUPPLY_HEALTH_DEAD": 3,
    #                 "POWER_SUPPLY_HEALTH_OVERVOLTAGE": 4,
    #                 "POWER_SUPPLY_HEALTH_UNSPEC_FAILURE": 5,
    #                 "POWER_SUPPLY_HEALTH_COLD": 6,
    #                 "POWER_SUPPLY_HEALTH_WATCHDOG_TIMER_EXPIRE": 7,
    #                 "POWER_SUPPLY_HEALTH_SAFETY_TIMER_EXPIRE": 8,
    #                 "POWER_SUPPLY_TECHNOLOGY_UNKNOWN": 0,
    #                 "POWER_SUPPLY_TECHNOLOGY_NIMH": 1,
    #                 "POWER_SUPPLY_TECHNOLOGY_LION": 2,
    #                 "POWER_SUPPLY_TECHNOLOGY_LIPO": 3,
    #                 "POWER_SUPPLY_TECHNOLOGY_LIFE": 4,
    #                 "POWER_SUPPLY_TECHNOLOGY_NICD": 5,
    #                 "POWER_SUPPLY_TECHNOLOGY_LIMN": 6,
    #             },
    #             "msgtype": "sensor_msgs/msg/BatteryState"
    #             "msgdef": "..."
    #         }
    #     }

    #     """
    #     # Check that ros_msg_type is handled by adapter
    #     if not cls.is_rosmsg_type_valid(ros_msg_type):
    #         return None

    #     ros_meta = RosSchemaMetadata(msgtype=ros_msg_type)

    #     # Check that ros_msg_type exists in typestore
    #     msg_def = typestore.fielddefs.get(ros_msg_type)

    #     # Extract ENUM associated to ros_msg_type and adding it to the out dict (if available in typestore)
    #     if msg_def:
    #         enum_list, _ = msg_def
    #         msgdef, _ = typestore.generate_msgdef(ros_msg_type, ros_version=ros_version)
    #         ros_meta.update(
    #             enums={name: val for name, _, val in enum_list},
    #             msgdef=msgdef,
    #         )

    #     return ros_meta.to_dict()

    @classmethod
    def ontology_data_type(cls) -> Type[T]:
        """Returns the Ontology class type associated with this adapter."""
        return cls.__mosaico_ontology_type__
