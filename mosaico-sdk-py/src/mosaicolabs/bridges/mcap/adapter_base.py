from abc import abstractmethod
from collections.abc import Hashable
from typing import Any, ClassVar, Dict, Generic, Optional, Tuple, Type, TypeVar, Union

from google.protobuf.message import Message as ProfobufMsg
from mcap.records import Message as MCAPRecordMessage

from mosaicolabs.models.core import Message as Message, Serializable

from ..base_schema_metadata import BaseSchemaMetadata
from ..bridge_adapter_base import BridgeAdapterBase
from .mcap_message import MCAPMessage


class MCAPSchemaMetadata(BaseSchemaMetadata):
    """
    Encapsulates Mosaico's reserved ``_mcap_`` topic-metadata namespace in a single place.

    Every topic ingested by the MCAP bridge carries MCAP-specific bookkeeping (original
    ``channel_name``, ``channel_encoding``, ``schema_name``, ``schema_encoding``, raw
    ``schema_def``) plus bridge-internal fields (e.g. the source mcap file) under one reserved
    key, so that:

    * The literal string ``"_mcap_"`` exists in exactly one place (:attr:`KEY`), instead of
      being duplicated across adapters, loaders, and the injector.
    * Callers build up this namespace incrementally via :meth:`update` without ever touching
      the wrapping dict shape by hand.

    Example:
        ```python
        meta = MCAPSchemaMetadata(channel_name="sensor_msgs.Imu").update(source_file="a.mcap")
        topic_metadata = meta.merge_into(user_supplied_metadata)
        # topic_metadata == {..., "_mcap_": {"channel_name": "sensor_msgs.Imu", "source_file": "a.mcap"}}
        ```
    """

    KEY: ClassVar[str] = "_mcap_"
    """The reserved metadata key. Adapters/loaders/the injector should reference this
    constant rather than the literal string, so the namespace can be renamed in one place."""


OntologyT = TypeVar("OntologyT", bound=Serializable)
# type of the object handled by to_mcap() that needs to be filled with data in Mosaico Message
McapT = TypeVar("McapT", Dict, ProfobufMsg)


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


class MCAPAdapterBase(
    BridgeAdapterBase[OntologyT, MCAPRecordMessage], Generic[OntologyT, McapT]
):
    """
    Abstract Base Class for converting MCAP messages to Mosaico Ontology types.

    The Adaptation Layer is the semantic core of the MCAP Bridge. Rather than
    performing simple parsing, adapters actively translate MCAP data into standardized,
    strongly-typed Mosaico Ontology objects.

    Attributes:
        schema_name: The MCAP schema name this adapter handles (e.g. ``"sensor_msgs/Imu"``).
        schema_encoding: The MCAP schema encoding this adapter handles (e.g. ``"protobuf"``).
        _REQUIRED_KEYS: Internal validation list for mandatory MCAP message fields.
    """

    schema_name: ClassVar[str]
    schema_encoding: ClassVar[str]
    skip_encoding_check: ClassVar[bool] = False
    _REQUIRED_KEYS: Tuple[str, ...]

    __mosaico_ontology_type__: Type[OntologyT]

    # --- API to be compliant with BridgeAdapterBase

    @classmethod
    def adapter_key(cls) -> Hashable:
        """
        Returns:
            Hashable: A unique key identifying this adapter, derived from its
            ``schema_name`` and ``schema_encoding``.
        """
        return hash(compute_mcap_msg_type(cls.schema_name, cls.schema_encoding))

    @classmethod
    @abstractmethod
    def from_dict(cls, mcap_data: dict) -> OntologyT:
        """
        Maps the raw MCAP dictionary to the Mosaico model.

        This method performs field validation and reconstruction.
        """
        pass

    @classmethod
    @abstractmethod
    def to_native(
        cls, mosaico_data: Union[Message, OntologyT], **kwargs
    ) -> MCAPRecordMessage:
        """
        Args:
            mosaico_data (Union[Message, T]): A ``Message`` wrapper or a raw ``Serializable``
                ontology instance to convert.
            **kwargs (Any): Extra arguments forwarded to :meth:`to_mcap`.

        Returns:
            MCAPRecordMessage: The native MCAP message obtained from ``mosaico_data``.
        """

        data, header = cls.unpack_mosaico_msg(mosaico_data)

        cls.to_mcap(data, **kwargs)

        return MCAPRecordMessage(
            channel_id=0, log_time=0, data=b"", publish_time=1, sequence=0
        )

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
    @abstractmethod
    def to_mcap(cls, mosaico_data: OntologyT, mcap_class: Type[McapT]) -> McapT:
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

    @classmethod
    def schema_metadata(
        cls, channel_name: str, channel_encoding: str, schema_def: str
    ) -> Optional[dict]:
        """
        Extract the MCAP message specific schema metadata, if any.

        Args:
            channel_name (str): The full name of the MCAP channel the adapter is associated to
            channel_encoding (str): The encoding of the MCAP channel the adapter is associated to
            schema_def (str): The string representation of the MCAP schema the adapter is
                associated to, as produced by the resolved `MCAPMsgDecoder.stringify_schema_def()`.

        Returns:
            Optional[dict]: A dictionary containing the schema metadata, or None if not applicable.

        """
        mcap_meta = MCAPSchemaMetadata(
            schema_name=cls.schema_name,
            schema_encoding=cls.schema_encoding,
            schema_def=schema_def,
            channel_name=channel_name,
            channel_encoding=channel_encoding,
        )

        return mcap_meta.to_dict()

    @classmethod
    def ontology_data_type(cls) -> Type[OntologyT]:
        """Returns the Ontology class type associated with this adapter."""
        return cls.__mosaico_ontology_type__


class MCAPAdapterBaseProtobuf(
    MCAPAdapterBase[OntologyT, ProfobufMsg], Generic[OntologyT]
):
    schema_encoding: ClassVar[str] = "protobuf"


class MCAPAdapterBaseJsonschema(MCAPAdapterBase[OntologyT, Dict], Generic[OntologyT]):
    schema_encoding: ClassVar[str] = "jsonschema"
