import json
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Generic, Optional, Tuple, Type, TypeVar, Union

from google.protobuf.message import Message as ProtobufMsg

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

    * The literal string ``"_mcap_"`` exists in exactly one place
      ([`KEY`][mosaicolabs.bridges.mcap.adapter_base.MCAPSchemaMetadata.KEY]), instead of
      being duplicated across adapters, loaders, and the injector.
    * Callers build up this namespace incrementally via
      [`update`][mosaicolabs.bridges.base_schema_metadata.BaseSchemaMetadata.update] without ever touching
      the wrapping dict shape by hand.

    Example:
        ```python
        # Every key in REQUIRED_KEYS must be provided, otherwise a ValueError is raised
        meta = MCAPSchemaMetadata(
            channel_name="/imu",
            channel_encoding="protobuf",
            schema_name="sensor_msgs.Imu",
            schema_encoding="protobuf",
            schema_def="<stringified schema definition>",
        ).update(source_file="a.mcap")
        topic_metadata = meta.merge_into(user_supplied_metadata)
        # topic_metadata == {..., "_mcap_": {"channel_name": "/imu", ..., "source_file": "a.mcap"}}
        ```
    """

    KEY: ClassVar[str] = "_mcap_"
    """The reserved metadata key. Adapters/loaders/the injector should reference this
    constant rather than the literal string, so the namespace can be renamed in one place."""

    REQUIRED_KEYS: ClassVar[Dict[str, Type]] = {
        "schema_name": str,
        "schema_encoding": str,
        "schema_def": str,
        "channel_name": str,
        "channel_encoding": str,
    }
    """The keys that need to be present in order to create the MCAPSchemaMetadata."""

    def __init__(self, **fields: Any):
        super().__init__(**fields)

    def get_channel_name(self) -> str:
        """Returns the channel name contained in the metadata."""
        return self.fields["channel_name"]

    def get_channel_encoding(self) -> str:
        """Returns the channel encoding contained in the metadata."""
        return self.fields["channel_encoding"]

    def get_schema_name(self) -> str:
        """Returns the schema name contained in the metadata."""
        return self.fields["schema_name"]

    def get_schema_encoding(self) -> str:
        """Returns the schema encoding contained in the metadata."""
        return self.fields["schema_encoding"]

    def get_schema_def(self) -> str:
        """Returns the schema def contained in the metadata."""
        return self.fields["schema_def"]

    def get_sequence_id(self) -> Optional[int]:
        """Returns the sequence id contained in the metadata, or None if not present."""
        # FIXME: the MCAP `sequence` is a per-message counter, but a single value is stored
        # per topic (see `MCAPAdapterBase.schema_metadata`). Unused until it is stored per
        # message.
        return self.fields.get("sequence_id")


OntologyT = TypeVar("OntologyT", bound=Serializable)
# type of the object handled by to_mcap() that needs to be filled with data in Mosaico Message
McapT = TypeVar("McapT", Dict, ProtobufMsg)


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


@dataclass(frozen=True)
class McapReturnType:
    """
    Container returned by [`MCAPAdapterBase.to_native`][mosaicolabs.bridges.mcap.adapter_base.MCAPAdapterBase.to_native].

    Attributes:
        data_bytes: The native MCAP message, serialized according to the adapter's `schema_encoding`.
        publish_time_ns: The message publish time (nanoseconds) returned by `to_mcap()`, or `None` if not available.
    """

    data_bytes: bytes
    publish_time_ns: Optional[int]


class MCAPAdapterBase(
    BridgeAdapterBase[OntologyT, McapReturnType], Generic[OntologyT, McapT]
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
    _REQUIRED_KEYS: Tuple[str, ...]

    __mosaico_ontology_type__: Type[OntologyT]

    # --- API to be compliant with BridgeAdapterBase
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
                timestamp_ns=msg.log_time_ns,
                data=cls.from_dict(msg.data_field, msg.publish_time_ns),
            )
        except Exception as e:
            raise Exception(f"Translation failed for {msg.schema_name}: {e}")

    @classmethod
    @abstractmethod
    def from_dict(cls, mcap_data: dict, publish_time_ns: int) -> OntologyT:
        """
        Maps the raw MCAP dictionary to the Mosaico model.

        This method performs field validation and reconstruction.

        Args:
            mcap_data (dict): The decoded MCAP message payload.
            publish_time_ns (int): The message publish time (nanoseconds).

        Returns:
            OntologyT: The constructed ontology instance.

        Raises:
            NotImplementedError: If the adapter does not override this method.
        """
        raise NotImplementedError(
            f"{cls.__name__} does not implement from_dict(). Unable to encode the MCAP message to Mosaico Message with {cls.schema_encoding} schema encoding"
        )

    @classmethod
    def to_native(
        cls, mosaico_data: Union[Message, OntologyT], **kwargs
    ) -> McapReturnType:
        """
        Converts a Mosaico message or ontology object into a serialized native MCAP message.

        Args:
            mosaico_data (Union[Message, OntologyT]): A `Message` wrapper or a raw `Serializable`
                ontology instance to convert.
            **kwargs (Any): Extra arguments forwarded to
                [`to_mcap`][mosaicolabs.bridges.mcap.adapter_base.MCAPAdapterBase.to_mcap].

        Returns:
            McapReturnType: The native MCAP message serialized according to `schema_encoding`
                (protobuf wire bytes or UTF-8 JSON), plus the publish time returned by `to_mcap()`.

        Raises:
            TypeError: If `to_mcap()` returns a type not matching `schema_encoding`.
            NotImplementedError: If `schema_encoding` is neither `"protobuf"` nor `"jsonschema"`,
                or if the adapter does not implement `to_mcap()`.
        """

        data, _ = cls.unpack_mosaico_msg(mosaico_data)

        result, publish_time_ns = cls.to_mcap(data, **kwargs)

        # FIXME: required to change this if/else and find a suitable location for this procedure
        # You need to turn to_mcap() output into `bytes`. This depends on the current encoding
        if cls.schema_encoding == "protobuf":
            if not isinstance(result, ProtobufMsg):
                raise TypeError(
                    f"Type mismatch in {cls.__name__} adapter. \
                      Adapter supports `{cls.schema_encoding}` encoding expecting `{ProtobufMsg.__name__}` type \
                      from `to_mcap()`. However it returned `{type(result).__name__}` type"
                )

            bytes_result: bytes = result.SerializeToString()

        elif cls.schema_encoding == "jsonschema":
            if not isinstance(result, Dict):
                raise TypeError(
                    f"Type mismatch in {cls.__name__} adapter. \
                      Adapter supports `{cls.schema_encoding}` encoding expecting `{Dict.__name__}` type \
                      from `to_mcap()`. However it returned `{type(result).__name__}` type"
                )

            bytes_result = json.dumps(result).encode("utf-8")

        else:
            raise NotImplementedError(
                f"Adapter with {cls.schema_encoding} encoding does not support `to_native()` yet."
            )

        return McapReturnType(bytes_result, publish_time_ns)

    # --- Custom API specific for MCAP adapter
    @classmethod
    @abstractmethod
    def to_mcap(cls, mosaico_data: OntologyT) -> Tuple[McapT, Optional[int]]:
        """
        Converts a Mosaico ontology object back into a native MCAP message.

        Args:
            mosaico_data (OntologyT): The ontology instance to convert.

        Returns:
            Tuple[McapT, Optional[int]]: The native MCAP message (a protobuf `Message` or a plain
                `Dict`, depending on `schema_encoding`) and its publish time (nanoseconds), or
                `None` if not available.

        Raises:
            NotImplementedError: If the adapter does not override this method.
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
            encoding_to_validate (str): The full MCAP encoding string to check
                (e.g., `"protobuf"`).
            schema_name_to_validate (str): The full MCAP schema name string to check
                (e.g., `"foxglove.Imu"`).

        Returns:
            bool: `True` if the adapter supports this type, `False` otherwise.
        """

        return (
            schema_name_to_validate == cls.schema_name
            and encoding_to_validate == cls.schema_encoding
        )

    @classmethod
    def schema_metadata(
        cls, channel_name: str, channel_encoding: str, schema_def: str, sequence_id: int
    ) -> Optional[dict]:
        """
        Builds the MCAP-specific schema metadata for this adapter.

        Args:
            channel_name (str): The full name of the MCAP channel the adapter is associated to.
            channel_encoding (str): The encoding of the MCAP channel the adapter is associated to.
            schema_def (str): The string representation of the MCAP schema the adapter is
                associated to, as produced by the resolved `McapSchemaConverter.stringify_schema_def()`.
            sequence_id (int): Message counter assigned by publisher. Set to 0 if not available.

        Returns:
            Optional[dict]: The schema metadata, wrapped under the
                [`MCAPSchemaMetadata.KEY`][mosaicolabs.bridges.mcap.adapter_base.MCAPSchemaMetadata.KEY] namespace.

        """
        mcap_meta = MCAPSchemaMetadata(
            schema_name=cls.schema_name,
            schema_encoding=cls.schema_encoding,
            schema_def=schema_def,
            channel_name=channel_name,
            channel_encoding=channel_encoding,
            # FIXME: the MCAP `sequence` is a per-message counter, but this metadata is built
            # once per topic, so only the first message's value is kept. Store it per message
            # instead (e.g. like `publish_time_ns`).
            # sequence_id=sequence_id,
        )

        return mcap_meta.to_dict()

    @classmethod
    def ontology_data_type(cls) -> Type[OntologyT]:
        """Returns the Ontology class type associated with this adapter."""
        return cls.__mosaico_ontology_type__
