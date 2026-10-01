from typing import Any, ClassVar, Dict

from google.protobuf.descriptor import Descriptor
from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.json_format import (
    _Printer,  # pyright: ignore[reportAttributeAccessIssue]
)
from google.protobuf.message import Message as ProtobufMsg
from mcap.decoder import DecoderFactory
from mcap.records import Schema
from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory

from ..decoder_base import MCAPMsgDecoderBase
from ..registry import register_decoder
from .rules_matcher import ProtobufRulesMatcher


# NOTE: temporary solution. `MscoPrinter` subclasses protobuf's private `_Printer`
# (the class behind `MessageToDict`) only to keep `google.protobuf.Any` as its original bytes
# instead of unpacking it. A home-made `MessageToDict` will be implemented to replace it.
class MscoPrinter(_Printer):
    """`MessageToDict` printer that keeps `google.protobuf.Any` as its own raw fields instead of
    unpacking it: `value` stays the packed message's original wire bytes, so the packed type
    (usually not even part of the channel's schema) never needs resolving."""

    def _AnyMessageToJsonObject(self, message):
        return {"type_url": message.type_url, "value": message.value}


@register_decoder
class MCAPProtobufMsgDecoder(MCAPMsgDecoderBase[ProtobufMsg]):
    SUPPORTED_CHANNEL_ENCODING: ClassVar[str] = "protobuf"

    def __init__(self) -> None:
        super().__init__()
        self._descriptor_pool: DescriptorPool = DescriptorPool()
        self._printer: MscoPrinter = MscoPrinter(
            preserving_proto_field_name=True,
            use_integers_for_enums=True,
            always_print_fields_with_no_presence=True,
            unquote_int64_if_possible=True,
        )

    @staticmethod
    def decoder_factory() -> DecoderFactory:
        return ProtobufDecoderFactory()

    def _get_schema_descriptor(self, schema_name: str, schema_def: bytes) -> Descriptor:
        """Extracts the schema descriptor from local descriptor pool if exists. Conversely, it
        first register the new schema and then return the descriptor"""
        try:
            descr = self._descriptor_pool.FindMessageTypeByName(schema_name)
        except KeyError:
            for file_proto in FileDescriptorSet.FromString(schema_def).file:
                self._descriptor_pool.Add(file_proto)
            descr = self._descriptor_pool.FindMessageTypeByName(schema_name)

        return descr

    def register_schema(self, schema: Schema) -> None:
        """Registers `schema`'s protobuf `FileDescriptorSet` into the pool so messages of this
        type can be decoded during iteration, then computes the per-field postprocessors this
        schema needs for PyArrow compliance. Skips schemas already registered in the pool."""
        descr = self._get_schema_descriptor(schema.name, schema.data)

        self._field_rule_mapper = ProtobufRulesMatcher.from_descriptor(descr)

    def _to_dict(self, msg_data: Any) -> Dict[str, Any]:

        # Check decoded message data type is supported
        if not isinstance(msg_data, ProtobufMsg):
            raise RuntimeError(
                f"{MCAPProtobufMsgDecoder.__name__} cannot decode messages that are not {ProtobufMsg.__name__}.\
                                 Provided message is of type {type(msg_data).__name__}"
            )

        # NOTE: This conversion is controlled for uint64 and int64 staying between [-2^53, +2^53].
        # whatever is outside this range is not guaranteed to be converted to a Python int but will
        # rather turn into a `string`, breaking the system.
        out = self._printer._MessageToJsonObject(msg_data)

        return out
