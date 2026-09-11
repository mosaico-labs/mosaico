from typing import Any, ClassVar, Dict

from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message as ProtobufMsg
from mcap.decoder import DecoderFactory
from mcap.records import Schema
from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory

from ..decoder_base import MCAPMsgDecoder
from ..registry import register_decoder
from .rules_matcher import ProtobufRulesMatcher


@register_decoder
class MCAPProtobufMsgDecoder(MCAPMsgDecoder):
    SUPPORTED_CHANNEL_ENCODING: ClassVar[str] = "protobuf"

    def __init__(self) -> None:
        super().__init__()
        self._descriptor_pool: DescriptorPool = DescriptorPool()

    def decoder_factory(self) -> DecoderFactory:
        return ProtobufDecoderFactory()

    def register_schema(self, schema: Schema) -> None:
        """Registers `schema`'s protobuf `FileDescriptorSet` into the pool so messages of this
        type can be decoded during iteration, then computes the per-field postprocessors this
        schema needs for PyArrow compliance. Skips schemas already registered in the pool."""
        try:
            descr = self._descriptor_pool.FindMessageTypeByName(schema.name)
        except KeyError:
            for file_proto in FileDescriptorSet.FromString(schema.data).file:
                self._descriptor_pool.Add(file_proto)
            descr = self._descriptor_pool.FindMessageTypeByName(schema.name)

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
        out = MessageToDict(
            msg_data,
            always_print_fields_with_no_presence=True,
            preserving_proto_field_name=True,
            use_integers_for_enums=True,
            descriptor_pool=None,
            unquote_int64_if_possible=True,
        )

        return out
