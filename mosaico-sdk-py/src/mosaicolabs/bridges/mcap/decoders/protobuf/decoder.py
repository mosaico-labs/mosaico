import base64
from typing import Any, ClassVar, Dict, Type

from google.protobuf.descriptor import Descriptor
from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message as ProtobufMsg
from google.protobuf.message_factory import GetMessageClass
from mcap.decoder import DecoderFactory
from mcap.records import Schema
from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory

from ..decoder_base import MCAPMsgDecoder
from ..registry import register_decoder
from .rules_matcher import ProtobufRulesMatcher


@register_decoder
class MCAPProtobufMsgDecoder(MCAPMsgDecoder[ProtobufMsg]):
    SUPPORTED_CHANNEL_ENCODING: ClassVar[str] = "protobuf"

    def __init__(self) -> None:
        super().__init__()
        self._descriptor_pool: DescriptorPool = DescriptorPool()

    @staticmethod
    def decoder_factory() -> DecoderFactory:
        return ProtobufDecoderFactory()

    def _get_schema_descriptor(self, schema_name: str, schema_def: bytes) -> Descriptor:
        """TODO: understand whether this should be a registration or not since you are updating the _descriptor_pool"""
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

        self._field_rule_mapper = ProtobufRulesMatcher.from_descriptor(
            descr
        )  # FIXME: should this be done here or be done explicitly by the user?

    def get_schema_class(
        self, schema_name: str, schema_def: bytes
    ) -> Type[ProtobufMsg]:
        """Returns the Python Object type eveloping the data. For protobuf encoding this"""
        descr = self._get_schema_descriptor(schema_name, schema_def)

        return GetMessageClass(descr)

    @staticmethod
    def stringify_schema_def(schema_def: bytes) -> str:
        """`schema_def` is a binary serialized `FileDescriptorSet`, not valid text, so it is
        base64-encoded: the only encoding that is always an exact inverse of
        `destringify_schema_def`, regardless of the descriptor's byte content."""
        return base64.b64encode(schema_def).decode("ascii")

    @staticmethod
    def destringify_schema_def(schema_def_str: str) -> bytes:
        """Opposite of stringify_schema_def. Returns the bytes value of the stringified schema definition.
        Function output can be passed directly to FileDescriptorSet.FromString() to reconstruct the FileDescriptorSet"""
        return base64.b64decode(schema_def_str)

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
