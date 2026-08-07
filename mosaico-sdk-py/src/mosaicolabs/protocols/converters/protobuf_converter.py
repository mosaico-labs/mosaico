from typing import ClassVar, Tuple

import pyarrow as pa
from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.descriptor_pb2 import (
    FileDescriptorSet,
)
from google.protobuf.descriptor_pool import DescriptorPool
from mcap.records import Schema

from mosaicolabs.protocols.base_converter import McapConverter
from mosaicolabs.protocols.mcap_bridge import McapBridge


@McapBridge.register
class ProtobufConverter(McapConverter):
    """Converts protobuf message descriptors into PyArrow types.

    Works on the *rich* descriptors from google.protobuf.descriptor (a Descriptor /
    FieldDescriptor), which resolve nested message and enum references for you.
    You get those by loading a FileDescriptorSet into a DescriptorPool -- exactly
    the pool you already build when decoding protobuf MCAP messages.
    """

    # One proto scalar value -> one Arrow type. The sint/sfixed/fixed variants
    # differ only in wire encoding, so they collapse onto the same Arrow width.
    _PROTOBUF_2_PYARROW_TYPE: ClassVar[dict[int, pa.DataType]] = {
        FieldDescriptor.TYPE_DOUBLE: pa.float64(),
        FieldDescriptor.TYPE_FLOAT: pa.float32(),
        FieldDescriptor.TYPE_INT32: pa.int32(),
        FieldDescriptor.TYPE_SFIXED32: pa.int32(),
        FieldDescriptor.TYPE_SINT32: pa.int32(),
        FieldDescriptor.TYPE_INT64: pa.int64(),
        FieldDescriptor.TYPE_SFIXED64: pa.int64(),
        FieldDescriptor.TYPE_SINT64: pa.int64(),
        FieldDescriptor.TYPE_UINT32: pa.uint32(),
        FieldDescriptor.TYPE_FIXED32: pa.uint32(),
        FieldDescriptor.TYPE_UINT64: pa.uint64(),
        FieldDescriptor.TYPE_FIXED64: pa.uint64(),
        FieldDescriptor.TYPE_BOOL: pa.bool_(),
        FieldDescriptor.TYPE_STRING: pa.string(),
        FieldDescriptor.TYPE_BYTES: pa.binary(),
        FieldDescriptor.TYPE_ENUM: pa.int32(),  # enum values are int32 on the wire
    }

    SUPPORTED_ENCODINGS: ClassVar[Tuple[str, ...]] = ("protobuf",)

    @classmethod
    def _base_type(cls, field: FieldDescriptor) -> pa.DataType | pa.StructType:
        """Arrow type for a single (non-repeated) value of this field."""
        if field.type in (FieldDescriptor.TYPE_MESSAGE, FieldDescriptor.TYPE_GROUP):
            assert (
                field.message_type is not None
            )  # FIXME: can this happen? What happens in this case?
            return cls._message_to_struct(field.message_type)
        try:
            return cls._PROTOBUF_2_PYARROW_TYPE[field.type]
        except KeyError:
            raise ValueError(f"Unmapped proto type {field.type} on {field.full_name}")

    @classmethod
    def _field_to_arrow(cls, field: FieldDescriptor) -> pa.Field:
        """Turn one protobuf FieldDescriptor into a pa.field()."""
        if field.is_repeated:
            arrow_type = pa.list_(cls._base_type(field))
            nullable = False  # a repeated field is an empty list, never null
        else:
            arrow_type = cls._base_type(field)
            nullable = True  # FIXME: should this be treted differently?

        return pa.field(field.name, arrow_type, nullable=nullable)

    @classmethod
    def _message_to_struct(cls, descriptor: Descriptor) -> pa.StructType:
        """A top-level message Descriptor -> pa.schema() (one column per field)."""
        return pa.struct([cls._field_to_arrow(f) for f in descriptor.fields])

    @classmethod
    def _convert(cls, mcap_schema: Schema) -> pa.StructType:
        """
        Abstract class implementation of McapConverter exploited when
        converting MCAPs using mcap library.
        TODO: improve documentation
        """

        return cls.convert_protobuf(mcap_schema.data, mcap_schema.name)

    @classmethod
    def convert_protobuf(cls, fds_bytes: bytes, msgtype: str) -> pa.StructType:
        """
        Conveverts a protobug MCAP schema into a pa.StrucType. Use this when
        you know at build time that you are dealing with protobuf encoding and
        you don't want to pass through the McapBridge returning a specialisation
        of McapConverter depending on the passed encoding at runtime.
        TODO: improve doc saying what are the args and the return args
        """

        pool = DescriptorPool()
        for file_proto in FileDescriptorSet.FromString(fds_bytes).file:
            pool.Add(file_proto)

        return cls._message_to_struct(pool.FindMessageTypeByName(msgtype))
