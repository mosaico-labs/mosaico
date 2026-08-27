from typing import ClassVar, Tuple

import pyarrow as pa
from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.descriptor_pb2 import (
    FileDescriptorSet,
)
from google.protobuf.descriptor_pool import DescriptorPool

from mcap.records import Schema

from ..base_converter import McapSchemaConverter
from ..registry import McapSchemaRegistry


@McapSchemaRegistry.register
class ProtobufSchemaConverter(McapSchemaConverter):
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
        Abstract class implementation of McapSchemaConverter exploited when
        converting MCAPs using mcap library.

        Args:
            mcap_schema: The MCAP ``Schema`` record whose ``data`` holds the raw,
                serialized ``FileDescriptorSet`` bytes to convert, and whose
                ``name`` is the fully-qualified message type it defines.

        Returns:
            A ``pa.StructType`` mirroring the message's fields (see ``convert_protobuf``).
        """

        return cls.convert_protobuf(mcap_schema.data, mcap_schema.name)

    @classmethod
    def convert_protobuf(cls, fds_bytes: bytes, msgtype: str) -> pa.StructType:
        """
        Converts a protobuf MCAP schema into a pa.StructType. Use this when
        you know at build time that you are dealing with protobuf encoding and
        you don't want to pass through the McapSchemaRegistry returning a specialisation
        of McapSchemaConverter depending on the passed encoding at runtime.

        Args:
            fds_bytes: Serialized ``FileDescriptorSet`` bytes (as found in the MCAP
                schema's ``data``) containing ``msgtype`` and any message/enum types
                it references.
            msgtype: Fully-qualified protobuf message name (e.g. ``"geometry_msgs.Point"``)
                to look up within ``fds_bytes`` and convert.

        Returns:
            A ``pa.StructType`` mirroring the top-level fields of ``msgtype``, with
            nested messages as nested structs and repeated fields as PyArrow lists.
        """

        pool = DescriptorPool()
        for file_proto in FileDescriptorSet.FromString(fds_bytes).file:
            pool.Add(file_proto)

        return cls._message_to_struct(pool.FindMessageTypeByName(msgtype))
