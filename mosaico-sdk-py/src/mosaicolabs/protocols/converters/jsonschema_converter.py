# from typing import ClassVar, Tuple

# import pyarrow as pa

# from mcap.records import Schema

# from mosaicolabs.protocols.base_converter import McapConverter
# from mosaicolabs.protocols.mcap_bridge import McapBridge


# @McapBridge.register
# class JsonschemaConverter(McapConverter):
#     """Converts jsonschema message descriptors into PyArrow types.
#     TODO
#     """

#     # One proto scalar value -> one Arrow type. The sint/sfixed/fixed variants
#     # differ only in wire encoding, so they collapse onto the same Arrow width.
#     _JSONSCHEMA_2_PYARROW_TYPE: ClassVar[dict[int, pa.DataType]] = {
#         FieldDescriptor.TYPE_DOUBLE: pa.float64(),
#         FieldDescriptor.TYPE_FLOAT: pa.float32(),
#         FieldDescriptor.TYPE_INT32: pa.int32(),
#         FieldDescriptor.TYPE_SFIXED32: pa.int32(),
#         FieldDescriptor.TYPE_SINT32: pa.int32(),
#         FieldDescriptor.TYPE_INT64: pa.int64(),
#         FieldDescriptor.TYPE_SFIXED64: pa.int64(),
#         FieldDescriptor.TYPE_SINT64: pa.int64(),
#         FieldDescriptor.TYPE_UINT32: pa.uint32(),
#         FieldDescriptor.TYPE_FIXED32: pa.uint32(),
#         FieldDescriptor.TYPE_UINT64: pa.uint64(),
#         FieldDescriptor.TYPE_FIXED64: pa.uint64(),
#         FieldDescriptor.TYPE_BOOL: pa.bool_(),
#         FieldDescriptor.TYPE_STRING: pa.string(),
#         FieldDescriptor.TYPE_BYTES: pa.binary(),
#         FieldDescriptor.TYPE_ENUM: pa.int32(),  # enum values are int32 on the wire
#     }

#     SUPPORTED_ENCODINGS: ClassVar[Tuple[str, ...]] = ("jsonschema",)

#     @classmethod
#     def _convert(cls, mcap_schema: Schema) -> pa.StructType:
#         """
#         Abstract class implementation of McapConverter exploited when
#         converting MCAPs using mcap library.
#         TODO: improve documentation
#         """

#         return cls.convert_jsonschema(mcap_schema.data, mcap_schema.name)

#     @classmethod
#     def convert_jsonschema(cls, fds_bytes: bytes, msgtype: str) -> pa.StructType:
#         """
#         Conveverts a protobug MCAP schema into a pa.StrucType. Use this when
#         you know at build time that you are dealing with jsonschema encoding and
#         you don't want to pass through the McapBridge returning a specialisation
#         of McapConverter depending on the passed encoding at runtime.
#         TODO: improve doc saying what are the args and the return args
#         """


#         return cls._message_to_struct(pool.FindMessageTypeByName(msgtype))
