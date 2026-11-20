from google.protobuf import any_pb2 as _any_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Variant(_message.Message):
    __slots__ = ["bool_value", "details", "int_value", "text_value"]
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    INT_VALUE_FIELD_NUMBER: _ClassVar[int]
    TEXT_VALUE_FIELD_NUMBER: _ClassVar[int]
    bool_value: bool
    details: _any_pb2.Any
    int_value: int
    text_value: str
    def __init__(self, text_value: _Optional[str] = ..., int_value: _Optional[int] = ..., bool_value: bool = ..., details: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...
