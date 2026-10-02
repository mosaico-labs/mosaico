from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor
FORMAT_DEFAULT: Format
FORMAT_IMAGE: Format
FORMAT_RAGGED: Format
FORMAT_UNSPECIFIED: Format

class Empty(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Format(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
