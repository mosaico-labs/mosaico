from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class TimestampRange(_message.Message):
    __slots__ = ["end_ns", "start_ns"]
    END_NS_FIELD_NUMBER: _ClassVar[int]
    START_NS_FIELD_NUMBER: _ClassVar[int]
    end_ns: int
    start_ns: int
    def __init__(self, start_ns: _Optional[int] = ..., end_ns: _Optional[int] = ...) -> None: ...
