from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Header(_message.Message):
    __slots__ = ["frame_id", "stamp"]
    FRAME_ID_FIELD_NUMBER: _ClassVar[int]
    STAMP_FIELD_NUMBER: _ClassVar[int]
    frame_id: str
    stamp: Stamp
    def __init__(self, stamp: _Optional[_Union[Stamp, _Mapping]] = ..., frame_id: _Optional[str] = ...) -> None: ...

class Quaternion(_message.Message):
    __slots__ = ["w", "x", "y", "z"]
    W_FIELD_NUMBER: _ClassVar[int]
    X_FIELD_NUMBER: _ClassVar[int]
    Y_FIELD_NUMBER: _ClassVar[int]
    Z_FIELD_NUMBER: _ClassVar[int]
    w: float
    x: float
    y: float
    z: float
    def __init__(self, x: _Optional[float] = ..., y: _Optional[float] = ..., z: _Optional[float] = ..., w: _Optional[float] = ...) -> None: ...

class Stamp(_message.Message):
    __slots__ = ["nanosec", "sec"]
    NANOSEC_FIELD_NUMBER: _ClassVar[int]
    SEC_FIELD_NUMBER: _ClassVar[int]
    nanosec: int
    sec: int
    def __init__(self, sec: _Optional[int] = ..., nanosec: _Optional[int] = ...) -> None: ...

class Vector3(_message.Message):
    __slots__ = ["x", "y", "z"]
    X_FIELD_NUMBER: _ClassVar[int]
    Y_FIELD_NUMBER: _ClassVar[int]
    Z_FIELD_NUMBER: _ClassVar[int]
    x: float
    y: float
    z: float
    def __init__(self, x: _Optional[float] = ..., y: _Optional[float] = ..., z: _Optional[float] = ...) -> None: ...
