import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
FIX_TYPE_DIFFERENTIAL: FixType
FIX_TYPE_FIX: FixType
FIX_TYPE_NO_FIX: FixType
FIX_TYPE_RTK: FixType

class GeoPoint(_message.Message):
    __slots__ = ["altitude", "latitude", "longitude"]
    ALTITUDE_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    altitude: float
    latitude: float
    longitude: float
    def __init__(self, latitude: _Optional[float] = ..., longitude: _Optional[float] = ..., altitude: _Optional[float] = ...) -> None: ...

class Gps(_message.Message):
    __slots__ = ["active_satellite_ids", "differential", "fix_type", "header", "horizontal_accuracy", "position", "position_covariance", "raw_nmea", "satellites_used", "satellites_visible", "station_id", "utc_time_micros", "vertical_accuracy"]
    ACTIVE_SATELLITE_IDS_FIELD_NUMBER: _ClassVar[int]
    DIFFERENTIAL_FIELD_NUMBER: _ClassVar[int]
    FIX_TYPE_FIELD_NUMBER: _ClassVar[int]
    HEADER_FIELD_NUMBER: _ClassVar[int]
    HORIZONTAL_ACCURACY_FIELD_NUMBER: _ClassVar[int]
    POSITION_COVARIANCE_FIELD_NUMBER: _ClassVar[int]
    POSITION_FIELD_NUMBER: _ClassVar[int]
    RAW_NMEA_FIELD_NUMBER: _ClassVar[int]
    SATELLITES_USED_FIELD_NUMBER: _ClassVar[int]
    SATELLITES_VISIBLE_FIELD_NUMBER: _ClassVar[int]
    STATION_ID_FIELD_NUMBER: _ClassVar[int]
    UTC_TIME_MICROS_FIELD_NUMBER: _ClassVar[int]
    VERTICAL_ACCURACY_FIELD_NUMBER: _ClassVar[int]
    active_satellite_ids: _containers.RepeatedScalarFieldContainer[str]
    differential: bool
    fix_type: FixType
    header: _common_pb2.Header
    horizontal_accuracy: float
    position: GeoPoint
    position_covariance: _containers.RepeatedScalarFieldContainer[float]
    raw_nmea: bytes
    satellites_used: int
    satellites_visible: int
    station_id: str
    utc_time_micros: int
    vertical_accuracy: float
    def __init__(self, header: _Optional[_Union[_common_pb2.Header, _Mapping]] = ..., position: _Optional[_Union[GeoPoint, _Mapping]] = ..., position_covariance: _Optional[_Iterable[float]] = ..., fix_type: _Optional[_Union[FixType, str]] = ..., satellites_visible: _Optional[int] = ..., satellites_used: _Optional[int] = ..., differential: bool = ..., horizontal_accuracy: _Optional[float] = ..., vertical_accuracy: _Optional[float] = ..., utc_time_micros: _Optional[int] = ..., station_id: _Optional[str] = ..., raw_nmea: _Optional[bytes] = ..., active_satellite_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class FixType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
