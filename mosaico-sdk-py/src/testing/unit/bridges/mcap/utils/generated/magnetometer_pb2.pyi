from typing import (
    ClassVar as _ClassVar,
    Iterable as _Iterable,
    Mapping as _Mapping,
    Optional as _Optional,
    Union as _Union,
)

import common_pb2 as _common_pb2
from google.protobuf import descriptor as _descriptor, message as _message
from google.protobuf.internal import containers as _containers

DESCRIPTOR: _descriptor.FileDescriptor

class AxisReading(_message.Message):
    __slots__ = ["axis", "saturated", "value"]
    AXIS_FIELD_NUMBER: _ClassVar[int]
    SATURATED_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    axis: str
    saturated: bool
    value: float
    def __init__(
        self,
        axis: _Optional[str] = ...,
        value: _Optional[float] = ...,
        saturated: bool = ...,
    ) -> None: ...

class Magnetometer(_message.Message):
    __slots__ = [
        "calibration_notes",
        "hardware_revision",
        "header",
        "magnetic_field",
        "magnetic_field_covariance",
        "raw_counter",
        "readings",
        "saturated",
        "sensor_id",
        "temperature_celsius",
    ]
    CALIBRATION_NOTES_FIELD_NUMBER: _ClassVar[int]
    HARDWARE_REVISION_FIELD_NUMBER: _ClassVar[int]
    HEADER_FIELD_NUMBER: _ClassVar[int]
    MAGNETIC_FIELD_COVARIANCE_FIELD_NUMBER: _ClassVar[int]
    MAGNETIC_FIELD_FIELD_NUMBER: _ClassVar[int]
    RAW_COUNTER_FIELD_NUMBER: _ClassVar[int]
    READINGS_FIELD_NUMBER: _ClassVar[int]
    SATURATED_FIELD_NUMBER: _ClassVar[int]
    SENSOR_ID_FIELD_NUMBER: _ClassVar[int]
    TEMPERATURE_CELSIUS_FIELD_NUMBER: _ClassVar[int]
    calibration_notes: _containers.RepeatedScalarFieldContainer[str]
    hardware_revision: int
    header: _common_pb2.Header
    magnetic_field: _common_pb2.Vector3
    magnetic_field_covariance: _containers.RepeatedScalarFieldContainer[float]
    raw_counter: int
    readings: _containers.RepeatedCompositeFieldContainer[AxisReading]
    saturated: bool
    sensor_id: int
    temperature_celsius: float
    def __init__(
        self,
        header: _Optional[_Union[_common_pb2.Header, _Mapping]] = ...,
        magnetic_field: _Optional[_Union[_common_pb2.Vector3, _Mapping]] = ...,
        magnetic_field_covariance: _Optional[_Iterable[float]] = ...,
        temperature_celsius: _Optional[float] = ...,
        sensor_id: _Optional[int] = ...,
        saturated: bool = ...,
        calibration_notes: _Optional[_Iterable[str]] = ...,
        readings: _Optional[_Iterable[_Union[AxisReading, _Mapping]]] = ...,
        hardware_revision: _Optional[int] = ...,
        raw_counter: _Optional[int] = ...,
    ) -> None: ...
