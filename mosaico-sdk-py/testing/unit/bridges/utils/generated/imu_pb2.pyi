import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
IMU_STATUS_ERROR: ImuStatus
IMU_STATUS_OK: ImuStatus
IMU_STATUS_WARNING: ImuStatus

class Imu(_message.Message):
    __slots__ = ["angular_velocity", "calibrated", "diagnostic_note", "drift_estimate", "header", "linear_acceleration", "linear_acceleration_covariance", "orientation", "sequence", "status", "temperature_millideg", "uptime_ns"]
    ANGULAR_VELOCITY_FIELD_NUMBER: _ClassVar[int]
    CALIBRATED_FIELD_NUMBER: _ClassVar[int]
    DIAGNOSTIC_NOTE_FIELD_NUMBER: _ClassVar[int]
    DRIFT_ESTIMATE_FIELD_NUMBER: _ClassVar[int]
    HEADER_FIELD_NUMBER: _ClassVar[int]
    LINEAR_ACCELERATION_COVARIANCE_FIELD_NUMBER: _ClassVar[int]
    LINEAR_ACCELERATION_FIELD_NUMBER: _ClassVar[int]
    ORIENTATION_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TEMPERATURE_MILLIDEG_FIELD_NUMBER: _ClassVar[int]
    UPTIME_NS_FIELD_NUMBER: _ClassVar[int]
    angular_velocity: _common_pb2.Vector3
    calibrated: bool
    diagnostic_note: str
    drift_estimate: float
    header: _common_pb2.Header
    linear_acceleration: _common_pb2.Vector3
    linear_acceleration_covariance: _containers.RepeatedScalarFieldContainer[float]
    orientation: _common_pb2.Quaternion
    sequence: int
    status: ImuStatus
    temperature_millideg: int
    uptime_ns: int
    def __init__(self, header: _Optional[_Union[_common_pb2.Header, _Mapping]] = ..., orientation: _Optional[_Union[_common_pb2.Quaternion, _Mapping]] = ..., angular_velocity: _Optional[_Union[_common_pb2.Vector3, _Mapping]] = ..., linear_acceleration: _Optional[_Union[_common_pb2.Vector3, _Mapping]] = ..., linear_acceleration_covariance: _Optional[_Iterable[float]] = ..., status: _Optional[_Union[ImuStatus, str]] = ..., calibrated: bool = ..., sequence: _Optional[int] = ..., temperature_millideg: _Optional[int] = ..., uptime_ns: _Optional[int] = ..., drift_estimate: _Optional[float] = ..., diagnostic_note: _Optional[str] = ...) -> None: ...

class ImuStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
