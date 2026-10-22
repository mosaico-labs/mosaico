from mosaicolabs.proto.v1 import time_pb2 as _time_pb2
from mosaicolabs.proto.v1 import core_pb2 as _core_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
SEQUENCE_ORDER_CREATED_AT: SequenceOrder
SEQUENCE_ORDER_NAME: SequenceOrder
SEQUENCE_ORDER_UNSPECIFIED: SequenceOrder

class DoPutCmd(_message.Message):
    __slots__ = ["locator", "topic_uuid"]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    TOPIC_UUID_FIELD_NUMBER: _ClassVar[int]
    locator: str
    topic_uuid: str
    def __init__(self, locator: _Optional[str] = ..., topic_uuid: _Optional[str] = ...) -> None: ...

class GetFlightInfoCmd(_message.Message):
    __slots__ = ["locator", "timestamp_range"]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_RANGE_FIELD_NUMBER: _ClassVar[int]
    locator: str
    timestamp_range: _time_pb2.TimestampRange
    def __init__(self, locator: _Optional[str] = ..., timestamp_range: _Optional[_Union[_time_pb2.TimestampRange, _Mapping]] = ...) -> None: ...

class GetSchemaCmd(_message.Message):
    __slots__ = ["locator"]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    locator: str
    def __init__(self, locator: _Optional[str] = ...) -> None: ...

class ListFlightsCmd(_message.Message):
    __slots__ = ["cursor", "limit", "order_by"]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    cursor: SequenceCursor
    limit: int
    order_by: SequenceOrder
    def __init__(self, order_by: _Optional[_Union[SequenceOrder, str]] = ..., limit: _Optional[int] = ..., cursor: _Optional[_Union[SequenceCursor, _Mapping]] = ...) -> None: ...

class SequenceAppMetadata(_message.Message):
    __slots__ = ["created_at_ns", "locator", "sessions", "user_metadata"]
    CREATED_AT_NS_FIELD_NUMBER: _ClassVar[int]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    SESSIONS_FIELD_NUMBER: _ClassVar[int]
    USER_METADATA_FIELD_NUMBER: _ClassVar[int]
    created_at_ns: int
    locator: str
    sessions: _containers.RepeatedCompositeFieldContainer[SessionAppMetadata]
    user_metadata: bytes
    def __init__(self, created_at_ns: _Optional[int] = ..., locator: _Optional[str] = ..., sessions: _Optional[_Iterable[_Union[SessionAppMetadata, _Mapping]]] = ..., user_metadata: _Optional[bytes] = ...) -> None: ...

class SequenceCursor(_message.Message):
    __slots__ = ["created_at", "name"]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    created_at: SequenceCursorCreatedAt
    name: str
    def __init__(self, created_at: _Optional[_Union[SequenceCursorCreatedAt, _Mapping]] = ..., name: _Optional[str] = ...) -> None: ...

class SequenceCursorCreatedAt(_message.Message):
    __slots__ = ["created_at_ns", "sequence_id"]
    CREATED_AT_NS_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_ID_FIELD_NUMBER: _ClassVar[int]
    created_at_ns: int
    sequence_id: int
    def __init__(self, created_at_ns: _Optional[int] = ..., sequence_id: _Optional[int] = ...) -> None: ...

class SequenceListItemAppMetadata(_message.Message):
    __slots__ = ["cursor"]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    cursor: SequenceCursor
    def __init__(self, cursor: _Optional[_Union[SequenceCursor, _Mapping]] = ...) -> None: ...

class SessionAppMetadata(_message.Message):
    __slots__ = ["completed_at_ns", "created_at_ns", "locator", "locked", "topics"]
    COMPLETED_AT_NS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_NS_FIELD_NUMBER: _ClassVar[int]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    LOCKED_FIELD_NUMBER: _ClassVar[int]
    TOPICS_FIELD_NUMBER: _ClassVar[int]
    completed_at_ns: int
    created_at_ns: int
    locator: str
    locked: bool
    topics: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, locator: _Optional[str] = ..., created_at_ns: _Optional[int] = ..., completed_at_ns: _Optional[int] = ..., topics: _Optional[_Iterable[str]] = ..., locked: bool = ...) -> None: ...

class TicketTopic(_message.Message):
    __slots__ = ["locator", "timestamp_range"]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_RANGE_FIELD_NUMBER: _ClassVar[int]
    locator: str
    timestamp_range: _time_pb2.TimestampRange
    def __init__(self, locator: _Optional[str] = ..., timestamp_range: _Optional[_Union[_time_pb2.TimestampRange, _Mapping]] = ...) -> None: ...

class TopicAppMetadata(_message.Message):
    __slots__ = ["completed_at_ns", "created_at_ns", "data_info", "locator", "locked", "ontology_tag", "serialization_format", "session_locator", "time_window_info", "user_metadata"]
    COMPLETED_AT_NS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_NS_FIELD_NUMBER: _ClassVar[int]
    DATA_INFO_FIELD_NUMBER: _ClassVar[int]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    LOCKED_FIELD_NUMBER: _ClassVar[int]
    ONTOLOGY_TAG_FIELD_NUMBER: _ClassVar[int]
    SERIALIZATION_FORMAT_FIELD_NUMBER: _ClassVar[int]
    SESSION_LOCATOR_FIELD_NUMBER: _ClassVar[int]
    TIME_WINDOW_INFO_FIELD_NUMBER: _ClassVar[int]
    USER_METADATA_FIELD_NUMBER: _ClassVar[int]
    completed_at_ns: int
    created_at_ns: int
    data_info: TopicAppMetadataDataInfo
    locator: str
    locked: bool
    ontology_tag: str
    serialization_format: _core_pb2.Format
    session_locator: str
    time_window_info: TopicAppMetadataTimeWindow
    user_metadata: bytes
    def __init__(self, created_at_ns: _Optional[int] = ..., completed_at_ns: _Optional[int] = ..., locked: bool = ..., locator: _Optional[str] = ..., ontology_tag: _Optional[str] = ..., serialization_format: _Optional[_Union[_core_pb2.Format, str]] = ..., user_metadata: _Optional[bytes] = ..., data_info: _Optional[_Union[TopicAppMetadataDataInfo, _Mapping]] = ..., time_window_info: _Optional[_Union[TopicAppMetadataTimeWindow, _Mapping]] = ..., session_locator: _Optional[str] = ...) -> None: ...

class TopicAppMetadataDataInfo(_message.Message):
    __slots__ = ["interval", "total_bytes", "total_chunks_count", "total_row_count"]
    INTERVAL_FIELD_NUMBER: _ClassVar[int]
    TOTAL_BYTES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_CHUNKS_COUNT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ROW_COUNT_FIELD_NUMBER: _ClassVar[int]
    interval: _time_pb2.TimestampRange
    total_bytes: int
    total_chunks_count: int
    total_row_count: int
    def __init__(self, interval: _Optional[_Union[_time_pb2.TimestampRange, _Mapping]] = ..., total_row_count: _Optional[int] = ..., total_bytes: _Optional[int] = ..., total_chunks_count: _Optional[int] = ...) -> None: ...

class TopicAppMetadataTimeWindow(_message.Message):
    __slots__ = ["interval", "row_count"]
    INTERVAL_FIELD_NUMBER: _ClassVar[int]
    ROW_COUNT_FIELD_NUMBER: _ClassVar[int]
    interval: _time_pb2.TimestampRange
    row_count: int
    def __init__(self, interval: _Optional[_Union[_time_pb2.TimestampRange, _Mapping]] = ..., row_count: _Optional[int] = ...) -> None: ...

class SequenceOrder(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
