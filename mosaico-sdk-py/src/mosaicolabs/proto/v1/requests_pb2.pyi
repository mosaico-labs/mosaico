from mosaicolabs.proto.v1 import core_pb2 as _core_pb2
from mosaicolabs.proto.v1 import time_pb2 as _time_pb2
from mosaicolabs.proto.v1 import query_pb2 as _query_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class NotificationCreate(_message.Message):
    __slots__ = ["locator", "msg", "notification_type"]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    MSG_FIELD_NUMBER: _ClassVar[int]
    NOTIFICATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    locator: str
    msg: str
    notification_type: str
    def __init__(self, locator: _Optional[str] = ..., notification_type: _Optional[str] = ..., msg: _Optional[str] = ...) -> None: ...

class Query(_message.Message):
    __slots__ = ["filter"]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    filter: _query_pb2.Filter
    def __init__(self, filter: _Optional[_Union[_query_pb2.Filter, _Mapping]] = ...) -> None: ...

class ResourceLocator(_message.Message):
    __slots__ = ["locator"]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    locator: str
    def __init__(self, locator: _Optional[str] = ...) -> None: ...

class SequenceCreate(_message.Message):
    __slots__ = ["locator", "user_metadata"]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    USER_METADATA_FIELD_NUMBER: _ClassVar[int]
    locator: str
    user_metadata: bytes
    def __init__(self, locator: _Optional[str] = ..., user_metadata: _Optional[bytes] = ...) -> None: ...

class SessionUuid(_message.Message):
    __slots__ = ["session_uuid"]
    SESSION_UUID_FIELD_NUMBER: _ClassVar[int]
    session_uuid: str
    def __init__(self, session_uuid: _Optional[str] = ...) -> None: ...

class TopicClusterizeParams(_message.Message):
    __slots__ = ["clustering_dt_ns", "locator", "ontology", "timestamp_range"]
    CLUSTERING_DT_NS_FIELD_NUMBER: _ClassVar[int]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    ONTOLOGY_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_RANGE_FIELD_NUMBER: _ClassVar[int]
    clustering_dt_ns: int
    locator: str
    ontology: _query_pb2.OntologyFilter
    timestamp_range: _time_pb2.TimestampRange
    def __init__(self, locator: _Optional[str] = ..., clustering_dt_ns: _Optional[int] = ..., ontology: _Optional[_Union[_query_pb2.OntologyFilter, _Mapping]] = ..., timestamp_range: _Optional[_Union[_time_pb2.TimestampRange, _Mapping]] = ...) -> None: ...

class TopicCreate(_message.Message):
    __slots__ = ["locator", "ontology_tag", "serialization_format", "session_uuid", "user_metadata"]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    ONTOLOGY_TAG_FIELD_NUMBER: _ClassVar[int]
    SERIALIZATION_FORMAT_FIELD_NUMBER: _ClassVar[int]
    SESSION_UUID_FIELD_NUMBER: _ClassVar[int]
    USER_METADATA_FIELD_NUMBER: _ClassVar[int]
    locator: str
    ontology_tag: str
    serialization_format: _core_pb2.Format
    session_uuid: str
    user_metadata: bytes
    def __init__(self, locator: _Optional[str] = ..., session_uuid: _Optional[str] = ..., serialization_format: _Optional[_Union[_core_pb2.Format, str]] = ..., ontology_tag: _Optional[str] = ..., user_metadata: _Optional[bytes] = ...) -> None: ...

class TopicFilterIntersect(_message.Message):
    __slots__ = ["intersect_dt_ns", "topics"]
    INTERSECT_DT_NS_FIELD_NUMBER: _ClassVar[int]
    TOPICS_FIELD_NUMBER: _ClassVar[int]
    intersect_dt_ns: int
    topics: _containers.RepeatedCompositeFieldContainer[TopicClusterizeParams]
    def __init__(self, topics: _Optional[_Iterable[_Union[TopicClusterizeParams, _Mapping]]] = ..., intersect_dt_ns: _Optional[int] = ...) -> None: ...
