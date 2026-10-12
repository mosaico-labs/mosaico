from mosaicolabs.proto.v1 import time_pb2 as _time_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class NotificationItem(_message.Message):
    __slots__ = ["created_datetime", "msg", "name", "notification_type"]
    CREATED_DATETIME_FIELD_NUMBER: _ClassVar[int]
    MSG_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    NOTIFICATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    created_datetime: str
    msg: str
    name: str
    notification_type: str
    def __init__(self, name: _Optional[str] = ..., notification_type: _Optional[str] = ..., msg: _Optional[str] = ..., created_datetime: _Optional[str] = ...) -> None: ...

class NotificationList(_message.Message):
    __slots__ = ["notifications"]
    NOTIFICATIONS_FIELD_NUMBER: _ClassVar[int]
    notifications: _containers.RepeatedCompositeFieldContainer[NotificationItem]
    def __init__(self, notifications: _Optional[_Iterable[_Union[NotificationItem, _Mapping]]] = ...) -> None: ...

class Query(_message.Message):
    __slots__ = ["items"]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[QueryItem]
    def __init__(self, items: _Optional[_Iterable[_Union[QueryItem, _Mapping]]] = ...) -> None: ...

class QueryItem(_message.Message):
    __slots__ = ["sequence", "topics"]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    TOPICS_FIELD_NUMBER: _ClassVar[int]
    sequence: str
    topics: _containers.RepeatedCompositeFieldContainer[QueryItemTopic]
    def __init__(self, sequence: _Optional[str] = ..., topics: _Optional[_Iterable[_Union[QueryItemTopic, _Mapping]]] = ...) -> None: ...

class QueryItemTopic(_message.Message):
    __slots__ = ["locator", "ontology_tag"]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    ONTOLOGY_TAG_FIELD_NUMBER: _ClassVar[int]
    locator: str
    ontology_tag: str
    def __init__(self, locator: _Optional[str] = ..., ontology_tag: _Optional[str] = ...) -> None: ...

class ResourceUuid(_message.Message):
    __slots__ = ["uuid"]
    UUID_FIELD_NUMBER: _ClassVar[int]
    uuid: str
    def __init__(self, uuid: _Optional[str] = ...) -> None: ...

class SemVerItem(_message.Message):
    __slots__ = ["major", "minor", "patch", "pre"]
    MAJOR_FIELD_NUMBER: _ClassVar[int]
    MINOR_FIELD_NUMBER: _ClassVar[int]
    PATCH_FIELD_NUMBER: _ClassVar[int]
    PRE_FIELD_NUMBER: _ClassVar[int]
    major: int
    minor: int
    patch: int
    pre: str
    def __init__(self, major: _Optional[int] = ..., minor: _Optional[int] = ..., patch: _Optional[int] = ..., pre: _Optional[str] = ...) -> None: ...

class ServerConfig(_message.Message):
    __slots__ = ["grpc_max_decode_message_size", "grpc_max_encode_message_size", "grpc_target_encode_message_size"]
    GRPC_MAX_DECODE_MESSAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    GRPC_MAX_ENCODE_MESSAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    GRPC_TARGET_ENCODE_MESSAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    grpc_max_decode_message_size: int
    grpc_max_encode_message_size: int
    grpc_target_encode_message_size: int
    def __init__(self, grpc_max_decode_message_size: _Optional[int] = ..., grpc_max_encode_message_size: _Optional[int] = ..., grpc_target_encode_message_size: _Optional[int] = ...) -> None: ...

class ServerInfo(_message.Message):
    __slots__ = ["config", "semver", "version"]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    SEMVER_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    config: ServerConfig
    semver: SemVerItem
    version: str
    def __init__(self, version: _Optional[str] = ..., semver: _Optional[_Union[SemVerItem, _Mapping]] = ..., config: _Optional[_Union[ServerConfig, _Mapping]] = ...) -> None: ...

class SessionCreate(_message.Message):
    __slots__ = ["locator", "uuid"]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    locator: str
    uuid: str
    def __init__(self, uuid: _Optional[str] = ..., locator: _Optional[str] = ...) -> None: ...

class TopicFilterClusterize(_message.Message):
    __slots__ = ["id", "ts"]
    ID_FIELD_NUMBER: _ClassVar[int]
    TS_FIELD_NUMBER: _ClassVar[int]
    id: int
    ts: _time_pb2.TimestampRange
    def __init__(self, ts: _Optional[_Union[_time_pb2.TimestampRange, _Mapping]] = ..., id: _Optional[int] = ...) -> None: ...
