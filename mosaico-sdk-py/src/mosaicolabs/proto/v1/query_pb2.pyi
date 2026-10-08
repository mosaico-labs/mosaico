from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

AGGREGATOR_COUNT: Aggregator
AGGREGATOR_DIFF: Aggregator
AGGREGATOR_FIRST: Aggregator
AGGREGATOR_LAST: Aggregator
AGGREGATOR_MAX: Aggregator
AGGREGATOR_MEAN: Aggregator
AGGREGATOR_MIN: Aggregator
AGGREGATOR_P50: Aggregator
AGGREGATOR_P90: Aggregator
AGGREGATOR_P99: Aggregator
AGGREGATOR_RANGE: Aggregator
AGGREGATOR_RATE: Aggregator
AGGREGATOR_STD: Aggregator
AGGREGATOR_UNSPECIFIED: Aggregator
DESCRIPTOR: _descriptor.FileDescriptor
OPERATOR_BETWEEN: Operator
OPERATOR_EQ: Operator
OPERATOR_EX: Operator
OPERATOR_GEQ: Operator
OPERATOR_GT: Operator
OPERATOR_IN: Operator
OPERATOR_LEQ: Operator
OPERATOR_LT: Operator
OPERATOR_MATCH: Operator
OPERATOR_NEQ: Operator
OPERATOR_NEX: Operator
OPERATOR_OUTSIDE: Operator
OPERATOR_UNSPECIFIED: Operator

class BooleanArray(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[bool]
    def __init__(self, values: _Optional[_Iterable[bool]] = ...) -> None: ...

class Condition(_message.Message):
    __slots__ = ["op", "value"]
    OP_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    op: Operator
    value: Value
    def __init__(self, op: _Optional[_Union[Operator, str]] = ..., value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...

class Filter(_message.Message):
    __slots__ = ["ontology", "sequence", "topic"]
    ONTOLOGY_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    ontology: OntologyFilter
    sequence: SequenceFilter
    topic: TopicFilter
    def __init__(self, sequence: _Optional[_Union[SequenceFilter, _Mapping]] = ..., topic: _Optional[_Union[TopicFilter, _Mapping]] = ..., ontology: _Optional[_Union[OntologyFilter, _Mapping]] = ...) -> None: ...

class FloatArray(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, values: _Optional[_Iterable[float]] = ...) -> None: ...

class IntegerArray(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, values: _Optional[_Iterable[int]] = ...) -> None: ...

class OntologyFilter(_message.Message):
    __slots__ = ["exprs"]
    EXPRS_FIELD_NUMBER: _ClassVar[int]
    exprs: _containers.RepeatedCompositeFieldContainer[OntologyPredicate]
    def __init__(self, exprs: _Optional[_Iterable[_Union[OntologyPredicate, _Mapping]]] = ...) -> None: ...

class OntologyPredicate(_message.Message):
    __slots__ = ["aggregator", "condition", "field"]
    AGGREGATOR_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    aggregator: Aggregator
    condition: Condition
    field: str
    def __init__(self, field: _Optional[str] = ..., aggregator: _Optional[_Union[Aggregator, str]] = ..., condition: _Optional[_Union[Condition, _Mapping]] = ...) -> None: ...

class SequenceFilter(_message.Message):
    __slots__ = ["created_at_ns", "name", "user_metadata"]
    class UserMetadataEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Condition
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Condition, _Mapping]] = ...) -> None: ...
    CREATED_AT_NS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    USER_METADATA_FIELD_NUMBER: _ClassVar[int]
    created_at_ns: Condition
    name: Condition
    user_metadata: _containers.MessageMap[str, Condition]
    def __init__(self, name: _Optional[_Union[Condition, _Mapping]] = ..., created_at_ns: _Optional[_Union[Condition, _Mapping]] = ..., user_metadata: _Optional[_Mapping[str, Condition]] = ...) -> None: ...

class TextArray(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, values: _Optional[_Iterable[str]] = ...) -> None: ...

class TopicFilter(_message.Message):
    __slots__ = ["created_at_ns", "name", "ontology_tag", "serialization_format", "user_metadata"]
    class UserMetadataEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Condition
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Condition, _Mapping]] = ...) -> None: ...
    CREATED_AT_NS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ONTOLOGY_TAG_FIELD_NUMBER: _ClassVar[int]
    SERIALIZATION_FORMAT_FIELD_NUMBER: _ClassVar[int]
    USER_METADATA_FIELD_NUMBER: _ClassVar[int]
    created_at_ns: Condition
    name: Condition
    ontology_tag: Condition
    serialization_format: Condition
    user_metadata: _containers.MessageMap[str, Condition]
    def __init__(self, name: _Optional[_Union[Condition, _Mapping]] = ..., created_at_ns: _Optional[_Union[Condition, _Mapping]] = ..., ontology_tag: _Optional[_Union[Condition, _Mapping]] = ..., serialization_format: _Optional[_Union[Condition, _Mapping]] = ..., user_metadata: _Optional[_Mapping[str, Condition]] = ...) -> None: ...

class Value(_message.Message):
    __slots__ = ["boolean", "boolean_array", "float", "float_array", "integer", "integer_array", "text", "text_array"]
    BOOLEAN_ARRAY_FIELD_NUMBER: _ClassVar[int]
    BOOLEAN_FIELD_NUMBER: _ClassVar[int]
    FLOAT_ARRAY_FIELD_NUMBER: _ClassVar[int]
    FLOAT_FIELD_NUMBER: _ClassVar[int]
    INTEGER_ARRAY_FIELD_NUMBER: _ClassVar[int]
    INTEGER_FIELD_NUMBER: _ClassVar[int]
    TEXT_ARRAY_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    boolean: bool
    boolean_array: BooleanArray
    float: float
    float_array: FloatArray
    integer: int
    integer_array: IntegerArray
    text: str
    text_array: TextArray
    def __init__(self, integer: _Optional[int] = ..., float: _Optional[float] = ..., text: _Optional[str] = ..., boolean: bool = ..., integer_array: _Optional[_Union[IntegerArray, _Mapping]] = ..., float_array: _Optional[_Union[FloatArray, _Mapping]] = ..., text_array: _Optional[_Union[TextArray, _Mapping]] = ..., boolean_array: _Optional[_Union[BooleanArray, _Mapping]] = ...) -> None: ...

class Operator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class Aggregator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
