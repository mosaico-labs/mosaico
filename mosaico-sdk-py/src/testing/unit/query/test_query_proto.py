import pytest

from mosaicolabs import IMU
from mosaicolabs.proto.v1 import query_pb2
from mosaicolabs.query import Query, QueryOntologyCatalog, QuerySequence, QueryTopic
from mosaicolabs.query.expressions import (
    _QueryCatalogExpression,
    _QueryTopicExpression,
    _value_to_proto,
)

#####################################
############### VALUES ##############
#####################################


@pytest.mark.parametrize(
    "value, kind, expected",
    [
        (True, "boolean", True),
        (False, "boolean", False),
        (42, "integer", 42),
        (-1, "integer", -1),
        (3.5, "float", 3.5),
        ("abc", "text", "abc"),
    ],
)
def test_scalar_value(value, kind, expected):
    msg = _value_to_proto(value)
    assert msg.WhichOneof("kind") == kind
    assert getattr(msg, kind) == expected


def test_bool_is_not_encoded_as_integer():
    """`bool` is a subclass of `int` in Python: it must still map to `boolean`."""
    assert _value_to_proto(True).WhichOneof("kind") == "boolean"
    assert _value_to_proto([True, False]).WhichOneof("kind") == "boolean_array"


@pytest.mark.parametrize(
    "value, kind",
    [
        ([True, False], "boolean_array"),
        ([1, 2, 3], "integer_array"),
        ([1.0, 2.5], "float_array"),
        (["a", "b"], "text_array"),
        ((1, 2), "integer_array"),
    ],
)
def test_array_value(value, kind):
    msg = _value_to_proto(value)
    assert msg.WhichOneof("kind") == kind
    assert list(getattr(msg, kind).values) == list(value)


@pytest.mark.parametrize("value", [[], None, {"a": 1}, object()])
def test_unsupported_value(value):
    with pytest.raises(TypeError, match="Unsupported query value"):
        _value_to_proto(value)


#####################################
############# CONDITIONS ############
#####################################


@pytest.mark.parametrize(
    "expr, op, value",
    [
        (IMU.Q.acceleration.x.eq(1.0), query_pb2.OPERATOR_EQ, 1.0),
        (IMU.Q.acceleration.x.neq(1.0), query_pb2.OPERATOR_NE, 1.0),
        (IMU.Q.acceleration.x.lt(1.0), query_pb2.OPERATOR_LT, 1.0),
        (IMU.Q.acceleration.x.leq(1.0), query_pb2.OPERATOR_LE, 1.0),
        (IMU.Q.acceleration.x.gt(1.0), query_pb2.OPERATOR_GT, 1.0),
        (IMU.Q.acceleration.x.geq(1.0), query_pb2.OPERATOR_GE, 1.0),
        (
            IMU.Q.acceleration.x.between(1.0, 2.0),
            query_pb2.OPERATOR_BETWEEN,
            [1.0, 2.0],
        ),
        (IMU.Q.acceleration.x.in_(1.0, 2.0), query_pb2.OPERATOR_IN, [1.0, 2.0]),
    ],
)
def test_condition_with_value(expr, op, value):
    msg = expr._to_condition()
    assert msg.op == op
    assert msg.value == _value_to_proto(value)


@pytest.mark.parametrize(
    "exists, op",
    [(True, query_pb2.OPERATOR_EX), (False, query_pb2.OPERATOR_NEX)],
)
def test_condition_without_value(exists, op):
    qt = QueryTopic().with_user_metadata("sensor.model", ex=exists)
    msg = qt.expressions()[0]._to_condition()
    assert msg.op == op
    assert not msg.HasField("value")


def test_predicate():
    msg = IMU.Q.acceleration.x.gt(5.0)._to_predicate()
    assert msg.field == f"{IMU.ontology_tag()}.acceleration.x"
    assert msg.aggregator == query_pb2.AGGREGATOR_UNSPECIFIED
    assert msg.condition == query_pb2.Condition(
        op=query_pb2.OPERATOR_GT, value=query_pb2.Value(float=5.0)
    )


#####################################
############## FILTERS ##############
#####################################


def test_ontology_filter_keeps_every_expression():
    msg = Query(
        QueryOntologyCatalog(
            IMU.Q.acceleration.x.gt(5.0),
            IMU.Q.acceleration.y.between(1.0, 2.0),
        )
    )._to_proto()

    assert [p.field for p in msg.ontology.predicates] == [
        f"{IMU.ontology_tag()}.acceleration.x",
        f"{IMU.ontology_tag()}.acceleration.y",
    ]
    assert msg.ontology.predicates[0].condition.op == query_pb2.OPERATOR_GT
    assert msg.ontology.predicates[1].condition.op == query_pb2.OPERATOR_BETWEEN


def test_filter_all_domains():
    msg = Query(
        QuerySequence()
        .with_name("my_sequence")
        .with_user_metadata("driver", eq="luigi"),
        QueryTopic()
        .with_name_match("/camera*")
        .with_created_timestamp(time_start=1000)
        .with_ontology_tag("imu")
        .with_user_metadata("sensor.model", ex=True),
        QueryOntologyCatalog(IMU.Q.acceleration.x.lt(3.0)),
    )._to_proto()

    assert msg.sequence.name.op == query_pb2.OPERATOR_EQ
    assert msg.sequence.name.value.text == "my_sequence"
    assert msg.sequence.user_metadata["driver"].value.text == "luigi"
    assert not msg.sequence.HasField("created_at_ns")

    assert msg.topic.name.op == query_pb2.OPERATOR_MATCH
    assert msg.topic.created_at_ns.op == query_pb2.OPERATOR_GE
    assert msg.topic.created_at_ns.value.integer == 1000
    assert msg.topic.ontology_tag.value.text == "imu"
    assert msg.topic.user_metadata["sensor.model"].op == query_pb2.OPERATOR_EX
    assert not msg.topic.HasField("serialization_format")

    assert [p.field for p in msg.ontology.predicates] == [
        f"{IMU.ontology_tag()}.acceleration.x"
    ]


def test_filter_only_present_domains_are_set():
    msg = Query(QueryTopic().with_name("/a"))._to_proto()
    assert msg.HasField("topic")
    assert not msg.HasField("sequence")
    assert not msg.HasField("ontology")


def test_filter_unknown_field_is_rejected():
    """Unknown fields must fail instead of being silently dropped."""
    qt = QueryTopic._from_expressions(
        _QueryTopicExpression("locator", query_pb2.OPERATOR_EQ, "x")
    )
    with pytest.raises(AttributeError):
        Query(qt)._to_proto()


def test_filter_round_trip():
    msg = Query(
        QueryOntologyCatalog(
            _QueryCatalogExpression(
                f"{IMU.ontology_tag()}.acceleration.x", query_pb2.OPERATOR_IN, [1, 2]
            )
        )
    )._to_proto()
    assert query_pb2.Filter.FromString(msg.SerializeToString()) == msg
