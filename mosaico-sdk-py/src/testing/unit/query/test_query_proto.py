import pytest

from mosaicolabs import IMU
from mosaicolabs.proto.v1 import query_pb2
from mosaicolabs.query import Query, QueryOntologyCatalog, QuerySequence, QueryTopic
from mosaicolabs.query._proto_query import (
    _condition_to_proto,
    _value_to_proto,
    filter_to_proto,
    ontology_filter_to_proto,
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
    "op, value, expected_op",
    [
        ("$eq", 1, query_pb2.OPERATOR_EQ),
        ("$neq", 1, query_pb2.OPERATOR_NEQ),
        ("$lt", 1, query_pb2.OPERATOR_LT),
        ("$leq", 1, query_pb2.OPERATOR_LEQ),
        ("$gt", 1, query_pb2.OPERATOR_GT),
        ("$geq", 1, query_pb2.OPERATOR_GEQ),
        ("$between", [1, 2], query_pb2.OPERATOR_BETWEEN),
        ("$outside", [1, 2], query_pb2.OPERATOR_OUTSIDE),
        ("$in", [1, 2, 3], query_pb2.OPERATOR_IN),
        ("$match", "abc*", query_pb2.OPERATOR_MATCH),
    ],
)
def test_condition_with_value(op, value, expected_op):
    msg = _condition_to_proto({op: value})
    assert msg.op == expected_op
    assert msg.HasField("value")
    assert msg.value == _value_to_proto(value)


@pytest.mark.parametrize(
    "op, expected_op",
    [("$ex", query_pb2.OPERATOR_EX), ("$nex", query_pb2.OPERATOR_NEX)],
)
def test_condition_without_value(op, expected_op):
    msg = _condition_to_proto({op: None})
    assert msg.op == expected_op
    assert not msg.HasField("value")


def test_condition_unknown_operator():
    with pytest.raises(ValueError, match="Unsupported query operator"):
        _condition_to_proto({"$wrong": 1})


#####################################
############## FILTERS ##############
#####################################


def test_ontology_filter_keeps_every_expression():
    msg = ontology_filter_to_proto(
        {
            "imu.acceleration.x": {"$gt": 5.0},
            "imu.acceleration.y": {"$between": [1.0, 2.0]},
        }
    )

    assert [e.field for e in msg.exprs] == ["imu.acceleration.x", "imu.acceleration.y"]
    assert msg.exprs[0].condition.op == query_pb2.OPERATOR_GT
    assert msg.exprs[1].condition.op == query_pb2.OPERATOR_BETWEEN
    assert msg.exprs[1].condition.value.WhichOneof("kind") == "float_array"
    assert all(e.aggregator == query_pb2.AGGREGATOR_UNSPECIFIED for e in msg.exprs)


def test_ontology_filter_empty():
    assert len(ontology_filter_to_proto({}).exprs) == 0


def test_filter_all_domains():
    msg = filter_to_proto(
        {
            "sequence": {
                "name": {"$eq": "my_sequence"},
                "user_metadata": {"driver": {"$eq": "luigi"}},
            },
            "topic": {
                "name": {"$match": "/camera*"},
                "created_at_ns": {"$geq": 1000},
                "ontology_tag": {"$eq": "imu"},
                "user_metadata": {"sensor.model": {"$ex": None}},
            },
            "ontology": {"imu.acceleration.x": {"$lt": 3.0}},
        }
    )

    assert msg.sequence.name.op == query_pb2.OPERATOR_EQ
    assert msg.sequence.name.value.text == "my_sequence"
    assert msg.sequence.user_metadata["driver"].value.text == "luigi"
    assert not msg.sequence.HasField("created_at_ns")

    assert msg.topic.name.op == query_pb2.OPERATOR_MATCH
    assert msg.topic.created_at_ns.value.integer == 1000
    assert msg.topic.ontology_tag.value.text == "imu"
    assert msg.topic.user_metadata["sensor.model"].op == query_pb2.OPERATOR_EX
    assert not msg.topic.HasField("serialization_format")

    assert [e.field for e in msg.ontology.exprs] == ["imu.acceleration.x"]


def test_filter_only_present_domains_are_set():
    msg = filter_to_proto({"topic": {"name": {"$eq": "/a"}}})
    assert msg.HasField("topic")
    assert not msg.HasField("sequence")
    assert not msg.HasField("ontology")


def test_filter_unknown_field_is_rejected():
    """Unknown keys must fail instead of being silently dropped."""
    with pytest.raises(AttributeError):
        filter_to_proto({"topic": {"locator": {"$eq": "x"}}})


def test_filter_round_trip():
    msg = filter_to_proto({"ontology": {"imu.acceleration.x": {"$in": [1, 2]}}})
    assert query_pb2.Filter.FromString(msg.SerializeToString()) == msg


#####################################
############# BUILDERS ##############
#####################################


def test_filter_from_query_builders():
    """The dictionaries produced by the builders convert without errors."""
    query = Query(
        QuerySequence().with_user_metadata("driver", eq="luigi"),
        QueryTopic().with_user_metadata("sensor.rate", geq=10),
        QueryOntologyCatalog().with_expression(IMU.Q.acceleration.x.gt(5.0)),
    )

    msg = filter_to_proto(query.to_dict())

    assert msg.sequence.user_metadata["driver"].value.text == "luigi"
    assert msg.topic.user_metadata["sensor.rate"].op == query_pb2.OPERATOR_GEQ
    assert msg.topic.user_metadata["sensor.rate"].value.integer == 10
    assert len(msg.ontology.exprs) == 1
    assert msg.ontology.exprs[0].field == f"{IMU.ontology_tag()}.acceleration.x"
    assert msg.ontology.exprs[0].condition.value.float == 5.0
