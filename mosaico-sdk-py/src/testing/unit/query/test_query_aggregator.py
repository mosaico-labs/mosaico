import pytest

from mosaicolabs import IMU
from mosaicolabs.proto.v1.query_pb2 import (
    AGGREGATOR_MAX,
    AGGREGATOR_MIN,
    AGGREGATOR_RANGE,
    OPERATOR_OUTSIDE,
)
from mosaicolabs.query import QueryOntologyCatalog
from mosaicolabs.query.queryable_fields import (
    QueryableBool,
    QueryableNumeric,
    QueryableString,
)


def test_numeric_aggregator():
    expr = IMU.Q.acceleration.x.max().gt(5.0)
    assert expr.aggregator == AGGREGATOR_MAX
    expr = IMU.Q.acceleration.x.min().gt(5.0)
    assert expr.aggregator == AGGREGATOR_MIN
    expr = IMU.Q.acceleration.x.range().gt(5.0)
    assert expr.aggregator == AGGREGATOR_RANGE


def test_string_aggregator():
    expr = QueryableString("imu.frame_id").max().gt("m")
    assert expr.aggregator == AGGREGATOR_MAX
    expr = QueryableString("imu.frame_id").min().gt("m")
    assert expr.aggregator == AGGREGATOR_MIN


def test_string_has_no_range_aggregator():
    with pytest.raises(AttributeError):
        QueryableString("imu.frame_id").range()


def test_bool_aggregator():
    expr = QueryableBool("roi.do_rectify").max().eq(True)
    assert expr.aggregator == AGGREGATOR_MAX
    expr = QueryableBool("roi.do_rectify").min().eq(True)
    assert expr.aggregator == AGGREGATOR_MIN


def test_bool_has_no_range_aggregator():
    with pytest.raises(AttributeError):
        QueryableBool("roi.do_rectify").range()


def test_aggregator_operand_type_is_validated():
    with pytest.raises(TypeError):
        IMU.Q.acceleration.x.max().gt("5")


def test_chained_aggregators_are_rejected():
    with pytest.raises(TypeError, match="already aggregated"):
        IMU.Q.acceleration.x.max().min()


def test_aggregator_on_list_field_is_rejected():
    with pytest.raises(TypeError, match="not supported on list fields"):
        QueryableNumeric("imu.values[?]").max()


# FIXME: check with backend
# def test_match_on_aggregated_field_is_rejected():
#     with pytest.raises(ValueError, match="not supported on aggregator"):
#         QueryableString("imu.frame_id").max().match("a*")


def test_query_accepts_plain_and_aggregated_expressions_on_same_field():
    query = QueryOntologyCatalog(
        IMU.Q.acceleration.x.gt(0.0),
        IMU.Q.acceleration.x.max().gt(5.0),
        IMU.Q.acceleration.x.min().lt(1.0),
    )

    assert len(query.expressions()) == 3


def test_query_rejects_duplicate_aggregated_expression():
    with pytest.raises(NotImplementedError, match="already contains the key"):
        QueryOntologyCatalog(
            IMU.Q.acceleration.x.max().gt(5.0),
            IMU.Q.acceleration.x.max().lt(9.0),
        )


def test_outside_on_aggregator():
    expr = IMU.Q.acceleration.x.max().outside(0.0, 1.0)
    assert expr.aggregator == AGGREGATOR_MAX
    assert expr.op == OPERATOR_OUTSIDE
