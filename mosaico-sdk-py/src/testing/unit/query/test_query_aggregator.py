import pytest

from mosaicolabs import IMU, RobotJoint
from mosaicolabs.models.data import RobotPath
from mosaicolabs.proto.v1.query_pb2 import (
    AGGREGATOR_COUNT,
    AGGREGATOR_COUNT_VALID,
    AGGREGATOR_FIRST,
    AGGREGATOR_LAST,
    AGGREGATOR_MAX,
    AGGREGATOR_MEAN,
    AGGREGATOR_MIN,
    AGGREGATOR_RANGE,
    AGGREGATOR_RATE,
    AGGREGATOR_STD,
    AGGREGATOR_VAR,
    OPERATOR_MATCH,
    OPERATOR_OUTSIDE,
)
from mosaicolabs.query import QueryOntologyCatalog
from mosaicolabs.query.generation.mixins import _QueryableNumeric
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
    expr = IMU.Q.acceleration.x.count().gt(5)
    assert expr.aggregator == AGGREGATOR_COUNT
    expr = IMU.Q.acceleration.x.count_valid().gt(5)
    assert expr.aggregator == AGGREGATOR_COUNT_VALID
    expr = IMU.Q.acceleration.x.mean().gt(5.0)
    assert expr.aggregator == AGGREGATOR_MEAN
    expr = IMU.Q.acceleration.x.var().gt(5.0)
    assert expr.aggregator == AGGREGATOR_VAR
    expr = IMU.Q.acceleration.x.std().gt(5.0)
    assert expr.aggregator == AGGREGATOR_STD
    expr = IMU.Q.acceleration.x.first().gt(5.0)
    assert expr.aggregator == AGGREGATOR_FIRST
    expr = IMU.Q.acceleration.x.last().gt(5.0)
    assert expr.aggregator == AGGREGATOR_LAST
    expr = IMU.Q.acceleration.x.rate().gt(5.0)
    assert expr.aggregator == AGGREGATOR_RATE


def test_string_aggregator():
    expr = QueryableString("imu.frame_id").max().gt("m")
    assert expr.aggregator == AGGREGATOR_MAX
    expr = QueryableString("imu.frame_id").min().gt("m")
    assert expr.aggregator == AGGREGATOR_MIN
    expr = QueryableString("imu.frame_id").first().eq("m")
    assert expr.aggregator == AGGREGATOR_FIRST
    expr = QueryableString("imu.frame_id").last().match("m*")
    assert expr.aggregator == AGGREGATOR_LAST


def test_string_numeric_aggregator():
    # The count and the rate of a string field are numbers
    for aggregator, field in [
        (AGGREGATOR_COUNT, QueryableString("imu.frame_id").count()),
        (AGGREGATOR_COUNT_VALID, QueryableString("imu.frame_id").count_valid()),
        (AGGREGATOR_RATE, QueryableString("imu.frame_id").rate()),
    ]:
        assert isinstance(field, _QueryableNumeric)
        expr = field.gt(100)
        assert expr.key == "imu.frame_id"
        assert expr.aggregator == aggregator
        assert expr.value == 100

        with pytest.raises(TypeError):
            field.gt("100")


def test_string_has_no_moments_aggregator():
    for aggregator in ("mean", "var", "std"):
        with pytest.raises(AttributeError):
            getattr(QueryableString("imu.frame_id"), aggregator)()


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
    with pytest.raises(TypeError, match=r"already aggregated with 'max\(\)'"):
        IMU.Q.acceleration.x.max().min()
    with pytest.raises(TypeError, match=r"already aggregated with 'count_valid\(\)'"):
        IMU.Q.acceleration.x.count_valid().max()
    with pytest.raises(TypeError, match="already aggregated"):
        QueryableString("imu.frame_id").max().count()
    with pytest.raises(TypeError, match="already aggregated"):
        QueryableString("imu.frame_id").count().max()


def test_aggregator_does_not_modify_the_field():
    # A field stored in a variable can be aggregated and still compared plainly
    x = IMU.Q.acceleration.x
    assert x.max().lt(5).aggregator == AGGREGATOR_MAX
    assert x.gt(0).aggregator is None
    assert x.min().lt(1).aggregator == AGGREGATOR_MIN

    n = QueryableNumeric("imu.x")
    assert n.max().lt(1).aggregator == AGGREGATOR_MAX
    assert n.min().lt(1).aggregator == AGGREGATOR_MIN
    assert n.eq(1).aggregator is None

    # Also the aggregators returning a numeric field, e.g. the count of a string
    s = QueryableString("imu.frame_id")
    assert s.count().gt(1).aggregator == AGGREGATOR_COUNT
    expr = s.eq("a")
    assert expr.aggregator is None
    assert expr.value == "a"
    assert s.max().eq("a").aggregator == AGGREGATOR_MAX


def test_aggregated_field_keeps_the_field_type():
    assert isinstance(QueryableNumeric("imu.x").max(), QueryableNumeric)
    assert isinstance(QueryableString("imu.frame_id").max(), QueryableString)
    assert isinstance(QueryableBool("imu.flag").max(), QueryableBool)


def test_nested_list_selectors_are_rejected():
    # The server supports a single list access expression per path
    with pytest.raises(TypeError, match="inside a list selected"):
        RobotPath.Q.poses.any().position.covariance.any()
    with pytest.raises(TypeError, match="inside a list selected"):
        RobotPath.Q.poses.all().position.covariance.all()
    with pytest.raises(TypeError, match="inside a list selected"):
        RobotPath.Q.poses[0].position.covariance[2]
    # ... nor aggregates a list inside a selected list
    with pytest.raises(TypeError, match="not supported on list fields"):
        RobotPath.Q.poses[0].position.covariance.max()


def test_nested_list_of_structs_aggregator():
    # A list inside a list of structs, both without selectors, aggregates all the
    # elements of the inner lists
    expr = RobotPath.Q.poses.position.covariance.max().lt(1.0)
    assert expr.key == f"{RobotPath.ontology_tag()}.poses.position.covariance"
    assert expr.aggregator == AGGREGATOR_MAX


def test_aggregator_on_list_field_is_rejected():
    with pytest.raises(TypeError, match="not supported on list fields"):
        QueryableNumeric("imu.values[?]").max()


def test_match_on_aggregator():
    expr = QueryableString("imu.frame_id").max().match("a*")
    assert expr.aggregator == AGGREGATOR_MAX
    assert expr.op == OPERATOR_MATCH


def test_query_accepts_plain_and_aggregated_expressions_on_same_field():
    query = QueryOntologyCatalog(
        IMU.Q.acceleration.x.gt(0.0),
        IMU.Q.acceleration.x.max().gt(5.0),
        IMU.Q.acceleration.x.min().lt(1.0),
    )

    assert len(query.expressions()) == 3


def test_outside_on_aggregator():
    expr = IMU.Q.acceleration.x.max().outside(0.0, 1.0)
    assert expr.aggregator == AGGREGATOR_MAX
    assert expr.op == OPERATOR_OUTSIDE


def test_list_aggregator():
    expr = RobotJoint.Q.positions.max().lt(3.14)
    assert expr.key == f"{RobotJoint.ontology_tag()}.positions"
    assert expr.aggregator == AGGREGATOR_MAX
    expr = RobotJoint.Q.positions.min().gt(0.0)
    assert expr.aggregator == AGGREGATOR_MIN
    expr = RobotJoint.Q.positions.range().lt(1.0)
    assert expr.aggregator == AGGREGATOR_RANGE
    expr = RobotJoint.Q.positions.count().gt(10)
    assert expr.aggregator == AGGREGATOR_COUNT
    expr = RobotJoint.Q.positions.rate().gt(10)
    assert expr.aggregator == AGGREGATOR_RATE

    # On a list of strings the elements compare as strings, the count as a number
    expr = RobotJoint.Q.names.max().eq("joint9")
    assert expr.key == f"{RobotJoint.ontology_tag()}.names"
    assert expr.aggregator == AGGREGATOR_MAX
    expr = RobotJoint.Q.names.count().gt(10)
    assert expr.aggregator == AGGREGATOR_COUNT


def test_list_of_structs_aggregator():
    expr = RobotPath.Q.poses.position.z.max().lt(1.0)
    assert expr.key == f"{RobotPath.ontology_tag()}.poses.position.z"
    assert expr.aggregator == AGGREGATOR_MAX
    expr = RobotPath.Q.poses.position.z.count().gt(10)
    assert expr.aggregator == AGGREGATOR_COUNT


def test_unsupported_list_aggregator_is_rejected():
    with pytest.raises(AttributeError):
        RobotJoint.Q.positions.mean()
    with pytest.raises(AttributeError, match="is a list field"):
        RobotPath.Q.poses.position.z.mean()
    with pytest.raises(AttributeError, match="is a list field"):
        RobotPath.Q.poses.position.z.first()


def test_list_of_structs_field_without_aggregator_is_rejected():
    with pytest.raises(AttributeError, match="is a list field"):
        RobotPath.Q.poses.position.z.gt(1.0)
