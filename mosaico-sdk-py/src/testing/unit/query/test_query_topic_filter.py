import pytest

from mosaicolabs import IMU, Pressure, Temperature, TimestampRange
from mosaicolabs.proto.v1 import query_pb2, responses_pb2
from mosaicolabs.query import (
    QueryResponseItem,
    QueryResponseItemTopic,
)
from mosaicolabs.query.response import (
    _build_clusterize_payload,
    _build_intersect_payload,
)

#####################################
############## FIXTURES #############
#####################################


@pytest.fixture
def empty_query_topic() -> QueryResponseItemTopic:
    qri = QueryResponseItemTopic._from_proto(
        responses_pb2.QueryItemTopic(
            locator="seq1/empty",
            ontology_tag="",
        )
    )

    return qri


@pytest.fixture
def imu_query_topic() -> QueryResponseItemTopic:
    qri = QueryResponseItemTopic._from_proto(
        responses_pb2.QueryItemTopic(
            locator="seq1/front_imu",
            ontology_tag=f"{IMU.ontology_tag()}",
        )
    )

    expr1 = IMU.Q.acceleration.x.gt(5.0)
    expr2 = IMU.Q.acceleration.y.eq(20.0)
    qri._set_query_expressions([expr1, expr2])

    return qri


@pytest.fixture
def pressure_query_topic() -> QueryResponseItemTopic:
    qrit = QueryResponseItemTopic._from_proto(
        responses_pb2.QueryItemTopic(
            locator="seq1/gasoline_tank_press",
            ontology_tag=f"{Pressure.ontology_tag()}",
        )
    )

    expr1 = Pressure.Q.value.between([1.0, 5.0])
    qrit._set_query_expressions([expr1])

    return qrit


@pytest.fixture
def temperature_query_sequence() -> QueryResponseItem:

    qri = QueryResponseItem._from_proto(
        responses_pb2.QueryItem(
            sequence="seq2",
            topics=[
                responses_pb2.QueryItemTopic(
                    locator="seq2/car_temp",
                    ontology_tag=f"{Temperature.ontology_tag()}",
                ),
            ],
        )
    )

    expr1 = Temperature.Q.value.lt(3.0)

    qri._set_query_expressions([expr1])

    return qri


def _predicate(field: str, op: query_pb2.Operator, value: query_pb2.Value):
    return query_pb2.OntologyPredicate(
        field=field, condition=query_pb2.Condition(op=op, value=value)
    )


IMU_ONTOLOGY_FILTER = query_pb2.OntologyFilter(
    predicates=[
        _predicate(
            f"{IMU.ontology_tag()}.acceleration.x",
            query_pb2.OPERATOR_GT,
            query_pb2.Value(float=5.0),
        ),
        _predicate(
            f"{IMU.ontology_tag()}.acceleration.y",
            query_pb2.OPERATOR_EQ,
            query_pb2.Value(float=20.0),
        ),
    ]
)

PRESSURE_ONTOLOGY_FILTER = query_pb2.OntologyFilter(
    predicates=[
        _predicate(
            f"{Pressure.ontology_tag()}.value",
            query_pb2.OPERATOR_BETWEEN,
            query_pb2.Value(float_array=query_pb2.FloatArray(values=[1.0, 5.0])),
        ),
    ]
)

TEMPERATURE_ONTOLOGY_FILTER = query_pb2.OntologyFilter(
    predicates=[
        _predicate(
            f"{Temperature.ontology_tag()}.value",
            query_pb2.OPERATOR_LT,
            query_pb2.Value(float=3.0),
        ),
    ]
)

#####################################
############### TESTS ###############
#####################################


def test_create_clusterize_payload(imu_query_topic):
    """Tests correct construction of topic_filter_clusterize payload"""

    # Creating payload default
    clusterize_payload = _build_clusterize_payload(imu_query_topic)

    assert clusterize_payload.locator == "seq1/front_imu"
    assert clusterize_payload.clustering_dt_ns == 0
    assert clusterize_payload.ontology == IMU_ONTOLOGY_FILTER
    assert not clusterize_payload.HasField("timestamp_range")

    # Creating payload with clustering_dt_ns
    clustering_dt_ns = 200
    clusterize_payload_w_dt_ns = _build_clusterize_payload(
        imu_query_topic, clustering_dt_ns
    )

    assert clusterize_payload_w_dt_ns.locator == "seq1/front_imu"
    assert clusterize_payload_w_dt_ns.clustering_dt_ns == 200
    assert clusterize_payload_w_dt_ns.ontology == IMU_ONTOLOGY_FILTER
    assert not clusterize_payload_w_dt_ns.HasField("timestamp_range")

    # Creating payload with clustering_dt_ns and timestamp_range
    clusterize_payload_w_dt_ns_w_timestamp = _build_clusterize_payload(
        imu_query_topic,
        clustering_dt_ns,
        TimestampRange(start=1, end=10),
    )

    assert clusterize_payload_w_dt_ns_w_timestamp.locator == "seq1/front_imu"
    assert clusterize_payload_w_dt_ns_w_timestamp.clustering_dt_ns == 200
    assert clusterize_payload_w_dt_ns_w_timestamp.ontology == IMU_ONTOLOGY_FILTER
    assert clusterize_payload_w_dt_ns_w_timestamp.HasField("timestamp_range")
    assert clusterize_payload_w_dt_ns_w_timestamp.timestamp_range.start_ns == 1
    assert clusterize_payload_w_dt_ns_w_timestamp.timestamp_range.end_ns == 10


def test_create_clusterize_empty_payload(empty_query_topic):

    # Creating payload and comparison (without clustering_dt_ns and timestamp_range)
    clusterize_payload = _build_clusterize_payload(empty_query_topic)

    assert clusterize_payload.locator == "seq1/empty"
    assert clusterize_payload.clustering_dt_ns == 0
    assert clusterize_payload.ontology == query_pb2.OntologyFilter()
    assert not clusterize_payload.HasField("timestamp_range")


def test_create_intersect_payload(
    temperature_query_sequence: QueryResponseItem,
    imu_query_topic: QueryResponseItemTopic,
    pressure_query_topic: QueryResponseItemTopic,
):
    """Tests correct construction of topic_filter_intersect payload"""

    # Creating payload default (without clustering_map and override_clustering_dt_ns)
    intersect_dt_ns = 10

    intersect_payload = _build_intersect_payload(
        [imu_query_topic, pressure_query_topic] + temperature_query_sequence.topics,
        intersect_dt_ns,
    )

    assert intersect_payload.intersect_dt_ns == intersect_dt_ns
    assert [t.locator for t in intersect_payload.topics] == [
        "seq1/front_imu",
        "seq1/gasoline_tank_press",
        "seq2/car_temp",
    ]
    assert [t.clustering_dt_ns for t in intersect_payload.topics] == [0, 0, 0]
    assert [t.ontology for t in intersect_payload.topics] == [
        IMU_ONTOLOGY_FILTER,
        PRESSURE_ONTOLOGY_FILTER,
        TEMPERATURE_ONTOLOGY_FILTER,
    ]

    # Creating payload with clustering_map and clustering_dt_ns default overridden
    clustering_map = {
        IMU.ontology_tag(): 10,
        Pressure.ontology_tag(): 50,
    }

    override_clustering_dt_ns = 300

    intersect_payload = _build_intersect_payload(
        [imu_query_topic, pressure_query_topic] + temperature_query_sequence.topics,
        intersect_dt_ns,
        clustering_map,
        override_clustering_dt_ns,
    )

    assert intersect_payload.intersect_dt_ns == intersect_dt_ns
    assert [t.locator for t in intersect_payload.topics] == [
        "seq1/front_imu",
        "seq1/gasoline_tank_press",
        "seq2/car_temp",
    ]
    assert [t.clustering_dt_ns for t in intersect_payload.topics] == [
        10,
        50,
        300,  # not present in map and default overridden
    ]
    assert [t.ontology for t in intersect_payload.topics] == [
        IMU_ONTOLOGY_FILTER,
        PRESSURE_ONTOLOGY_FILTER,
        TEMPERATURE_ONTOLOGY_FILTER,
    ]


def test_create_intersect_empty_payload(
    empty_query_topic: QueryResponseItemTopic,
):
    with pytest.raises(ValueError, match="non-aggregated expression"):
        _build_intersect_payload(
            [empty_query_topic, empty_query_topic, empty_query_topic], 100
        )


# def test_create_clusterize_payload_skips_aggregated_expressions():
#     """Aggregated expressions describe the whole topic, so they are not sent to clusterize"""
#     qri = QueryResponseItemTopic._from_proto(
#         responses_pb2.QueryItemTopic(
#             locator="seq1/front_imu",
#             ontology_tag=f"{IMU.ontology_tag()}",
#         )
#     )
#     qri._set_query_expressions(
#         [IMU.Q.acceleration.x.gt(5.0), IMU.Q.acceleration.y.max().lt(20.0)]
#     )

#     clusterize_payload = _build_clusterize_payload(qri)

#     assert clusterize_payload.ontology == Query(
#         {f"{IMU.ontology_tag()}.acceleration.x": {"$gt": 5.0}}
#     )

#     # Only aggregated expressions: rejected before reaching the server
#     qri._set_query_expressions([IMU.Q.acceleration.y.max().lt(20.0)])

#     with pytest.raises(ValueError, match="non-aggregated expression"):
#         _build_clusterize_payload(qri)
