import pytest

from mosaicolabs import IMU, Pressure, Temperature, TimestampRange
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
    qri = QueryResponseItemTopic._from_dict(
        {
            "locator": "seq1/empty",
            "ontology_tag": "",
        }
    )

    return qri


@pytest.fixture
def imu_query_topic() -> QueryResponseItemTopic:
    qri = QueryResponseItemTopic._from_dict(
        {
            "locator": "seq1/front_imu",
            "ontology_tag": f"{IMU.ontology_tag()}",
        }
    )

    expr1 = IMU.Q.acceleration.x.gt(5.0)
    expr2 = IMU.Q.acceleration.y.eq(20.0)
    qri._set_query_expressions([expr1, expr2])

    return qri


@pytest.fixture
def pressure_query_topic() -> QueryResponseItemTopic:
    qrit = QueryResponseItemTopic._from_dict(
        {
            "locator": "seq1/gasoline_tank_press",
            "ontology_tag": f"{Pressure.ontology_tag()}",
        }
    )

    expr1 = Pressure.Q.value.between([1.0, 5.0])
    qrit._set_query_expressions([expr1])

    return qrit


@pytest.fixture
def temperature_query_sequence() -> QueryResponseItem:

    qri = QueryResponseItem._from_dict(
        {
            "sequence": "seq2",
            "topics": [
                {
                    "locator": "seq2/car_temp",
                    "ontology_tag": f"{Temperature.ontology_tag()}",
                },
            ],
        }
    )

    expr1 = Temperature.Q.value.lt(3.0)

    qri._set_query_expressions([expr1])

    return qri


#####################################
############### TESTS ###############
#####################################


def test_create_clusterize_payload(imu_query_topic):
    """Tests correct construction of topic_filter_clusterize payload"""

    # Creating payload default
    clusterize_payload = _build_clusterize_payload(imu_query_topic)

    expected_payload = {
        "locator": "seq1/front_imu",
        "clustering_dt_ns": 0,
        "ontology": {
            f"{IMU.ontology_tag()}.acceleration.x": {"$gt": 5.0},
            f"{IMU.ontology_tag()}.acceleration.y": {"$eq": 20.0},
        },
        "timestamp_range": None,
    }

    assert clusterize_payload == expected_payload

    # Creating payload with clustering_dt_ns
    clustering_dt_ns = 200
    clusterize_payload_w_dt_ns = _build_clusterize_payload(
        imu_query_topic, clustering_dt_ns
    )

    expected_payload = {
        "locator": "seq1/front_imu",
        "clustering_dt_ns": 200,
        "ontology": {
            f"{IMU.ontology_tag()}.acceleration.x": {"$gt": 5.0},
            f"{IMU.ontology_tag()}.acceleration.y": {"$eq": 20.0},
        },
        "timestamp_range": None,
    }

    assert clusterize_payload_w_dt_ns == expected_payload

    # Creating payload with clustering_dt_ns and timestamp_range
    clusterize_payload_w_dt_ns_w_timestamp = _build_clusterize_payload(
        imu_query_topic,
        clustering_dt_ns,
        TimestampRange._from_dict({"start_ns": 1, "end_ns": 10}),
    )

    expected_payload = {
        "locator": "seq1/front_imu",
        "clustering_dt_ns": 200,
        "ontology": {
            f"{IMU.ontology_tag()}.acceleration.x": {"$gt": 5.0},
            f"{IMU.ontology_tag()}.acceleration.y": {"$eq": 20.0},
        },
        "timestamp_range": {"start_ns": 1, "end_ns": 10},
    }

    assert clusterize_payload_w_dt_ns_w_timestamp == expected_payload


def test_create_clusterize_empty_payload(empty_query_topic):

    # Creating payload and comparison (without clustering_dt_ns and timestamp_range)
    clusterize_payload = _build_clusterize_payload(empty_query_topic)

    expected_payload = {
        "locator": "seq1/empty",
        "clustering_dt_ns": 0,
        "ontology": {},
        "timestamp_range": None,
    }

    assert clusterize_payload == expected_payload


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

    expected_payload = {
        "topics": [
            {
                "locator": "seq1/front_imu",
                "clustering_dt_ns": 0,
                "ontology": {
                    f"{IMU.ontology_tag()}.acceleration.x": {"$gt": 5.0},
                    f"{IMU.ontology_tag()}.acceleration.y": {"$eq": 20.0},
                },
            },
            {
                "locator": "seq1/gasoline_tank_press",
                "clustering_dt_ns": 0,
                "ontology": {
                    f"{Pressure.ontology_tag()}.value": {"$between": [1.0, 5.0]},
                },
            },
            {
                "locator": "seq2/car_temp",
                "clustering_dt_ns": 0,
                "ontology": {
                    f"{Temperature.ontology_tag()}.value": {"$lt": 3.0},
                },
            },
        ],
        "intersect_dt_ns": intersect_dt_ns,
    }

    assert intersect_payload == expected_payload

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

    expected_payload = {
        "topics": [
            {
                "locator": "seq1/front_imu",
                "clustering_dt_ns": 10,
                "ontology": {
                    f"{IMU.ontology_tag()}.acceleration.x": {"$gt": 5.0},
                    f"{IMU.ontology_tag()}.acceleration.y": {"$eq": 20.0},
                },
            },
            {
                "locator": "seq1/gasoline_tank_press",
                "clustering_dt_ns": 50,
                "ontology": {
                    f"{Pressure.ontology_tag()}.value": {"$between": [1.0, 5.0]},
                },
            },
            {
                "locator": "seq2/car_temp",
                "clustering_dt_ns": 300,  # not present in map and default overridden
                "ontology": {
                    f"{Temperature.ontology_tag()}.value": {"$lt": 3.0},
                },
            },
        ],
        "intersect_dt_ns": intersect_dt_ns,
    }

    assert intersect_payload == expected_payload


def test_create_intersect_empty_payload(
    empty_query_topic: QueryResponseItemTopic,
):
    """Tests correct construction of topic_filter_intersect payload"""

    # Creating payload default (without clustering_map and override_clustering_dt_ns)
    intersect_dt_ns = 100

    intersect_payload = _build_intersect_payload(
        [empty_query_topic, empty_query_topic, empty_query_topic],
        intersect_dt_ns,
    )

    expected_payload = {
        "topics": [
            {
                "locator": "seq1/empty",
                "clustering_dt_ns": 0,
                "ontology": {},
            },
            {
                "locator": "seq1/empty",
                "clustering_dt_ns": 0,
                "ontology": {},
            },
            {
                "locator": "seq1/empty",
                "clustering_dt_ns": 0,
                "ontology": {},
            },
        ],
        "intersect_dt_ns": intersect_dt_ns,
    }

    assert intersect_payload == expected_payload
