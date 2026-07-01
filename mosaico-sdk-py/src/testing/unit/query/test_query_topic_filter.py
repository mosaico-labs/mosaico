from mosaicolabs import IMU
from mosaicolabs.models.query import QueryResponseItemTopic, _build_clusterize_payload


def test_query_clusterize_payload():
    """Tests correct construction of topic_filter_clusterize payload"""

    # Composing the QueryResponseItemTopic with expressions
    qri = QueryResponseItemTopic._from_dict(
        {
            "locator": "seq1/front_imu",
            "ontology_tag": "imu",
        }
    )

    expr1 = IMU.Q.accelaration.x.gt(5.0)
    expr2 = IMU.Q.accelaration.y.gt(20.0)
    qri._set_query_expressions([expr1, expr2])

    # Creating payload and comparison (without clustering_dt_ns and timestamp_range)
    clusterize_payload = _build_clusterize_payload(qri)

    expected_payload = {
        "topic": "seq1/front_imu",
        "clustering_dt_ns": 0,
        "ontology": {
            "imu.acceleration.x": {"$gt": 5},
            "imu.acceleration.y": {"$eq": 20},
        },
        "timestamp_range": None,
    }

    assert clusterize_payload == expected_payload

    # Creating payload and comparison with clustering_dt_ns (without timestamp_range)
    clustering_dt_ns = 200
    clusterize_payload_w_dt_ns = _build_clusterize_payload(qri, clustering_dt_ns)

    expected_payload = {
        "topic": "seq1/front_imu",
        "clustering_dt_ns": 200,
        "ontology": {
            "imu.acceleration.x": {"$gt": 5},
            "imu.acceleration.y": {"$eq": 20},
        },
        "timestamp_range": None,
    }

    assert clusterize_payload_w_dt_ns == expected_payload

    # Creating payload and comparison with clustering_dt_ns and timestamp_range
    clustering_dt_ns = 200
    clusterize_payload_w_dt_ns = _build_clusterize_payload(qri, clustering_dt_ns)

    expected_payload = {
        "topic": "seq1/front_imu",
        "clustering_dt_ns": 200,
        "ontology": {
            "imu.acceleration.x": {"$gt": 5},
            "imu.acceleration.y": {"$eq": 20},
        },
        "timestamp_range": None,
    }

    assert clusterize_payload_w_dt_ns == expected_payload
