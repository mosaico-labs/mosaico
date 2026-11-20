import pytest

from mosaicolabs import Message, RobotJoint, SessionLevelErrorPolicy
from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models.data import (
    Header,
    Point3d,
    Pose,
    Quaternion,
    RobotPath,
    Vector3d,
)
from mosaicolabs.query import QueryOntologyCatalog, QuerySequence

SEQUENCE_NAME = "test_sequence_query_aggregator"

# Samples of each topic, as (x, frame_id):
# - /vec/a: x min=1, max=6, range=5, count=6, mean=3.5, var=3.5, std~1.87,
#           first=1, last=6;    frame_id max="kiwi", first="apple", last="kiwi"
# - /vec/b: x min=10, max=30, range=20, count=3, mean=20, var=100, std=10,
#           first=10, last=30;  frame_id max="zebra", first="zebra", last="zebra"
# - /vec/c: x min=2, max=50, range=48, count=2, mean=26, var=1152, std~33.94,
#           first=2, last=50;   frame_id max="banana", first="banana", last="banana"
TOPICS = {
    "/vec/a": [
        (1, "apple"),
        (2, "kiwi"),
        (3, "apple"),
        (4, "kiwi"),
        (5, "apple"),
        (6, "kiwi"),
    ],
    "/vec/b": [(10, "zebra"), (20, "zebra"), (30, "zebra")],
    "/vec/c": [(2, "banana"), (50, "banana")],
}

# Samples are one second apart, i.e. every topic has a rate of 1 Hz
SAMPLE_DT_NS = 1_000_000_000

LIST_SEQUENCE_NAME = "test_sequence_query_list_aggregator"

# Samples of each RobotJoint topic, as (positions, sample period):
# - /joints/a: max=9, min=1, range=8, count=3, rate=2 Hz; names max="j2"
# - /joints/b: max=0.5, min=0.1, range=0.4, count=2, rate=1 Hz; names max="j1"
JOINT_TOPICS = {
    "/joints/a": ([[1, 4], [7], [2, 3, 9]], SAMPLE_DT_NS // 2),
    "/joints/b": ([[0.5], [0.2, 0.1]], SAMPLE_DT_NS),
}

# Samples of each RobotPath topic, as the z of the poses:
# - /path/a: max=5, min=1, count=2
# - /path/b: max=30, min=10, count=2
PATH_TOPICS = {
    "/path/a": [[1, 5], [3]],
    "/path/b": [[10], [20, 30]],
}

EDGE_SEQUENCE_NAME = "test_sequence_query_list_aggregator_edge_cases"

# Samples of each RobotJoint topic, as the positions, one second apart:
# - /joints/some_empty: max=5, min=1, range=4, count=3 (the empty list included)
# - /joints/all_empty: no element, i.e. no max, min nor range; count=2
# - /joints/single: max=4, min=3, count=1, no rate (a single sample has no duration)
EDGE_JOINT_TOPICS = {
    "/joints/some_empty": [[1, 2], [], [5]],
    "/joints/all_empty": [[], []],
    "/joints/single": [[3, 4]],
}

# Samples of each RobotPath topic, as the (z, position covariance) of the poses, one
# second apart:
# - /path/a: z range=4; covariance max=9, min=1
# - /path/b: z range=20; no covariance, i.e. no max nor min
# - /path/c: z range=0 (a single pose, after an empty list); covariance max=min=0.5
EDGE_PATH_TOPICS = {
    "/path/a": [[(1, [1.0, 2.0, 3.0]), (5, None)], [(3, [9.0])]],
    "/path/b": [[(10, None)], [(20, None), (30, None)]],
    "/path/c": [[], [(7, [0.5])]],
}


@pytest.fixture(scope="module")
def inject_aggregator_sequence(host, port, tls_cert_path, api_key_manage, compression):
    with MosaicoClient.connect(
        host=host,
        port=port,
        tls_cert_path=tls_cert_path,
        api_key=api_key_manage,
        compression=compression,
    ) as client:
        with client.sequence_create(
            SEQUENCE_NAME, {}, SessionLevelErrorPolicy.Delete
        ) as seqw:
            for topic, samples in TOPICS.items():
                tw = seqw.topic_create(topic, {}, Vector3d)
                assert tw is not None
                for i, (x, frame_id) in enumerate(samples):
                    tw.push(
                        Message(
                            timestamp_ns=i * SAMPLE_DT_NS,
                            data=Vector3d(
                                header=Header(frame_id=frame_id), x=x, y=0, z=0
                            ),
                        )
                    )

        yield

        client.sequence_delete(SEQUENCE_NAME)


@pytest.fixture(scope="module")
def inject_list_aggregator_sequence(
    host, port, tls_cert_path, api_key_manage, compression
):
    with MosaicoClient.connect(
        host=host,
        port=port,
        tls_cert_path=tls_cert_path,
        api_key=api_key_manage,
        compression=compression,
    ) as client:
        with client.sequence_create(
            LIST_SEQUENCE_NAME, {}, SessionLevelErrorPolicy.Delete
        ) as seqw:
            for topic, (samples, dt_ns) in JOINT_TOPICS.items():
                tw = seqw.topic_create(topic, {}, RobotJoint)
                assert tw is not None
                for i, positions in enumerate(samples):
                    zeros = [0.0] * len(positions)
                    tw.push(
                        Message(
                            timestamp_ns=i * dt_ns,
                            data=RobotJoint(
                                names=[f"j{k}" for k in range(len(positions))],
                                positions=positions,
                                velocities=zeros,
                                efforts=zeros,
                            ),
                        )
                    )

            for topic, samples in PATH_TOPICS.items():
                tw = seqw.topic_create(topic, {}, RobotPath)
                assert tw is not None
                for i, zs in enumerate(samples):
                    poses = [
                        Pose(
                            position=Point3d(x=0, y=0, z=z),
                            orientation=Quaternion(x=0, y=0, z=0, w=1),
                        )
                        for z in zs
                    ]
                    tw.push(
                        Message(
                            timestamp_ns=i * SAMPLE_DT_NS,
                            data=RobotPath(poses=poses),
                        )
                    )

        yield

        client.sequence_delete(LIST_SEQUENCE_NAME)


@pytest.fixture(scope="module")
def inject_list_edge_cases_sequence(
    host, port, tls_cert_path, api_key_manage, compression
):
    with MosaicoClient.connect(
        host=host,
        port=port,
        tls_cert_path=tls_cert_path,
        api_key=api_key_manage,
        compression=compression,
    ) as client:
        with client.sequence_create(
            EDGE_SEQUENCE_NAME, {}, SessionLevelErrorPolicy.Delete
        ) as seqw:
            for topic, samples in EDGE_JOINT_TOPICS.items():
                tw = seqw.topic_create(topic, {}, RobotJoint)
                assert tw is not None
                for i, positions in enumerate(samples):
                    zeros = [0.0] * len(positions)
                    tw.push(
                        Message(
                            timestamp_ns=i * SAMPLE_DT_NS,
                            data=RobotJoint(
                                names=[f"j{k}" for k in range(len(positions))],
                                positions=positions,
                                velocities=zeros,
                                efforts=zeros,
                            ),
                        )
                    )

            for topic, samples in EDGE_PATH_TOPICS.items():
                tw = seqw.topic_create(topic, {}, RobotPath)
                assert tw is not None
                for i, poses in enumerate(samples):
                    tw.push(
                        Message(
                            timestamp_ns=i * SAMPLE_DT_NS,
                            data=RobotPath(
                                poses=[
                                    Pose(
                                        position=Point3d(
                                            x=0, y=0, z=z, covariance=covariance
                                        ),
                                        orientation=Quaternion(x=0, y=0, z=0, w=1),
                                    )
                                    for z, covariance in poses
                                ]
                            ),
                        )
                    )

        yield

        client.sequence_delete(EDGE_SEQUENCE_NAME)


def _query_topics(
    client: MosaicoClient, *exprs, sequence_name: str = SEQUENCE_NAME
) -> list[str]:
    """Runs the ontology expressions on the test sequence, returning the sorted topic names"""
    query_resp = client.query(
        QuerySequence().with_name(sequence_name),
        QueryOntologyCatalog(*exprs),
    )
    assert query_resp is not None
    return sorted(t.name for item in query_resp for t in item.topics)


def test_query_max_aggregator(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    # Every sample below 40
    assert _query_topics(mosaico_client, Vector3d.Q.x.max().lt(40)) == [
        "/vec/a",
        "/vec/b",
    ]


def test_query_min_aggregator(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    # Every sample above 1
    assert _query_topics(mosaico_client, Vector3d.Q.x.min().gt(1)) == [
        "/vec/b",
        "/vec/c",
    ]


def test_query_range_aggregator(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    assert _query_topics(mosaico_client, Vector3d.Q.x.range().between(4, 10)) == [
        "/vec/a"
    ]
    assert _query_topics(mosaico_client, Vector3d.Q.x.range().outside(4, 10)) == [
        "/vec/b",
        "/vec/c",
    ]


def test_query_count_aggregators(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    assert _query_topics(mosaico_client, Vector3d.Q.x.count().gt(2)) == [
        "/vec/a",
        "/vec/b",
    ]
    assert _query_topics(mosaico_client, Vector3d.Q.x.count_valid().eq(3)) == ["/vec/b"]


def test_query_moments_aggregators(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    assert _query_topics(mosaico_client, Vector3d.Q.x.mean().between(15, 30)) == [
        "/vec/b",
        "/vec/c",
    ]
    assert _query_topics(mosaico_client, Vector3d.Q.x.var().lt(200)) == [
        "/vec/a",
        "/vec/b",
    ]
    assert _query_topics(mosaico_client, Vector3d.Q.x.std().gt(5)) == [
        "/vec/b",
        "/vec/c",
    ]


def test_query_first_last_aggregators(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    assert _query_topics(mosaico_client, Vector3d.Q.x.first().eq(2)) == ["/vec/c"]
    assert _query_topics(mosaico_client, Vector3d.Q.x.last().geq(30)) == [
        "/vec/b",
        "/vec/c",
    ]


def test_query_rate_aggregator(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    assert _query_topics(mosaico_client, Vector3d.Q.x.rate().between(0.9, 1.1)) == [
        "/vec/a",
        "/vec/b",
        "/vec/c",
    ]


def test_query_string_aggregator(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    # Lexicographic: "zebra" > "m", while "kiwi" and "banana" are not
    assert _query_topics(mosaico_client, Vector3d.Q.header.frame_id.max().gt("m")) == [
        "/vec/b"
    ]


def test_query_string_first_last_aggregators(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    assert _query_topics(
        mosaico_client, Vector3d.Q.header.frame_id.first().eq("apple")
    ) == ["/vec/a"]
    assert _query_topics(
        mosaico_client, Vector3d.Q.header.frame_id.last().match("*a")
    ) == ["/vec/b", "/vec/c"]


def test_query_string_numeric_aggregators(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    # The count and the rate of a string field compare against a number
    assert _query_topics(mosaico_client, Vector3d.Q.header.frame_id.count().eq(3)) == [
        "/vec/b"
    ]
    assert _query_topics(
        mosaico_client, Vector3d.Q.header.frame_id.count_valid().gt(2)
    ) == ["/vec/a", "/vec/b"]
    assert _query_topics(
        mosaico_client, Vector3d.Q.header.frame_id.rate().between(0.9, 1.1)
    ) == ["/vec/a", "/vec/b", "/vec/c"]


def test_query_multiple_aggregators(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    assert _query_topics(
        mosaico_client,
        Vector3d.Q.x.min().gt(1),
        Vector3d.Q.x.max().lt(40),
    ) == ["/vec/b"]


def test_query_regular_and_aggregated_expressions(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    # At least one sample above 5, and every sample below 40
    assert _query_topics(
        mosaico_client,
        Vector3d.Q.x.gt(5),
        Vector3d.Q.x.max().lt(40),
    ) == ["/vec/a", "/vec/b"]


def test_clusterize_server_raises_with_aggregated_expressions(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    def topic_a_clusters(*exprs):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(SEQUENCE_NAME),
            QueryOntologyCatalog(*exprs),
        )
        assert query_resp is not None
        (topic,) = [t for item in query_resp for t in item.topics if t.name == "/vec/a"]
        return topic.clusterize(2 * SAMPLE_DT_NS)

    # The time ranges come from the regular expression only: just the last sample is > 5
    with pytest.raises(ValueError, match="unexpected aggregator"):
        topic_a_clusters(Vector3d.Q.x.gt(5), Vector3d.Q.x.max().lt(40))


def test_query_list_aggregators(
    mosaico_client: MosaicoClient, inject_list_aggregator_sequence
):
    def topics(expr):
        return _query_topics(mosaico_client, expr, sequence_name=LIST_SEQUENCE_NAME)

    # MAX, MIN and RANGE on the elements of all the lists
    assert topics(RobotJoint.Q.positions.max().gt(5)) == ["/joints/a"]
    assert topics(RobotJoint.Q.positions.min().lt(0.5)) == ["/joints/b"]
    assert topics(RobotJoint.Q.positions.range().between(7, 9)) == ["/joints/a"]
    # COUNT and RATE on the samples, whatever the length of their lists
    assert topics(RobotJoint.Q.positions.count().eq(3)) == ["/joints/a"]
    assert topics(RobotJoint.Q.positions.rate().gt(1.5)) == ["/joints/a"]
    # A list of strings
    assert topics(RobotJoint.Q.names.max().eq("j2")) == ["/joints/a"]


def test_query_list_of_structs_aggregators(
    mosaico_client: MosaicoClient, inject_list_aggregator_sequence
):
    def topics(expr):
        return _query_topics(mosaico_client, expr, sequence_name=LIST_SEQUENCE_NAME)

    assert topics(RobotPath.Q.poses.position.z.max().lt(10)) == ["/path/a"]
    assert topics(RobotPath.Q.poses.position.z.min().geq(10)) == ["/path/b"]
    assert topics(RobotPath.Q.poses.position.z.count().eq(2)) == [
        "/path/a",
        "/path/b",
    ]


def test_query_list_of_structs_range_and_rate_aggregators(
    mosaico_client: MosaicoClient, inject_list_edge_cases_sequence
):
    def topics(expr):
        return _query_topics(mosaico_client, expr, sequence_name=EDGE_SEQUENCE_NAME)

    assert topics(RobotPath.Q.poses.position.z.range().eq(4)) == ["/path/a"]
    assert topics(RobotPath.Q.poses.position.z.range().eq(20)) == ["/path/b"]
    assert topics(RobotPath.Q.poses.position.z.range().eq(0)) == ["/path/c"]
    assert topics(RobotPath.Q.poses.position.z.rate().eq(1)) == [
        "/path/a",
        "/path/b",
        "/path/c",
    ]


def test_query_list_aggregators_on_empty_lists(
    mosaico_client: MosaicoClient, inject_list_edge_cases_sequence
):
    def topics(expr):
        return _query_topics(mosaico_client, expr, sequence_name=EDGE_SEQUENCE_NAME)

    # The empty lists have no element: a topic made only of them has no max, min
    # nor range, so it matches no comparison
    for expr in (
        RobotJoint.Q.positions.max().gt(-1e9),
        RobotJoint.Q.positions.min().lt(1e9),
        RobotJoint.Q.positions.range().geq(0),
    ):
        assert topics(expr) == ["/joints/single", "/joints/some_empty"]
    # ... while the empty lists still count as samples
    assert topics(RobotJoint.Q.positions.count().eq(3)) == ["/joints/some_empty"]
    assert topics(RobotJoint.Q.positions.count().eq(2)) == ["/joints/all_empty"]
    assert topics(RobotJoint.Q.names.count().eq(2)) == ["/joints/all_empty"]
    # A topic whose lists of structs start empty
    assert topics(RobotPath.Q.poses.position.z.min().eq(7)) == ["/path/c"]
    assert topics(RobotPath.Q.poses.position.z.count().eq(2)) == [
        "/path/a",
        "/path/b",
        "/path/c",
    ]


def test_query_list_quantifiers_on_empty_lists(
    mosaico_client: MosaicoClient, inject_list_edge_cases_sequence
):
    def topics(expr):
        return _query_topics(mosaico_client, expr, sequence_name=EDGE_SEQUENCE_NAME)

    # An empty list matches neither any() nor all(), i.e. all() is not vacuously true
    assert topics(RobotJoint.Q.positions.any().gt(-1e9)) == [
        "/joints/single",
        "/joints/some_empty",
    ]
    assert topics(RobotJoint.Q.positions.all().gt(100)) == []
    assert topics(RobotPath.Q.poses.all().position.z.lt(0)) == []


def test_query_list_rate_aggregator_on_single_sample(
    mosaico_client: MosaicoClient, inject_list_edge_cases_sequence
):
    def topics(expr):
        return _query_topics(mosaico_client, expr, sequence_name=EDGE_SEQUENCE_NAME)

    # A single sample has no duration, so it has no rate
    assert topics(RobotJoint.Q.positions.rate().geq(0)) == [
        "/joints/all_empty",
        "/joints/some_empty",
    ]
    assert topics(RobotJoint.Q.positions.rate().eq(1)) == [
        "/joints/all_empty",
        "/joints/some_empty",
    ]


def test_query_list_aggregated_and_quantified_expressions(
    mosaico_client: MosaicoClient, inject_list_edge_cases_sequence
):
    def topics(*exprs):
        return _query_topics(mosaico_client, *exprs, sequence_name=EDGE_SEQUENCE_NAME)

    # At least one element above 4, and every element below 10
    assert topics(
        RobotJoint.Q.positions.any().gt(4),
        RobotJoint.Q.positions.max().lt(10),
    ) == ["/joints/some_empty"]
    assert (
        topics(
            RobotJoint.Q.positions.any().gt(4),
            RobotJoint.Q.positions.max().lt(5),
        )
        == []
    )
    assert topics(
        RobotJoint.Q.positions.all().lt(5),
        RobotJoint.Q.positions.count().eq(1),
    ) == ["/joints/single"]


def test_query_nested_list_of_structs_aggregators(
    mosaico_client: MosaicoClient, inject_list_edge_cases_sequence
):
    def topics(expr):
        return _query_topics(mosaico_client, expr, sequence_name=EDGE_SEQUENCE_NAME)

    # The elements of all the covariances of all the poses of the topic
    assert topics(RobotPath.Q.poses.position.covariance.max().eq(9)) == ["/path/a"]
    assert topics(RobotPath.Q.poses.position.covariance.min().eq(0.5)) == ["/path/c"]
    assert topics(RobotPath.Q.poses.position.covariance.range().eq(8)) == ["/path/a"]
    # The null covariances have no element
    assert topics(RobotPath.Q.poses.position.covariance.max().gt(-1e9)) == [
        "/path/a",
        "/path/c",
    ]
    # ... while their samples still count
    assert topics(RobotPath.Q.poses.position.covariance.count().eq(2)) == [
        "/path/a",
        "/path/b",
        "/path/c",
    ]
