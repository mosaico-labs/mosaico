import pytest

from mosaicolabs import Message, SessionLevelErrorPolicy
from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models.data import Header, Vector3d
from mosaicolabs.query import QueryOntologyCatalog, QuerySequence

SEQUENCE_NAME = "test_sequence_query_aggregator"

# Samples of each topic, as (x, frame_id):
# - /vec/a: x min=1, max=6, range=5;   frame_id max="kiwi"
# - /vec/b: x min=10, max=30, range=20; frame_id max="zebra"
# - /vec/c: x min=2, max=50, range=48;  frame_id max="banana"
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

# Samples are one second apart
SAMPLE_DT_NS = 1_000_000_000


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


def _query_topics(client: MosaicoClient, *exprs) -> list[str]:
    """Runs the ontology expressions on the test sequence, returning the sorted topic names"""
    query_resp = client.query(
        QuerySequence().with_name(SEQUENCE_NAME),
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


def test_query_string_aggregator(
    mosaico_client: MosaicoClient, inject_aggregator_sequence
):
    # Lexicographic: "zebra" > "m", while "kiwi" and "banana" are not
    assert _query_topics(mosaico_client, Vector3d.Q.header.frame_id.max().gt("m")) == [
        "/vec/b"
    ]


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
    with pytest.raises(ValueError, match="unsupported aggregator"):
        topic_a_clusters(Vector3d.Q.x.gt(5), Vector3d.Q.x.max().lt(40))
