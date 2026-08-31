from mosaicolabs import (
    Inertia,
    Message,
    MosaicoClient,
    Vector3d,
)
from mosaicolabs.query import QueryOntologyCatalog


def test_inertia_filter_clusterize(mosaico_client: MosaicoClient):

    with mosaico_client.sequence_create(
        "test_inertia_clusterize", metadata={}
    ) as writer:
        tw = writer.topic_create("/inertia_topic", metadata={}, ontology_type=Inertia)

        assert tw is not None

        for i in range(10):
            tw.push(
                message=Message(
                    timestamp_ns=1_000_000_000 + i * 1_000_000,
                    data=Inertia(
                        mass=float(i),
                        inertia=[float(i)] * 6,
                        center_of_mass=Vector3d(
                            x=float(i), y=0.0, z=0.0, covariance=[float(i)] * 9
                        ),
                    ),
                )
            )

    expressions_to_test = [
        Inertia.Q.center_of_mass.x.lt(3),
    ]

    for expr in expressions_to_test:
        resp = mosaico_client.query(QueryOntologyCatalog(expr))
        resp_topic = next(
            (
                t
                for item in resp or []
                for t in item.topics
                if t.locator.startswith("test_inertia_clusterize/")
            ),
            None,
        )

        assert resp_topic is not None
        assert len(resp_topic.clusterize(clustering_dt_ns=1)) == 3

    mosaico_client.sequence_delete("test_inertia_clusterize")
    mosaico_client.close()
