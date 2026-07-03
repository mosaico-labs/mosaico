import math

from mosaicolabs import MosaicoClient, Point3d, Pressure, Temperature, TimestampRange
from mosaicolabs.models.query import QueryOntologyCatalog, QuerySequence
from testing.integration.config import QUERY_FILTER_SEQUENCE_RESOLUTION_NS


def test_filter_clusterize_single_query(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences_filter,  # Ensure the data are available on the data platform
):
    query_resp = mosaico_client.query(
        QuerySequence().with_name_match("test-filter-sequence"),
        QueryOntologyCatalog().with_expression(
            Point3d.Q.x.gt(0.5)  # > pi/6
        ),
    )

    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()

    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1

    for item in query_resp.items:
        # We do expect one (1) topic
        assert len(item.topics) == 1

        for topic in item.topics:
            # Clusterize default parameters
            clusters = topic.clusterize()

            # Expected just one (1) cluster
            assert len(clusters) == 1

            assert clusters[0].timerange.start >= (math.pi / 12) * 1.0e9  # pi/12
            assert (
                clusters[0].timerange.end <= (3 * math.pi - math.pi / 12) * 1.0e9
            )  # 3pi - pi/12

            # Clusterize with clustering_dt_ns
            clustering_dt_ns = QUERY_FILTER_SEQUENCE_RESOLUTION_NS * 2
            clusters = topic.clusterize(int(clustering_dt_ns))

            # Expected two (2) clusters
            assert len(clusters) == 2

            assert clusters[0].timerange.start >= (math.pi / 12) * 1.0e9  # pi/12
            assert (
                clusters[0].timerange.end <= (math.pi - math.pi / 12) * 1.0e9
            )  # pi + pi/12

            assert (
                clusters[1].timerange.start >= (2 * math.pi + math.pi / 12) * 1.0e9
            )  # 2pi + pi/12
            assert (
                clusters[1].timerange.end <= (3 * math.pi - math.pi / 12) * 1.0e9
            )  # 3pi - pi/12

            # Clusterize with clustering_dt_ns and timerange limited to one period
            # -> despite same clustering_dt_ns as before, now the cluster should be reduced to one (1)
            # because of the timerange limitation
            clustering_dt_ns = QUERY_FILTER_SEQUENCE_RESOLUTION_NS * 2
            clusters = topic.clusterize(
                int(clustering_dt_ns),
                TimestampRange(start=0, end=int(2 * math.pi * 1e9)),
            )

            # Expected just one (1) cluster
            assert len(clusters) == 1

            assert clusters[0].timerange.start >= (math.pi / 6) * 1.0e9
            assert clusters[0].timerange.end <= (5 / 6 * math.pi) * 1.0e9


def test_filter_clusterize_multi_query_one_topic(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences_filter,  # Ensure the data are available on the data platform
):
    # Multi query single different topic
    query_resp = mosaico_client.query(
        QuerySequence().with_name_match("test-filter-sequence"),
        QueryOntologyCatalog()
        .with_expression(Point3d.Q.x.gt(0.5))  # > sin(pi/4)
        .with_expression(Point3d.Q.y.gt(0.866)),  # > sin(pi/3) -> more stringent!
    )

    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()

    for item in query_resp:
        # We do expect one (1) topic
        assert len(item.topics) == 1

        for topic in item.topics:
            # Clusterize with clustering_dt_ns
            clustering_dt_ns = QUERY_FILTER_SEQUENCE_RESOLUTION_NS * 2
            clusters = topic.clusterize(int(clustering_dt_ns))

            # Expected two (2) clusters
            assert len(clusters) == 2

            # This time though interval are more stingent
            assert clusters[0].timerange.start >= math.pi / 4 * 1.0e9  # pi/4
            assert (
                clusters[0].timerange.end <= (math.pi - math.pi / 4) * 1.0e9
            )  # pi - pi/4

            assert (
                clusters[1].timerange.start >= (2 * math.pi + math.pi / 4) * 1.0e9
            )  # 2pi + pi/4
            assert (
                clusters[1].timerange.end <= (3 * math.pi - math.pi / 4) * 1.0e9
            )  # 3pi - pi/4


def test_filter_clusterize_all_multi_query_multi_topic(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences_filter,  # Ensure the data are available on the data platform
):

    # Multi query with different topics
    query_resp = mosaico_client.query(
        QuerySequence().with_name_match("test-filter-sequence"),
        QueryOntologyCatalog()
        .with_expression(Temperature.Q.value.gt(0.5))  # > sin(pi/4)
        .with_expression(Pressure.Q.value.gt(0.5)),  # > cos(pi/6)
    )

    acceptance_intervals = {
        Temperature.ontology_tag(): {
            "intervals": [
                {
                    "start": (math.pi / 6) * 1.0e9,
                    "end": (math.pi - math.pi / 6) * 1.0e9,
                },
                {
                    "start": (2 * math.pi + math.pi / 6) * 1.0e9,
                    "end": (3 * math.pi - math.pi / 6) * 1.0e9,
                },
            ],
        },
        Pressure.ontology_tag(): {
            "intervals": [
                {
                    "start": 0.0,
                    "end": (math.pi / 3) * 1.0e9,
                },
                {
                    "start": (3 / 4 * math.pi) * 1.0e9,
                    "end": (5 / 2 * math.pi) * 1.0e9,
                },
                {
                    "start": (7 / 2 * math.pi) * 1.0e9,
                    "end": (4 * math.pi) * 1.0e9,
                },
            ],
        },
    }

    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()

    for item in query_resp:
        # We do expect two (2) topics
        assert len(item.topics) == 2

        # Clusterize with clustering_dt_ns
        clustering_dt_ns = int(QUERY_FILTER_SEQUENCE_RESOLUTION_NS * 2)

        all_clusters_dict: dict[
            str, list
        ] = {}  # used later to compare result with clusterize_all()
        for topic in item.topics:
            topic_acc_clusters = acceptance_intervals[topic.ontology_tag]
            clusters = topic.clusterize(clustering_dt_ns)

            all_clusters_dict.update({topic.name: clusters})

            # Checking expected clusters
            assert len(clusters) == len(topic_acc_clusters["intervals"])

            for cluster in clusters:
                assert (
                    cluster.timerange.start
                    >= topic_acc_clusters["intervals"][cluster.id]["start"]
                )
                assert (
                    cluster.timerange.end
                    <= topic_acc_clusters["intervals"][cluster.id]["end"]
                )

        # Merging all the response clusters coming from each topic.clusterize()
        # should be the same as calling .clusterize_all() on the QueryResponseItem

        # Case1: only override_clustering_dt_ns is provided
        result = item.clusterize_all(override_clustering_dt_ns=clustering_dt_ns)
        assert result == all_clusters_dict

        # Case2: clustering_map defined only for some ontologies, but override_clustering_dt_ns fills all the others
        partial_clustering_map = {Temperature.ontology_tag(): clustering_dt_ns}

        result = item.clusterize_all(partial_clustering_map, clustering_dt_ns)
        assert result == all_clusters_dict

        # Case3: clustering_map defined for ALL ontologies
        clustering_map = {
            Temperature.ontology_tag(): clustering_dt_ns,
            Pressure.ontology_tag(): clustering_dt_ns,
        }
        result = item.clusterize_all(clustering_map)
        assert result == all_clusters_dict

        # Case4: neither clustering_map nor override_clustering_dt_ns provided
        # -> necessary to recompute clusterize() for each topic with no clustering_dt_ns
        item_topic_clusterize = {
            topic.name: topic.clusterize() for topic in item.topics
        }
        item_clusterize_all = item.clusterize_all()
        assert item_topic_clusterize == item_clusterize_all


def test_filter_intersect_item_topic(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences_filter,  # Ensure the data are available on the data platform
):

    # Multi query with different topics
    query_resp = mosaico_client.query(
        QuerySequence().with_name_match("test-filter-sequence"),
        QueryOntologyCatalog()
        .with_expression(Temperature.Q.value.gt(0.5))  # > sin(pi/4)
        .with_expression(Pressure.Q.value.gt(0.5)),  # > cos(pi/6)
    )

    acceptance_intervals = {
        "intervals": [
            {
                "start": (math.pi / 6) * 1.0e9 - QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
                "end": (math.pi / 3) * 1.0e9 + QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
            },
            {
                "start": (2 * math.pi + math.pi / 6) * 1.0e9
                - QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
                "end": (2 * math.pi + math.pi / 3) * 1.0e9
                + QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
            },
        ],
    }

    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()

    for item in query_resp:
        # We do expect two (2) topics
        assert len(item.topics) == 2

        clustering_dt_ns = int(QUERY_FILTER_SEQUENCE_RESOLUTION_NS * 2)

        # Case 1): override_clustering_dt_ns. No clustering_map
        override_clustering_dt_ns = clustering_dt_ns
        clusters = item.topics[0].intersect(
            *item.topics,
            override_clustering_dt_ns=override_clustering_dt_ns,
        )

        assert len(clusters) == len(acceptance_intervals["intervals"])

        for cluster in clusters:
            assert (
                cluster.timerange.start
                >= acceptance_intervals["intervals"][cluster.id]["start"]
            )
            assert (
                cluster.timerange.end
                <= acceptance_intervals["intervals"][cluster.id]["end"]
            )

        # Case 2) Partial clustering_map and override_clustering_dt_ns
        override_clustering_dt_ns = clustering_dt_ns
        clusters = item.topics[0].intersect(
            *item.topics,
            clustering_map={
                Temperature.ontology_tag(): clustering_dt_ns,
            },
            override_clustering_dt_ns=override_clustering_dt_ns,
        )

        assert len(clusters) == len(acceptance_intervals["intervals"])

        for cluster in clusters:
            assert (
                cluster.timerange.start
                >= acceptance_intervals["intervals"][cluster.id]["start"]
            )
            assert (
                cluster.timerange.end
                <= acceptance_intervals["intervals"][cluster.id]["end"]
            )

        # Case 3) Full clustering_map
        override_clustering_dt_ns = clustering_dt_ns
        clusters = item.topics[0].intersect(
            *item.topics,
            clustering_map={
                Temperature.ontology_tag(): clustering_dt_ns,
                Pressure.ontology_tag(): clustering_dt_ns,
            },
        )

        assert len(clusters) == len(acceptance_intervals["intervals"])

        for cluster in clusters:
            assert (
                cluster.timerange.start
                >= acceptance_intervals["intervals"][cluster.id]["start"]
            )
            assert (
                cluster.timerange.end
                <= acceptance_intervals["intervals"][cluster.id]["end"]
            )


def test_filter_intersect_item_topic_no_overlapping(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences_filter,  # Ensure the data are available on the data platform
):
    """This test is very similar to previous one but there is not actual overlapping between the cluster"""

    # Multi query with different topics
    query_resp = mosaico_client.query(
        QuerySequence().with_name_match("test-filter-sequence"),
        QueryOntologyCatalog()
        .with_expression(Temperature.Q.value.gt(0.999999))  # >= sin(pi/2)
        .with_expression(Pressure.Q.value.gt(0.999999)),  # >= cos(0)
    )

    # Accepted intervals with intersect_dt_ns = pi
    acceptance_intervals = {
        "intervals": [
            {
                "start": 0 * 1.0e9 - QUERY_FILTER_SEQUENCE_RESOLUTION_NS * 2,
                "end": (math.pi / 2.0) * 1.0e9
                + QUERY_FILTER_SEQUENCE_RESOLUTION_NS * 2,
            },
            {
                "start": (7.0 / 2.0 * math.pi + math.pi / 2.0) * 1.0e9
                - QUERY_FILTER_SEQUENCE_RESOLUTION_NS
                * 2,  # 7/2pi + (pi) / 2 = 7/2pi + pi/2
                "end": (4.0 * math.pi - math.pi / 2.0) * 1.0e9
                + QUERY_FILTER_SEQUENCE_RESOLUTION_NS
                * 2,  # 4pi - (pi) / 2 = 4pi - pi/2
            },
        ],
    }

    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()

    for item in query_resp:
        # We do expect two (2) topics
        assert len(item.topics) == 2

        clustering_dt_ns = int(QUERY_FILTER_SEQUENCE_RESOLUTION_NS * 2)

        # Case 1): since clusters do not overlap, I expect that setting
        #          intersect_dt_ns=0 (default) returns no clusters
        override_clustering_dt_ns = clustering_dt_ns
        clusters = item.topics[0].intersect(
            item.topics[1],
            override_clustering_dt_ns=override_clustering_dt_ns,
        )

        assert len(clusters) == 0

        # Case 2): on the other hand, setting a pi intersect_dt_ns tollerance
        #          between clusters allow to still consider the overlapping valid

        intersect_dt_ns = math.pi * 1.0e9
        clusters = item.topics[0].intersect(
            item.topics[1],
            intersect_dt_ns=int(intersect_dt_ns),
            override_clustering_dt_ns=override_clustering_dt_ns,
        )

        assert len(clusters) == len(acceptance_intervals["intervals"])

        for cluster in clusters:
            assert (
                cluster.timerange.start
                >= acceptance_intervals["intervals"][cluster.id]["start"]
            )
            assert (
                cluster.timerange.end
                <= acceptance_intervals["intervals"][cluster.id]["end"]
            )
