import math

from mosaicolabs import MosaicoClient, Point3d, Pressure, Temperature, TimestampRange
from mosaicolabs.models.query import QueryOntologyCatalog, QuerySequence
from testing.integration.config import QUERY_FILTER_SEQUENCE_RESOLUTION_NS


def error_within_tollerance(
    timerange: TimestampRange,
    expected_start: float,
    expected_end: float,
    eps: float = 0,
) -> bool:
    return (
        abs(timerange.start - expected_start) <= eps
        and abs(timerange.end - expected_end) <= eps
    )


def test_filter_clusterize_single_expression_single_topic(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences_filter,  # Ensure the data are available on the data platform
):
    query_resp = mosaico_client.query(
        QuerySequence().with_name_match("test-filter-sequence-#"),
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

            assert error_within_tollerance(
                clusters[0].timerange,
                (math.pi / 6) * 1.0e9,
                (3 * math.pi - math.pi / 6) * 1.0e9,
                QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
            )  # pi/6 ... 3pi - pi/6

            # Clusterize with clustering_dt_ns
            clustering_dt_ns = QUERY_FILTER_SEQUENCE_RESOLUTION_NS * 2
            clusters = topic.clusterize(int(clustering_dt_ns))

            # Expected two (2) clusters
            assert len(clusters) == 2

            assert error_within_tollerance(
                clusters[0].timerange,
                (math.pi / 6) * 1.0e9,
                (math.pi - math.pi / 6) * 1.0e9,
                QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
            )  # pi/6 ... pi - pi/6

            assert error_within_tollerance(
                clusters[1].timerange,
                (2 * math.pi + math.pi / 6) * 1.0e9,
                (3 * math.pi - math.pi / 6) * 1.0e9,
                QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
            )  # 2pi + pi/6 ... 3pi - pi/6

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

            assert error_within_tollerance(
                clusters[0].timerange,
                (math.pi / 6) * 1.0e9,
                (5 / 6 * math.pi) * 1.0e9,
                QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
            )

    mosaico_client.close()


def test_filter_clusterize_multi_expression_single_topic(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences_filter,  # Ensure the data are available on the data platform
):
    # Multi query single different topic
    query_resp = mosaico_client.query(
        QuerySequence().with_name_match("test-filter-sequence-#"),
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
            assert error_within_tollerance(
                clusters[0].timerange,
                math.pi / 3 * 1.0e9,
                (math.pi - math.pi / 3) * 1.0e9,
                QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
            )  # pi/3 ... pi - pi/3

            assert error_within_tollerance(
                clusters[1].timerange,
                (2 * math.pi + math.pi / 3) * 1.0e9,
                (3 * math.pi - math.pi / 3) * 1.0e9,
                QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
            )  # 2pi + pi/3 ... 3pi - pi/3

    mosaico_client.close()


def test_filter_clusterize_all_multi_expression_multi_topic(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences_filter,  # Ensure the data are available on the data platform
):

    # Multi query with different topics
    query_resp = mosaico_client.query(
        QuerySequence().with_name_match("test-filter-sequence-#"),
        QueryOntologyCatalog()
        .with_expression(Temperature.Q.value.gt(0.5))  # > sin(pi/6)
        .with_expression(Pressure.Q.value.gt(0.5)),  # > cos(pi/3)
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
                    "start": (2 * math.pi - math.pi / 3) * 1.0e9,
                    "end": (2 * math.pi + math.pi / 3) * 1.0e9,
                },
                {
                    "start": (4 * math.pi - math.pi / 3) * 1.0e9,
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

            # Checking expected clusters
            assert len(clusters) == len(topic_acc_clusters["intervals"])

            all_clusters_dict.update({topic.name: clusters})

            for cluster in clusters:
                assert error_within_tollerance(
                    cluster.timerange,
                    topic_acc_clusters["intervals"][cluster.id]["start"],
                    topic_acc_clusters["intervals"][cluster.id]["end"],
                    QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
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

    mosaico_client.close()


def test_filter_intersect_single_sequence(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences_filter,  # Ensure the data are available on the data platform
):

    # Multi query with different topics
    query_resp = mosaico_client.query(
        QuerySequence().with_name_match("test-filter-sequence-#"),
        QueryOntologyCatalog()
        .with_expression(Temperature.Q.value.gt(0.5))  # > sin(pi/6)
        .with_expression(Pressure.Q.value.gt(0.5)),  # > cos(pi/3)
    )

    acceptance_intervals = {
        "intervals": [
            {
                "start": (math.pi / 6) * 1.0e9,
                "end": (math.pi / 3) * 1.0e9,
            },
            {
                "start": (2 * math.pi + math.pi / 6) * 1.0e9,
                "end": (2 * math.pi + math.pi / 3) * 1.0e9,
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

        assert clusters is not None and len(clusters) == len(
            acceptance_intervals["intervals"]
        )

        for cluster in clusters:
            assert error_within_tollerance(
                cluster.timerange,
                acceptance_intervals["intervals"][cluster.id]["start"],
                acceptance_intervals["intervals"][cluster.id]["end"],
                QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
            )

        # Result from multi topic intersection of the same sequence should be equal to QueryResponse.intersect() with the same parameters
        assert clusters == item.intersect(
            override_clustering_dt_ns=override_clustering_dt_ns
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
            assert error_within_tollerance(
                cluster.timerange,
                acceptance_intervals["intervals"][cluster.id]["start"],
                acceptance_intervals["intervals"][cluster.id]["end"],
                QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
            )

        # Result from multi topic intersection of the same sequence should be equal to QueryResponse.intersect() with the same parameters
        assert clusters == item.intersect(
            clustering_map={
                Temperature.ontology_tag(): clustering_dt_ns,
            },
            override_clustering_dt_ns=override_clustering_dt_ns,
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
            assert error_within_tollerance(
                cluster.timerange,
                acceptance_intervals["intervals"][cluster.id]["start"],
                acceptance_intervals["intervals"][cluster.id]["end"],
                QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
            )

        # Result from multi topic intersection of the same sequence should be equal to QueryResponse.intersect() with the same parameters
        assert clusters == item.intersect(
            clustering_map={
                Temperature.ontology_tag(): clustering_dt_ns,
                Pressure.ontology_tag(): clustering_dt_ns,
            },
        )

        # Case 4) no parameters specified -> expected just one intersection cluster
        clusters = item.topics[0].intersect(*item.topics)

        assert len(clusters) == 1

        assert error_within_tollerance(
            clusters[0].timerange,
            math.pi / 6.0 * 1.0e9,
            (3 * math.pi - math.pi / 6.0) * 1.0e9,
            QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
        )

        # Result from multi topic intersection of the same sequence should be equal to QueryResponse.intersect() with the same parameters
        assert clusters == item.intersect()

    mosaico_client.close()


def test_filter_intersect_item_topic_no_overlapping(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences_filter,  # Ensure the data are available on the data platform
):
    """This test is very similar to previous one but there is not actual overlapping between the cluster"""

    # Multi query with different topics
    query_resp = mosaico_client.query(
        QuerySequence().with_name_match("test-filter-sequence-#"),
        QueryOntologyCatalog()
        .with_expression(Temperature.Q.value.gt(0.999999))  # >= sin(pi/2)
        .with_expression(Pressure.Q.value.gt(0.999999)),  # >= cos(0)
    )

    # Accepted intervals with intersect_dt_ns = pi
    acceptance_intervals = {
        "intervals": [
            {
                "start": 0.0,
                "end": (math.pi / 2.0) * 1.0e9,
            },
            {
                "start": (2.0 * math.pi) * 1.0e9,
                "end": (5.0 / 2.0 * math.pi) * 1.0e9,
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

        # Result from multi topic intersection of the same sequence should be equal to QueryResponse.intersect() with the same parameters
        assert clusters == item.intersect(
            override_clustering_dt_ns=override_clustering_dt_ns
        )

        # Case 2): on the other hand, setting intersect_dt_ns tollerance between
        #          clusters to pi allows to still consider the overlapping valid

        intersect_dt_ns = math.pi * 1.0e9
        clusters = item.topics[0].intersect(
            item.topics[1],
            intersect_dt_ns=int(intersect_dt_ns),
            override_clustering_dt_ns=override_clustering_dt_ns,
        )

        assert len(clusters) == len(acceptance_intervals["intervals"])

        for cluster in clusters:
            assert error_within_tollerance(
                cluster.timerange,
                acceptance_intervals["intervals"][cluster.id]["start"],
                acceptance_intervals["intervals"][cluster.id]["end"],
                QUERY_FILTER_SEQUENCE_RESOLUTION_NS * 2,
            )

        # Result from multi topic intersection of the same sequence should be equal to QueryResponse.intersect() with the same parameters
        assert clusters == item.intersect(
            intersect_dt_ns=int(intersect_dt_ns),
            override_clustering_dt_ns=override_clustering_dt_ns,
        )

    mosaico_client.close()


# TODO: enable when backend enables intersection between different sequences
def _test_filter_intersect_multi_sequence_overlapping(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences_filter,  # Ensure the data are available on the data platform
):
    """Intersection between two distinct queries"""
    # Two distinct queries returning two different sequences
    query_resp1 = mosaico_client.query(
        QuerySequence().with_name_match("test-filter-sequence-1"),
        QueryOntologyCatalog()
        .with_expression(Temperature.Q.value.gt(0.5))  # >= sin(pi/6)
        .with_expression(Pressure.Q.value.gt(0.5)),  # >= cos(pi/3)
    )

    query_resp2 = mosaico_client.query(
        QuerySequence().with_name_match("test-filter-sequence-2"),
        QueryOntologyCatalog()
        .with_expression(Point3d.Q.x.gt(0.5))
        .with_expression(Point3d.Q.y.gt(0.5))
        .with_expression(Point3d.Q.z.gt(0.5)),
    )

    # Accepted intervals with intersect_dt_ns = 0 -> clusters are already overlapping
    acceptance_intervals = {
        "intervals": [
            {
                "start": (math.pi / 6) * 1.0e9,
                "end": (math.pi / 3) * 1.0e9,
            },
            {
                "start": (2 * math.pi + math.pi / 6) * 1.0e9,
                "end": (2 * math.pi + math.pi / 3) * 1.0e9,
            },
        ],
    }

    # We do expect a successful query
    assert query_resp1 is not None and not query_resp1.is_empty()
    assert query_resp2 is not None and not query_resp2.is_empty()

    # We do expect a single sequence from both queries
    assert len(query_resp1.items) == 1
    assert len(query_resp2.items) == 1

    # Case 1) calling intersect with just other SequenceItems
    clusters = query_resp1.items[0].intersect(*query_resp2.items)

    assert len(clusters) == 1

    assert error_within_tollerance(
        clusters[0].timerange,
        math.pi / 6.0 * 1.0e9,
        (3 * math.pi - math.pi / 6.0) * 1.0e9,
        QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
    )

    # Case 2) calling intersect with other SequenceItems and override_clustering_dt_ns defined

    clustering_dt_ns = int(QUERY_FILTER_SEQUENCE_RESOLUTION_NS * 2)

    clusters = query_resp1.items[0].intersect(
        *query_resp2.items, override_clustering_dt_ns=clustering_dt_ns
    )

    assert len(clusters) == len(acceptance_intervals["intervals"])

    for cluster in clusters:
        assert error_within_tollerance(
            cluster.timerange,
            acceptance_intervals["intervals"][cluster.id]["start"],
            acceptance_intervals["intervals"][cluster.id]["end"],
            QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
        )

    # Case 3) calling intersect with other SequenceItems, partial clustering_map and
    #  override_clustering_dt_ns defined

    clusters = query_resp1.items[0].intersect(
        *query_resp2.items,
        override_clustering_dt_ns=clustering_dt_ns,
        clustering_map={
            Pressure.ontology_tag(): clustering_dt_ns,
        },
    )

    assert len(clusters) == len(acceptance_intervals["intervals"])

    for cluster in clusters:
        assert error_within_tollerance(
            cluster.timerange,
            acceptance_intervals["intervals"][cluster.id]["start"],
            acceptance_intervals["intervals"][cluster.id]["end"],
            QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
        )

    # Case 4) calling intersect with other SequenceItems, total clustering_map

    clusters = query_resp1.items[0].intersect(
        *query_resp2.items,
        clustering_map={
            Temperature.ontology_tag(): clustering_dt_ns,
            Pressure.ontology_tag(): clustering_dt_ns,
        },
    )

    assert len(clusters) == len(acceptance_intervals["intervals"])

    for cluster in clusters:
        assert error_within_tollerance(
            cluster.timerange,
            acceptance_intervals["intervals"][cluster.id]["start"],
            acceptance_intervals["intervals"][cluster.id]["end"],
            QUERY_FILTER_SEQUENCE_RESOLUTION_NS,
        )

    mosaico_client.close()
