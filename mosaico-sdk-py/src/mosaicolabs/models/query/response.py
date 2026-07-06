from dataclasses import dataclass, field
from typing import Any, Iterator, List, Optional

from pyarrow.flight import FlightClient

from mosaicolabs.helpers import unpack_topic_full_path

from ...comm.do_action_page import (
    _do_action_page,
    _DoActionPageResponseFilterClusterize,
    _DoActionPageResponseFilterIntersect,
)
from ...enum.flight_action import FlightAction
from ...logging_config import get_logger
from .builders import QueryOntologyCatalog, QuerySequence, QueryTopic
from .expressions import (
    _QueryCatalogExpression,
    _QuerySequenceExpression,
    _QueryTopicExpression,
)
from .protocols import QueryableProtocol
from .topic_cluster import TimestampRange, TopicCluster

# Set the hierarchical logger
logger = get_logger(__name__)


def _build_clusterize_payload(
    item_topic: "QueryResponseItemTopic",
    clustering_dt_ns: Optional[int] = None,
    timestamp_range: Optional["TimestampRange"] = None,
    include_timestamp_range: bool = True,
) -> dict[str, Any]:
    """Creates the payload for the topic filter clusterize do_action_page"""

    # Merging expressions within a single dict
    merged_exprs = {
        k: v for expr in item_topic._query_exprs for k, v in expr.to_dict().items()
    }

    payload = {
        "locator": item_topic.locator,
        "clustering_dt_ns": clustering_dt_ns
        if clustering_dt_ns
        else item_topic.DEFAULT_CLUSTERING_DT,
        "ontology": merged_exprs,
    }

    if include_timestamp_range:
        payload["timestamp_range"] = (
            timestamp_range.to_dict() if timestamp_range else None
        )

    return payload


def _build_intersect_payload(
    item_topics: list["QueryResponseItemTopic"],
    intersect_dt_ns: int,
    clustering_map: Optional[dict[str, int]] = None,
    override_clustering_dt_ns: Optional[int] = None,
):
    """Creates the payload for the topic filter intersect do_action_page"""

    return {
        "topics": [
            _build_clusterize_payload(
                t,
                clustering_map.get(t.ontology_tag, override_clustering_dt_ns)
                if clustering_map
                else override_clustering_dt_ns,
                include_timestamp_range=False,
            )
            for t in item_topics
        ],
        "intersect_dt_ns": intersect_dt_ns,
    }


@dataclass
class QueryResponseItemSequence:
    """
    Metadata container for a single sequence discovered during a query.

    Attributes:
        name (str): The unique identifier of the sequence in the Mosaico database.
    """

    name: str

    @classmethod
    def _from_dict(cls, qdict: dict[str, str]) -> "QueryResponseItemSequence":
        return cls(name=qdict["sequence"])


@dataclass
class QueryResponseItemTopic:
    """
    Metadata for a specific topic (sensor stream) within a sequence.

    Contains information about the topic's identity and its available
    time range in the archive.

    Attributes:
        locator (str): The locator path of the topic (e.g., 'seq1/front_camera/image_raw').
        ontology_tag (str): Ontology of the topic in string format (e.g. image)
        timestamp_range (Optional[TimestampRange]): The availability window of the data
            for this specific topic.
        name: The name of the topic itself (extracted at construction from locator attribute)
    """

    locator: str
    ontology_tag: str
    timestamp_range: Optional[TimestampRange]
    name: str = field(default="", init=False)

    DEFAULT_CLUSTERING_DT: int = 0

    _client: Optional[FlightClient] = field(default=None, init=False)
    _query_exprs: List[_QueryCatalogExpression] = field(
        default_factory=list, init=False
    )

    def __post_init__(self):

        # Extracts the topic's name from the locator
        seq_topic_tuple = unpack_topic_full_path(self.locator)
        if not seq_topic_tuple:
            raise ValueError(f"Invalid topic name in response '{self.locator}'")
        _, self.name = seq_topic_tuple

    def clusterize(
        self,
        clustering_dt_ns: Optional[int] = None,
        timestamp_range: Optional[TimestampRange] = None,
    ) -> list[TopicCluster]:
        """
        The query computes an interval representing the very first and very last time instant in which the query results satisfied.
        This function divides the such interval in clusters. Cluster distance can be set using clustering_dt_ns
        Therefore:
            - smaller clustering_dt_ns would create more clusters
            - bigger clustering_dt_ns would create less clusters since more samples are merged

        Set clustering_dt_ns to zero (default) returns a unique cluster coinciding with the whole original interval

        Args:
            clustering_dt_ns (Optional[int]): The minimal gap (in nanoseconds) there needs to be between two clusters
                to be considered different. If None, fallbacks to default (0), meaning returning a single [min, max] cluster.
            timestamp_range (Optional[TimestampRange]): timerange to restrict the search. Cluster outside this range are negletted set_clustering_gap()

        Returns:
            A list[TopicCluster] with all the clusters where the query is true.

        Raises:
            Exception in case of an internal error
        """
        ACTION = FlightAction.TOPIC_FILTER_CLUSTERIZE

        # Making the request
        try:
            act_resp = _do_action_page(
                client=self._client,
                action=ACTION,
                payload=_build_clusterize_payload(
                    self, clustering_dt_ns, timestamp_range
                ),
                expected_type=_DoActionPageResponseFilterClusterize,
            )

            return act_resp.clusters

        except Exception as e:
            logger.error(f"Topic filter clusterize returned an internal error: '{e}'")
            raise

    def intersect(
        self,
        *query_response_item_topics: "QueryResponseItemTopic",
        intersect_dt_ns: int = 0,
        clustering_map: Optional[dict[str, int]] = None,
        override_clustering_dt_ns: Optional[int] = None,
    ) -> list[TopicCluster]:
        """
        Computes the temporal intersection of this topic with one or more other topics. Nevertheless, setting
        intersect_dt_ns > 0 relaxes the overlapping constraint, allowing distant clusters to still be considered overlapping.
        This is useful when your signal satisfies your query for a short period of time and you want to compare it with another
        signal that is temporally close but not happening in the same moment.

        Args:
            *query_response_item_topics: Additional topics to include in the intersection.
            intersect_dt_ns (int): Max allowed distance (in nanoseconds) between clusters to be considered overlapped.
                Setting it to zero (default) ensures the existance for inter-cluster overlapping.
            clustering_map (Optional[dict[str, int]]): Map from ontology tag to clustering_dt_ns.
                When provided, each topic uses the value for its ontology tag as
                its clustering gap; missing tags fall back to ``override_clustering_dt_ns`` or default (0).
            override_clustering_dt_ns (Optional[int]): Override for the default clustering
                gap (0) applied when ``clustering_map`` is None or does not contain the
                topic's ontology tag.

        Returns:
            A list of :class:`TopicCluster` representing the time windows where all topics' query
              expressions are simultaneously true, above the given ``intersect_dt_ns`` tolerance.

        Raises:
            Exception: Propagated from the underlying action call on internal server errors.
        """
        ACTION = FlightAction.TOPIC_FILTER_INTERSECT

        try:
            act_resp = _do_action_page(
                client=self._client,
                action=ACTION,
                payload=_build_intersect_payload(
                    [self, *query_response_item_topics],
                    intersect_dt_ns,
                    clustering_map,
                    override_clustering_dt_ns,
                ),
                expected_type=_DoActionPageResponseFilterIntersect,
            )

            return act_resp.clusters

        except Exception as e:
            logger.error(f"Topic filter intersect returned an internal error: '{e}'")
            raise

    @classmethod
    def _from_dict(cls, tdict: dict[str, Any]) -> "QueryResponseItemTopic":
        locator = tdict["locator"]
        t_ontology_tag = tdict["ontology_tag"]
        tsrange = tdict.get("timestamp_range")

        return cls(
            locator=locator,
            ontology_tag=t_ontology_tag,
            timestamp_range=TimestampRange(start=int(tsrange[0]), end=int(tsrange[1]))
            if tsrange
            else None,
        )

    def _set_client(self, client: FlightClient):
        self._client = client

    def _set_query_expressions(self, exprs: List[_QueryCatalogExpression]):
        if not self.ontology_tag:
            raise RuntimeError(
                f"Impossible to set expressions of {self.name} QueryResponseItemTopic. Ontology tag has not been set"
            )

        self._query_exprs = self._get_related_expressions(exprs)

    def _get_related_expressions(
        self, exprs: List[_QueryCatalogExpression]
    ) -> List[_QueryCatalogExpression]:

        if not self.ontology_tag:
            return []

        return [expr for expr in exprs if expr.get_expr_tag() == self.ontology_tag]


@dataclass
class QueryResponseItem:
    """
    A unified result item representing a sequence and its associated topics.

    This serves as the primary unit of data returned when querying the
    Mosaico metadata catalog.

    Attributes:
        sequence (QueryResponseItemSequence): The parent sequence metadata.
        topics (List[QueryResponseItemTopic]): The list of topics available
            within this sequence that matched the query criteria.
    """

    sequence: QueryResponseItemSequence
    topics: List[QueryResponseItemTopic]
    _client: Optional[FlightClient] = field(default=None, init=False)
    _query_exprs: List[_QueryCatalogExpression] = field(
        default_factory=list, init=False
    )

    def clusterize_all(
        self,
        clustering_map: Optional[dict[str, int]] = None,
        override_clustering_dt_ns: Optional[int] = None,
    ) -> dict[str, list[TopicCluster]]:
        """
        Calls clusterize on every topic in this response item and returns
        the results indexed by topic name.

        Iterates over all topics in this sequence and invokes
        :meth:`QueryResponseItemTopic.clusterize` on each, using each
        topic's own ``clustering_dt_ns`` gap setting.

        Args:
            clustering_map (dict[str, int]): An optional map indicating
              for each ontology tag within the query the minimal gap
              (in nanoseconds) there needs to be between two clusters to
              be considered different
            override_clustering_dt_ns (Optional[int]): Override for the
                default clustering gap (0) applied when ``clustering_map``
                is None or does not contain the topic's ontology tag.

        Returns:
            A ``dict`` mapping each topic name (str) to its list of
            :class:`TopicCluster` objects, where each cluster represents
            a contiguous time window in which the query expression
            evaluated to true.

        Raises:
            Exception: Propagated from :meth:`QueryResponseItemTopic.clusterize`
                if any topic's action call fails.
        """

        output = {}
        for topic in self.topics:
            clustering_dt_ns = (
                clustering_map.get(topic.ontology_tag, override_clustering_dt_ns)
                if clustering_map
                else override_clustering_dt_ns
            )
            output.update({topic.name: topic.clusterize(clustering_dt_ns)})

        return output

    def intersect(
        self,
        *query_response_item: "QueryResponseItem",
        intersect_dt_ns: int = 0,
        clustering_map: Optional[dict[str, int]] = None,
        override_clustering_dt_ns: Optional[int] = None,
    ) -> list[TopicCluster]:
        """
        Computes the temporal intersection of all topics within the response item.

        For each topic, the query expressions are merged and sent to the server together with
        the topic's ``clustering_dt_ns`` gap (if not present default (0) is set). The server
        returns the time windows (clusters) in which *all* topics simultaneously satisfy their
        respective query expressions. Nevertheless, setting intersect_dt_ns > 0 relaxes the
        overlapping constraint, allowing distant clusters to still be considered overlapping.
        This is useful when your signal satisfies your query for a short period of time and
        you want to compare it with another signal that is temporally close but not happening
        in the same moment.

        Args:
            intersect_dt_ns (int): Max allowed distance (in nanoseconds) between clusters to be considered overlapped.
                Setting it to zero (default) ensures the existance for inter-cluster overlapping.
            clustering_map (Optional[dict[str, int]]): An optional map indicating for each ontology tag within the query
              the minimal gap (in nanoseconds) there needs to be between two clusters to be considered different. If not
              specified all topics use default value (0). If specified but topic's ontology is missing, fallback using
              default value (0).
            override_clustering_dt_ns: An optional integer to override the default minimal gap between clusters (0).
            *query_response_item: Additional response items whose topics are included in the
              intersection. All topics from every extra item are flattened together with the
              topics of this item before the intersect payload is built.

        Returns:
            A list of :class:`TopicCluster` representing the time windows where all topics' query
              expressions are simultaneously true, above the given ``intersect_dt_ns`` tolerance.

        Raises:
            Exception: Propagated from the underlying action call on internal server errors.
        """

        ACTION = FlightAction.TOPIC_FILTER_INTERSECT

        total_topics = self.topics
        if query_response_item:
            total_topics += [t for item in query_response_item for t in item.topics]

        try:
            act_resp = _do_action_page(
                client=self._client,
                action=ACTION,
                payload=_build_intersect_payload(
                    total_topics,
                    intersect_dt_ns,
                    clustering_map,
                    override_clustering_dt_ns,
                ),
                expected_type=_DoActionPageResponseFilterIntersect,
            )

            return act_resp.clusters

        except Exception as e:
            logger.error(f"Topic filter intersect returned an internal error: '{e}'")
            raise

    @classmethod
    def _from_dict(cls, qdict: dict[str, Any]) -> "QueryResponseItem":
        return cls(
            sequence=QueryResponseItemSequence._from_dict(qdict),
            topics=[
                QueryResponseItemTopic._from_dict(tdict) for tdict in qdict["topics"]
            ],
        )

    def _set_client(self, client: FlightClient):
        self._client = client
        for t in self.topics:
            t._set_client(client)

    def _set_query_expressions(self, exprs: List[_QueryCatalogExpression]):
        self._query_exprs = exprs
        for t in self.topics:
            t._set_query_expressions(exprs)


@dataclass
class QueryResponse:
    """
    An iterable collection of results returned by a Mosaico metadata query.

    This class provides convenience methods to transform search results back into
    query builders, enabling a fluid, multi-stage filtering workflow.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, IMU, Floating64, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter IMU data by a specific acquisition second
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.timestamp_ns.lt(1770282868))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter primitive Floating64 telemetry by frame identifier
            qresponse = client.query(
                QueryOntologyCatalog(Floating64.Q.frame_id.eq("robot_base"))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```

    Attributes:
        items (List[QueryResponseItem]): The list of items matching the query.
    """

    # Use field(default_factory=list) to handle cases where no items are passed
    items: List[QueryResponseItem] = field(default_factory=list)

    def to_query_sequence(self) -> QuerySequence:
        """
        Converts the current response into a QuerySequence builder.

        This allows for further filtering or operations on the specific set of
        sequences returned in this response.

        Example:
            This demonstrates query chaining to narrow your search to specific sequences and topics.
            This is necessary when criteria span different data channels; otherwise,
            the resulting filters chained in `AND` in a single query would produce an empty result.

            ```python
            from mosaicolabs import MosaicoClient, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Broad Search: Find sequences with high-precision GPS
                initial_response = client.query(QueryOntologyCatalog(GPS.Q.status.status.eq(2)))

                # Chaining: Use results to "lock" the domain and find specific data in those sequences
                # on different data channels
                if not initial_response.is_empty():
                    final_response = client.query(
                        initial_response.to_query_sequence(),              # The "locked" sequence domain
                        QueryTopic().with_name("/localization/log_string"), # Target a specific log topic
                        QueryOntologyCatalog(String.Q.data.match("[ERR]"))  # Filter by content
                    )
            ```

        Returns:
            QuerySequence: A builder initialized with an '$in' filter on the sequence names.

        Raises:
            ValueError: If the response is empty.
        """
        if not self.items:
            raise ValueError(
                "Cannot create a 'QuerySequence' builder from an empty response"
            )
        return QuerySequence._from_expressions(
            _QuerySequenceExpression(
                full_path="name",
                op="$in",
                value=[it.sequence.name for it in self.items],
            )
        )

    def to_query_topic(self) -> QueryTopic:
        """
        Converts the current response into a QueryTopic builder.

        Useful for narrowing down a search to specific topics found within
        the retrieved sequences.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, QueryTopic

            with MosaicoClient.connect("localhost", 6726) as client:
                # Broad Search: Find sequences with high-precision GPS
                initial_response = client.query(
                        QueryTopic().with_name("/localization/log_string"), # Target a specific log topic
                        QuerySequence().with_name_match("test_winter_2025_")  # Filter by content
                    )

                # Chaining: Use results to "lock" the domain and find specific log-patterns in those sequences
                if not initial_response.is_empty():
                    final_response = client.query(
                        initial_response.to_query_topic(),              # The "locked" topic domain
                        QueryOntologyCatalog(String.Q.data.match("[ERR]"))  # Filter by content
                    )
            ```

        Returns:
            QueryTopic: A builder initialized with an '$in' filter on the topic names.

        Raises:
            ValueError: If the response is empty.

        """
        if not self.items:
            raise ValueError(
                "Cannot create a 'QueryTopic' builder from an empty response"
            )
        return QueryTopic._from_expressions(
            _QueryTopicExpression(
                "name",
                "$in",
                [t.name for it in self.items for t in it.topics],
            )
        )

    def _set_client(self, client: FlightClient):
        for it in self.items:
            it._set_client(client)

    def _set_queries(self, queries: List[QueryableProtocol]):

        query_ontology_catalog = next(
            (q for q in queries if isinstance(q, QueryOntologyCatalog)), None
        )

        if not query_ontology_catalog:
            return

        ontology_catalog_exprs = query_ontology_catalog.expressions()

        for it in self.items:
            it._set_query_expressions(ontology_catalog_exprs)

    def __len__(self) -> int:
        """Returns the number of items in the response."""
        return len(self.items)

    def __iter__(self) -> Iterator[QueryResponseItem]:
        """Iterates over the QueryResponseItem instances in the response."""
        return iter(self.items)

    def __getitem__(self, index: int) -> QueryResponseItem:
        """Retrieves a specific result item by its index."""
        return self.items[index]

    def is_empty(self) -> bool:
        """Returns True if the response contains no results."""
        return len(self.items) == 0
