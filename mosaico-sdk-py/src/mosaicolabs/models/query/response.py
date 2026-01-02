from dataclasses import dataclass, field
from typing import Iterator, List

from mosaicolabs.helpers import unpack_topic_full_path
from mosaicolabs.helpers.helpers import pack_topic_resource_name

from .builders import QuerySequence, QueryTopic
from .expressions import _QuerySequenceExpression, _QueryTopicExpression


@dataclass
class QueryResponseItem:
    sequence: str
    topics: List[str]

    def __post_init__(self):
        """
        Returned topics are the full resource names, e.g. 'sequence_name/the/topic/name'.
        Retrieve the topic name only, i.e. '/the/topic/name'
        """
        tnames = []
        for top in self.topics:
            seq_topic_tuple = unpack_topic_full_path(top)
            if not seq_topic_tuple:
                raise ValueError(f"Invalid topic name in response {top}")
            _, tname = seq_topic_tuple
            tnames.append(tname)
        # reset topic names
        self.topics = tnames


@dataclass
class QueryResponse:
    # Use field(default_factory=list) to handle cases where no items are passed
    items: List[QueryResponseItem] = field(default_factory=list)

    def to_query_sequence(self) -> QuerySequence:
        return QuerySequence(
            _QuerySequenceExpression(
                "name",
                "$in",
                [it.sequence for it in self.items],
            )
        )

    def to_query_topic(self) -> QueryTopic:
        return QueryTopic(
            _QueryTopicExpression(
                "name",
                "$in",
                [t for it in self.items for t in it.topics],
            )
        )

    def __len__(self) -> int:
        """Allows using len(response)."""
        return len(self.items)

    def __iter__(self) -> Iterator[QueryResponseItem]:
        """
        Allows using 'for item in response'.
        Delegates to the underlying list's iterator.
        """
        return iter(self.items)

    def __getitem__(self, index: int) -> QueryResponseItem:
        """
        Allows access via index: response[0]
        """
        return self.items[index]

    def is_empty(self) -> bool:
        """Helper to check if response has data."""
        return len(self.items) == 0
