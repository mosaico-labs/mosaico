from dataclasses import dataclass, field
from typing import Iterator, List, Optional, Any

from mosaicolabs.helpers import unpack_topic_full_path

from .builders import QuerySequence, QueryTopic
from .expressions import _QuerySequenceExpression, _QueryTopicExpression


@dataclass
class TimestampRange:
    start: int
    end: int


@dataclass
class QueryResponseItemSequence:
    name: str

    @classmethod
    def _from_dict(cls, qdict: dict[str, str]) -> "QueryResponseItemSequence":
        return cls(name=qdict["sequence"])


@dataclass
class QueryResponseItemTopic:
    name: str
    timestamp_range: Optional[TimestampRange]

    @classmethod
    def _from_dict(cls, tdict: dict[str, Any]) -> "QueryResponseItemTopic":
        seq_topic_tuple = unpack_topic_full_path(tdict["locator"])
        if not seq_topic_tuple:
            raise ValueError(f"Invalid topic name in response '{tdict['locator']}'")
        _, tname = seq_topic_tuple
        tsrange = tdict.get("timestamp_range")

        return cls(
            name=tname,
            timestamp_range=TimestampRange(start=int(tsrange[0]), end=int(tsrange[1]))
            if tsrange
            else None,
        )


@dataclass
class QueryResponseItem:
    sequence: QueryResponseItemSequence
    topics: List[QueryResponseItemTopic]

    @classmethod
    def _from_dict(cls, qdict: dict[str, Any]) -> "QueryResponseItem":
        return cls(
            sequence=QueryResponseItemSequence._from_dict(qdict),
            topics=[
                QueryResponseItemTopic._from_dict(tdict) for tdict in qdict["topics"]
            ],
        )


@dataclass
class QueryResponse:
    # Use field(default_factory=list) to handle cases where no items are passed
    items: List[QueryResponseItem] = field(default_factory=list)

    def to_query_sequence(self) -> QuerySequence:
        return QuerySequence(
            _QuerySequenceExpression(
                "name",
                "$in",
                [it.sequence.name for it in self.items],
            )
        )

    def to_query_topic(self) -> QueryTopic:
        return QueryTopic(
            _QueryTopicExpression(
                "name",
                "$in",
                [t.name for it in self.items for t in it.topics],
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
