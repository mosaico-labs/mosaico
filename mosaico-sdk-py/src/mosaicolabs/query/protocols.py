from typing import Any, Dict, List, Optional, Protocol, Tuple, Type

from ..proto.v1 import query_pb2
from .expressions import _QueryExpression


class _QueryableMixinProtocol(Protocol):
    __mixin_supported_types__: tuple[type, ...]


class QueryableProtocol(Protocol):
    """
    Structural protocol for classes that integrate into a multi-domain [`Query`][mosaicolabs.query.builders.Query].

    A class implicitly satisfies this protocol if it can fill its own field of the
    protobuf `Filter` message via `_apply_to()`. This protocol ensures that the root
    [`Query`][mosaicolabs.query.builders.Query] can orchestrate complex requests
    without knowing the specific internal logic of each sub-query.

    ### Reference Implementations
    The following classes are standard examples of this protocol:

    * [`QueryTopic`][mosaicolabs.query.builders.QueryTopic]: Filters Topic-level metadata.
    * [`QuerySequence`][mosaicolabs.query.builders.QuerySequence]: Filters Sequence-level metadata.
    * [`QueryOntologyCatalog`][mosaicolabs.query.builders.QueryOntologyCatalog]: Filters fine-grained sensor field data.
    """

    __supported_query_expressions__: Tuple[Type[_QueryExpression], ...]

    @classmethod
    def _from_expressions(cls, *exprs: _QueryExpression) -> "QueryableProtocol":
        """
        Internal method for creating a new query builder from a list of expressions.

        Args:
            exprs (_QueryExpression): A list of `_QueryExpression` instances.

        Returns:
            QueryableProtocol: A new `QueryableProtocol` instance containing the provided expressions.
        """
        ...

    def _apply_to(self, msg: query_pb2.Filter):
        """
        Fills the field of the `Filter` message targeted by this sub-query
        (`sequence`, `topic` or `ontology`).
        """
        ...

    def expressions(self) -> List[_QueryExpression]:
        """
        Return the list of query expressions.
        """
        ...


class FieldMapperProtocol(Protocol):
    """
    Protocol for a stateless field mapper.
    Its job is to inspect a class and return a nested dictionary
    (a "field schema") of all queryable paths.
    """

    def build_schema(
        self,
        class_type: Type,
        path_prefix: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Builds the queryable field schema for a given class.

        Args:
            class_type (Type): The Pydantic or Arrow class to inspect.
            path_prefix (Optional[str]): The current path prefix.
        """
        ...
