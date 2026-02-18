from typing import Any, Dict, Optional, Protocol, Tuple, Type
from .expressions import _QueryExpression


class _QueryableMixinProtocol(Protocol):
    __mixin_supported_types__: tuple[type, ...]


class QueryableProtocol(Protocol):
    """
    Structural protocol for classes that integrate into a multi-domain [`Query`][mosaicolabs.models.query.builders.Query].

    A class implicitly satisfies this protocol if it provides a unique identification tag
    via `name()` and a serialization method via `to_dict()`. This
    protocol ensures that the root [`Query`][mosaicolabs.models.query.builders.Query]
    or the  can
    orchestrate complex requests without knowing the specific internal logic of
    each sub-query.

    ### Reference Implementations
    The following classes are standard examples of this protocol:

    * [`QueryTopic`][mosaicolabs.models.query.builders.QueryTopic]: Filters Topic-level metadata.
    * [`QuerySequence`][mosaicolabs.models.query.builders.QuerySequence]: Filters Sequence-level metadata.
    * [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]: Filters fine-grained sensor field data.
    """

    __supported_query_expressions__: Tuple[Type[_QueryExpression], ...]

    def with_expression(self, expr: _QueryExpression) -> "QueryableProtocol":
        """
        Appends a new filter expression using a fluent interface.

        Args:
            expr: A valid `_QueryExpression`
        """
        ...

    def name(self) -> str:
        """
        Returns the unique key identifying this sub-query within the root request.

        Examples include `"topic"`, `"sequence"`, or `"ontology"`.
        """
        ...

    def to_dict(self) -> Dict[str, Any]:
        """
        Serializes the internal expressions into a platform-compatible dictionary.
        """
        ...


class FieldMapperProtocol(Protocol):
    """
    Protocol for a stateless field mapper.
    Its job is to inspect a class and return a nested dictionary
    (a "field map") of all queryable paths.
    """

    def build_map(
        self,
        class_type: Type,
        query_expression_type: Type[_QueryExpression],
        path_prefix: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Builds the queryable field map for a given class.

        Args:
            class_type: The Pydantic or Arrow class to inspect.
            query_expression_type: The _QueryExpression class (e.g., _QueryTopicExpression)
                                   to inject into the final _QueryableField.
            path_prefix: The current path prefix.
        """
        ...
