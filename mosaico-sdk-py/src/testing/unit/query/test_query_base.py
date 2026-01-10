from typing import Optional
from mosaicolabs.models.platform import Sequence, Topic
from mosaicolabs.models.query.expressions import _QueryExpression
from mosaicolabs.models.query.generation.mixins import (
    _QueryableComparable,
    _QueryableUnsupported,
    _QueryableString,
    _QueryableBool,
    _QueryableDynamicValue,
    _QueryableField,
)
from mosaicolabs.models.query import (
    QueryOntologyCatalog,
    QuerySequence,
    QueryTopic,
    Query,
    QueryResponse,
    QueryResponseItem,
    QueryResponseItemTopic,
    QueryResponseItemSequence,
)

from mosaicolabs.models.query.expressions import (
    _QueryCatalogExpression,
    _QuerySequenceExpression,
    _QueryTopicExpression,
)

from mosaicolabs.models.query.protocols import QueryableProtocol

import pytest


_QUERY_TYPES = [QueryOntologyCatalog, QueryTopic, QuerySequence]
_QUERY_EXPRESSION_TYPES = [
    _QueryCatalogExpression,
    _QueryTopicExpression,
    _QuerySequenceExpression,
]


@pytest.mark.parametrize(
    "query_type",
    _QUERY_TYPES,
)
def test_query_base_succeeds(query_type: type[QueryableProtocol]):
    """Tests the validation that prevents two instances of the same query builder."""

    # Single expression
    qexprt = query_type.__supported_query_expressions__[0]
    query_type(qexprt("tag.key", "$eq", 0))

    # Multiple expressions
    qexprt = query_type.__supported_query_expressions__[0]
    query_type(
        qexprt("tag.key", "$eq", 0),
        qexprt("tag.another_key", "$eq", 1),
        qexprt("tag.key.field", "$eq", 2),
        qexprt("tag.another_key.field", "$eq", 3),
    )


@pytest.mark.parametrize(
    "query_type",
    _QUERY_TYPES,
)
def test_query_base_fails_on_duplicate_type(query_type: type[QueryableProtocol]):
    """Tests the validation that prevents two instances of the same query builder."""

    qdc = query_type()

    # Fail on __init__
    with pytest.raises(ValueError, match="Duplicate query type detected"):
        Query(qdc, qdc)

    # Fail on append
    root_query = Query(qdc)
    with pytest.raises(ValueError, match="Duplicate query type detected"):
        root_query.append(query_type())

    # try appending other query types, different from current 'query_type'
    other_query_types = [oqt for oqt in _QUERY_TYPES if oqt is not query_type]
    for other_query_type in other_query_types:
        root_query.append(other_query_type())  # This must be ok!


@pytest.mark.parametrize(
    "query_type",
    _QUERY_TYPES,
)
def test_query_base_fails_on_bad_expression_type(query_type):
    """Tests the validation that prevents wrong expressions in query builders."""
    for query_expr_type in _QUERY_EXPRESSION_TYPES:
        qexpr = query_expr_type("key.field", "$eq", 0)
        if query_expr_type not in query_type.__supported_query_expressions__:
            with pytest.raises(TypeError, match="Invalid expression type"):
                query_type(qexpr)
        else:
            # must pass
            query_type(qexpr)


@pytest.mark.parametrize(
    "query_type",
    _QUERY_TYPES,
)
def test_query_base_fails_on_duplicate_key(query_type: type[QueryableProtocol]):
    """Tests the validation that prevents two instances of the same query builder."""

    # Fail on __init__
    qexprt = query_type.__supported_query_expressions__[0]
    qexpr = qexprt("tag.duplicate-key", "$eq", 0)
    with pytest.raises(
        NotImplementedError, match="Query builder already contains the key"
    ):
        qdc = query_type(qexpr, qexpr)

    qdc = query_type()
    # Fail on with_expression
    with pytest.raises(
        NotImplementedError, match="Query builder already contains the key"
    ):
        # implement a fake expression, just for generating duplicate keys
        qdc.with_expression(qexpr).with_expression(qexpr)


def test_query_succeed_on_metadata_multi_key():
    """Tests the validation that allows mulitple query criteria on user_metadata."""

    QuerySequence().with_expression(
        Sequence.Q.user_metadata["some-key"].eq(0)
    ).with_expression(Sequence.Q.user_metadata["some-other-key"].eq("value"))

    # Still fails if the key is repeated
    with pytest.raises(NotImplementedError):
        QuerySequence().with_expression(
            Sequence.Q.user_metadata["same-key"].geq(0)
        ).with_expression(Sequence.Q.user_metadata["same-key"].lt(3))

    QueryTopic().with_expression(
        Topic.Q.user_metadata["some-key"].eq(0)
    ).with_expression(Topic.Q.user_metadata["some-other-key"].eq("value"))

    # Still fails if the key is repeated
    with pytest.raises(NotImplementedError):
        QueryTopic().with_expression(
            Topic.Q.user_metadata["same-key"].geq(0)
        ).with_expression(Topic.Q.user_metadata["same-key"].lt(3))


def test_construction_query_from_response():
    qresp = QueryResponse(
        items=[
            QueryResponseItem(
                sequence=QueryResponseItemSequence(name="seq0"),
                topics=[
                    QueryResponseItemTopic(name="seq0/top00", timestamp_range=None),
                    QueryResponseItemTopic(name="seq0/top01", timestamp_range=None),
                ],  # constructor expects topic resource name
            ),
            QueryResponseItem(
                sequence=QueryResponseItemSequence(name="seq1"),
                topics=[
                    QueryResponseItemTopic(name="seq0/top10", timestamp_range=None),
                    QueryResponseItemTopic(name="seq0/top11", timestamp_range=None),
                ],  # constructor expects topic resource name
            ),
        ]
    )
    expected_expr_seq_values = [it.sequence.name for it in qresp]
    qseq = qresp.to_query_sequence()
    assert len(qseq._expressions) == 1
    assert qseq._expressions[0].key == "name"
    assert all(s in expected_expr_seq_values for s in qseq._expressions[0].value)
    assert all(s in qseq._expressions[0].value for s in expected_expr_seq_values)

    expected_expr_top_values = [
        # QueryResponseItem.__post_init__ normalizes topic name
        t.name
        for it in qresp
        for t in it.topics
    ]
    qtop = qresp.to_query_topic()
    assert len(qtop._expressions) == 1
    assert qtop._expressions[0].key == "name"
    assert all(t in expected_expr_top_values for t in qtop._expressions[0].value)
    assert all(t in qtop._expressions[0].value for t in expected_expr_top_values)



def test_invalid_construction_query_from_response():
    qresp = QueryResponse(
        items=[]
    )
    with pytest.raises(ValueError, match="Cannot create a 'QuerySequence' builder from an empty response"):
        qresp.to_query_sequence()
    with pytest.raises(ValueError, match="Cannot create a 'QueryTopic' builder from an empty response"):
        qresp.to_query_topic()


@pytest.mark.parametrize(
    "query_type",
    _QUERY_TYPES,
)
def test_query_base_fails_on_bad_operator_format(query_type: type[QueryableProtocol]):
    """Tests the validation that prevents two instances of the same query builder."""

    qdc = query_type()
    qexpr = query_type.__supported_query_expressions__[0]
    # Fail on __init__
    with pytest.raises(ValueError):
        # implement a fake expression, just for generating duplicate keys
        qdc.with_expression(
            qexpr("key", "eq", 0),
        )


_ALL_TESTING_TYPES = [int, float, str, bool]
_ALL_TESTING_OPERATORS = [
    "eq",
    "neq",
    "lt",
    "leq",
    "gt",
    "geq",
    "in_",
    "between",
    "match",
]


def _test_operators(
    queryable_type: type,
    operator: str,
    value_type: type,
    allowed_types: list[type],
    all_allowed_operators: list[str],
    allowed_varargs_operators: Optional[list[str]],
):
    # evaluation function
    def eval_func(val, allowed_varargs_operators, operator, op_fun):
        if not allowed_varargs_operators or operator not in allowed_varargs_operators:
            op_fun(val)
        else:
            test_pair = (val, val)
            op_fun(*test_pair)

    # create the queryable field type (which contains the operators and the comparison logics)
    # then instantiate the operator function 'op_fun'
    cls = type(
        f"{queryable_type.__name__}Field",
        (queryable_type, _QueryableField),
        {},
    )
    field = cls("", _QueryExpression)
    op_fun = getattr(field, operator)

    # Test 1: Calling not allowed operators: must call _QueryableField.__getattr__ AttributeError
    for other_operator in [
        oop for oop in _ALL_TESTING_OPERATORS if oop not in all_allowed_operators
    ]:
        with pytest.raises(
            AttributeError,
            match="object has no operator",
        ):
            getattr(field, other_operator)

    # instantiate the current test value type
    test_value = value_type(1)
    # Test 2: if we are provided with a value type which is not among the one allowed for the queryable field,
    # we must have a TypeError with that specific string.
    # This is necessary because 'value_type' can be any type, so we need a fallback
    if value_type not in allowed_types:
        with pytest.raises(
            TypeError, match=f"Invalid type for '{queryable_type.__name__}Field'"
        ):
            return eval_func(test_value, allowed_varargs_operators, operator, op_fun)
    else:
        eval_func(test_value, allowed_varargs_operators, operator, op_fun)
        # go over...

    # Test 3: For each possible (base, non-iterable) type, test the returns
    for other_type in _ALL_TESTING_TYPES:
        # we are specifically interested in types not allowed only; the others will be (or have been) tested in
        # future (or past) test runs
        if other_type not in allowed_types:
            with pytest.raises(
                TypeError, match=f"Invalid type for '{queryable_type.__name__}Field'"
            ):
                other_type_value = other_type(0)
                if (
                    not allowed_varargs_operators
                    or operator not in allowed_varargs_operators
                ):
                    op_fun(other_type_value)
                else:
                    op_fun(other_type_value, other_type_value)

    # Test 4: Specific test for operators taking varargs: check the ValueError when passing
    # values of different type
    for other_type in _ALL_TESTING_TYPES:
        if other_type is not value_type and (
            allowed_varargs_operators and operator in allowed_varargs_operators
        ):
            with pytest.raises(TypeError, match="All values must be of the same type"):
                other_type_value = other_type(0)
                op_fun(other_type_value, test_value)


class TestQueryableComparable:
    _allowed_types = [int, float]
    _allowed_unary_operators = ["eq", "neq", "lt", "leq", "gt", "geq"]
    _allowed_varargs_operators = ["in_", "between"]

    @pytest.mark.parametrize("value_type", _allowed_types)
    @pytest.mark.parametrize(
        "operator", _allowed_unary_operators + _allowed_varargs_operators
    )
    def test_operators(self, operator: str, value_type: type):
        return _test_operators(
            _QueryableComparable,
            operator,
            value_type,
            self._allowed_types,
            self._allowed_unary_operators + self._allowed_varargs_operators,
            self._allowed_varargs_operators,
        )


class TestQueryableString:
    _allowed_types = [str]
    _allowed_unary_operators = ["eq", "neq", "match"]
    _allowed_varargs_operators = ["in_"]

    @pytest.mark.parametrize("value_type", _allowed_types)
    @pytest.mark.parametrize(
        "operator", _allowed_unary_operators + _allowed_varargs_operators
    )
    def test_operators(self, operator: str, value_type: type):
        return _test_operators(
            _QueryableString,
            operator,
            value_type,
            self._allowed_types,
            self._allowed_unary_operators + self._allowed_varargs_operators,
            self._allowed_varargs_operators,
        )


class TestQueryableBool:
    _allowed_types = [bool]
    _allowed_unary_operators = ["eq"]

    @pytest.mark.parametrize("value_type", _allowed_types)
    @pytest.mark.parametrize("operator", _allowed_unary_operators)
    def test_operators(self, operator: str, value_type: type):
        return _test_operators(
            _QueryableBool,
            operator,
            value_type,
            self._allowed_types,
            self._allowed_unary_operators,
            None,
        )


class TestQueryableDynamic:
    _allowed_types = _ALL_TESTING_TYPES
    _allowed_unary_operators = ["eq", "lt", "leq", "gt", "geq"]
    _allowed_varargs_operators = ["between"]

    @pytest.mark.parametrize("value_type", _allowed_types)
    @pytest.mark.parametrize(
        "operator", _allowed_unary_operators + _allowed_varargs_operators
    )
    def test_operators(self, operator: str, value_type: type):
        # The allowed operators depend on the data type: decide here
        if operator in ["lt", "leq", "gt", "geq"]:
            return _test_operators(
                _QueryableDynamicValue,
                operator,
                value_type,
                [int, float],
                self._allowed_unary_operators + self._allowed_varargs_operators,
                None,
            )
        if operator in ["eq", "between"]:
            return _test_operators(
                _QueryableDynamicValue,
                operator,
                value_type,
                [int, float, str, bool],
                self._allowed_unary_operators + self._allowed_varargs_operators,
                ["between"],
            )


# Cuistom test for _QueryableUnsupported
@pytest.mark.parametrize(
    "operator",
    _ALL_TESTING_OPERATORS,
)
def test_qyeryable_unsupported_no_operators(operator: str):
    cls = type(
        f"{_QueryableUnsupported.__name__}Field",
        (_QueryableUnsupported, _QueryableField),
        {},
    )
    field = cls("", _QueryExpression)
    with pytest.raises(AttributeError, match="provides no operators."):
        getattr(field, operator)
