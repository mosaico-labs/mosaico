# ======================================================================
# 3. UNIT TESTS
# ======================================================================
import pytest

from mosaicolabs.models.platform import Sequence, Topic
from mosaicolabs.models.query import QuerySequence, QueryTopic, Query
from mosaicolabs.models.query.generation.mixins import (
    _DynamicFieldFactoryMixin,
    _QueryableDynamicValue,
)
from mosaicolabs.models.query.expressions import (
    _QueryTopicExpression,
    _QuerySequenceExpression,
)


class TestQueryTopicMetadataAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        Topic.Q.user_metadata
        Topic.Q.user_metadata["something"]

        with pytest.raises(AttributeError):
            Topic.Q.name  # name is not queryable
        with pytest.raises(AttributeError):
            Topic.Q.user_metadata.something  # user_metadata is not dot-accessible

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # === IMU ===
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(Topic.Q.user_metadata), _DynamicFieldFactoryMixin)
        assert issubclass(type(Topic.Q.user_metadata["key"]), _QueryableDynamicValue)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """

        # --- Catalog Context: dict access Field & Operator ---
        test_numeric_value = 12345.67
        # Call: Topic.Q.user_metadata["some_field"].gt(test_numeric_value)
        # Expected: {'user_metadata.some-field': {'$gt': 12345.67}} - _QueryTopicExpression
        expr_mdata = Topic.Q.user_metadata["some-field"].gt(test_numeric_value)
        assert isinstance(expr_mdata, _QueryTopicExpression)
        assert expr_mdata.to_dict() == {
            "user_metadata.some-field": {"$gt": test_numeric_value}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Simulate the User Query
        q = Query(
            QueryTopic()
            .with_expression(Topic.Q.user_metadata["some-field"].eq("some_value"))
            .with_expression(
                Topic.Q.user_metadata["field.nested"].leq(0.1234)
            ),  # support numeric operators
        )

        # Define Expected Output
        expected_dict = {
            "topic": {
                "user_metadata": {
                    "some-field": {"$eq": "some_value"},
                    "field.nested": {"$leq": 0.1234},
                },
            }
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["topic"])

        # Check topic flatness (the simple part)
        assert result["topic"] == expected_dict["topic"]


class TestQuerySequenceMetadataAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        Sequence.Q.user_metadata
        Sequence.Q.user_metadata["something"]

        with pytest.raises(AttributeError):
            Sequence.Q.name  # name is not queryable
        with pytest.raises(AttributeError):
            Sequence.Q.user_metadata.something  # user_metadata is not dot-accessible

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(Sequence.Q.user_metadata), _DynamicFieldFactoryMixin)
        assert issubclass(type(Sequence.Q.user_metadata["key"]), _QueryableDynamicValue)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: dict access Field & Operator ---
        test_numeric_value = 12345.67
        # Call: Sequence.Q.user_metadata["some_field"].gt(test_numeric_value)
        # Expected: {'user_metadata.some-field': {'$gt': 12345.67}} - _QuerySequenceExpression
        expr_mdata = Sequence.Q.user_metadata["some-field"].gt(test_numeric_value)
        assert isinstance(expr_mdata, _QuerySequenceExpression)
        assert expr_mdata.to_dict() == {
            "user_metadata.some-field": {"$gt": test_numeric_value}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Simulate the User Query
        q = Query(
            QuerySequence()
            .with_expression(Sequence.Q.user_metadata["some-field"].eq("some_value"))
            .with_expression(
                Sequence.Q.user_metadata["field.nested"].leq(0.1234)
            ),  # support numeric operators
        )

        # Define Expected Output
        expected_dict = {
            "sequence": {
                "user_metadata": {
                    "some-field": {"$eq": "some_value"},
                    "field.nested": {"$leq": 0.1234},
                },
            }
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["sequence"])

        # Check topic flatness (the simple part)
        assert result["sequence"] == expected_dict["sequence"]
