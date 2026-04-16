# ======================================================================
# 3. UNIT TESTS
# ======================================================================
import pytest

from mosaicolabs.models.query import QuerySequence, QueryTopic


class TestQueryTopicMetadataAPI:
    def test_expression_generation(self):
        # Simulate the User Query
        qt = (
            QueryTopic()
            .with_user_metadata("some-field", eq="some_value")
            .with_user_metadata("field.nested", leq=0.1234)
        )
        # Define Expected Output
        expected_dict = {
            "user_metadata": {
                "some-field": {"$eq": "some_value"},
                "field.nested": {"$leq": 0.1234},
            },
        }
        # Assert the result
        result = qt.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["user_metadata"])

        # Check topic flatness (the simple part)
        assert result["user_metadata"] == expected_dict["user_metadata"]

    def test_wrong_operator(self):
        # Simulate the User Query
        with pytest.raises(
            AttributeError,
            match="'_QueryableDynamicValueField' object has no operator.",
        ):
            QueryTopic().with_user_metadata("some-field", wrong_op="some_value")

    def test_wrong_type_on_operator(self):
        # Simulate the User Query
        with pytest.raises(
            TypeError,
            match="Invalid type for '_QueryableDynamicValueField' comparison",
        ):
            QueryTopic().with_user_metadata("some-field", lt="some_value")


class TestQuerySequenceMetadataAPI:
    def test_expression_generation(self):
        # Simulate the User Query
        qt = (
            QuerySequence()
            .with_user_metadata("some-field", eq="some_value")
            .with_user_metadata("field.nested", leq=0.1234)
        )
        # Define Expected Output
        expected_dict = {
            "user_metadata": {
                "some-field": {"$eq": "some_value"},
                "field.nested": {"$leq": 0.1234},
            },
        }
        # Assert the result
        result = qt.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["user_metadata"])

        # Check topic flatness (the simple part)
        assert result["user_metadata"] == expected_dict["user_metadata"]

    def test_wrong_operator(self):
        # Simulate the User Query
        with pytest.raises(
            AttributeError,
            match="'_QueryableDynamicValueField' object has no operator.",
        ):
            QuerySequence().with_user_metadata("some-field", wrong_op="some_value")

    def test_wrong_type_on_operator(self):
        # Simulate the User Query
        with pytest.raises(
            TypeError,
            match="Invalid type for '_QueryableDynamicValueField' comparison",
        ):
            QuerySequence().with_user_metadata("some-field", lt="some_value")
