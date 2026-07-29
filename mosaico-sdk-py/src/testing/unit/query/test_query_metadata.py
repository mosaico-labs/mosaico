# ======================================================================
# 3. UNIT TESTS
# ======================================================================
import pytest

from mosaicolabs.query import QuerySequence, QueryTopic


class TestQueryTopicMetadataAPI:
    def test_expression_generation(self):
        # Simulate the User Query
        qt = (
            QueryTopic()
            .with_user_metadata("some-field", eq="some_value")
            .with_user_metadata("field.nested", leq=0.1234)
            .with_user_metadata("another-field.nested", match="#str_value[a-z]")
        )
        # Define Expected Output
        expected_dict = {
            "user_metadata": {
                "some-field": {"$eq": "some_value"},
                "field.nested": {"$leq": 0.1234},
                "another-field.nested": {"$match": "#str_value[a-z]"},
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
            QueryTopic().with_user_metadata("some-field", ex="some_value")
        with pytest.raises(
            TypeError,
            match="Invalid type for '_QueryableDynamicValueField' comparison",
        ):
            QueryTopic().with_user_metadata("some-field", ex=3.2)

        with pytest.raises(
            TypeError,
            match="Invalid type for '_QueryableDynamicValueField' comparison",
        ):
            QueryTopic().with_user_metadata("some-field", match=3.2)

    def test_expected_operators(self):
        # Simulate the User Query
        QueryTopic().with_user_metadata("some-field", eq="some_value")
        QueryTopic().with_user_metadata("some-field", lt=0)
        QueryTopic().with_user_metadata("some-field", gt=0)
        QueryTopic().with_user_metadata("some-field", geq=0)
        QueryTopic().with_user_metadata("some-field", leq=0)
        QueryTopic().with_user_metadata("some-field", lt="abcd")
        QueryTopic().with_user_metadata("some-field", gt="abcd")
        QueryTopic().with_user_metadata("some-field", geq="abcd")
        QueryTopic().with_user_metadata("some-field", leq="abcd")
        QueryTopic().with_user_metadata("some-field", ex=True)
        QueryTopic().with_user_metadata("some-field", ex=False)
        QueryTopic().with_user_metadata("some-field", between=[0, 1])
        # works with strings too
        QueryTopic().with_user_metadata("some-field", between=["a", "b"])
        with pytest.raises(TypeError, match="All values must be of the same type"):
            QueryTopic().with_user_metadata("some-field", between=["a", 1])
        with pytest.raises(ValueError, match="requires exactly two numeric values"):
            QueryTopic().with_user_metadata("some-field", between=["a", "b", "c"])
        QueryTopic().with_user_metadata("some-field", outside=[0, 1])
        # works with strings too
        QueryTopic().with_user_metadata("some-field", outside=["a", "b"])
        with pytest.raises(TypeError, match="All values must be of the same type"):
            QueryTopic().with_user_metadata("some-field", outside=["a", 1])
        with pytest.raises(ValueError, match="requires exactly two numeric values"):
            QueryTopic().with_user_metadata("some-field", outside=["a", "b", "c"])
        QueryTopic().with_user_metadata("some-field", match="abcd")


class TestQuerySequenceMetadataAPI:
    def test_expression_generation(self):
        # Simulate the User Query
        qt = (
            QuerySequence()
            .with_user_metadata("some-field", eq="some_value")
            .with_user_metadata("field.nested", leq=0.1234)
            .with_user_metadata("another-field.nested", match="*str_value?")
        )
        # Define Expected Output
        expected_dict = {
            "user_metadata": {
                "some-field": {"$eq": "some_value"},
                "field.nested": {"$leq": 0.1234},
                "another-field.nested": {"$match": "*str_value?"},
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
            QuerySequence().with_user_metadata("some-field", ex="some_value")
        with pytest.raises(
            TypeError,
            match="Invalid type for '_QueryableDynamicValueField' comparison",
        ):
            QuerySequence().with_user_metadata("some-field", ex=3.2)
        with pytest.raises(
            TypeError,
            match="Invalid type for '_QueryableDynamicValueField' comparison",
        ):
            QuerySequence().with_user_metadata("some-field", match=3.2)

    def test_expected_operators(self):
        # Simulate the User Query
        QuerySequence().with_user_metadata("some-field", eq="some_value")
        QuerySequence().with_user_metadata("some-field", lt=0)
        QuerySequence().with_user_metadata("some-field", gt=0)
        QuerySequence().with_user_metadata("some-field", geq=0)
        QuerySequence().with_user_metadata("some-field", leq=0)
        QuerySequence().with_user_metadata("some-field", lt="abcd")
        QuerySequence().with_user_metadata("some-field", gt="abcd")
        QuerySequence().with_user_metadata("some-field", geq="abcd")
        QuerySequence().with_user_metadata("some-field", leq="abcd")
        QuerySequence().with_user_metadata("some-field", ex=True)
        QuerySequence().with_user_metadata("some-field", ex=False)
        QuerySequence().with_user_metadata("some-field", between=[0, 1])
        # works with strings too
        QuerySequence().with_user_metadata("some-field", between=["a", "b"])
        with pytest.raises(TypeError, match="All values must be of the same type"):
            QuerySequence().with_user_metadata("some-field", between=["a", 1])
        with pytest.raises(ValueError, match="requires exactly two numeric values"):
            QuerySequence().with_user_metadata("some-field", between=["a", "b", "c"])
        QuerySequence().with_user_metadata("some-field", outside=[0, 1])
        # works with strings too
        QuerySequence().with_user_metadata("some-field", outside=["a", "b"])
        with pytest.raises(TypeError, match="All values must be of the same type"):
            QuerySequence().with_user_metadata("some-field", outside=["a", 1])
        with pytest.raises(ValueError, match="requires exactly two numeric values"):
            QuerySequence().with_user_metadata("some-field", outside=["a", "b", "c"])
        QuerySequence().with_user_metadata("some-field", match="abcd")
