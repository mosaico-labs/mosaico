# ======================================================================
# 3. UNIT TESTS
# ======================================================================

from mosaicolabs.models.data import (
    ForceTorque,
    Pose,
    Transform,
    Acceleration,
    MotionState,
    Velocity,
    ROI,
    String,
    Integer8,
    Integer16,
    Integer32,
    Integer64,
    Unsigned8,
    Unsigned16,
    Unsigned32,
    Unsigned64,
    Boolean,
    Floating16,
    Floating32,
    Floating64,
    LargeString,
)

from mosaicolabs.models.query.generation.mixins import (
    _QueryableNumeric,
    _QueryableString,
    _QueryableBool,
)
from mosaicolabs.models.query.expressions import (
    _QueryCatalogExpression,
)
from mosaicolabs.models.query import Query, QueryOntologyCatalog
import pytest


class TestQueryTransformAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        Transform.Q.translation.x
        Transform.Q.translation.y
        Transform.Q.translation.z
        # Inherited from Vector3d
        Transform.Q.translation.covariance_type
        Transform.Q.rotation.x
        Transform.Q.rotation.y
        Transform.Q.rotation.z
        Transform.Q.rotation.w
        # Inherited from Quaternion
        Transform.Q.rotation.covariance_type
        Transform.Q.target_frame_id
        # Inherited from HeaderMixin
        Transform.Q.header.seq
        Transform.Q.header.stamp.sec
        Transform.Q.header.stamp.nanosec
        Transform.Q.header.frame_id
        # Inherited from Message
        Transform.Q.timestamp_ns
        # Inherited from CovarianceMixin
        Transform.Q.covariance_type

        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            Transform.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(Transform.Q.translation.x), _QueryableNumeric)
        assert issubclass(type(Transform.Q.translation.y), _QueryableNumeric)
        assert issubclass(type(Transform.Q.translation.z), _QueryableNumeric)
        assert issubclass(
            type(Transform.Q.translation.covariance_type), _QueryableNumeric
        )
        assert issubclass(type(Transform.Q.rotation.x), _QueryableNumeric)
        assert issubclass(type(Transform.Q.rotation.y), _QueryableNumeric)
        assert issubclass(type(Transform.Q.rotation.z), _QueryableNumeric)
        assert issubclass(type(Transform.Q.rotation.w), _QueryableNumeric)
        assert issubclass(type(Transform.Q.target_frame_id), _QueryableString)
        assert issubclass(type(Transform.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(Transform.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(Transform.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(Transform.Q.header.frame_id), _QueryableString)
        assert issubclass(type(Transform.Q.covariance_type), _QueryableNumeric)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: Nested Field & Operator ---
        test_numeric_value = 12345.67
        expr_trans_nested = Transform.Q.translation.y.gt(test_numeric_value)
        assert isinstance(expr_trans_nested, _QueryCatalogExpression)
        assert expr_trans_nested.to_dict() == {
            "transform.translation.y": {"$gt": test_numeric_value}
        }
        expr_trans_nested = Transform.Q.translation.y.eq(test_numeric_value)
        assert isinstance(expr_trans_nested, _QueryCatalogExpression)
        assert expr_trans_nested.to_dict() == {
            "transform.translation.y": {"$eq": test_numeric_value}
        }

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        expr_trans_between = Transform.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_trans_between, _QueryCatalogExpression)
        assert expr_trans_between.to_dict() == {
            "transform.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Simulate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(Transform.Q.timestamp_ns.gt(12345.67))
            .with_expression(Transform.Q.translation.y.gt(12345.67)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "transform.timestamp_ns": {"$gt": 12345.67},
                "transform.translation.y": {"$gt": 12345.67},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]


class TestQueryPoseAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        Pose.Q.position.x
        Pose.Q.position.y
        Pose.Q.position.z
        # Inherited from Vector3d
        Pose.Q.position.covariance_type
        Pose.Q.orientation.x
        Pose.Q.orientation.y
        Pose.Q.orientation.z
        Pose.Q.orientation.w
        # Inherited from Quaternion
        Pose.Q.orientation.covariance_type
        # Inherited from HeaderMixin
        Pose.Q.header.seq
        Pose.Q.header.stamp.sec
        Pose.Q.header.stamp.nanosec
        Pose.Q.header.frame_id
        # Inherited from Message
        Pose.Q.timestamp_ns
        # Inherited from CovarianceMixin
        Pose.Q.covariance_type

        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            Pose.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(Pose.Q.position.x), _QueryableNumeric)
        assert issubclass(type(Pose.Q.position.y), _QueryableNumeric)
        assert issubclass(type(Pose.Q.position.z), _QueryableNumeric)
        assert issubclass(type(Pose.Q.position.covariance_type), _QueryableNumeric)
        assert issubclass(type(Pose.Q.orientation.x), _QueryableNumeric)
        assert issubclass(type(Pose.Q.orientation.y), _QueryableNumeric)
        assert issubclass(type(Pose.Q.orientation.z), _QueryableNumeric)
        assert issubclass(type(Pose.Q.orientation.w), _QueryableNumeric)
        assert issubclass(type(Pose.Q.orientation.covariance_type), _QueryableNumeric)
        assert issubclass(type(Pose.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(Pose.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(Pose.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(Pose.Q.header.frame_id), _QueryableString)
        assert issubclass(type(Pose.Q.covariance_type), _QueryableNumeric)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: Nested Field & Operator ---
        test_numeric_value = 12345.67
        expr_nested = Pose.Q.position.y.gt(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "pose.position.y": {"$gt": test_numeric_value},
        }
        expr_nested = Pose.Q.position.y.eq(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "pose.position.y": {"$eq": test_numeric_value},
        }

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        expr_between = Pose.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "pose.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Simulate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(Pose.Q.timestamp_ns.gt(12345.67))
            .with_expression(Pose.Q.position.y.gt(12345.67)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "pose.timestamp_ns": {"$gt": 12345.67},
                "pose.position.y": {"$gt": 12345.67},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]


class TestQueryVelocityAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        Velocity.Q.linear.x
        Velocity.Q.linear.y
        Velocity.Q.linear.z
        # Inherited from Vector3d
        Velocity.Q.linear.covariance_type
        Velocity.Q.angular.x
        Velocity.Q.angular.y
        Velocity.Q.angular.z
        # Inherited from Vector3d
        Velocity.Q.angular.covariance_type
        # Inherited from HeaderMixin
        Velocity.Q.header.seq
        Velocity.Q.header.stamp.sec
        Velocity.Q.header.stamp.nanosec
        Velocity.Q.header.frame_id
        # Inherited from Message
        Velocity.Q.timestamp_ns
        # Inherited from CovarianceMixin
        Velocity.Q.covariance_type

        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            Velocity.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(Velocity.Q.linear.x), _QueryableNumeric)
        assert issubclass(type(Velocity.Q.linear.y), _QueryableNumeric)
        assert issubclass(type(Velocity.Q.linear.z), _QueryableNumeric)
        assert issubclass(type(Velocity.Q.linear.covariance_type), _QueryableNumeric)
        assert issubclass(type(Velocity.Q.angular.x), _QueryableNumeric)
        assert issubclass(type(Velocity.Q.angular.y), _QueryableNumeric)
        assert issubclass(type(Velocity.Q.angular.z), _QueryableNumeric)
        assert issubclass(type(Velocity.Q.angular.covariance_type), _QueryableNumeric)
        assert issubclass(type(Velocity.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(Velocity.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(Velocity.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(Velocity.Q.header.frame_id), _QueryableString)
        assert issubclass(type(Velocity.Q.covariance_type), _QueryableNumeric)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: Nested Field & Operator ---
        test_numeric_value = 12345.67
        expr_nested = Velocity.Q.linear.y.gt(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "velocity.linear.y": {"$gt": test_numeric_value},
        }
        expr_nested = Velocity.Q.linear.y.eq(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "velocity.linear.y": {"$eq": test_numeric_value},
        }

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        expr_between = Velocity.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "velocity.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Simulate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(Velocity.Q.timestamp_ns.gt(12345.67))
            .with_expression(Velocity.Q.linear.y.gt(12345.67)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "velocity.timestamp_ns": {"$gt": 12345.67},
                "velocity.linear.y": {"$gt": 12345.67},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]


class TestQueryAccelerationAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        Acceleration.Q.linear.x
        Acceleration.Q.linear.y
        Acceleration.Q.linear.z
        # Inherited from Vector3d
        Acceleration.Q.linear.covariance_type
        Acceleration.Q.angular.x
        Acceleration.Q.angular.y
        Acceleration.Q.angular.z
        # Inherited from Vector3d
        Acceleration.Q.angular.covariance_type
        # Inherited from HeaderMixin
        Acceleration.Q.header.seq
        Acceleration.Q.header.stamp.sec
        Acceleration.Q.header.stamp.nanosec
        Acceleration.Q.header.frame_id
        # Inherited from Message
        Acceleration.Q.timestamp_ns
        # Inherited from CovarianceMixin
        Acceleration.Q.covariance_type

        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            Acceleration.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(Acceleration.Q.linear.x), _QueryableNumeric)
        assert issubclass(type(Acceleration.Q.linear.y), _QueryableNumeric)
        assert issubclass(type(Acceleration.Q.linear.z), _QueryableNumeric)
        assert issubclass(
            type(Acceleration.Q.linear.covariance_type), _QueryableNumeric
        )
        assert issubclass(type(Acceleration.Q.angular.x), _QueryableNumeric)
        assert issubclass(type(Acceleration.Q.angular.y), _QueryableNumeric)
        assert issubclass(type(Acceleration.Q.angular.z), _QueryableNumeric)
        assert issubclass(
            type(Acceleration.Q.angular.covariance_type), _QueryableNumeric
        )
        assert issubclass(type(Acceleration.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(Acceleration.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(Acceleration.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(Acceleration.Q.header.frame_id), _QueryableString)
        assert issubclass(type(Acceleration.Q.covariance_type), _QueryableNumeric)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: Nested Field & Operator ---
        test_numeric_value = 12345.67
        expr_nested = Acceleration.Q.linear.y.gt(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "acceleration.linear.y": {"$gt": test_numeric_value},
        }
        expr_nested = Acceleration.Q.linear.y.eq(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "acceleration.linear.y": {"$eq": test_numeric_value},
        }

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        expr_between = Acceleration.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "acceleration.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Simulate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(Acceleration.Q.timestamp_ns.gt(12345.67))
            .with_expression(Acceleration.Q.linear.y.gt(12345.67)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "acceleration.timestamp_ns": {"$gt": 12345.67},
                "acceleration.linear.y": {"$gt": 12345.67},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]


class TestQueryMotionStateAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        MotionState.Q.pose.position.x
        MotionState.Q.pose.position.y
        MotionState.Q.pose.position.z
        MotionState.Q.pose.orientation.x
        MotionState.Q.pose.orientation.y
        MotionState.Q.pose.orientation.z
        MotionState.Q.pose.orientation.w
        # Inherited from Vector3d
        MotionState.Q.pose.position.covariance_type
        MotionState.Q.pose.orientation.covariance_type
        # Inherited from HeaderMixin
        MotionState.Q.velocity.linear.x
        MotionState.Q.velocity.linear.y
        MotionState.Q.velocity.linear.z
        MotionState.Q.velocity.angular.x
        MotionState.Q.velocity.angular.y
        MotionState.Q.velocity.angular.z
        # Inherited from Vector3d
        MotionState.Q.velocity.linear.covariance_type
        MotionState.Q.velocity.angular.covariance_type
        # Inherited from HeaderMixin
        MotionState.Q.header.seq
        MotionState.Q.header.stamp.sec
        MotionState.Q.header.stamp.nanosec
        MotionState.Q.header.frame_id
        # Inherited from Message
        MotionState.Q.timestamp_ns
        # Inherited from CovarianceMixin
        MotionState.Q.covariance_type
        MotionState.Q.target_frame_id

        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            MotionState.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(MotionState.Q.pose.position.x), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.pose.position.y), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.pose.position.z), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.pose.orientation.x), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.pose.orientation.y), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.pose.orientation.z), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.pose.orientation.w), _QueryableNumeric)
        assert issubclass(
            type(MotionState.Q.pose.position.covariance_type), _QueryableNumeric
        )
        assert issubclass(
            type(MotionState.Q.pose.orientation.covariance_type), _QueryableNumeric
        )
        assert issubclass(type(MotionState.Q.velocity.linear.x), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.velocity.linear.y), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.velocity.linear.z), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.velocity.angular.x), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.velocity.angular.y), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.velocity.angular.z), _QueryableNumeric)
        assert issubclass(
            type(MotionState.Q.velocity.linear.covariance_type), _QueryableNumeric
        )
        assert issubclass(
            type(MotionState.Q.velocity.angular.covariance_type), _QueryableNumeric
        )
        assert issubclass(type(MotionState.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.header.frame_id), _QueryableString)
        assert issubclass(type(MotionState.Q.covariance_type), _QueryableNumeric)
        assert issubclass(type(MotionState.Q.target_frame_id), _QueryableString)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: Nested Field & Operator ---
        test_numeric_value = 12345.67
        expr_nested = MotionState.Q.pose.position.y.gt(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "motion_state.pose.position.y": {"$gt": test_numeric_value},
        }
        expr_nested = MotionState.Q.pose.position.y.eq(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "motion_state.pose.position.y": {"$eq": test_numeric_value},
        }

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        expr_between = MotionState.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "motion_state.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Simulate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(MotionState.Q.timestamp_ns.gt(12345.67))
            .with_expression(MotionState.Q.velocity.linear.y.gt(12345.67)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "motion_state.timestamp_ns": {"$gt": 12345.67},
                "motion_state.velocity.linear.y": {"$gt": 12345.67},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]


class TestQueryForceTorqueAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        ForceTorque.Q.force.x
        ForceTorque.Q.force.y
        ForceTorque.Q.force.z
        # Inherited from Vector3d
        ForceTorque.Q.force.covariance_type
        ForceTorque.Q.torque.x
        ForceTorque.Q.torque.y
        ForceTorque.Q.torque.z
        # Inherited from Vector3d
        ForceTorque.Q.torque.covariance_type
        # Inherited from HeaderMixin
        ForceTorque.Q.header.seq
        ForceTorque.Q.header.stamp.sec
        ForceTorque.Q.header.stamp.nanosec
        ForceTorque.Q.header.frame_id
        # Inherited from Message
        ForceTorque.Q.timestamp_ns
        # Inherited from CovarianceMixin
        ForceTorque.Q.covariance_type

        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            ForceTorque.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(ForceTorque.Q.force.x), _QueryableNumeric)
        assert issubclass(type(ForceTorque.Q.force.y), _QueryableNumeric)
        assert issubclass(type(ForceTorque.Q.force.z), _QueryableNumeric)
        assert issubclass(type(ForceTorque.Q.force.covariance_type), _QueryableNumeric)
        assert issubclass(type(ForceTorque.Q.torque.x), _QueryableNumeric)
        assert issubclass(type(ForceTorque.Q.torque.y), _QueryableNumeric)
        assert issubclass(type(ForceTorque.Q.torque.z), _QueryableNumeric)
        assert issubclass(type(ForceTorque.Q.torque.covariance_type), _QueryableNumeric)
        assert issubclass(type(ForceTorque.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(ForceTorque.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(ForceTorque.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(ForceTorque.Q.header.frame_id), _QueryableString)
        assert issubclass(type(ForceTorque.Q.covariance_type), _QueryableNumeric)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: Nested Field & Operator ---
        test_numeric_value = 12345.67
        expr_nested = ForceTorque.Q.force.y.gt(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "force_torque.force.y": {"$gt": test_numeric_value},
        }
        expr_nested = ForceTorque.Q.torque.y.eq(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "force_torque.torque.y": {"$eq": test_numeric_value},
        }

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        expr_between = ForceTorque.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "force_torque.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Simulate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(ForceTorque.Q.timestamp_ns.gt(12345.67))
            .with_expression(ForceTorque.Q.torque.y.gt(12345.67)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "force_torque.timestamp_ns": {"$gt": 12345.67},
                "force_torque.torque.y": {"$gt": 12345.67},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]


class TestQueryROIAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        ROI.Q.offset.x
        ROI.Q.offset.y
        ROI.Q.offset.covariance_type
        ROI.Q.height
        ROI.Q.width
        ROI.Q.do_rectify
        # Inherited from HeaderMixin
        ROI.Q.header.seq
        ROI.Q.header.stamp.sec
        ROI.Q.header.stamp.nanosec
        ROI.Q.header.frame_id
        # Inherited from Message
        ROI.Q.timestamp_ns

        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            ROI.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(ROI.Q.offset.x), _QueryableNumeric)
        assert issubclass(type(ROI.Q.offset.y), _QueryableNumeric)
        assert issubclass(type(ROI.Q.offset.covariance_type), _QueryableNumeric)
        assert issubclass(type(ROI.Q.height), _QueryableNumeric)
        assert issubclass(type(ROI.Q.width), _QueryableNumeric)
        assert issubclass(type(ROI.Q.do_rectify), _QueryableBool)
        assert issubclass(type(ROI.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(ROI.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(ROI.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(ROI.Q.header.frame_id), _QueryableString)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: Nested Field & Operator ---
        test_numeric_value = 12345.67
        expr_nested = ROI.Q.offset.y.gt(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "roi.offset.y": {"$gt": test_numeric_value},
        }
        expr_ = ROI.Q.width.eq(test_numeric_value)
        assert isinstance(expr_, _QueryCatalogExpression)
        assert expr_.to_dict() == {
            "roi.width": {"$eq": test_numeric_value},
        }

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        expr_between = ROI.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "roi.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Simulate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(ROI.Q.timestamp_ns.gt(12345.67))
            .with_expression(ROI.Q.offset.y.gt(12345.67)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "roi.timestamp_ns": {"$gt": 12345.67},
                "roi.offset.y": {"$gt": 12345.67},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]


@pytest.mark.parametrize(
    "base_type",
    [
        Integer8,
        Integer16,
        Integer32,
        Integer64,
        Unsigned8,
        Unsigned16,
        Unsigned32,
        Unsigned64,
        Floating16,
        Floating32,
        Floating64,
        Boolean,
        String,
        LargeString,
    ],
)
class TestQueryBaseTypesAPI:
    def test_accessibility(self, base_type):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        base_type.Q.data

        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            base_type.Q.non_existing_field

    def test_field_queryable_inheritance(self, base_type):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        if base_type.model_fields["data"].annotation in (int, float):
            assert issubclass(type(base_type.Q.data), _QueryableNumeric)
        elif base_type.model_fields["data"].annotation is bool:
            assert issubclass(type(base_type.Q.data), _QueryableBool)
        elif base_type.model_fields["data"].annotation is str:
            assert issubclass(type(String.Q.data), _QueryableString)

    def test_full_sdk_query_to_dict_structure(self, base_type):
        """Tests the final output structure of an example query."""

        # Simulate the User Query
        val = base_type.model_fields["data"].annotation(
            0
        )  # convert to the 0 value of the actual 'data' type
        q = Query(
            QueryOntologyCatalog().with_expression(base_type.Q.data.eq(val))
        )  # eq() is provided by all the _QueryableField
        # Define Expected Output
        expected_dict = {
            "ontology": {
                f"{base_type.ontology_tag()}.data": {"$eq": val},
            },
        }
        # Assert the result
        result = q.to_dict()
        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]

        # For the other types are the same. Skip testing for now...
