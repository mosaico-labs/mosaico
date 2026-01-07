# ======================================================================
# 3. UNIT TESTS
# ======================================================================
import pytest

from mosaicolabs.models.query import (
    Query,
    QueryOntologyCatalog,
)

from mosaicolabs.models.query.generation.mixins import (
    _QueryableNumeric,
    _QueryableString,
    _QueryableBool,
)

from mosaicolabs.models.query.expressions import (
    _QueryCatalogExpression,
)

from mosaicolabs.models.sensors import (
    IMU,
    GPS,
    Image,
    Magnetometer,
    Temperature,
    Pressure,
    Range,
)


class TestQueryIMUAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        IMU.Q.acceleration.x
        IMU.Q.acceleration.y
        IMU.Q.acceleration.z
        # Inherited from Vector3d
        IMU.Q.acceleration.covariance_type
        IMU.Q.angular_velocity.x
        IMU.Q.angular_velocity.y
        IMU.Q.angular_velocity.z
        # Inherited from Vector3d
        IMU.Q.angular_velocity.covariance_type
        IMU.Q.orientation.x
        IMU.Q.orientation.y
        IMU.Q.orientation.z
        IMU.Q.orientation.w
        # Inherited from Quaternion
        IMU.Q.orientation.covariance_type
        # Inherited from HeaderMixin
        IMU.Q.header.seq
        IMU.Q.header.stamp.sec
        IMU.Q.header.stamp.nanosec
        IMU.Q.header.frame_id
        # Inherited from Message
        IMU.Q.timestamp_ns
        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            IMU.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # === IMU ===
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(IMU.Q.acceleration.x), _QueryableNumeric)
        assert issubclass(type(IMU.Q.acceleration.y), _QueryableNumeric)
        assert issubclass(type(IMU.Q.acceleration.z), _QueryableNumeric)
        assert issubclass(type(IMU.Q.acceleration.covariance_type), _QueryableNumeric)
        assert issubclass(type(IMU.Q.angular_velocity.x), _QueryableNumeric)
        assert issubclass(type(IMU.Q.angular_velocity.y), _QueryableNumeric)
        assert issubclass(type(IMU.Q.angular_velocity.z), _QueryableNumeric)
        assert issubclass(
            type(IMU.Q.angular_velocity.covariance_type), _QueryableNumeric
        )
        assert issubclass(type(IMU.Q.orientation.x), _QueryableNumeric)
        assert issubclass(type(IMU.Q.orientation.y), _QueryableNumeric)
        assert issubclass(type(IMU.Q.orientation.z), _QueryableNumeric)
        assert issubclass(type(IMU.Q.orientation.w), _QueryableNumeric)
        assert issubclass(type(IMU.Q.orientation.covariance_type), _QueryableNumeric)
        assert issubclass(type(IMU.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(IMU.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(IMU.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(IMU.Q.header.frame_id), _QueryableString)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: Nested Field & Operator ---
        test_numeric_value = 12345.67
        # Call: IMU.Q.acceleration.y.gt(test_numeric_value)
        # Expected: {'imu.acceleration.y': {'$gt': 12345.67}} - _QueryCatalogExpression
        expr_nested = IMU.Q.acceleration.y.gt(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "imu.acceleration.y": {"$gt": test_numeric_value}
        }
        expr_nested = IMU.Q.acceleration.y.eq(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "imu.acceleration.y": {"$eq": test_numeric_value}
        }

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        # Call: GPS.Q.timestamp_ns.between(10000, 30000)
        # Expected: {'gps.timestamp_ns': {'$between': [10000, 30000]}} - _QueryCatalogExpression
        expr_between = IMU.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "imu.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Simulate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(IMU.Q.timestamp_ns.gt(12345.67))
            .with_expression(IMU.Q.acceleration.y.gt(12345.67)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "imu.timestamp_ns": {"$gt": 12345.67},
                "imu.acceleration.y": {"$gt": 12345.67},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]


class TestQueryGPSAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        GPS.Q.position.x
        GPS.Q.position.y
        GPS.Q.position.z
        # Inherited from Vector3d
        GPS.Q.position.covariance_type
        GPS.Q.velocity.x
        GPS.Q.velocity.y
        GPS.Q.velocity.z
        # Inherited from Vector3d
        GPS.Q.velocity.covariance_type
        GPS.Q.status.status
        GPS.Q.status.satellites
        GPS.Q.status.hdop
        GPS.Q.status.vdop
        # Inherited from HeaderMixin
        GPS.Q.header.seq
        GPS.Q.header.stamp.sec
        GPS.Q.header.stamp.nanosec
        GPS.Q.header.frame_id
        # Inherited from Message
        GPS.Q.timestamp_ns
        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            GPS.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(GPS.Q.position.x), _QueryableNumeric)
        assert issubclass(type(GPS.Q.position.y), _QueryableNumeric)
        assert issubclass(type(GPS.Q.position.z), _QueryableNumeric)
        assert issubclass(type(GPS.Q.position.covariance_type), _QueryableNumeric)
        assert issubclass(type(GPS.Q.velocity.x), _QueryableNumeric)
        assert issubclass(type(GPS.Q.velocity.y), _QueryableNumeric)
        assert issubclass(type(GPS.Q.velocity.z), _QueryableNumeric)
        assert issubclass(type(GPS.Q.velocity.covariance_type), _QueryableNumeric)
        assert issubclass(type(GPS.Q.status.status), _QueryableNumeric)
        assert issubclass(type(GPS.Q.status.satellites), _QueryableNumeric)
        assert issubclass(type(GPS.Q.status.hdop), _QueryableNumeric)
        assert issubclass(type(GPS.Q.status.vdop), _QueryableNumeric)
        assert issubclass(type(GPS.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(GPS.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(GPS.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(GPS.Q.header.frame_id), _QueryableString)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: Nested Field & Operator ---
        test_numeric_value = 12345.67
        # Call: GPS.Q.position.y.gt(test_numeric_value)
        # Expected: {'gps.position.y': {'$gt': 12345.67}} - _QueryCatalogExpression
        expr_nested = GPS.Q.position.y.gt(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {"gps.position.y": {"$gt": test_numeric_value}}
        expr_nested = GPS.Q.position.y.eq(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {"gps.position.y": {"$eq": test_numeric_value}}

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        # Call: GPS.Q.timestamp_ns.between(10000, 30000)
        # Expected: {'gps.timestamp_ns': {'$between': [10000, 30000]}} - _QueryCatalogExpression
        expr_between = GPS.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "gps.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Sgpslate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(GPS.Q.timestamp_ns.gt(12345.67))
            .with_expression(GPS.Q.position.y.gt(12345.67)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "gps.timestamp_ns": {"$gt": 12345.67},
                "gps.position.y": {"$gt": 12345.67},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]


class TestQueryImageAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        Image.Q.format
        Image.Q.width
        Image.Q.height
        Image.Q.stride
        Image.Q.is_bigendian
        Image.Q.encoding
        # Inherited from HeaderMixin
        Image.Q.header.seq
        Image.Q.header.stamp.sec
        Image.Q.header.stamp.nanosec
        Image.Q.header.frame_id
        # Inherited from Message
        Image.Q.timestamp_ns
        with pytest.raises(Exception):
            Image.Q.data.eq(0)  # data is binary and does not provide operators
        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            Image.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(Image.Q.format), _QueryableString)
        assert issubclass(type(Image.Q.width), _QueryableNumeric)
        assert issubclass(type(Image.Q.height), _QueryableNumeric)
        assert issubclass(type(Image.Q.stride), _QueryableNumeric)
        assert issubclass(type(Image.Q.is_bigendian), _QueryableBool)
        assert issubclass(type(Image.Q.encoding), _QueryableString)
        assert issubclass(type(Image.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(Image.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(Image.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(Image.Q.header.frame_id), _QueryableString)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """

        # --- Catalog Context: Field & Operator ---
        test_str_value = "test-str"
        # Call: Image.Q.encoding.match(test_str_value)
        # Expected: {'gps.position.y': {'$gt': 12345.67}} - _QueryCatalogExpression
        expr_image = Image.Q.encoding.match(test_str_value)
        assert isinstance(expr_image, _QueryCatalogExpression)
        assert expr_image.to_dict() == {
            "image.encoding": {"$match": test_str_value},
        }

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        # Call: Image.Q.timestamp_ns.between(10000, 30000)
        # Expected: {'image.timestamp_ns': {'$between': [10000, 30000]}} - _QueryCatalogExpression
        expr_between = Image.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "image.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Sgpslate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(Image.Q.timestamp_ns.gt(12345.67))
            .with_expression(Image.Q.stride.gt(12345.67)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "image.timestamp_ns": {"$gt": 12345.67},
                "image.stride": {"$gt": 12345.67},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]


class TestQueryMagnetometerAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        Magnetometer.Q.magnetic_field.x
        Magnetometer.Q.magnetic_field.y
        Magnetometer.Q.magnetic_field.z
        # Inherited from Vector3d
        Magnetometer.Q.magnetic_field.covariance_type
        # Inherited from HeaderMixin
        Magnetometer.Q.header.seq
        Magnetometer.Q.header.stamp.sec
        Magnetometer.Q.header.stamp.nanosec
        Magnetometer.Q.header.frame_id
        # Inherited from Message
        Magnetometer.Q.timestamp_ns
        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            Magnetometer.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(Magnetometer.Q.magnetic_field.x), _QueryableNumeric)
        assert issubclass(type(Magnetometer.Q.magnetic_field.y), _QueryableNumeric)
        assert issubclass(type(Magnetometer.Q.magnetic_field.z), _QueryableNumeric)
        assert issubclass(
            type(Magnetometer.Q.magnetic_field.covariance_type), _QueryableNumeric
        )
        assert issubclass(type(Magnetometer.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(Magnetometer.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(Magnetometer.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(Magnetometer.Q.header.frame_id), _QueryableString)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: Field & Operator ---
        test_numeric_value = 12345.67
        # Call: Image.Q.encoding.match(test_str_value)
        # Expected: {'gps.position.y': {'$gt': 12345.67}} - _QueryCatalogExpression
        expr_nested = Magnetometer.Q.magnetic_field.x.leq(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "magnetometer.magnetic_field.x": {"$leq": test_numeric_value},
        }

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        # Call: Image.Q.timestamp_ns.between(10000, 30000)
        # Expected: {'image.timestamp_ns': {'$between': [10000, 30000]}} - _QueryCatalogExpression
        expr_between = Magnetometer.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "magnetometer.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Sgpslate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(Magnetometer.Q.timestamp_ns.gt(12345.67))
            .with_expression(Magnetometer.Q.magnetic_field.z.gt(12345.67)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "magnetometer.timestamp_ns": {"$gt": 12345.67},
                "magnetometer.magnetic_field.z": {"$gt": 12345.67},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]


class TestQueryTemperatureAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        Temperature.Q.value
        # Inherited from HeaderMixin
        Temperature.Q.header.seq
        Temperature.Q.header.stamp.sec
        Temperature.Q.header.stamp.nanosec
        Temperature.Q.header.frame_id
        # Inherited from VarianceMixin
        Temperature.Q.variance
        Temperature.Q.variance_type
        # Inherited from Message
        Temperature.Q.timestamp_ns
        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            Temperature.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(Temperature.Q.value), _QueryableNumeric)
        assert issubclass(type(Temperature.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(Temperature.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(Temperature.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(Temperature.Q.header.frame_id), _QueryableString)
        assert issubclass(type(Temperature.Q.variance), _QueryableNumeric)
        assert issubclass(type(Temperature.Q.variance_type), _QueryableNumeric)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: Field & Operator ---
        test_numeric_value = 303.15
        # Call: Image.Q.encoding.match(test_str_value)
        # Expected: {'gps.position.y': {'$gt': 12345.67}} - _QueryCatalogExpression
        expr_nested = Temperature.Q.value.leq(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "temperature.value": {"$leq": test_numeric_value},
        }

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        # Call: Image.Q.timestamp_ns.between(10000, 30000)
        # Expected: {'image.timestamp_ns': {'$between': [10000, 30000]}} - _QueryCatalogExpression
        expr_between = Temperature.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "temperature.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Sgpslate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(Temperature.Q.timestamp_ns.gt(12345.67))
            .with_expression(Temperature.Q.value.gt(303.15)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "temperature.timestamp_ns": {"$gt": 12345.67},
                "temperature.value": {"$gt": 303.15},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]


class TestQueryPressureAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        Pressure.Q.value
        # Inherited from HeaderMixin
        Pressure.Q.header.seq
        Pressure.Q.header.stamp.sec
        Pressure.Q.header.stamp.nanosec
        Pressure.Q.header.frame_id
        # Inherited from VarianceMixin
        Pressure.Q.variance
        Pressure.Q.variance_type
        # Inherited from Message
        Pressure.Q.timestamp_ns
        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            Pressure.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(Pressure.Q.value), _QueryableNumeric)
        assert issubclass(type(Pressure.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(Pressure.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(Pressure.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(Pressure.Q.header.frame_id), _QueryableString)
        assert issubclass(type(Pressure.Q.variance), _QueryableNumeric)
        assert issubclass(type(Pressure.Q.variance_type), _QueryableNumeric)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: Field & Operator ---
        test_numeric_value = 200123.15
        # Call: Image.Q.encoding.match(test_str_value)
        # Expected: {'gps.position.y': {'$gt': 12345.67}} - _QueryCatalogExpression
        expr_nested = Pressure.Q.value.leq(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "pressure.value": {"$leq": test_numeric_value},
        }

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        # Call: Image.Q.timestamp_ns.between(10000, 30000)
        # Expected: {'image.timestamp_ns': {'$between': [10000, 30000]}} - _QueryCatalogExpression
        expr_between = Pressure.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "pressure.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Sgpslate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(Pressure.Q.timestamp_ns.gt(12345.67))
            .with_expression(Pressure.Q.value.gt(200123.15)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "pressure.timestamp_ns": {"$gt": 12345.67},
                "pressure.value": {"$gt": 200123.15},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]


class TestQueryRangeAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        Range.Q.radiation_type
        Range.Q.field_of_view
        Range.Q.min_range
        Range.Q.max_range
        Range.Q.range
        # Inherited from HeaderMixin
        Range.Q.header.seq
        Range.Q.header.stamp.sec
        Range.Q.header.stamp.nanosec
        Range.Q.header.frame_id
        # Inherited from VarianceMixin
        Range.Q.variance
        Range.Q.variance_type
        # Inherited from Message
        Range.Q.timestamp_ns
        # --- Catalog Context: Non-existing field ---
        with pytest.raises(Exception):
            Range.Q.non_existing_field.eq(0)

    def test_field_queryable_inheritance(self):
        """
        Tests the queryable type of the model fields.
        This test ensure that for each field, only specified operators are defined and callable
        """
        # --- Fields Accessibility Test ---
        # Local fields
        assert issubclass(type(Range.Q.radiation_type), _QueryableNumeric)
        assert issubclass(type(Range.Q.field_of_view), _QueryableNumeric)
        assert issubclass(type(Range.Q.min_range), _QueryableNumeric)
        assert issubclass(type(Range.Q.max_range), _QueryableNumeric)
        assert issubclass(type(Range.Q.range), _QueryableNumeric)
        assert issubclass(type(Range.Q.header.seq), _QueryableNumeric)
        assert issubclass(type(Range.Q.header.stamp.sec), _QueryableNumeric)
        assert issubclass(type(Range.Q.header.stamp.nanosec), _QueryableNumeric)
        assert issubclass(type(Range.Q.header.frame_id), _QueryableString)
        assert issubclass(type(Range.Q.variance), _QueryableNumeric)
        assert issubclass(type(Range.Q.variance_type), _QueryableNumeric)

    def test_expression_generation_paths_and_operators(self):
        """
        Tests that complex query chains correctly generate the final, flat expression
        dictionary with the right keys, operators, and types.
        """
        # --- Catalog Context: Field & Operator ---
        test_numeric_value = 0.52
        # Call: Image.Q.encoding.match(test_str_value)
        # Expected: {'gps.position.y': {'$gt': 12345.67}} - _QueryCatalogExpression
        expr_nested = Range.Q.field_of_view.leq(test_numeric_value)
        assert isinstance(expr_nested, _QueryCatalogExpression)
        assert expr_nested.to_dict() == {
            "range.field_of_view": {"$leq": test_numeric_value},
        }

        # --- Catalog Context: Range Operator ---
        test_time_range = [10000, 30000]
        # Call: Image.Q.timestamp_ns.between(10000, 30000)
        # Expected: {'image.timestamp_ns': {'$between': [10000, 30000]}} - _QueryCatalogExpression
        expr_between = Range.Q.header.stamp.sec.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "range.header.stamp.sec": {"$between": test_time_range}
        }

    def test_full_sdk_query_to_dict_structure(self):
        """Tests the final output structure of an example query."""

        # Sgpslate the User Query
        q = Query(
            QueryOntologyCatalog()
            .with_expression(Range.Q.timestamp_ns.gt(12345.67))
            .with_expression(Range.Q.field_of_view.gt(0.52)),
        )

        # Define Expected Output
        expected_dict = {
            "ontology": {
                "range.timestamp_ns": {"$gt": 12345.67},
                "range.field_of_view": {"$gt": 0.52},
            },
        }

        # Assert the result
        result = q.to_dict()

        # Check top-level structure
        assert set(result.keys()) == set(["ontology"])

        # Check topic nesting (the complex part)
        # Check ontology flatness (the simple part)
        assert result["ontology"] == expected_dict["ontology"]

    def test_validation_min_and_max_range(self):
        """Tests the validator for `min_range` and `max_range`."""
        # min_range < max_range
        Range(field_of_view=30, min_range=0, max_range=1, range=0.5, radiation_type=0)

        # min_range == max_range
        Range(field_of_view=30, min_range=0, max_range=0, range=0, radiation_type=0)

        # min_range
        with pytest.raises(ValueError):
            Range(
                field_of_view=30, min_range=1, max_range=0, range=0.5, radiation_type=0
            )

    def test_validation_range(self):
        """Tests the validator for `range`."""
        # min_range < range < max_range
        Range(field_of_view=30, min_range=0, max_range=1, range=0.5, radiation_type=0)

        # range == min_range
        Range(field_of_view=30, min_range=0, max_range=1, range=0, radiation_type=0)

        # range == max_range
        Range(field_of_view=30, min_range=0, max_range=1, range=1, radiation_type=0)

        # range < min_range
        with pytest.raises(ValueError):
            Range(field_of_view=30, min_range=1, max_range=2, range=0, radiation_type=0)

        # range > max_range
        with pytest.raises(ValueError):
            Range(field_of_view=30, min_range=0, max_range=1, range=2, radiation_type=0)
