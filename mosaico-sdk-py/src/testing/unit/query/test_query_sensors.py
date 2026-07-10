# ======================================================================
# 3. UNIT TESTS
# ======================================================================
import pytest

from mosaicolabs.models.data import RobotPath
from mosaicolabs.models.query import (
    Query,
    QueryOntologyCatalog,
)
from mosaicolabs.models.query.expressions import (
    _QueryCatalogExpression,
)
from mosaicolabs.models.query.generation.mixins import (
    _QueryableBool,
    _QueryableNumeric,
    _QueryableString,
)
from mosaicolabs.models.sensors import (
    GPS,
    IMU,
    Image,
    Magnetometer,
    Pressure,
    Range,
    RobotJoint,
    Temperature,
)
from mosaicolabs.ros_bridge.data_ontology import FrameTransform


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
        # Inherited from Message
        IMU.Q.timestamp_ns
        # Inherited from HeaderMixin
        IMU.Q.header.sample_counter
        IMU.Q.header.timestamp.seconds
        IMU.Q.header.timestamp.nanoseconds
        IMU.Q.header.frame_id
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
        assert issubclass(type(IMU.Q.timestamp_ns), _QueryableNumeric)
        assert issubclass(type(IMU.Q.header.sample_counter), _QueryableNumeric)
        assert issubclass(type(IMU.Q.header.timestamp.seconds), _QueryableNumeric)
        assert issubclass(type(IMU.Q.header.timestamp.nanoseconds), _QueryableNumeric)
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
        # Call: IMU.Q.timestamp_ns.between(10000, 30000)
        # Expected: {'imu.timestamp_ns': {'$between': [10000, 30000]}} - _QueryCatalogExpression
        expr_between = IMU.Q.timestamp_ns.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "imu.timestamp_ns": {"$between": test_time_range}
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
        # Inherited from Message
        GPS.Q.timestamp_ns
        # Inherited from HeaderMixin
        GPS.Q.header.sample_counter
        GPS.Q.header.timestamp.seconds
        GPS.Q.header.timestamp.nanoseconds
        GPS.Q.header.frame_id
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
        assert issubclass(type(GPS.Q.timestamp_ns), _QueryableNumeric)
        assert issubclass(type(GPS.Q.header.sample_counter), _QueryableNumeric)
        assert issubclass(type(GPS.Q.header.timestamp.seconds), _QueryableNumeric)
        assert issubclass(type(GPS.Q.header.timestamp.nanoseconds), _QueryableNumeric)
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
        expr_between = GPS.Q.timestamp_ns.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "gps.timestamp_ns": {"$between": test_time_range}
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
        # Inherited from Message
        Image.Q.timestamp_ns
        # Inherited from HeaderMixin
        Image.Q.header.sample_counter
        Image.Q.header.timestamp.seconds
        Image.Q.header.timestamp.nanoseconds
        Image.Q.header.frame_id
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
        assert issubclass(type(Image.Q.timestamp_ns), _QueryableNumeric)
        assert issubclass(type(Image.Q.header.sample_counter), _QueryableNumeric)
        assert issubclass(type(Image.Q.header.timestamp.seconds), _QueryableNumeric)
        assert issubclass(type(Image.Q.header.timestamp.nanoseconds), _QueryableNumeric)
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
        expr_between = Image.Q.timestamp_ns.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "image.timestamp_ns": {"$between": test_time_range}
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
        # Inherited from Message
        Magnetometer.Q.timestamp_ns
        # Inherited from HeaderMixin
        Magnetometer.Q.header.sample_counter
        Magnetometer.Q.header.timestamp.seconds
        Magnetometer.Q.header.timestamp.nanoseconds
        Magnetometer.Q.header.frame_id
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
        assert issubclass(type(Magnetometer.Q.timestamp_ns), _QueryableNumeric)
        assert issubclass(type(Magnetometer.Q.header.sample_counter), _QueryableNumeric)
        assert issubclass(
            type(Magnetometer.Q.header.timestamp.seconds), _QueryableNumeric
        )
        assert issubclass(
            type(Magnetometer.Q.header.timestamp.nanoseconds), _QueryableNumeric
        )
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
        expr_between = Magnetometer.Q.timestamp_ns.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "magnetometer.timestamp_ns": {"$between": test_time_range}
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
        # Inherited from VarianceMixin
        Temperature.Q.variance
        Temperature.Q.variance_type
        # Inherited from Message
        Temperature.Q.timestamp_ns
        # Inherited from HeaderMixin
        Temperature.Q.header.sample_counter
        Temperature.Q.header.timestamp.seconds
        Temperature.Q.header.timestamp.nanoseconds
        Temperature.Q.header.frame_id
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
        assert issubclass(type(Temperature.Q.timestamp_ns), _QueryableNumeric)
        assert issubclass(type(Temperature.Q.header.sample_counter), _QueryableNumeric)
        assert issubclass(
            type(Temperature.Q.header.timestamp.seconds), _QueryableNumeric
        )
        assert issubclass(
            type(Temperature.Q.header.timestamp.nanoseconds), _QueryableNumeric
        )
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
        expr_between = Temperature.Q.timestamp_ns.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "temperature.timestamp_ns": {"$between": test_time_range}
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

    def test_conversion_helper_methods(self):
        """
        Tests the conversion helper methods for creating and converting the
        `Temperature` `value` in both Celsius and Fahrenheit.
        """
        # Assert the creation and conversion in Celsius
        temperature = Temperature.from_celsius(value=10)
        assert temperature.value == 283.15
        assert temperature.to_celsius() == 10

        # Assert the creation and conversion in Fahrenheit
        temperature = Temperature.from_fahrenheit(value=5)
        assert temperature.value == 258.15
        assert temperature.to_fahrenheit() == 5


class TestQueryPressureAPI:
    def test_accessibility(self):
        """
        Tests that inner fields are accessable from the _QueryProxy.
        """
        # --- Fields Accessibility Test ---
        # Local fields
        Pressure.Q.value
        # Inherited from VarianceMixin
        Pressure.Q.variance
        Pressure.Q.variance_type
        # Inherited from Message
        Pressure.Q.timestamp_ns
        # Inherited from HeaderMixin
        Pressure.Q.header.sample_counter
        Pressure.Q.header.timestamp.seconds
        Pressure.Q.header.timestamp.nanoseconds
        Pressure.Q.header.frame_id
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
        assert issubclass(type(Pressure.Q.timestamp_ns), _QueryableNumeric)
        assert issubclass(type(Pressure.Q.header.sample_counter), _QueryableNumeric)
        assert issubclass(type(Pressure.Q.header.timestamp.seconds), _QueryableNumeric)
        assert issubclass(
            type(Pressure.Q.header.timestamp.nanoseconds), _QueryableNumeric
        )
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
        expr_between = Pressure.Q.timestamp_ns.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "pressure.timestamp_ns": {"$between": test_time_range}
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

    def test_conversion_helper_methods(self):
        """
        Tests the conversion helper methods for creating and converting the
        `Pressure` `value` in both Atm, Bar and Psi.
        """
        # Assert the creation and conversion in Atm
        pressure = Pressure.from_atm(value=10)
        assert pressure.value == 1013250
        assert pressure.to_atm() == 10

        # Assert the creation and conversion in Bar
        pressure = Pressure.from_bar(value=10)
        assert pressure.value == 1000000
        assert pressure.to_bar() == 10

        # Assert the creation and conversion in Psi
        pressure = Pressure.from_psi(value=1000)
        assert pressure.value == 6894757.293178299
        assert pressure.to_psi() == 1000


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
        # Inherited from VarianceMixin
        Range.Q.variance
        Range.Q.variance_type
        # Inherited from Message
        Range.Q.timestamp_ns
        # Inherited from HeaderMixin
        Range.Q.header.sample_counter
        Range.Q.header.timestamp.seconds
        Range.Q.header.timestamp.nanoseconds
        Range.Q.header.frame_id
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
        assert issubclass(type(Range.Q.timestamp_ns), _QueryableNumeric)
        assert issubclass(type(Range.Q.header.sample_counter), _QueryableNumeric)
        assert issubclass(type(Range.Q.header.timestamp.seconds), _QueryableNumeric)
        assert issubclass(type(Range.Q.header.timestamp.nanoseconds), _QueryableNumeric)
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
        expr_between = Range.Q.timestamp_ns.between(test_time_range)
        assert isinstance(expr_between, _QueryCatalogExpression)
        assert expr_between.to_dict() == {
            "range.timestamp_ns": {"$between": test_time_range}
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


class TestQueryRobotJoint:
    def test_list(self):
        assert isinstance(RobotJoint.Q.positions[0], _QueryableNumeric)
        assert isinstance(RobotJoint.Q.positions.any(), _QueryableNumeric)
        assert isinstance(RobotJoint.Q.positions.all(), _QueryableNumeric)
        assert isinstance(RobotJoint.Q.names[0], _QueryableString)
        assert isinstance(RobotJoint.Q.names.any(), _QueryableString)
        assert isinstance(RobotJoint.Q.names.all(), _QueryableString)

        with pytest.raises(
            AttributeError, match="Field 'robot_joint.names' is a list."
        ):
            RobotJoint.Q.names.field

        expr = RobotJoint.Q.positions[0].eq(0)
        assert expr.key == "robot_joint.positions[0]"
        assert expr.op == "$eq"
        assert expr.value == 0

        expr = RobotJoint.Q.positions.all().eq(0)
        assert expr.key == "robot_joint.positions[!]"
        assert expr.op == "$eq"
        assert expr.value == 0

        expr = RobotJoint.Q.positions.any().eq(0)
        assert expr.key == "robot_joint.positions[?]"
        assert expr.op == "$eq"
        assert expr.value == 0


class TestQueryRobotPath:
    def test_list(self):
        assert isinstance(RobotPath.Q.poses[0].position.x, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses[0].position.y, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses[0].position.z, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses[0].orientation.x, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses[0].orientation.y, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses[0].orientation.z, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses[0].orientation.w, _QueryableNumeric)

        assert isinstance(RobotPath.Q.poses.any().position.x, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses.any().position.y, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses.any().position.z, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses.any().orientation.x, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses.any().orientation.y, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses.any().orientation.z, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses.any().orientation.w, _QueryableNumeric)

        assert isinstance(RobotPath.Q.poses.all().position.x, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses.all().position.y, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses.all().position.z, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses.all().orientation.x, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses.all().orientation.y, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses.all().orientation.z, _QueryableNumeric)
        assert isinstance(RobotPath.Q.poses.all().orientation.w, _QueryableNumeric)

        with pytest.raises(AttributeError, match="Field 'robot_path.poses' is a list."):
            RobotPath.Q.poses.field

        with pytest.raises(
            TypeError, match="Field 'robot_path.poses\\[0\\]' is not a list."
        ):
            RobotPath.Q.poses[0][0]

        with pytest.raises(
            TypeError, match="Field 'robot_path.poses\\[0\\]' is not a list."
        ):
            RobotPath.Q.poses[0].any()

        with pytest.raises(
            TypeError, match="Field 'robot_path.poses\\[0\\]' is not a list."
        ):
            RobotPath.Q.poses[0].all()

        with pytest.raises(
            TypeError, match="Field 'robot_path.poses\\[0\\].position' is not a list."
        ):
            RobotPath.Q.poses[0].position[0]

        with pytest.raises(
            TypeError, match="Field 'robot_path.poses\\[0\\].position' is not a list."
        ):
            RobotPath.Q.poses[0].position.any()

        with pytest.raises(
            TypeError, match="Field 'robot_path.poses\\[0\\].position' is not a list."
        ):
            RobotPath.Q.poses[0].position.all()

        expr = RobotPath.Q.poses[0].position.x.eq(0)
        assert expr.key == "robot_path.poses[0].position.x"
        assert expr.op == "$eq"
        assert expr.value == 0

        expr = RobotPath.Q.poses.all().position.x.eq(0)
        assert expr.key == "robot_path.poses[!].position.x"
        assert expr.op == "$eq"
        assert expr.value == 0

        expr = RobotPath.Q.poses.any().position.x.eq(0)
        assert expr.key == "robot_path.poses[?].position.x"
        assert expr.op == "$eq"
        assert expr.value == 0


class TestFrameTransform:
    def test_list(self):
        assert isinstance(
            FrameTransform.Q.transforms[0].translation.x, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms[0].translation.y, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms[0].translation.z, _QueryableNumeric
        )
        assert isinstance(FrameTransform.Q.transforms[0].rotation.x, _QueryableNumeric)
        assert isinstance(FrameTransform.Q.transforms[0].rotation.y, _QueryableNumeric)
        assert isinstance(FrameTransform.Q.transforms[0].rotation.z, _QueryableNumeric)
        assert isinstance(FrameTransform.Q.transforms[0].rotation.w, _QueryableNumeric)

        assert isinstance(
            FrameTransform.Q.transforms.any().translation.x, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms.any().translation.y, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms.any().translation.z, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms.any().rotation.x, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms.any().rotation.y, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms.any().rotation.z, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms.any().rotation.w, _QueryableNumeric
        )

        assert isinstance(
            FrameTransform.Q.transforms.all().translation.x, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms.all().translation.y, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms.all().translation.z, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms.all().rotation.x, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms.all().rotation.y, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms.all().rotation.z, _QueryableNumeric
        )
        assert isinstance(
            FrameTransform.Q.transforms.all().rotation.w, _QueryableNumeric
        )

        with pytest.raises(
            AttributeError, match="Field 'frame_transform.transforms' is a list."
        ):
            FrameTransform.Q.transforms.field

        with pytest.raises(
            TypeError, match="Field 'frame_transform.transforms\\[0\\]' is not a list."
        ):
            FrameTransform.Q.transforms[0][0]

        with pytest.raises(
            TypeError, match="Field 'frame_transform.transforms\\[0\\]' is not a list."
        ):
            FrameTransform.Q.transforms[0].any()

        with pytest.raises(
            TypeError, match="Field 'frame_transform.transforms\\[0\\]' is not a list."
        ):
            FrameTransform.Q.transforms[0].all()

        with pytest.raises(
            TypeError,
            match="Field 'frame_transform.transforms\\[0\\].translation' is not a list.",
        ):
            FrameTransform.Q.transforms[0].translation[0]

        with pytest.raises(
            TypeError,
            match="Field 'frame_transform.transforms\\[0\\].translation' is not a list.",
        ):
            FrameTransform.Q.transforms[0].translation.any()

        with pytest.raises(
            TypeError,
            match="Field 'frame_transform.transforms\\[0\\].translation' is not a list.",
        ):
            FrameTransform.Q.transforms[0].translation.all()

        expr = FrameTransform.Q.transforms[0].translation.x.eq(0)
        assert expr.key == "frame_transform.transforms[0].translation.x"
        assert expr.op == "$eq"
        assert expr.value == 0

        expr = FrameTransform.Q.transforms.all().translation.x.eq(0)
        assert expr.key == "frame_transform.transforms[!].translation.x"
        assert expr.op == "$eq"
        assert expr.value == 0

        expr = FrameTransform.Q.transforms.any().translation.x.eq(0)
        assert expr.key == "frame_transform.transforms[?].translation.x"
        assert expr.op == "$eq"
        assert expr.value == 0
