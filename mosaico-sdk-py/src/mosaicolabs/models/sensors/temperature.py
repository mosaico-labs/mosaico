"""
Temperature Ontology Module.

Defines the data structure for temperature sensors.
"""

from typing import Optional
import pyarrow as pa

from ..mixins import HeaderMixin, VarianceMixin
from ..serializable import Serializable
from ..header import Header


class Temperature(Serializable, HeaderMixin, VarianceMixin):
    """
    Represents a thermodynamic temperature. The internal representation is always stored in **Kelvin (K)**.

    Users are encouraged to use the `from_*` factory methods when initializing
    temperature values expressed in units other than Kelvin.

    Attributes:
        value (float): Temperature value in **Kelvin (K)**. When using the constructor directly,
            the value **must** be provided in Kelvin.

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter temperature data based
    on temperature values within a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Temperature, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for temperature values within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(Temperature.Q.value.between([273.15, 373.15]))
                .with_expression(Temperature.Q.header.stamp.sec.between(1700000000, 1800000000)),
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Temperature.Q.value.between([273.15, 373.15]), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    # --- Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "value",
                pa.float64(),
                nullable=False,
                metadata={"description": "Temperature value in Kelvin."},
            ),
        ]
    )

    value: float
    """
    Temperature value in Kelvin.

    ### Querying with the **`.Q` Proxy**
    The temperature value is queryable via the `value` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Temperature.Q.value` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Temperature, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for temperature values within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(Temperature.Q.value.between([273.15, 373.15]))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Temperature.Q.value.between([273.15, 373.15]), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    @classmethod
    def from_celsius(
        cls,
        *,
        value: float,
        header: Optional[Header] = None,
        variance: Optional[float] = None,
        variance_type: Optional[int] = None,
    ) -> "Temperature":
        """
        Creates a `Temperature` instance using the value in Celsius and converting it in Kelvin using the formula
        `Kelvin = Celsius + 273.15`.

        Args:
            value (float): The temperature value in Celsius.
            header (Optional[Header]): The standard metadata header (optional).
            variance (Optional[float]): The variance of the data.
            variance_type (Optional[int]): Enum integer representing the variance parameterization.

        Returns:
            Temperature: A `Temperature` instance with value in Kelvin.
        """
        value_in_kelvin = value + 273.15
        return cls(
            value=value_in_kelvin,
            header=header,
            variance=variance,
            variance_type=variance_type,
        )

    @classmethod
    def from_fahrenheit(
        cls,
        *,
        value: float,
        header: Optional[Header] = None,
        variance: Optional[float] = None,
        variance_type: Optional[int] = None,
    ) -> "Temperature":
        """
        Creates a `Temperature` instance using the value in Fahrenheit and converting it in Kelvin using the formula
        `Kelvin = (Fahrenheit - 32) * 5 / 9 + 273.15`.

        Args:
            value (float): The temperature value in Celsius.
            header (Optional[Header]): The standard metadata header (optional).
            variance (Optional[float]): The variance of the data.
            variance_type (Optional[int]): Enum integer representing the variance parameterization.

        Returns:
            Temperature: A `Temperature` instance with value in Kelvin.
        """
        value_in_kelvin = (value - 32) * 5 / 9 + 273.15
        return cls(
            value=value_in_kelvin,
            header=header,
            variance=variance,
            variance_type=variance_type,
        )

    def to_celsius(self) -> float:
        """
        Converts and returns the `Temperature` value in Celsius using the formula
        `Celsius = Kelvin - 273.15`.

        Returns:
            float: The `Temperature` value in Celsius.
        """
        return self.value - 273.15

    def to_fahrenheit(self) -> float:
        """
        Converts and returns the `Temperature` value in Fahrenheit using the formula
        `Fahrenheit = (Kelvin - 273.15) * 9 / 5 + 32`.

        Returns:
            float: The `Temperature` value in Fahrenheit.
        """
        return (self.value - 273.15) * 9 / 5 + 32
