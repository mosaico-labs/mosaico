"""
Temperature Ontology Module.

Defines the data structure for temperature sensors.
"""

from typing import Optional

from ..core import MosaicoField, MosaicoType, Serializable
from ..data import HeaderMixin, VarianceMixin


class Temperature(
    Serializable,
    HeaderMixin,  # Adds Header support
    VarianceMixin,
):
    """
    Represents a thermodynamic temperature. The internal representation is always stored in **Kelvin (K)**.

    Users are encouraged to use the `from_*` factory methods when initializing
    temperature values expressed in units other than Kelvin.

    Attributes:
        value (float): Temperature value in **Kelvin (K)**. When using the constructor directly,
            the value **must** be provided in Kelvin.
        variance (Optional[float]): The variance of the data.
        variance_type (Optional[int]): Enum integer representing the variance parameterization.
        header (optional[Header]): Optional heading containing measurement metadata

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter temperature data based
    on temperature values within a [`QueryOntologyCatalog`][mosaicolabs.query.builders.QueryOntologyCatalog].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Temperature, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for temperature values within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(Temperature.Q.value.between([273.15, 373.15]))
                .with_expression(Temperature.Q.timestamp_ns.between(1700000000, 1800000000)),
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

                    # Clusterize all topics within the sequence to extract the time intervals
                    clusters_dict = item.clusterize_all()

                    # Since clusterize_all() used default clustering_dt_ns, each topic will have
                    # just one cluster representing the first and last moment the query was satisfied
                    for t_name, clusters in clusters_dict.items():
                        print(f"{t_name}:\\n", "\\n".join(f"{cluster}" for cluster in clusters))
        ```
    """

    value: MosaicoType.float64 = MosaicoField(
        description="Temperature value in Kelvin."
    )
    """
    Temperature value in Kelvin.

    ### Querying with the **`.Q` Proxy**
    The temperature value is queryable via the `value` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Temperature.Q.value` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

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

                    # Clusterize all topics within the sequence to extract the time intervals
                    clusters_dict = item.clusterize_all()

                    # Since clusterize_all() used default clustering_dt_ns, each topic will have
                    # just one cluster representing the first and last moment the query was satisfied
                    for t_name, clusters in clusters_dict.items():
                        print(f"{t_name}:\\n", "\\n".join(f"{cluster}" for cluster in clusters))
        ```
    """

    @classmethod
    def from_celsius(
        cls,
        *,
        value: float,
        variance: Optional[float] = None,
        variance_type: Optional[int] = None,
    ) -> "Temperature":
        """
        Creates a `Temperature` instance using the value in Celsius and converting it in Kelvin using the formula
        `Kelvin = Celsius + 273.15`.

        Args:
            value (float): The temperature value in Celsius.
            variance (Optional[float]): The variance of the data.
            variance_type (Optional[int]): Enum integer representing the variance parameterization.

        Returns:
            Temperature: A `Temperature` instance with value in Kelvin.
        """
        value_in_kelvin = value + 273.15
        return cls(
            value=value_in_kelvin,
            variance=variance,
            variance_type=variance_type,
        )

    @classmethod
    def from_fahrenheit(
        cls,
        *,
        value: float,
        variance: Optional[float] = None,
        variance_type: Optional[int] = None,
    ) -> "Temperature":
        """
        Creates a `Temperature` instance using the value in Fahrenheit and converting it in Kelvin using the formula
        `Kelvin = (Fahrenheit - 32) * 5 / 9 + 273.15`.

        Args:
            value (float): The temperature value in Celsius.
            variance (Optional[float]): The variance of the data.
            variance_type (Optional[int]): Enum integer representing the variance parameterization.

        Returns:
            Temperature: A `Temperature` instance with value in Kelvin.
        """
        value_in_kelvin = (value - 32) * 5 / 9 + 273.15
        return cls(
            value=value_in_kelvin,
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
