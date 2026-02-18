"""
Pressure Ontology Module.

Defines the data structure for pressure sensors.
"""

from typing import Optional
import pyarrow as pa

from ..mixins import HeaderMixin, VarianceMixin
from ..serializable import Serializable
from ..header import Header


class Pressure(Serializable, HeaderMixin, VarianceMixin):
    """
    Represents a physical pressure value. The internal representation is always stored in **Pascals (Pa)**.

    Users are encouraged to use the `from_*` factory methods when initializing
    pressure values expressed in units other than Pascals.

    Attributes:
        value (float): Pressure value in **Pascals (Pa)**. When using the constructor directly,
            the value **must** be provided in Pascals.

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter pressure data based
    on pressure values within a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Pressure, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for pressure values within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(Pressure.Q.value.between([100000, 200000]))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Pressure.Q.value.between([100000, 200000]), include_timestamp_range=True)
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

    # --- Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "value",
                pa.float64(),
                nullable=False,
                metadata={
                    "description": "The absolute pressure reading from the sensor in Pascals."
                },
            ),
        ]
    )

    value: float
    """
    The absolute pressure reading from the sensor in Pascals.

    ### Querying with the **`.Q` Proxy**
    The pressure value is queryable via the `value` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Pressure.Q.value` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Pressure, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for pressure values within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(Pressure.Q.value.between([100000, 200000]))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Pressure.Q.value.between([100000, 200000]), include_timestamp_range=True)
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
    def from_atm(
        cls,
        *,
        value: float,
        header: Optional[Header] = None,
        variance: Optional[float] = None,
        variance_type: Optional[int] = None,
    ) -> "Pressure":
        """
        Creates a `Pressure` instance using the value in Atm and converting it in Pascal using the formula
        `Pascal = Atm * 101325`.

        Args:
            value (float): The pressure value in Atm.
            header (Optional[Header]): The standard metadata header (optional).
            variance (Optional[float]): The variance of the data.
            variance_type (Optional[int]): Enum integer representing the variance parameterization.

        Returns:
            Pressure: A `Pressure` instance with value in Pascal.
        """
        value_in_pascal = value * 101325
        return cls(
            value=value_in_pascal,
            header=header,
            variance=variance,
            variance_type=variance_type,
        )

    @classmethod
    def from_bar(
        cls,
        *,
        value: float,
        header: Optional[Header] = None,
        variance: Optional[float] = None,
        variance_type: Optional[int] = None,
    ) -> "Pressure":
        """
        Creates a `Pressure` instance using the value in Bar and converting it in Pascal using the formula
        `Pascal = Bar * 100000`.

        Args:
            value (float): The pressure value in Bar.
            header (Optional[Header]): The standard metadata header (optional).
            variance (Optional[float]): The variance of the data.
            variance_type (Optional[int]): Enum integer representing the variance parameterization.

        Returns:
            Pressure: A `Pressure` instance with value in Pascal.
        """
        value_in_pascal = value * 100000
        return cls(
            value=value_in_pascal,
            header=header,
            variance=variance,
            variance_type=variance_type,
        )

    @classmethod
    def from_psi(
        cls,
        *,
        value: float,
        header: Optional[Header] = None,
        variance: Optional[float] = None,
        variance_type: Optional[int] = None,
    ) -> "Pressure":
        """
        Creates a `Pressure` instance using the value in Psi and converting it in Pascal using the formula
        `Pascal = Psi * 6894.7572931783`.

        Args:
            value (float): The pressure value in Psi.
            header (Optional[Header]): The standard metadata header (optional).
            variance (Optional[float]): The variance of the data.
            variance_type (Optional[int]): Enum integer representing the variance parameterization.

        Returns:
            Pressure: A `Pressure` instance with value in Pascal.
        """
        value_in_pascal = value * 6894.7572931783
        return cls(
            value=value_in_pascal,
            header=header,
            variance=variance,
            variance_type=variance_type,
        )

    def to_atm(self) -> float:
        """
        Converts and returns the `Pressure` value in Atm using the formula
        `Atm = Pascal / 101325`.

        Returns:
            float: The `Pressure` value in Atm.
        """
        return self.value / 101325

    def to_bar(self) -> float:
        """
        Converts and returns the `Pressure` value in Bar using the formula
        `Bar = Pascal / 100000`.

        Returns:
            float: The `Pressure` value in Bar.
        """
        return self.value / 100000

    def to_psi(self) -> float:
        """
        Converts and returns the `Pressure` value in Psi using the formula
        `Psi = Pascal / 6894.7572931783`.

        Returns:
            float: The `Pressure` value in Psi.
        """
        return self.value / 6894.7572931783
