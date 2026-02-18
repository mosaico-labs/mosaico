"""
Range Ontology Module.

Defines the data structure for range sensors.
"""

import pyarrow as pa
from typing_extensions import Self
from pydantic import model_validator

from ..mixins import HeaderMixin, VarianceMixin
from ..serializable import Serializable


class Range(Serializable, HeaderMixin, VarianceMixin):
    """
    Represents a range measurement that defines a valid distance interval between the minimum and the maximum value.
    This with also the field of view, the radiation type and the range value.
    The internal representation is always stored in **meters (m)**.

    Attributes:
        radiation_type (int): Which type of radiation the sensor used.
        field_of_view (float): The arc angle, in **Radians (rad)**, over which the distance reading is valid.
        min_range (float): Minimum range value in **Meters (m)**. Fixed distance means that the minimum range
            must be equal to the maximum range.
        max_range (float): Maximum range value in **Meters (m)**. Fixed distance means that the minimum range
            must be equal to the maximum range.
        range (float): Range value in **Meters (m)**.

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter range data based
    on range parameters within a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Range, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for range data based on range parameters
            qresponse = client.query(
                QueryOntologyCatalog(Range.Q.range.between(0.0, 10.0))
                .with_epression(Range.Q.radiation_type.eq(0))
                .with_epression(Range.Q.max_range.between(70.0, 90.0)),
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Range.Q.range.between(0.0, 10.0), include_timestamp_range=True)
                .with_epression(Range.Q.radiation_type.eq(0))
                .with_epression(Range.Q.max_range.between(70.0, 90.0)),
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
                "radiation_type",
                pa.uint8(),
                nullable=False,
                metadata={"description": "Which type of radiation the sensor used."},
            ),
            pa.field(
                "field_of_view",
                pa.float32(),
                nullable=False,
                metadata={
                    "description": "The arc angle, in radians, over which the distance reading is valid."
                },
            ),
            pa.field(
                "min_range",
                pa.float32(),
                nullable=False,
                metadata={
                    "description": "Minimum range value in meters. Fixed distance means that the minimum range"
                    "must be equal to the maximum range."
                },
            ),
            pa.field(
                "max_range",
                pa.float32(),
                nullable=False,
                metadata={
                    "description": "Maximum range value in meters. Fixed distance means that the minimum range"
                    "must be equal to the maximum range."
                },
            ),
            pa.field(
                "range",
                pa.float32(),
                nullable=False,
                metadata={"description": "Range value in meters."},
            ),
        ]
    )

    radiation_type: int
    """
    Which type of radiation the sensor used.

    ### Querying with the **`.Q` Proxy**
    The radiation_type is queryable via the `radiation_type` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Range.Q.radiation_type` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Range, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for range data based on radiation type
            qresponse = client.query(
                QueryOntologyCatalog(Range.Q.radiation_type.eq(0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    field_of_view: float
    """
    The arc angle, in radians, over which the distance reading is valid.

    ### Querying with the **`.Q` Proxy**
    The field_of_view is queryable via the `field_of_view` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Range.Q.field_of_view` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Range, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for range data based on field of view
            qresponse = client.query(
                QueryOntologyCatalog(Range.Q.field_of_view.between(0.0, 1.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    min_range: float
    """
    Minimum range value in meters. Fixed distance means that the minimum range must be equal to the maximum range.

    ### Querying with the **`.Q` Proxy**
    The min_range is queryable via the `min_range` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Range.Q.min_range` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Range, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for range data based on minimum range
            qresponse = client.query(
                QueryOntologyCatalog(Range.Q.min_range.between(0.0, 10.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    max_range: float
    """
    Maximum range value in meters. Fixed distance means that the minimum range must be equal to the maximum range.

    ### Querying with the **`.Q` Proxy**
    The max_range is queryable via the `max_range` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Range.Q.max_range` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Range, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for range data based on maximum range
            qresponse = client.query(
                QueryOntologyCatalog(Range.Q.max_range.between(0.0, 10.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    range: float
    """
    Range value in meters.

    ### Querying with the **`.Q` Proxy**
    The range is queryable via the `range` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Range.Q.range` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Range, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for range data based on range
            qresponse = client.query(
                QueryOntologyCatalog(Range.Q.range.between(0.0, 10.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Range.Q.range.between(0.0, 10.0), include_timestamp_range=True)
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

    @model_validator(mode="after")
    def validate_min_and_max_range(self) -> Self:
        """Ensures that `min_range` is smaller or equal to `max_range`."""
        if self.min_range > self.max_range:
            raise ValueError(
                "The min_range must be smaller or equal to max_range. "
                f"Got {self.min_range} as min_range and {self.max_range} as max_range."
            )

        return self

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        """Ensures that `range` is between `min_range` and `max_range`."""
        if not self.min_range <= self.range <= self.max_range:
            raise ValueError(
                "The range must be between min_range and max_range. "
                f"Got {self.range} as range, {self.min_range} as min_range and {self.max_range} as max_range."
            )

        return self
