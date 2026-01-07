"""
Range Ontology Module.

Defines the data structure for range sensors.
"""

import pyarrow as pa
from typing_extensions import Self
from pydantic import model_validator

from ..mixins import HeaderMixin
from ..serializable import Serializable


class Range(Serializable, HeaderMixin):
    """
    Range measurement data.
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
    """Which type of radiation the sensor used."""

    field_of_view: float
    """The arc angle, in radians, over which the distance reading is valid."""

    min_range: float
    """Minimum range value in meters. Fixed distance means that the minimum range must be equal to the maximum range."""

    max_range: float
    """Maximum range value in meters. Fixed distance means that the minimum range must be equal to the maximum range."""

    range: float
    """Range value in meters."""

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
