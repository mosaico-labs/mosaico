"""
Range Ontology Module.

Defines the data structure for range sensors.
"""

import pyarrow as pa
from typing_extensions import Self
from pydantic import model_validator

from ..header_mixin import HeaderMixin
from ..covariance_mixin import CovarianceMixin
from ..serializable import Serializable


class Range(Serializable, HeaderMixin, CovarianceMixin):
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
                metadata={
                    "description": "the type of radiation used by the sensor (sound, IR, etc) [enum]"
                },
            ),
            pa.field(
                "field_of_view",
                pa.float32(),
                nullable=False,
                metadata={
                    "description": "The size of the arc that the distance reading is valid for [rad] "
                    "the object causing the range reading may have been anywhere within -field_of_view/2 and "
                    "field_of_view/2 at the measured range. 0 angle corresponds to the x-axis of the sensor."
                },
            ),
            pa.field(
                "min_range",
                pa.float32(),
                nullable=False,
                metadata={
                    "description": "Minimum range value [m] in meters. Fixed distance rangers require min_range==max_range."
                },
            ),
            pa.field(
                "max_range",
                pa.float32(),
                nullable=False,
                metadata={
                    "description": "Maximum range value [m] in meters. Fixed distance rangers require min_range==max_range."
                },
            ),
            pa.field(
                "range",
                pa.float32(),
                nullable=False,
                metadata={
                    "description": "Range data [m] in meters. "
                    "(Note: values < range_min or > range_max should be discarded) "
                    "Fixed distance rangers only output -Inf or +Inf. -Inf represents a detection within fixed distance "
                    "(Detection too close to the sensor to quantify). "
                    "+Inf represents no detection within the fixed distance (Object out of range)."
                },
            ),
        ]
    )

    radiation_type: int
    """The type of radiation used by the sensor."""

    field_of_view: float
    """
    The size of the arc that the distance reading is valid for [rad]
    the object causing the range reading may have been anywhere within -field_of_view/2 and
    field_of_view/2 at the measured range. 0 angle corresponds to the x-axis of the sensor.
    """

    min_range: float
    """Minimum range value [m] in meters. Fixed distance rangers require min_range==max_range."""

    max_range: float
    """Maximum range value [m] in meters. Fixed distance rangers require min_range==max_range."""

    range: float
    """
    Range data [m] in meters.

    (Note: values < range_min or > range_max should be discarded)
    Fixed distance rangers only output -Inf or +Inf. -Inf represents a detection within fixed distance (Detection too close to the sensor to quantify).
    +Inf represents no detection within the fixed distance (Object out of range).
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
