"""
Range Ontology Module.

Defines the data structure for range sensors.
"""

import pyarrow as pa

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

    radiation_type: int
    """The type of radiation used by the sensor."""
