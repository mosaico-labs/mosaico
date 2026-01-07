"""
Pressure Ontology Module.

Defines the data structure for pressure sensors.
"""

import pyarrow as pa

from ..mixins import HeaderMixin
from ..serializable import Serializable


class Pressure(Serializable, HeaderMixin):
    """
    Pressure measurement data.
    """

    # --- Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "fluid_pressure",
                pa.float64(),
                nullable=False,
                metadata={
                    "description": "The absolute pressure reading from the sensor in Pascals."
                },
            ),
            pa.field(
                "variance",
                pa.float64(),
                nullable=False,
                metadata={
                    "description": "Pressure variance. 0 means that the variance is unknown."
                },
            ),
        ]
    )

    fluid_pressure: float
    """The absolute pressure reading from the sensor in Pascals."""

    variance: float
    """Pressure variance. 0 means that the variance is unknown."""
