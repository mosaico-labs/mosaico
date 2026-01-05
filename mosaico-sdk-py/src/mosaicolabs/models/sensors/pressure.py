"""
Pressure Ontology Module.

Defines the data structure for pressure sensors.
"""

import pyarrow as pa

from ..header_mixin import HeaderMixin
from ..covariance_mixin import CovarianceMixin
from ..serializable import Serializable


class Pressure(Serializable, HeaderMixin, CovarianceMixin):
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
                metadata={"description": "Absolute pressure reading in Pascals."},
            ),
            pa.field(
                "variance",
                pa.float64(),
                nullable=False,
                metadata={
                    "description": "Pressure variance. 0 is interpreted as variance unknown."
                },
            ),
        ]
    )

    fluid_pressure: float
    """Absolute pressure reading in Pascals."""

    variance: float
    """Pressure variance. 0 is interpreted as variance unknown."""
