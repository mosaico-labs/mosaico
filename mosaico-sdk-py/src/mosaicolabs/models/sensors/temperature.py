"""
Temperature Ontology Module.

Defines the data structure for temperature sensors.
"""

import pyarrow as pa

from ..mixins import HeaderMixin
from ..serializable import Serializable


class Temperature(Serializable, HeaderMixin):
    """
    Temperature measurement data.
    """

    # --- Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "temperature",
                pa.float64(),
                nullable=False,
                metadata={"description": "Temperature value in Kelvin."},
            ),
            pa.field(
                "variance",
                pa.float64(),
                nullable=False,
                metadata={
                    "description": "Temperature variance. 0 means that the variance is unknown."
                },
            ),
        ]
    )

    temperature: float
    """Temperature value in Kelvin."""

    variance: float
    """Temperature variance. 0 means that the variance is unknown."""
