"""
Pressure Ontology Module.

Defines the data structure for pressure sensors.
"""

import pyarrow as pa

from ..mixins import HeaderMixin, VarianceMixin
from ..serializable import Serializable


class Pressure(Serializable, HeaderMixin, VarianceMixin):
    """
    Pressure measurement data.
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
    """The absolute pressure reading from the sensor in Pascals."""
