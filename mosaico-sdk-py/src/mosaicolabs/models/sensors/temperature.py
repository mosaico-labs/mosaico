"""
Temperature Ontology Module.

Defines the data structure for temperature sensors.
"""

import pyarrow as pa

from ..mixins import HeaderMixin, VarianceMixin
from ..serializable import Serializable


class Temperature(Serializable, HeaderMixin, VarianceMixin):
    """
    Temperature measurement data.
    """

    # --- Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "value",
                pa.float64(),
                nullable=False,
                metadata={"description": "Temperature value in Kelvin."},
            ),
        ]
    )

    value: float
    """Temperature value in Kelvin."""
