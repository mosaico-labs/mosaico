"""
Magnetometer Ontology Module.

Defines the data structure for magnetic field sensors.
"""

import pyarrow as pa

from ..mixins import HeaderMixin
from ..serializable import Serializable
from ..data import Vector3d


class Magnetometer(Serializable, HeaderMixin):
    """
    Magnetic field measurement data.
    """

    # --- Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "magnetic_field",
                Vector3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={
                    "description": "Magnetic field vector [mx, my, mz] in microTesla."
                },
            ),
        ]
    )

    magnetic_field: Vector3d
    """Magnetic field vector [mx, my, mz] in microTesla."""
