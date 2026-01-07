"""
Dynamics Data Structures.

This module defines structures for forces and moments (torques).
These can be assigned to Message.data field to send data to the platform.
"""

import pyarrow as pa

from ..serializable import Serializable
from ..mixins import HeaderMixin, CovarianceMixin
from .geometry import Vector3d


class ForceTorque(
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Represents a Wrench (Force + Torque).

    Used to describe forces applied to a rigid body at a specific point.

    Components:
    - Force (Newtons): Linear force vector.
    - Torque (Newton-meters): Rotational moment vector.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "force",
                Vector3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={"description": "3D linear force vector"},
            ),
            pa.field(
                "torque",
                Vector3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={"description": "3D torque vector"},
            ),
        ]
    )

    force: Vector3d
    """3D linear force vector"""

    torque: Vector3d
    """3D torque vector"""
