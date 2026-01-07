"""
IMU Ontology Module.

This module defines the `IMU` model for Inertial Measurement Units.
It aggregates data from accelerometers and gyroscopes.

"""

from typing import Optional
import pyarrow as pa

from ..mixins import HeaderMixin
from ..serializable import Serializable
from ..data import Quaternion, Vector3d


class IMU(Serializable, HeaderMixin):
    """
    Inertial Measurement Unit data.

    Aggregates:
    - Linear Acceleration (m/s^2)
    - Angular Velocity (rad/s)
    - Orientation (Quaternion, optional)
    """

    # --- Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "acceleration",
                Vector3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={
                    "description": "Linear acceleration vector [ax, ay, az] in m/s^2."
                },
            ),
            pa.field(
                "angular_velocity",
                Vector3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={
                    "description": "Angular velocity vector [wx, wy, wz] in rad/s."
                },
            ),
            pa.field(
                "orientation",
                Quaternion.__msco_pyarrow_struct__,
                nullable=True,
                metadata={
                    "description": "Estimated orientation [qx, qy, qz, qw] (optional)."
                },
            ),
        ]
    )

    acceleration: Vector3d
    """Linear acceleration vector [ax, ay, az] in m/s^2"""

    angular_velocity: Vector3d
    """Angular velocity vector [wx, wy, wz] in rad/s."""

    orientation: Optional[Quaternion] = None
    """Estimated orientation [qx, qy, qz, qw] (optional)."""
