"""
Kinematics Data Structures.

This module defines structures for analyzing motion:
1.  **Velocity (Twist)**: Linear and angular speed.
2.  **Acceleration**: Linear and angular acceleration.
3.  **MotionState**: A complete snapshot of an object's kinematics (Pose + Velocity + Acceleration).

These can be assigned to Message.data field to send data to the platform.
"""

from typing import Optional
import pyarrow as pa
from pydantic import model_validator

from ..serializable import Serializable
from ..mixins import HeaderMixin, CovarianceMixin
from .geometry import Pose, Vector3d


class Velocity(
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Represents 6-Degree-of-Freedom Velocity (Twist).

    Composed of:
    - Linear Velocity (v_x, v_y, v_z)
    - Angular Velocity (omega_x, omega_y, omega_z)
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "linear",
                Vector3d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "3D linear velocity vector"},
            ),
            pa.field(
                "angular",
                Vector3d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "3D angular velocity vector"},
            ),
        ]
    )

    linear: Optional[Vector3d] = None
    """3D linear velocity vector"""

    angular: Optional[Vector3d] = None
    """3D angular velocity vector"""

    @model_validator(mode="after")
    def check_at_least_one_exists(self) -> "Velocity":
        """
        Ensures the velocity object is not empty.

        Raises:
            ValueError: If both `linear` and `angular` are None.
        """
        if self.linear is None and self.angular is None:
            raise ValueError("User must provide at least 'linear' or 'angular'.")
        return self


class Acceleration(
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Represents 6-Degree-of-Freedom Acceleration.

    Composed of:
    - Linear Acceleration (a_x, a_y, a_z)
    - Angular Acceleration (alpha_x, alpha_y, alpha_z)
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "linear",
                Vector3d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "3D linear acceleration vector"},
            ),
            pa.field(
                "angular",
                Vector3d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "3D angular acceleration vector"},
            ),
        ]
    )

    linear: Optional[Vector3d] = None
    """3D linear acceleration vector"""

    angular: Optional[Vector3d] = None
    """3D angular acceleration vector"""

    @model_validator(mode="after")
    def check_at_least_one_exists(self) -> "Acceleration":
        """
        Ensures the acceleration object is not empty.

        Raises:
            ValueError: If both `linear` and `angular` are None.
        """
        if self.linear is None and self.angular is None:
            raise ValueError("User must provide at least 'linear' or 'angular'.")
        return self


class MotionState(
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Aggregated Kinematic State.

    This class groups Pose, Velocity, and Acceleration into a single atomic update.
    Commonly used for trajectory tracking, state estimation outputs, or ground truth logging.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "pose",
                Pose.__msco_pyarrow_struct__,
                nullable=False,
                metadata={
                    "description": "6D pose with optional time and covariance info."
                },
            ),
            pa.field(
                "velocity",
                Velocity.__msco_pyarrow_struct__,
                nullable=False,
                metadata={
                    "description": "6D velocity with optional time and covariance info."
                },
            ),
            pa.field(
                "target_frame_id",
                pa.string(),
                nullable=False,
                metadata={"description": "Target frame identifier."},
            ),
            pa.field(
                "acceleration",
                Acceleration.__msco_pyarrow_struct__,
                nullable=True,
                metadata={
                    "description": "6D acceleration with optional time and covariance info."
                },
            ),
        ]
    )

    pose: Pose
    """6D pose with optional time and covariance info"""

    velocity: Velocity
    """6D velocity with optional time and covariance info"""

    target_frame_id: str
    """Target frame identifier"""

    acceleration: Optional[Acceleration] = None
    """6D acceleration with optional time and covariance info"""
