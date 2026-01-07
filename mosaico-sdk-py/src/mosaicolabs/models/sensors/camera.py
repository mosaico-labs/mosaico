"""
Camera  Module.

This module defines the `CameraInfo` model, which provides the meta-information
required to interpret an image geometrically. It defines the camera's intrinsic
properties (focal length, optical center), extrinsic properties (rectification),
and lens distortion model.

"""

from typing import Optional
import pyarrow as pa

from ..mixins import HeaderMixin
from ..data import ROI, Vector2d
from ..serializable import Serializable


class CameraInfo(Serializable, HeaderMixin):
    """
    Meta-information for interpreting images from a calibrated camera.

    This structure mirrors standard robotics camera models (e.g., ROS `sensor_msgs/CameraInfo`).
    It enables pipelines to rectify distorted images or project 3D points onto the 2D image plane.
    """

    # --- Schema Definition ---
    # Defines the memory layout for serialization.
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "height",
                pa.uint32(),
                nullable=False,
                metadata={
                    "description": "Height in pixels of the image with which the camera was calibrated."
                },
            ),
            pa.field(
                "width",
                pa.uint32(),
                nullable=False,
                metadata={
                    "description": "Width in pixels of the image with which the camera was calibrated."
                },
            ),
            pa.field(
                "distortion_model",
                pa.string(),
                nullable=False,
                metadata={
                    "description": "The distortion model used (e.g., 'plumb_bob', 'rational_polynomial')."
                },
            ),
            pa.field(
                "distortion_parameters",
                pa.list_(value_type=pa.float64()),
                nullable=False,
                metadata={
                    "description": "The distortion coefficients (k1, k2, t1, t2, k3...). Size depends on the model."
                },
            ),
            pa.field(
                "intrinsic_parameters",
                pa.list_(value_type=pa.float64(), list_size=9),
                nullable=False,
                metadata={
                    "description": "The 3x3 Intrinsic Matrix (K) flattened row-major. "
                    "Projects 3D points in the camera coordinate frame to 2D pixel coordinates."
                },
            ),
            pa.field(
                "rectification_parameters",
                pa.list_(value_type=pa.float64(), list_size=9),
                nullable=False,
                metadata={
                    "description": "The 3x3 Rectification Matrix (R) flattened row-major. "
                    "Used for stereo cameras to align the two image planes."
                },
            ),
            pa.field(
                "projection_parameters",
                pa.list_(value_type=pa.float64(), list_size=12),
                nullable=False,
                metadata={
                    "description": "The 3x4 Projection Matrix (P) flattened row-major. "
                    "Projects 3D world points directly into the rectified image pixel coordinates."
                },
            ),
            pa.field(
                "binning",
                Vector2d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={
                    "description": "Hardware binning factor (x, y). If null, assumes (0, 0) (no binning)."
                },
            ),
            pa.field(
                "roi",
                ROI.__msco_pyarrow_struct__,
                nullable=True,
                metadata={
                    "description": "Region of Interest. Used if the image is a sub-crop of the full resolution."
                },
            ),
        ]
    )

    height: int
    """Height in pixels of the image with which the camera was calibrated"""

    width: int
    """Width in pixels of the image with which the camera was calibrated"""

    distortion_model: str
    """The distortion model used"""

    distortion_parameters: list[float]
    """The distortion coefficients (k1, k2, t1, t2, k3...). Size depends on the model."""

    intrinsic_parameters: list[float]
    """The 3x3 Intrinsic Matrix (K) flattened row-major."""

    rectification_parameters: list[float]
    """The 3x3 Rectification Matrix (R) flattened row-major."""

    projection_parameters: list[float]
    """The 3x4 Projection Matrix (P) flattened row-major."""

    binning: Optional[Vector2d] = None
    """Hardware binning factor (x, y). If null, assumes (0, 0) (no binning)."""

    roi: Optional[ROI] = None
    """Region of Interest. Used if the image is a sub-crop of the full resolution."""
