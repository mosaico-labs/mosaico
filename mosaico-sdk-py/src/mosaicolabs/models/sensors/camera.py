"""
Camera  Module.

This module defines the `CameraInfo` model, which provides the meta-information
required to interpret an image geometrically. It defines the camera's intrinsic
properties (focal length, optical center), extrinsic properties (rectification),
and lens distortion model.

"""

from typing import Optional

from mosaicolabs.models import MosaicoType
from mosaicolabs.models.types import MosaicoField

from ..data import ROI, Vector2d
from ..serializable import Serializable


class CameraInfo(Serializable):
    """
    Meta-information for interpreting images from a calibrated camera.

    This structure mirrors standard robotics camera models (e.g., ROS `sensor_msgs/CameraInfo`).
    It enables pipelines to rectify distorted images or project 3D points onto the 2D image plane.

    Attributes:
        height: Height in pixels of the image with which the camera was calibrated
        width: Width in pixels of the image with which the camera was calibrated
        distortion_model: The distortion model used
        distortion_parameters: The distortion coefficients (k1, k2, t1, t2, k3...). Size depends on the model.
        intrinsic_parameters: The 3x3 Intrinsic Matrix (K) flattened row-major.
        rectification_parameters: The 3x3 Rectification Matrix (R) flattened row-major.
        projection_parameters: The 3x4 Projection Matrix (P) flattened row-major.
        binning: Hardware binning factor (x, y). If null, assumes (0, 0) (no binning).
        roi: Region of Interest. Used if the image is a sub-crop of the full resolution.

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter camera data based
    on camera parameters within a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, CameraInfo

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for camera data based on camera parameters
            qresponse = client.query(
                QueryOntologyCatalog(CameraInfo.Q.height.between(1080, 2160))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    height: MosaicoType.uint32 = MosaicoField(
        description="Height in pixels of the image with which the camera was calibrated."
    )
    """
    Height in pixels of the image with which the camera was calibrated

    ### Querying with the **`.Q` Proxy**
    The height is queryable via the `height` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `CameraInfo.Q.height` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, CameraInfo

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for camera data based on camera parameters
            qresponse = client.query(
                QueryOntologyCatalog(CameraInfo.Q.height.between(1080, 2160))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    width: MosaicoType.uint32 = MosaicoField(
        description="Width in pixels of the image with which the camera was calibrated."
    )
    """
    Width in pixels of the image with which the camera was calibrated

    ### Querying with the **`.Q` Proxy**
    The width is queryable via the `width` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `CameraInfo.Q.width` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, CameraInfo

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for camera data based on camera parameters
            qresponse = client.query(
                QueryOntologyCatalog(CameraInfo.Q.width.between(1920, 3840))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    distortion_model: MosaicoType.string = MosaicoField(
        description="The distortion model used (e.g., 'plumb_bob', 'rational_polynomial')."
    )
    """
    The distortion model used

    ### Querying with the **`.Q` Proxy**
    The distortion model is queryable via the `distortion_model` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `CameraInfo.Q.distortion_model` | `Categorical` | `.eq()`, `.neq()`, `.in_()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, CameraInfo

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for camera data based on camera parameters
            qresponse = client.query(
                QueryOntologyCatalog(CameraInfo.Q.distortion_model.eq("plumb_bob"))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    distortion_parameters: MosaicoType.list_(MosaicoType.float64) = MosaicoField(
        description="The distortion coefficients (k1, k2, t1, t2, k3...). Size depends on the model."
    )
    """
    The distortion coefficients (k1, k2, t1, t2, k3...). Size depends on the model.

    ### Querying with the **`.Q` Proxy**
    The distortion parameters are not queryable via the `.Q` proxy (Lists are not supported yet).
    """

    intrinsic_parameters: MosaicoType.list_(MosaicoType.float64, list_size=9) = (
        MosaicoField(
            description="The 3x3 Intrinsic Matrix (K) flattened row-major. "
            "Projects 3D points in the camera coordinate frame to 2D pixel coordinates."
        )
    )
    """
    The 3x3 Intrinsic Matrix (K) flattened row-major.

    ### Querying with the **`.Q` Proxy**
    The intrinsic parameters are not queryable via the `.Q` proxy (Lists are not supported yet).
    """

    rectification_parameters: MosaicoType.list_(MosaicoType.float64, list_size=9) = (
        MosaicoField(
            description="The 3x3 Rectification Matrix (R) flattened row-major. "
            "Used for stereo cameras to align the two image planes."
        )
    )
    """
    The 3x3 Rectification Matrix (R) flattened row-major.

    ### Querying with the **`.Q` Proxy**
    The rectification parameters cannot be queried via the `.Q` proxy (Lists are not supported yet).
    """

    projection_parameters: MosaicoType.list_(MosaicoType.float64, list_size=12) = (
        MosaicoField(
            description="The 3x4 Projection Matrix (P) flattened row-major. "
            "Projects 3D world points directly into the rectified image pixel coordinates."
        )
    )
    """
    The 3x4 Projection Matrix (P) flattened row-major.

    ### Querying with the **`.Q` Proxy**
    The projection parameters cannot be queried via the `.Q` proxy (Lists are not supported yet).
    """

    binning: Optional[Vector2d] = MosaicoField(
        default=None,
        description="Hardware binning factor (x, y). If null, assumes (0, 0) (no binning).",
    )
    """
    Hardware binning factor (x, y). If null, assumes (0, 0) (no binning).

    ### Querying with the **`.Q` Proxy**
    The binning parameters are queryable via the `binning` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `CameraInfo.Q.binning.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.binning.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, CameraInfo

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for camera data based on camera parameters
            qresponse = client.query(
                QueryOntologyCatalog(CameraInfo.Q.binning.x.eq(2))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    roi: Optional[ROI] = MosaicoField(
        default=None,
        description="Region of Interest. Used if the image is a sub-crop of the full resolution.",
    )
    """
    Region of Interest. Used if the image is a sub-crop of the full resolution.

    ### Querying with the **`.Q` Proxy**
    The roi parameters are queryable via the `roi` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `CameraInfo.Q.roi.offset.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.roi.offset.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.roi.width` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.roi.height` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, CameraInfo

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for camera data based on camera parameters
            qresponse = client.query(
                QueryOntologyCatalog(CameraInfo.Q.roi.offset.x.eq(2))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """
