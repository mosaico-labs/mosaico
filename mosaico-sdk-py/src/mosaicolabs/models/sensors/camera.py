"""
Camera  Module.

This module defines the `CameraInfo` model, which provides the meta-information
required to interpret an image geometrically. It defines the camera's intrinsic
properties (focal length, optical center), extrinsic properties (rectification),
and lens distortion model.

"""

from typing import Optional

from ..core import MosaicoField, MosaicoType, Serializable
from ..data import ROI, HeaderMixin, Vector2d


class CameraInfo(Serializable, HeaderMixin):
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
        header (optional[Header]): Optional heading containing measurement metadata

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter camera data based
    on camera parameters within a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog].
    Expressions entailing lists of values can be queried using any between `all()`, `any()` or index access `[i]`
    followed by the contained type supported operations.

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
    | `CameraInfo.Q.height` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

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
    | `CameraInfo.Q.width` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

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
    | `CameraInfo.Q.distortion_model` | `String` | `.eq()`, `.match()`, `.in_()`, `.lt()`, `.gt()`, `.leq()`, `.geq()` |

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
    The distortion parameters are queryable via the `distortion_parameters` field. Since it
    represents a list of values, use `all()`, `any()` or index access `[i]` to narrow down to
    the list element and compose a correct expression.
        - CameraInfo.Q.distortion_parameters.all()       -> invalid expression
        - CameraInfo.Q.distortion_parameters.gt(1)       -> invalid expression
        - CameraInfo.Q.distortion_parameters.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `CameraInfo.Q.distortion_parameters.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.distortion_parameters.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.distortion_parameters.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, CameraInfo

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for cameras with a strong radial distortion coefficient
            qresponse = client.query(
                QueryOntologyCatalog(CameraInfo.Q.distortion_parameters.any().gt(0.1))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

                    # Clusterize all topics within the sequence to extract the time intervals
                    clusters_dict = item.clusterize_all()

                    # Since clusterize_all() used default clustering_dt_ns, each topic will have
                    # just one cluster representing the first and last moment the query was satisfied
                    for t_name, clusters in clusters_dict.items():
                        print(f"{t_name}:\\n", "\\n".join(f"{cluster}" for cluster in clusters))
        ```
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
    The intrinsic parameters are queryable via the `intrinsic_parameters` field. 
    Use `all()`, `any()` or index access `[i]` to narrow down to the list 
    element and compose a correct expression.
        - CameraInfo.Q.intrinsic_parameters.all()       -> invalid expression
        - CameraInfo.Q.intrinsic_parameters.gt(1)       -> invalid expression
        - CameraInfo.Q.intrinsic_parameters.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `CameraInfo.Q.intrinsic_parameters.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.intrinsic_parameters.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.intrinsic_parameters.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, CameraInfo

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for cameras with a specific focal length fx (K[0,0], index 0)
            qresponse = client.query(
                QueryOntologyCatalog(CameraInfo.Q.intrinsic_parameters[0].between(500.0, 700.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

                    # Clusterize all topics within the sequence to extract the time intervals
                    clusters_dict = item.clusterize_all()

                    # Since clusterize_all() used default clustering_dt_ns, each topic will have
                    # just one cluster representing the first and last moment the query was satisfied
                    for t_name, clusters in clusters_dict.items():
                        print(f"{t_name}:\\n", "\\n".join(f"{cluster}" for cluster in clusters))
        ```
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
    The rectification parameters are queryable via the `rectification_parameters` field. 
    Use `all()`, `any()` or index access `[i]` to narrow down to the list element and 
    compose a correct expression.
        - CameraInfo.Q.rectification_parameters.all()       -> invalid expression
        - CameraInfo.Q.rectification_parameters.gt(1)       -> invalid expression
        - CameraInfo.Q.rectification_parameters.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `CameraInfo.Q.rectification_parameters.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.rectification_parameters.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.rectification_parameters.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, CameraInfo

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for cameras with a near-identity rectification matrix
            qresponse = client.query(
                QueryOntologyCatalog(CameraInfo.Q.rectification_parameters.all().between(-1.0, 1.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

                    # Clusterize all topics within the sequence to extract the time intervals
                    clusters_dict = item.clusterize_all()

                    # Since clusterize_all() used default clustering_dt_ns, each topic will have
                    # just one cluster representing the first and last moment the query was satisfied
                    for t_name, clusters in clusters_dict.items():
                        print(f"{t_name}:\\n", "\\n".join(f"{cluster}" for cluster in clusters))
        ```
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
    The projection parameters are queryable via the `projection_parameters` field. 
    Use `all()`, `any()` or index access `[i]` to narrow down to the list element 
    and compose a correct expression.
        - CameraInfo.Q.projection_parameters.all()       -> invalid expression
        - CameraInfo.Q.projection_parameters.gt(1)       -> invalid expression
        - CameraInfo.Q.projection_parameters.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `CameraInfo.Q.projection_parameters.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.projection_parameters.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.projection_parameters.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, CameraInfo

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for stereo cameras with a non-zero baseline term Tx (P[0,3], index 3)
            qresponse = client.query(
                QueryOntologyCatalog(CameraInfo.Q.projection_parameters[3].lt(0.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

                    # Clusterize all topics within the sequence to extract the time intervals
                    clusters_dict = item.clusterize_all()

                    # Since clusterize_all() used default clustering_dt_ns, each topic will have
                    # just one cluster representing the first and last moment the query was satisfied
                    for t_name, clusters in clusters_dict.items():
                        print(f"{t_name}:\\n", "\\n".join(f"{cluster}" for cluster in clusters))
        ```
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
    | `CameraInfo.Q.binning.x` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.binning.y` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

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
    | `CameraInfo.Q.roi.offset.x` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.roi.offset.y` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.roi.width` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `CameraInfo.Q.roi.height` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

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
