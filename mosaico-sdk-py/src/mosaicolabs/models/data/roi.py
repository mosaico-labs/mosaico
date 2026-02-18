import pyarrow as pa
from typing import Optional

from ..serializable import Serializable
from ..mixins import HeaderMixin
from .geometry import Vector2d


class ROI(Serializable, HeaderMixin):
    """
    Represents a rectangular Region of Interest (ROI) within a 2D coordinate system.

    This class is primarily used in imaging and computer vision pipelines to define
    sub-windows for processing or rectification.

    Attributes:
        offset: A [`Vector2d`][mosaicolabs.models.data.geometry.Vector2d] representing
            the top-left (leftmost, topmost) pixel coordinates of the ROI.
        height: The vertical extent of the ROI in pixels.
        width: The horizontal extent of the ROI in pixels.
        do_rectify: Optional flag; `True` if a sub-window is captured and requires
            rectification.
        header: Standard metadata header providing temporal and spatial context.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, ROI, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter ROIs with offset X-component AND offset Y-component
            qresponse = client.query(
                QueryOntologyCatalog(ROI.Q.offset.x.gt(5.0))
                    .with_expression(ROI.Q.offset.y.lt(10))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(ROI.Q.offset.x.gt(5.0), include_timestamp_range=True)
                .with_expression(ROI.Q.offset.y.lt(10))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "offset",
                Vector2d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={"description": "(Leftmost, Rightmost) pixels of the ROI."},
            ),
            pa.field(
                "height",
                pa.uint32(),
                nullable=False,
                metadata={"description": "Height pixel of the ROI."},
            ),
            pa.field(
                "width",
                pa.uint32(),
                nullable=False,
                metadata={"description": "Width pixel of the ROI."},
            ),
            pa.field(
                "do_rectify",
                pa.bool_(),
                nullable=True,
                metadata={
                    "description": "False if the full image is captured (ROI not used)"
                    " and True if a subwindow is captured (ROI used) (optional). False if Null"
                },
            ),
        ]
    )

    offset: Vector2d
    """
    The top-left pixel coordinates of the ROI.

    ### Querying with the **`.Q` Proxy**
    Offset components are queryable through the `offset` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ROI.Q.offset.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `ROI.Q.offset.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, ROI, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for ROIs starting between the 10th and 350th pixel vertically
            qresponse = client.query(
                QueryOntologyCatalog(ROI.Q.offset.x.gt(100))
                    .with_expression(ROI.Q.offset.y.between(10, 350))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(ROI.Q.offset.x.gt(100), include_timestamp_range=True)
                .with_expression(ROI.Q.offset.y.between(10, 350))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    height: int
    """
    Height of the ROI in pixels.

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ROI.Q.height` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, ROI, QueryOntologyCatalog
        
        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for ROIs with height beyond 100 pixels
            qresponse = client.query(
                QueryOntologyCatalog(ROI.Q.height.gt(100))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(ROI.Q.height.gt(100), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    width: int
    """
    Width of the ROI in pixels.

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ROI.Q.width` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, ROI, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for ROIs with width below (or equal to) 250 pixels
            qresponse = client.query(
                QueryOntologyCatalog(ROI.Q.width.leq(250))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(ROI.Q.width.gt(100), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    do_rectify: Optional[bool] = None
    """
    Flag indicating if the ROI requires rectification.

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ROI.Q.do_rectify` | `Boolean` | `.eq()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, ROI, QueryOntologyCatalog
        
        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for explicitly non-rectified ROIs (not None)
            qresponse = client.query(
                QueryOntologyCatalog(ROI.Q.do_rectify.eq(False))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """
