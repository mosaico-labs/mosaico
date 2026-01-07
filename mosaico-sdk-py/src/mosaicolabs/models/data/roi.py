import pyarrow as pa
from typing import Optional

from ..serializable import Serializable
from ..mixins import HeaderMixin
from .geometry import Vector2d


class ROI(Serializable, HeaderMixin):
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

    offset: Vector2d  # (Leftmost, Rightmost) pixels of the ROI
    height: int  # Height of ROI
    width: int  # Width of ROI

    # False if the full image is captured (ROI not used), and True if a subwindow is captured (ROI used).
    do_rectify: Optional[bool] = None
