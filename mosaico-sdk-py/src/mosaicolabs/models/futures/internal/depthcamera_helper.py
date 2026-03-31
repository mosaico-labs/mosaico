from typing import List

import pyarrow as pa

_COMMON_FIELD: List[pa.Field] = [
    pa.field(
        "x",
        pa.list_(pa.float32()),
        nullable=False,
        metadata={"description": "Horizontal position derived from depth"},
    ),
    pa.field(
        "y",
        pa.list_(pa.float32()),
        nullable=False,
        metadata={"description": "Vertical position derived from depth"},
    ),
    pa.field(
        "z",
        pa.list_(pa.float32()),
        nullable=False,
        metadata={
            "description": "Depth value directly (distance along optical axis in meter)"
        },
    ),
    pa.field(
        "intensity",
        pa.list_(pa.float32()),
        nullable=True,
        metadata={"description": ("Signal amplitude/intensity.")},
    ),
    pa.field(
        "rgb",
        pa.list_(pa.float32()),
        nullable=True,
        metadata={"description": "Packed RGB color value"},
    ),
    pa.field(
        "extra_attributes",
        pa.string(),
        nullable=True,
        metadata={"description": "Vendor-specific attributes."},
    ),
]

_TOF_FIELDS: list[pa.Field] = [
    pa.field(
        "noise",
        pa.list_(pa.float32()),
        nullable=True,
        metadata={"description": "Noise value per pixel."},
    ),
    pa.field(
        "grayscale",
        pa.list_(pa.float32()),
        nullable=True,
        metadata={"description": "Grayscale amplitude."},
    ),
]

_STEREO_FIELDS: list[pa.Field] = [
    pa.field(
        "luma",
        pa.list_(pa.uint8()),
        nullable=True,
        metadata={
            "description": "Luminance of the corresponding pixel in the rectified image."
        },
    ),
    pa.field(
        "cost",
        pa.list_(pa.uint8()),
        nullable=True,
        metadata={
            "description": "Stereo matching cost (disparity confidence measure, 0 = high confidence)."
        },
    ),
]


def _build_struct(*field_groups: List[pa.Field]) -> pa.StructType:
    """Utilities for building struct starting from the fields passed as argument."""
    fields = [field for group in field_groups for field in group]
    return pa.struct(fields)
