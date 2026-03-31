from typing import Dict

import pyarrow as pa
from typing_extensions import get_args, get_origin

_SCALAR_ARROW_MAP: Dict[type, pa.DataType] = {
    float: pa.float32(),
}


def _get_arrow_type(range_type: type) -> pa.DataType:
    """Resolves the pyarrow DataType for a given range type."""
    origin = get_origin(range_type)

    if origin is list:
        (inner,) = get_args(range_type)
        return pa.list_(_get_arrow_type(inner))

    if range_type in _SCALAR_ARROW_MAP:
        return _SCALAR_ARROW_MAP[range_type]

    raise ValueError(
        f"Unsupported laser_type '{range_type}'. Supported: {list(_SCALAR_ARROW_MAP)}"
    )


def _build_struct(range_type: type) -> pa.StructType:
    """Resolves the range type and then build the struct."""
    arrow_type = _get_arrow_type(range_type)
    return pa.struct(
        [
            pa.field(
                "angle_min",
                pa.float32(),
                nullable=False,
                metadata={"description": "start angle of the scan in rad."},
            ),
            pa.field(
                "angle_max",
                pa.float32(),
                nullable=False,
                metadata={"description": "end angle of the scan in rad."},
            ),
            pa.field(
                "angle_increment",
                pa.float32(),
                nullable=False,
                metadata={
                    "description": "angular distance between measurements in rad."
                },
            ),
            pa.field(
                "time_increment",
                pa.float32(),
                nullable=False,
                metadata={"description": "time between measurements in seconds."},
            ),
            pa.field(
                "scan_time",
                pa.float32(),
                nullable=False,
                metadata={"description": "time between scans in seconds."},
            ),
            pa.field(
                "range_min",
                pa.float32(),
                nullable=False,
                metadata={"description": "minimum range value in meters."},
            ),
            pa.field(
                "range_max",
                pa.float32(),
                nullable=False,
                metadata={"description": "maximum range value in meters."},
            ),
            pa.field(
                "ranges",
                arrow_type,
                nullable=False,
                metadata={
                    "description": "range data in meters. Ranges need to be between range min and max otherwise discarded."
                },
            ),
            pa.field(
                "intensities",
                arrow_type,
                nullable=True,
                metadata={"description": "intensity data."},
            ),
        ]
    )
