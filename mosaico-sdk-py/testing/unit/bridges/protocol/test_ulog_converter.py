from types import SimpleNamespace as Fmt

import pyarrow as pa

from mosaicolabs.bridges.protocols.ulog import ULogSchemaConverter
from mosaicolabs.bridges.protocols.ulog.converter import _PRIMITIVES

FORMATS = {
    "esc_report": Fmt(
        fields=[
            ("uint64_t", 0, "timestamp"),
            ("int32_t", 0, "esc_rpm"),
            ("uint8_t", 3, "_padding0"),
        ]
    ),
    "esc_status": Fmt(
        fields=[
            ("uint64_t", 0, "timestamp"),
            ("bool", 0, "armed"),
            ("char", 4, "name"),
            ("char", 0, "c"),
            ("float", 3, "v"),
            ("esc_report", 2, "esc"),
            ("esc_report", 0, "main"),
            ("uint8_t", 7, "_padding0"),
        ]
    ),
    "all_primitives": Fmt(
        fields=[("uint64_t", 0, "timestamp")] + [(t, 0, f"f_{t}") for t in _PRIMITIVES]
    ),
}

ESC_REPORT = pa.struct([("timestamp", pa.uint64()), ("esc_rpm", pa.int32())])


def test_to_pyarrow():
    """Padding and top-level timestamp dropped, nested timestamp kept, arrays and chars mapped."""
    assert ULogSchemaConverter.to_pyarrow(FORMATS, "esc_status") == pa.struct(
        [
            ("armed", pa.bool_()),
            ("name", pa.string()),
            ("c", pa.string()),
            ("v", pa.list_(pa.float32(), 3)),
            ("esc", pa.list_(ESC_REPORT, 2)),
            ("main", ESC_REPORT),
        ]
    )
    assert ULogSchemaConverter.to_pyarrow(FORMATS, "all_primitives") == pa.struct(
        [(f"f_{t}", arrow_t) for t, arrow_t in _PRIMITIVES.items()]
    )


def test_format_definitions():
    """The original `F` definitions are rebuilt verbatim, padding included."""
    report_def = "esc_report:uint64_t timestamp;int32_t esc_rpm;uint8_t[3] _padding0;"
    assert ULogSchemaConverter.format_def(FORMATS, "esc_report") == report_def
    assert ULogSchemaConverter.nested_formats(FORMATS, "esc_status") == {
        "esc_report": report_def
    }
    assert ULogSchemaConverter.nested_formats(FORMATS, "esc_report") == {}
