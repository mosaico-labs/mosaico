"""
GNSS/GPS Ontology Module.

This module defines data structures for Global Navigation Satellite Systems.
It includes Status flags, processed Fixes (Position/Velocity), and raw NMEA strings.

"""

from typing import Optional
import pyarrow as pa

from ..data import Point3d, Vector3d
from ..mixins import HeaderMixin
from ..serializable import Serializable


class GPSStatus(Serializable, HeaderMixin):
    """
    Status of the GNSS receiver and satellite fix.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "status",
                pa.int8(),
                nullable=False,
                metadata={"description": "Fix status."},
            ),
            pa.field(
                "service",
                pa.uint16(),
                nullable=False,
                metadata={"description": "Service used (GPS, GLONASS, etc)."},
            ),
            pa.field(
                "satellites",
                pa.int8(),
                nullable=True,
                metadata={"description": "Satellites visible/used."},
            ),
            pa.field(
                "hdop",
                pa.float64(),
                nullable=True,
                metadata={"description": "Horizontal Dilution of Precision."},
            ),
            pa.field(
                "vdop",
                pa.float64(),
                nullable=True,
                metadata={"description": "Vertical Dilution of Precision."},
            ),
        ]
    )

    status: int
    """Fix status."""

    service: int
    """Service used (GPS, GLONASS, etc)."""

    satellites: Optional[int] = None
    """Satellites visible/used."""

    hdop: Optional[float] = None
    """Horizontal Dilution of Precision."""

    vdop: Optional[float] = None
    """Vertical Dilution of Precision."""


class GPS(Serializable, HeaderMixin):
    """
    Processed GNSS fix containing Position, Velocity, and Status.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "position",
                Point3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={"description": "Lat/Lon/Alt (WGS 84)."},
            ),
            pa.field(
                "velocity",
                Vector3d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "Velocity vector [North, East, Alt] m/s."},
            ),
            pa.field(
                "status",
                GPSStatus.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "Receiver status info."},
            ),
        ]
    )

    position: Point3d
    """Lat/Lon/Alt (WGS 84)."""

    velocity: Optional[Vector3d] = None
    """Velocity vector [North, East, Alt] m/s."""

    status: Optional[GPSStatus] = None
    """Receiver status info."""


class NMEASentence(Serializable, HeaderMixin):
    """
    Raw NMEA 0183 sentence string.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "sentence",
                pa.string(),
                nullable=False,
                metadata={"description": "Raw ASCII sentence."},
            ),
        ]
    )

    sentence: str
    """Raw ASCII sentence."""
