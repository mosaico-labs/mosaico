"""
LiDAR Ontology Model.

This module defines the LiDAR ontology model, which represents a 3D point cloud
obtained from a LiDAR sensor.
"""

from typing import Any, Dict, List, Optional

import pyarrow as pa

from mosaicolabs.models.serializable import Serializable


class Lidar(Serializable):
    """
    LiDAR Ontology.

    This model represents a 3D point cloud acquired from a LiDAR sensor.
    Each field is a flat list whose *i*-th element corresponds to the *i*-th point
    in the scan. All lists within a single instance are therefore guaranteed to have
    the same length.

    Attributes:
        x: X coordinates of each point in meters.
        y: Y coordinates of each point in meters.
        z: Z coordinates of each point in meters.
        intensity: Strength of the returned signal for each point (optional).
        reflectivity: Surface reflectivity per point (optional).
        beam_id: Laser beam index (ring / channel / line) that fired each point (optional).
        range: Distance from the sensor origin to each point in meters (optional).
        near_ir: Near-infrared ambient light reading per point, useful as a noise/ambient
            estimate (optional).
        azimuth: Azimuth angle in radians for each point (optional).
        elevation: Elevation angle in radians for each point (optional).
        confidence: Per-point validity or confidence flags as a manufacturer-specific
            bitmask (optional).
        return_type: Single/dual return classification, manufacturer-specific (optional).
        point_timestamp: Per-point acquisition time offset from the scan start,
            in seconds (optional).
        extra_attributes: Additional manufacturer-specific attributes serialised as
            raw binary data (optional).

    Note:
        List-typed fields are **not queryable** via the `.Q` proxy. The `.Q` proxy
        is not available on this model.

    Example:
        ```python
        from mosaicolabs import MosaicoClient
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Fetch all sequences that contain at least one LiDAR topic
            sequences = client.get_sequences(ontology=Lidar)

            for sequence in sequences:
                print(f"Sequence: {sequence.name}")
                print(f"Topics:   {[topic.name for topic in sequence.topics]}")
        ```
    """

    # --- Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "x",
                pa.list_(pa.float32()),
                nullable=False,
                metadata={"description": "x coordinates in meters"},
            ),
            pa.field(
                "y",
                pa.list_(pa.float32()),
                nullable=False,
                metadata={"description": "y coordinates in meters"},
            ),
            pa.field(
                "z",
                pa.list_(pa.float32()),
                nullable=False,
                metadata={"description": "z coordinates in meters"},
            ),
            pa.field(
                "intensity",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "strength of the returned signal"},
            ),
            pa.field(
                "reflectivity",
                pa.list_(pa.uint16()),
                nullable=True,
                metadata={"description": "Surface reflectivity"},
            ),
            pa.field(
                "beam_id",
                pa.list_(pa.uint16()),
                nullable=True,
                metadata={
                    "description": "beam index (ring, channel, line), identifies which laser fired the point"
                },
            ),
            pa.field(
                "range",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "range in meters"},
            ),
            pa.field(
                "near_ir",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={
                    "description": "near-infrared ambient light (noise, ambient)"
                },
            ),
            pa.field(
                "azimuth",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "azimuth angle in radians"},
            ),
            pa.field(
                "elevation",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "elevation angle in radians"},
            ),
            pa.field(
                "confidence",
                pa.list_(pa.uint8()),
                nullable=True,
                metadata={
                    "description": "per-point validity/confidence flags (tag, flags), manufacturer-specific bitmask"
                },
            ),
            pa.field(
                "return_type",
                pa.list_(pa.uint8()),
                nullable=True,
                metadata={
                    "description": "single/dual return classification, manufacturer-specific"
                },
            ),
            pa.field(
                "point_timestamp",
                pa.list_(pa.float64()),
                nullable=True,
                metadata={
                    "description": "per-point acquisition time offset from scan start"
                },
            ),
            pa.field(
                "extra_attributes",
                pa.string(),
                nullable=True,
                metadata={"description": "extra attributes, manufacturer-specific"},
            ),
        ]
    )

    x: List[float]
    """X coordinates of each point in the cloud, in meters."""

    y: List[float]
    """Y coordinates of each point in the cloud, in meters."""

    z: List[float]
    """Z coordinates of each point in the cloud, in meters."""

    intensity: Optional[List[float]] = None
    """Strength of the returned laser signal for each point."""

    reflectivity: Optional[List[int]] = None
    """
    Surface reflectivity per point.

    Encodes the estimated reflectance of the surface that produced each return,
    independently of the distance. Manufacturer-specific scaling applies.
    """

    beam_id: Optional[List[int]] = None
    """
    Laser beam index (ring / channel / line) that fired each point.

    Identifies which physical emitter in the sensor array produced the return.
    Equivalent to the ``ring`` field commonly found in ROS ``PointCloud2`` messages
    from multi-beam sensors such as Velodyne or Ouster.
    """

    range: Optional[List[float]] = None
    """
    Distance from the sensor origin to each point, in meters.

    Represents the raw radial distance along the beam axis, before projection
    onto Cartesian coordinates. Not always provided by all sensor drivers.
    """

    near_ir: Optional[List[float]] = None
    """
    Near-infrared ambient light reading per point.

    Captured passively by the sensor between laser pulses. Useful as a proxy
    for ambient illumination or for filtering sun-noise artefacts.
    Exposed as the ``ambient`` channel in Ouster drivers.
    """

    azimuth: Optional[List[float]] = None
    """Horizontal (azimuth) angle of each point in radians."""

    elevation: Optional[List[float]] = None
    """Vertical (elevation) angle of each point in radians."""

    confidence: Optional[List[int]] = None
    """
    Per-point validity or confidence flags.
    
    Stored as a manufacturer-specific bitmask (equivalent to the ``tag`` or
    ``flags`` fields in Ouster point clouds). Individual bits may signal
    saturated returns, calibration issues, or other quality indicators.
    """

    return_type: Optional[List[int]] = None
    """
    Single/dual return classification per point.
    
    Indicates whether a point originates from the first return, last return,
    strongest return, etc. Encoding is manufacturer-specific.
    """

    point_timestamp: Optional[List[float]] = None
    """
    Per-point acquisition time offset from the scan start, in seconds.
    
    Allows precise temporal localisation of individual points within a single
    sweep, which is important for motion-distortion correction during
    point-cloud registration.
    """

    extra_attributes: Optional[Dict[str, Any]] = None
    """
    Additional manufacturer-specific attributes serialised as raw binary data.
    
    Provides a forward-compatible escape hatch for vendor extensions that do
    not map to any of the standardised fields above.
    """
