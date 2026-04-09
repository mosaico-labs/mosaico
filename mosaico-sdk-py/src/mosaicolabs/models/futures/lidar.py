"""
LiDAR Ontology Model.

This module defines the LiDAR ontology model, which represents a 3D point cloud
obtained from a LiDAR sensor.
"""

from typing import Optional

from mosaicolabs import MosaicoField, MosaicoType, Serializable


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

    x: MosaicoType.list_(MosaicoType.float32) = MosaicoField(
        description="x coordinates in meters"
    )
    """X coordinates of each point in the cloud, in meters."""

    y: MosaicoType.list_(MosaicoType.float32) = MosaicoField(
        description="y coordinates in meters"
    )
    """Y coordinates of each point in the cloud, in meters."""

    z: MosaicoType.list_(MosaicoType.float32) = MosaicoField(
        description="z coordinates in meters"
    )
    """Z coordinates of each point in the cloud, in meters."""

    intensity: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None,
        description="Surface reflectivity per point.",
    )
    """Strength of the returned laser signal for each point."""

    reflectivity: Optional[MosaicoType.list_(MosaicoType.uint16)] = MosaicoField(
        default=None,
        description="Surface reflectivity per point.",
    )
    """
    Surface reflectivity per point.

    Encodes the estimated reflectance of the surface that produced each return,
    independently of the distance. Manufacturer-specific scaling applies.
    """

    beam_id: Optional[MosaicoType.list_(MosaicoType.uint16)] = MosaicoField(
        default=None,
        description="Laser beam index (ring / channel / line) that fired each point.",
    )
    """
    Laser beam index (ring / channel / line) that fired each point.

    Identifies which physical emitter in the sensor array produced the return.
    Equivalent to the ``ring`` field commonly found in ROS ``PointCloud2`` messages
    from multi-beam sensors such as Velodyne or Ouster.
    """

    range: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None,
        description="Distance from the sensor origin to each point, in meters.",
    )
    """
    Distance from the sensor origin to each point, in meters.

    Represents the raw radial distance along the beam axis, before projection
    onto Cartesian coordinates. Not always provided by all sensor drivers.
    """

    near_ir: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None,
        description="Near-infrared ambient light reading per point.",
    )
    """
    Near-infrared ambient light reading per point.

    Captured passively by the sensor between laser pulses. Useful as a proxy
    for ambient illumination or for filtering sun-noise artefacts.
    Exposed as the ``ambient`` channel in Ouster drivers.
    """

    azimuth: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None,
        description="Horizontal (azimuth) angle of each point in radians.",
    )
    """Horizontal (azimuth) angle of each point in radians."""

    elevation: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None,
        description="Vertical (elevation) angle of each point in radians.",
    )
    """Vertical (elevation) angle of each point in radians."""

    confidence: Optional[MosaicoType.list_(MosaicoType.uint8)] = MosaicoField(
        default=None,
        description="Per-point validity or confidence flags.",
    )
    """
    Per-point validity or confidence flags.
    
    Stored as a manufacturer-specific bitmask (equivalent to the ``tag`` or
    ``flags`` fields in Ouster point clouds). Individual bits may signal
    saturated returns, calibration issues, or other quality indicators.
    """

    return_type: Optional[MosaicoType.list_(MosaicoType.uint8)] = MosaicoField(
        default=None,
        description="Single/dual return classification per point.",
    )
    """
    Single/dual return classification per point.
    
    Indicates whether a point originates from the first return, last return,
    strongest return, etc. Encoding is manufacturer-specific.
    """

    point_timestamp: Optional[MosaicoType.list_(MosaicoType.float64)] = MosaicoField(
        default=None,
        description="Per-point acquisition time offset from the scan start, in seconds.",
    )
    """
    Per-point acquisition time offset from the scan start, in seconds.
    
    Allows precise temporal localisation of individual points within a single
    sweep, which is important for motion-distortion correction during
    point-cloud registration.
    """
