"""
Radar Ontology Model.

This module defines the Radar ontology model, which represents a sparse point cloud
of detections obtained from a Radar sensor.
"""

from typing import Any, Dict, List, Optional

import pyarrow as pa

from mosaicolabs.models import Serializable


class Radar(Serializable):
    """
    Radar Ontology.

    This model represents a set of detections acquired from a Radar sensor. Each
    detection corresponds to a target or a reflection point in the sensor's field
    of view, characterised by its position, optional velocity, and signal-quality
    metrics.

    Each field is a flat list whose *i*-th element corresponds to the *i*-th
    detection in the scan.

    Unlike a LiDAR, Radar detections are inherently sparse and carry
    additional electromagnetic attributes such as Radar Cross Section (RCS),
    Signal-to-Noise Ratio (SNR), and Doppler velocity, which are not available
    from purely optical sensors.

    Attributes:
        x: X coordinates of each detection in meters.
        y: Y coordinates of each detection in meters.
        z: Z coordinates of each detection in meters.
        range: Radial distance from the sensor origin to each detection in meters (optional).
        azimuth: Azimuth angle in radians for each detection (optional).
        elevation: Elevation angle in radians for each detection (optional).
        rcs: Radar Cross Section of each detection in dBm (optional).
        snr: Signal-to-Noise Ratio of each detection in dB (optional).
        doppler_velocity: Doppler radial velocity of each detection in m/s (optional).
        vx: X component of the velocity of each detection in m/s (optional).
        vy: Y component of the velocity of each detection in m/s (optional).
        vx_comp: Ego-motion-compensated X velocity of each detection in m/s (optional).
        vy_comp: Ego-motion-compensated Y velocity of each detection in m/s (optional).
        ax: X component of the acceleration of each detection in m/s² (optional).
        ay: Y component of the acceleration of each detection in m/s² (optional).
        radial_speed: Radial speed of each detection in m/s (optional).
        extra_attributes: Additional manufacturer-specific attributes serialised as
            raw binary data (optional).

    Note:
        List-typed fields are **not queryable** via the `.Q` proxy. The `.Q` proxy
        is not available on this model.

    Example:
        ```python
        from mosaicolabs import MosaicoClient
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Fetch all sequences that contain at least one Radar topic
            sequences = client.get_sequences(ontology=Radar)

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
                metadata={"description": "x coordinates in meters."},
            ),
            pa.field(
                "y",
                pa.list_(pa.float32()),
                nullable=False,
                metadata={"description": "y coordinates in meters."},
            ),
            pa.field(
                "z",
                pa.list_(pa.float32()),
                nullable=False,
                metadata={"description": "z coordinates in meters."},
            ),
            pa.field(
                "range",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "radial distance in meters."},
            ),
            pa.field(
                "azimuth",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "azimuth angle in radians."},
            ),
            pa.field(
                "elevation",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "elevation angle in radians."},
            ),
            pa.field(
                "rcs",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "radar cross section in dBm."},
            ),
            pa.field(
                "snr",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "signal to noise ratio in dB."},
            ),
            pa.field(
                "doppler_velocity",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "doppler velocity in m/s."},
            ),
            pa.field(
                "vx",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "x velocity in m/s."},
            ),
            pa.field(
                "vy",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "y velocity in m/s."},
            ),
            pa.field(
                "vx_comp",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "x compensated velocity in m/s."},
            ),
            pa.field(
                "vy_comp",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "y compensated velocity in m/s."},
            ),
            pa.field(
                "ax",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "x acceleration in m/s^2."},
            ),
            pa.field(
                "ay",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "y acceleration in m/s^2."},
            ),
            pa.field(
                "radial_speed",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "radial speed in m/s."},
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
    """X coordinates of each detection, in meters."""

    y: List[float]
    """Y coordinates of each detection, in meters."""

    z: List[float]
    """Z coordinates of each detection, in meters."""

    range: Optional[List[float]] = None
    """
    Radial distance from the sensor origin to each detection, in meters.

    Represents the straight-line distance along the beam axis.
    """

    azimuth: Optional[List[float]] = None
    """
    Horizontal (azimuth) angle of each detection in radians.

    Measured in the sensor's horizontal plane, typically from 0 to 2π,
    with 0 aligned to the sensor's forward axis.
    """

    elevation: Optional[List[float]] = None
    """
    Vertical (elevation) angle of each detection in radians.

    Measured from the sensor's horizontal plane; positive values point upward.
    """

    rcs: Optional[List[float]] = None
    """
    Radar Cross Section (RCS) of each detection, in dBm.

    Quantifies the effective scattering area of the target as seen by the
    sensor. Higher values typically correspond to larger or more reflective
    objects. Useful for target classification and false-positive filtering.
    """

    snr: Optional[List[float]] = None
    """
    Signal-to-Noise Ratio (SNR) of each detection, in dB.

    Indicates the quality of the received echo relative to background noise.
    Low-SNR detections are generally less reliable and may be filtered out
    during object-level processing.
    """

    doppler_velocity: Optional[List[float]] = None
    """
    Doppler radial velocity of each detection, in m/s.

    Represents the component of the target's velocity along the sensor's
    line of sight, derived directly from the frequency shift of the returned
    signal. Positive values conventionally indicate motion away from the sensor.
    """

    vx: Optional[List[float]] = None
    """
    X component of the estimated velocity of each detection, in m/s.

    Expressed in the sensor frame. This is a Cartesian decomposition of the
    target velocity, as opposed to the purely radial ``doppler_velocity``.
    """

    vy: Optional[List[float]] = None
    """
    Y component of the estimated velocity of each detection, in m/s.

    Expressed in the sensor frame. See ``vx`` for further context.
    """

    vx_comp: Optional[List[float]] = None
    """
    Ego-motion-compensated X velocity of each detection, in m/s.

    Obtained by subtracting the host vehicle's own velocity from ``vx``,
    yielding the detection's absolute velocity in the world frame along the
    X axis.
    """

    vy_comp: Optional[List[float]] = None
    """
    Ego-motion-compensated Y velocity of each detection, in m/s.

    Analogous to ``vx_comp`` along the Y axis. See ``vx_comp`` for further context.
    """

    ax: Optional[List[float]] = None
    """
    X component of the estimated acceleration of each detection, in m/s².

    Available only on sensors that track detections across multiple scans and
    report per-point kinematic state (e.g. high-level object-list outputs).
    """

    ay: Optional[List[float]] = None
    """
    Y component of the estimated acceleration of each detection, in m/s².

    Analogous to ``ax`` along the Y axis. See ``ax`` for further context.
    """

    radial_speed: Optional[List[float]] = None
    """
    Radial speed of each detection, in m/s.

    Represents the magnitude of the velocity component along the line of sight,
    without sign convention. Distinct from ``doppler_velocity``, which may carry
    a directional sign depending on the sensor's convention.
    """

    extra_attributes: Optional[Dict[str, Any]] = None
    """
    Additional manufacturer-specific attributes serialised as raw binary data.

    Provides a forward-compatible escape hatch for vendor extensions that do
    not map to any of the standardised fields above.
    """
