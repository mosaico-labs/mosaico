"""
Depth Camera Ontology Models.

This module defines ontology models for depth-sensing cameras. Three concrete
variants are provided, each targeting a specific acquisition technology:

- `RGBDCamera`: RGB-D sensors that combine a colour image with a
  registered depth map (e.g. Intel RealSense, Microsoft Azure Kinect).
- `ToFCamera`: Time-of-Flight sensors that measure depth by emitting
  modulated infrared light and computing the phase shift of the return signal
  (e.g. PMD Flexx2, ifm O3R).
- `StereoCamera`: Passive stereo cameras that estimate depth via
  disparity between a rectified left/right image pair
  (e.g. ZED, Stereolabs OAK-D).

All three models share a common spatial core (``x``, ``y``, ``z``) and optional
colour/intensity channels, and extend it with technology-specific fields.
"""

from typing import Optional, Tuple

import numpy as np

from mosaicolabs.models import BaseModel, MosaicoField, MosaicoType, Serializable


def pack_rgb(r: int, g: int, b: int) -> float:
    """Packs three RGB channels (8-bit each) into a single float32 value.

    This utility combines Red, Green, and Blue components into a 32-bit integer
    and then bit-casts the result into a float. This is a standard technique
    used in point cloud processing to store color data efficiently within
    a single field.

    Args:
        r: Red intensity (0-255).
        g: Green intensity (0-255).
        b: Blue intensity (0-255).

    Returns:
        The RGB color encoded as a 32-bit float.
    """
    packed = np.uint32((r << 16) | (g << 8) | b)
    return float(np.frombuffer(packed.tobytes(), dtype=np.float32)[0])


def unpack_rgb(packed_rgb: float) -> Tuple[int, int, int]:
    """Unpacks a float32 value back into its original RGB components.

    This is the inverse operation of [`pack_rgb()`][mosaicolabs.models.futures.depth_camera.pack_rgb].
    It reinterprets the float's bits as an unsigned 32-bit integer and
    extracts the individual color bytes.

    Args:
        packed_rgb: The encoded float value containing RGB data.

    Returns:
        A tuple of `(red, green, blue)`, where each value is between 0 and 255.
    """
    packed = np.frombuffer(np.float32(packed_rgb).tobytes(), dtype=np.uint32)[0]

    red = int((packed >> 16) & 0xFF)
    green = int((packed >> 8) & 0xFF)
    blue = int(packed & 0xFF)

    return red, green, blue


class _DepthCameraBase(BaseModel):
    """
    Internal base model shared by all depth camera ontologies.

    Defines the spatial core and common optional channels that every depth
    camera variant exposes, regardless of the underlying acquisition technology.

    **This class is not intended to be instantiated directly**. Use one of the
    concrete subclasses: [`RGBDCamera`][mosaicolabs.models.futures.RGBDCamera], [`ToFCamera`][mosaicolabs.models.futures.ToFCamera], or
    [`StereoCamera`][mosaicolabs.models.futures.StereoCamera].

    Attributes:
        x: Horizontal positions of each point, derived from depth, in meters.
        y: Vertical positions of each point, derived from depth, in meters.
        z: Depth values (distance along the optical axis) of each point, in meters.
        rgb: Packed RGB colour value per point (optional).
        intensity: Signal amplitude or intensity per point (optional).
    """

    x: MosaicoType.list_(MosaicoType.float32) = MosaicoField(
        description="Horizontal position derived from depth."
    )
    """Horizontal position of each point derived from the depth map, in meters."""

    y: MosaicoType.list_(MosaicoType.float32) = MosaicoField(
        description="Vertical position derived from depth."
    )
    """Vertical position of each point derived from the depth map, in meters."""

    z: MosaicoType.list_(MosaicoType.float32) = MosaicoField(
        description="Depth value directly (distance along optical axis)."
    )
    """
    Depth value of each point, in meters.

    Represents the distance along the camera's optical axis (Z-forward convention).
    This is the primary measurement from which ``x`` and ``y`` are projected using
    the sensor's intrinsic parameters.
    """

    rgb: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="Packed RGB color value."
    )
    """
    Packed RGB colour value per point.

    Each element encodes the red, green, and blue channels of the pixel
    co-registered with the corresponding depth sample. The packing convention
    ``rgb`` field (bits 16–23 = R, 8–15 = G, 0–7 = B, stored as a float32 reinterpretation of a uint32).
    There are useful utilities for [`pack_rgb()`][mosaicolabs.models.futures.depth_camera.pack_rgb] and [`unpack_rgb()`][mosaicolabs.models.futures.depth_camera.unpack_rgb] rgb in in the internal of depth camera.
    """

    intensity: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="Signal amplitude/intensity."
    )
    """Signal amplitude or intensity per point."""


class RGBDCamera(_DepthCameraBase, Serializable):
    """
    RGB-D camera ontology.

    This model represents a registered depth-and-colour point cloud produced
    by an RGB-D sensor (e.g. Intel RealSense D-series, Microsoft Azure Kinect).
    Each point carries a 3D position in the camera frame together with an
    optional packed RGB colour value and an optional intensity channel.

    RGB-D sensors typically fuse a structured-light or active-infrared depth
    map with a co-located colour camera, yielding a dense, pixel-aligned point
    cloud at video frame rates.

    Each field is a flat list whose *i*-th element corresponds to the *i*-th
    point in the frame. All lists within a single instance are therefore
    guaranteed to have the same length.

    Attributes:
        x: Horizontal positions of each point, derived from depth, in meters.
        y: Vertical positions of each point, derived from depth, in meters.
        z: Depth values (distance along the optical axis) of each point, in meters.
        rgb: Packed RGB colour value per point (optional).
        intensity: Signal amplitude or intensity per point (optional).

    Note:
        List-typed fields are **not queryable** via the `.Q` proxy. The `.Q` proxy
        is not available on this model.

    Example:
    ```python
            from mosaicolabs import MosaicoClient
            from mosaicolabs.models.futures import RGBDCamera

            with MosaicoClient.connect("localhost", 6726) as client:
                # Fetch all sequences that contain at least one RGB-D camera topic
                sequences = client.get_sequences(ontology=RGBDCamera)

                for sequence in sequences:
                    print(f"Sequence: {sequence.name}")
                    print(f"Topics:   {[topic.name for topic in sequence.topics]}")
    ```
    """

    ...


class ToFCamera(_DepthCameraBase, Serializable):
    """
    Time-of-Flight (ToF) camera ontology.

    This model represents a point cloud produced by a Time-of-Flight sensor
    (e.g. PMD Flexx2, ifm O3R, Sony DepthSense). ToF sensors measure depth by
    emitting amplitude-modulated infrared light and computing the phase shift of
    the returning signal, yielding per-pixel depth, amplitude, and noise
    estimates in a single acquisition.

    In addition to the common spatial and colour channels inherited from
    `_DepthCamera`, this model exposes two ToF-specific fields:
        ``noise``, which quantifies the per-pixel measurement uncertainty, and
        ``grayscale``, which carries the passive greyscale amplitude captured
        alongside the active depth measurement.

    Each field is a flat list whose *i*-th element corresponds to the *i*-th
    pixel in the depth frame (in row-major order). All lists within a single
    instance are therefore guaranteed to have the same length.

    Attributes:
        x: Horizontal positions of each point, derived from depth, in meters.
        y: Vertical positions of each point, derived from depth, in meters.
        z: Depth values (distance along the optical axis) of each point, in meters.
        rgb: Packed RGB colour value per point (optional).
        intensity: Signal amplitude or intensity per point (optional).
        noise: Per-pixel noise estimate of the depth measurement (optional).
        grayscale: Passive greyscale amplitude per pixel (optional).

    Note:
        List-typed fields are **not queryable** via the `.Q` proxy. The `.Q` proxy
        is not available on this model.

    Example:
        ```python
            from mosaicolabs import MosaicoClient
            from mosaicolabs.models.futures import ToFCamera

            with MosaicoClient.connect("localhost", 6726) as client:
                # Fetch all sequences that contain at least one ToF camera topic
                sequences = client.get_sequences(ontology=ToFCamera)

                for sequence in sequences:
                    print(f"Sequence: {sequence.name}")
                    print(f"Topics:   {[topic.name for topic in sequence.topics]}")
        ```
    """

    noise: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="Noise value per pixel."
    )
    """
    Per-pixel noise estimate of the depth measurement.

    High noise values typically indicate low-confidence
    depth samples caused by low signal return, multi-path interference, or
    motion blur, and should be treated with caution during downstream processing.
    """

    grayscale: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="Grayscale amplitude."
    )
    """
    Passive greyscale amplitude per pixel.

    Captured by the sensor's infrared photodiodes independently of the active
    modulation cycle. Provides a texture channel that can be used for feature
    extraction or visual odometry without requiring a separate colour camera.
    """


class StereoCamera(_DepthCameraBase, Serializable):
    """
    Stereo camera ontology.

    This model represents a dense point cloud produced by a passive stereo
    camera system (e.g. Stereolabs ZED, Luxonis OAK-D, Carnegie Robotics
    MultiSense). Depth is estimated by computing the horizontal disparity
    between a rectified left/right image pair and projecting it into 3D space
    using the known baseline and intrinsic parameters.

    In addition to the common spatial and colour channels inherited from
    `_DepthCamera`, this model exposes two stereo-specific fields:
    ``luma``, which carries the luminance of the source rectified image pixel,
    and ``cost``, which encodes the confidence of the disparity estimate at
    each point.

    Each field is a flat list whose *i*-th element corresponds to the *i*-th
    pixel in the disparity map (in row-major order). All lists within a single
    instance are therefore guaranteed to have the same length.

    Attributes:
        x: Horizontal positions of each point in meters.
        y: Vertical positions of each point in meters.
        z: Depth values (distance along the optical axis) of each point, in meters.
        rgb: Packed RGB colour value per point (optional).
        intensity: Signal amplitude or intensity per point (optional).
        luma: Luminance of the corresponding pixel in the rectified image (optional).
        cost: Stereo matching cost per point; lower values indicate higher
            disparity confidence (optional).

    Note:
        List-typed fields are **not queryable** via the `.Q` proxy. The `.Q` proxy
        is not available on this model.

    Example:
    ```python
            from mosaicolabs import MosaicoClient
            from mosaicolabs.models.futures import StereoCamera

            with MosaicoClient.connect("localhost", 6726) as client:
                # Fetch all sequences that contain at least one stereo camera topic
                sequences = client.get_sequences(ontology=StereoCamera)

                for sequence in sequences:
                    print(f"Sequence: {sequence.name}")
                    print(f"Topics:   {[topic.name for topic in sequence.topics]}")
    ```
    """

    luma: Optional[MosaicoType.list_(MosaicoType.uint8)] = MosaicoField(
        default=None,
        description="Luminance of the corresponding pixel in the rectified image.",
    )
    """
        Luminance of the corresponding pixel in the rectified image.
    """

    cost: Optional[MosaicoType.list_(MosaicoType.uint8)] = MosaicoField(
        default=None,
        description="Stereo matching cost (disparity confidence measure, 0 = high confidence).",
    )
    """
        Stereo matching cost per point; lower values indicate higher
        disparity confidence.
    """
