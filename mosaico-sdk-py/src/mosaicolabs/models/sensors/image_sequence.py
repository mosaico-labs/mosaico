"""
Image Sequence Ontology Module.

Defines the data structures for an image of a sequence, with the time it was
captured:

- [`ImageSequence`][mosaicolabs.models.sensors.ImageSequence]: a PNG, compressed
  losslessly, stored with the `ImageSequence` serialization format. Lossy
  formats, such as JPEG, are not supported.
- [`ImageSequenceRaw`][mosaicolabs.models.sensors.ImageSequenceRaw]: raw pixels,
  as a camera driver writes them (e.g. a ROS `sensor_msgs/Image`), stored with
  the `ImageSequenceRaw` serialization format.

Both keep, optionally, what the ROS header of the image has besides its
capture time: its `frame_id` and `sample_counter` (`seq`, ROS 1 only).

The server turns the images of a topic of either into a lossless video, and
reads them back as they were sent: raw images byte for byte, PNGs pixel for
pixel.
"""

from enum import Enum
from typing import TYPE_CHECKING, Optional

from pydantic import field_validator

from mosaicolabs.enum import SerializationFormat

from ..core import MosaicoField, MosaicoType, Serializable

if TYPE_CHECKING:
    from PIL import Image as PILImage


def _check_capture_timestamp_ns(capture_timestamp_ns: int) -> int:
    """
    Refuses an image with no capture time: the server orders the frames of the video by
    it, so it must be a positive time. A ROS image whose stamp is zero has none.
    """
    if capture_timestamp_ns <= 0:
        raise ValueError(
            f"no capture time: capture_timestamp_ns must be positive, got "
            f"{capture_timestamp_ns}"
        )
    return capture_timestamp_ns


class ImageSequenceFormat(str, Enum):
    """
    Defines the encodings an [`ImageSequence`][mosaicolabs.models.sensors.ImageSequence] can hold.

    Only lossless compressed encodings are supported: lossy ones, such as
    JPEG, are not.
    """

    PNG = "png"
    """
    Portable Network Graphics, always lossless: grayscale, grayscale with
    alpha, RGB or RGBA, with 8 or 16 bits per sample.
    """


class ImageSequence(Serializable):
    """
    Represents an image of a sequence, a PNG, compressed losslessly, with the
    time it was captured. Lossy formats, such as JPEG, are not supported.

    All the images of a topic must be valid PNGs. The server keeps a topic whose
    images are not as it was sent, and writes the issue in an `error`
    notification on it: `store optimizer/<issue>: <details>`.

    The capture time is the time the camera took the image, while the
    `timestamp_ns` of the [`Message`][mosaicolabs.models.core.Message]
    wrapping it is the time the image was recorded.

    The class uses the `ImageSequence` serialization format: the server
    compresses `capture_timestamp_ns` and `format`, and keeps statistics on
    them for filtering, and keeps `data`, already compressed, without
    statistics.
    The format relies on these field names, so they must not be renamed.

    Attributes:
        format (ImageSequenceFormat): The image encoding ('png').
        capture_timestamp_ns (int): The capture time, in nanoseconds since the Unix epoch.
        frame_id (Optional[str]): The coordinate frame of the camera, as in the ROS header.
        sample_counter (Optional[int]): The sequence number of the image, as in the ROS 1 header (`seq`).
        data (bytes): The encoded image.

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**, except for `data`
    (bytes are not comparable).

    Example:
        ```python
        from mosaicolabs import MosaicoClient, ImageSequence, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for PNG images captured within a time range
            qresponse = client.query(
                QueryOntologyCatalog(ImageSequence.Q.format.eq("png"))
                .with_expression(
                    ImageSequence.Q.capture_timestamp_ns.between(1700000000, 1800000000)
                ),
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    __serialization_format__ = SerializationFormat.ImageSequence

    format: ImageSequenceFormat = MosaicoField(
        description="The image encoding ('png')."
    )
    """
    The image encoding ('png').

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequence.Q.format` | `String` | `.eq()`, `.match()`, `.in_()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.between()`, `.outside()` |
    """

    capture_timestamp_ns: MosaicoType.int64 = MosaicoField(
        description="Capture time, in nanoseconds since the Unix epoch."
    )
    """
    The capture time, in nanoseconds since the Unix epoch.

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequence.Q.capture_timestamp_ns` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    """

    @field_validator("capture_timestamp_ns")
    @classmethod
    def _positive_capture_timestamp_ns(cls, capture_timestamp_ns: int) -> int:
        return _check_capture_timestamp_ns(capture_timestamp_ns)

    frame_id: Optional[MosaicoType.string] = MosaicoField(
        default=None,
        description="Coordinate frame of the camera, as in the ROS header.",
    )
    """
    The coordinate frame of the camera, as in the ROS header (e.g. 'camera_optical_frame').

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequence.Q.frame_id` | `String` | `.eq()`, `.match()`, `.in_()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.between()`, `.outside()` |
    """

    sample_counter: Optional[MosaicoType.uint64] = MosaicoField(
        default=None,
        description="Sequence number of the image, as in the ROS 1 header (`seq`).",
    )
    """
    The sequence number of the image, as in the ROS 1 header (`seq`).

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequence.Q.sample_counter` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    """

    data: MosaicoType.binary = MosaicoField(description="The encoded image.")
    """
    The encoded image.

    ### Querying with the **`.Q` Proxy**
    The data is not queryable via the `data` field (bytes are not comparable).
    """


class ImageSequenceRaw(Serializable):
    """
    Represents an image of a sequence as raw pixels, as a camera driver writes
    them (e.g. a ROS `sensor_msgs/Image`), with the time it was captured.

    It replaces [`Image`][mosaicolabs.models.sensors.Image] for raw images,
    which is deprecated.

    The server turns the images of a topic into a lossless video, and reads
    them back byte for byte. These encodings can be a video: `mono8`, `mono16`,
    `8UC1`, `16UC1`, `16SC1`, `rgb8`, `bgr8`, `rgba8`, `bgra8`, `8UC3`,
    `8UC4`, `rgb16`, `bgr16`, `rgba16`, `bgra16`, `16UC3`, `16UC4` and the
    Bayer ones (`bayer_rggb8`, `bayer_grbg16`...). The images of a topic of
    another encoding (e.g. `32FC1` depth images, `yuv422`), or with row padding
    other than zeros, are kept as they were sent, with an `unsupported`
    notification on the topic: `store optimizer/<issue>: <details>`.

    The capture time is the time the camera took the image, while the
    `timestamp_ns` of the [`Message`][mosaicolabs.models.core.Message]
    wrapping it is the time the image was recorded.

    The class uses the `ImageSequenceRaw` serialization format, which relies on
    these field names: they must not be renamed.

    Attributes:
        capture_timestamp_ns (int): The capture time, in nanoseconds since the Unix epoch.
        width (int): The width of the image, in pixels.
        height (int): The height of the image, in pixels.
        stride (int): The bytes of a row, its padding included.
        encoding (str): The pixel encoding, as in ROS (e.g. 'bgr8', 'mono16').
        is_bigendian (Optional[bool]): Whether the samples of 16 bits are big-endian: little-endian if null.
        frame_id (Optional[str]): The coordinate frame of the camera, as in the ROS header.
        sample_counter (Optional[int]): The sequence number of the image, as in the ROS 1 header (`seq`).
        data (bytes): The pixels, `stride * height` bytes.

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**, except for `data`
    (bytes are not comparable).

    Example:
        ```python
        from mosaicolabs import MosaicoClient, ImageSequenceRaw, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for large BGR images
            qresponse = client.query(
                QueryOntologyCatalog(ImageSequenceRaw.Q.encoding.eq("bgr8"))
                .with_expression(ImageSequenceRaw.Q.width.geq(1920)),
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    __serialization_format__ = SerializationFormat.ImageSequenceRaw

    # FIXME: evaluate whether the client must check the images it writes. Nothing checks
    # client side that the fields describe the bytes: `data` can differ from `stride` ×
    # `height` bytes, `stride` can be smaller than a row of pixels, and `encoding` can be
    # unknown, or not the one of the bytes. Raw images do not only come from ROS, so any
    # client can write them. The store optimizer finds some of these issues only later, and
    # the export to ROS writes the fields back as they are. These checks are complex on
    # images (each encoding has its own bytes per pixel), so they need their own design.

    capture_timestamp_ns: MosaicoType.int64 = MosaicoField(
        description="Capture time, in nanoseconds since the Unix epoch."
    )
    """
    The capture time, in nanoseconds since the Unix epoch.

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequenceRaw.Q.capture_timestamp_ns` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    """

    @field_validator("capture_timestamp_ns")
    @classmethod
    def _positive_capture_timestamp_ns(cls, capture_timestamp_ns: int) -> int:
        return _check_capture_timestamp_ns(capture_timestamp_ns)

    width: MosaicoType.uint32 = MosaicoField(description="Image width in pixels.")
    """
    The width of the image, in pixels.

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequenceRaw.Q.width` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    """

    height: MosaicoType.uint32 = MosaicoField(description="Image height in pixels.")
    """
    The height of the image, in pixels.

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequenceRaw.Q.height` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    """

    stride: MosaicoType.uint32 = MosaicoField(
        description="Bytes per row, its padding included."
    )
    """
    The bytes of a row, its padding included.

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequenceRaw.Q.stride` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    """

    encoding: MosaicoType.string = MosaicoField(
        description="Pixel encoding, as in ROS (e.g. 'bgr8', 'mono16')."
    )
    """
    The pixel encoding, as in ROS (e.g. 'bgr8', 'mono16').

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequenceRaw.Q.encoding` | `String` | `.eq()`, `.match()`, `.in_()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.between()`, `.outside()` |
    """

    is_bigendian: Optional[MosaicoType.bool] = MosaicoField(
        default=None,
        description="True if the samples of 16 bits are big-endian: little-endian if null.",
    )
    """
    Whether the samples of 16 bits are big-endian: little-endian if null.

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequenceRaw.Q.is_bigendian` | `Boolean` | `.eq()`, `.neq()` |
    """

    frame_id: Optional[MosaicoType.string] = MosaicoField(
        default=None,
        description="Coordinate frame of the camera, as in the ROS header.",
    )
    """
    The coordinate frame of the camera, as in the ROS header (e.g. 'camera_optical_frame').

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequenceRaw.Q.frame_id` | `String` | `.eq()`, `.match()`, `.in_()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.between()`, `.outside()` |
    """

    sample_counter: Optional[MosaicoType.uint64] = MosaicoField(
        default=None,
        description="Sequence number of the image, as in the ROS 1 header (`seq`).",
    )
    """
    The sequence number of the image, as in the ROS 1 header (`seq`).

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequenceRaw.Q.sample_counter` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    """

    data: MosaicoType.binary = MosaicoField(
        description="The pixels, `stride * height` bytes."
    )
    """
    The pixels, `stride * height` bytes.

    ### Querying with the **`.Q` Proxy**
    The data is not queryable via the `data` field (bytes are not comparable).
    """

    def to_pillow(self) -> "PILImage.Image":
        """
        Converts the pixels into a PIL Image, to look at them: BGR(A) is
        swapped into RGB(A), and the samples of 16 bits of color images are
        reduced to 8 bits, which Pillow needs.

        Returns:
            PILImage.Image: A visualizable image object.

        Raises:
            NotImplementedError: If the encoding is not supported by Pillow.
            ValueError: If the data size doesn't match the size of the image.
        """
        from .image import Image, ImageFormat

        # The deprecated `Image` reads raw pixels into Pillow: built without its validation.
        return Image.model_construct(
            data=self.data,
            format=ImageFormat.RAW,
            width=self.width,
            height=self.height,
            stride=self.stride,
            encoding=self.encoding,
            is_bigendian=bool(self.is_bigendian),
        ).to_pillow()
