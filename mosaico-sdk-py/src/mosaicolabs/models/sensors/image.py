"""
Image Transport Module.

This module handles the serialization and deserialization of image data.
It provides:
1.  **Raw Image Handling (`Image`)**: Manages uncompressed pixel data with explicit
    control over memory strides, endianness, and pixel formats (encoding).
2.  **Compressed Image Handling (`CompressedImage`)**: Manages encoded data streams
    (JPEG, PNG, H.264) using a pluggable codec architecture.
"""

# TODO: This module needs a deep refactoring:
# - It could be envisioned to collapse Image and CompressedImage in a single type;
# - Using ImageFormat as enum limits the applicability to other formats; the library may not providing codecs
#   for 'all' the formats, but doing so we are limiting the user from providing custom codecs for more clever extensibility;
# - (related to previous) Envision the use of codecs, for 'to_image' conversions

from enum import Enum
import io
import sys
from typing import Dict, List, Optional

# dependencies for video handling
import av
import numpy as np
import pyarrow as pa
from PIL import Image as PILImage

from mosaicolabs.enum import SerializationFormat
from mosaicolabs.logging_config import get_logger

from ..header import Header
from ..mixins import HeaderMixin
from ..serializable import Serializable

# Set the hierarchical logger
logger = get_logger(__name__)


class ImageFormat(str, Enum):
    """Supported containers for image formats."""

    RAW = "raw"
    PNG = "png"
    JPEG = "jpeg"
    TIFF = "tiff"
    H264 = "h264"
    HEVC = "hevc"


# Configuration mapping string encodings (transport layer) to Python types (application layer).
# Format: "encoding_name": (NumPy_Dtype, Channel_Count, PIL_Mode)
_IMG_ENCODING_MAP: dict = {
    # 8-bit Color
    "rgb8": (np.uint8, 3, "RGB"),
    "bgr8": (np.uint8, 3, "RGB"),  # Requires BGR->RGB Swap for PIL
    "rgba8": (np.uint8, 4, "RGBA"),
    "bgra8": (np.uint8, 4, "RGBA"),  # Requires BGRA->RGBA Swap for PIL
    "mono8": (np.uint8, 1, "L"),
    "8UC1": (np.uint8, 1, "L"),
    "8UC3": (np.uint8, 3, "RGB"),
    "8UC4": (np.uint8, 4, "RGBA"),
    # --- Bayer (Treat as Grayscale "L" to preserve raw mosaic pattern) ---
    "bayer_rggb8": (np.uint8, 1, "L"),
    "bayer_bggr8": (np.uint8, 1, "L"),
    "bayer_gbrg8": (np.uint8, 1, "L"),
    "bayer_grbg8": (np.uint8, 1, "L"),
    # --- 16-bit Unsigned (Depth/IR) ---
    "mono16": (np.uint16, 1, "I;16"),
    "rgb16": (np.uint16, 3, "RGB"),
    "bgr16": (np.uint16, 3, "RGB"),
    "rgba16": (np.uint16, 4, "RGBA"),
    "bgra16": (np.uint16, 4, "RGBA"),
    "16UC1": (np.uint16, 1, "I;16"),
    "16UC3": (np.uint16, 3, "RGB"),
    "16UC4": (np.uint16, 4, "RGBA"),
    # --- 16-bit Signed ---
    "16SC1": (np.int16, 1, "I"),
    # --- 32-bit Integer ---
    "32SC1": (np.int32, 1, "I"),
    # --- 32-bit Float ---
    "32FC1": (np.float32, 1, "F"),
    # --- RAW Fallback ---
    "8UC2": (np.uint8, 2, None),
    "16UC2": (np.uint16, 2, None),
    "32FC2": (np.float32, 2, None),
    "32FC3": (np.float32, 3, None),
    "32FC4": (np.float32, 4, None),
    "64FC1": (np.float64, 1, None),
}

_DEFAULT_IMG_FORMAT = ImageFormat.PNG


class Image(Serializable, HeaderMixin):
    """
    Represents raw, uncompressed image data.

    This class provides a flattened, row-major binary representation of an image.
    It is designed to handle:
    1.  **Arbitrary Data Types**: From standard uint8 RGB to float32 Depth and uint16 IR.
    2.  **Memory Layouts**: Explicit control over `stride` (stride) and endianness (`is_bigendian`).
    3.  **Transport**: Can act as a container for RAW bytes or wrap them in lossless containers (PNG).

    Attributes:
        data (bytes): The flattened image memory buffer.
        format (ImageFormat): The format used for serialization ('png' or 'raw').
        width (int): The width of the image in pixels.
        height (int): The height of the image in pixels.
        stride (int): Bytes per row. Essential for alignment.
        encoding (str): Pixel format (e.g., 'bgr8', 'mono16').
        is_bigendian (bool): True if data is Big-Endian. Defaults to system endianness if null.

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter image data based
    on image parameters within a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, Image

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for image data based on image parameters
            qresponse = client.query(
                QueryOntologyCatalog(Image.Q.width.between(1500, 2000))
                .with_expression(Image.Q.height.between(1500, 2000))
                .with_expression(Image.Q.format.eq("png")),
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    # --- Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.binary(),
                nullable=False,
                metadata={"description": "The flattened image memory buffer."},
            ),
            pa.field(
                "format",
                pa.string(),
                nullable=False,
                metadata={"description": "Container format (e.g., 'raw', 'png')."},
            ),
            pa.field(
                "width",
                pa.int32(),
                nullable=False,
                metadata={"description": "Image width in pixels."},
            ),
            pa.field(
                "height",
                pa.int32(),
                nullable=False,
                metadata={"description": "Image height in pixels."},
            ),
            pa.field(
                "stride",
                pa.int32(),
                nullable=False,
                metadata={"description": "Bytes per row. Essential for alignment."},
            ),
            pa.field(
                "encoding",
                pa.string(),
                nullable=False,
                metadata={"description": "Pixel format (e.g., 'bgr8', 'mono16')."},
            ),
            pa.field(
                "is_bigendian",
                pa.bool_(),
                nullable=True,
                metadata={
                    "description": "True if data is Big-Endian. Defaults to system endianness if null."
                },
            ),
        ]
    )

    # Sets the batching strategy to 'Bytes' (instead of 'Count') for better network performance
    __serialization_format__ = SerializationFormat.Image
    __supported_image_formats__ = [ImageFormat.PNG, ImageFormat.RAW]

    # Pydantic Fields
    data: bytes
    """
    The flattened image memory buffer.

    ### Querying with the **`.Q` Proxy**
    The data is not queryable via the `data` field (bytes are not comparable).
    """

    format: ImageFormat
    """
    The format used for serialization ('png' or 'raw').

    ### Querying with the **`.Q` Proxy**
    The format is queryable via the `format` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Image.Q.format` | `String` | `.eq()`, `.neq()`, `.match()`, `.in_()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, Image

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for image data based on format
            qresponse = client.query(
                QueryOntologyCatalog(Image.Q.format.eq(ImageFormat.PNG))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    width: int
    """
    The width of the image in pixels.

    ### Querying with the **`.Q` Proxy**
    The width is queryable via the `width` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Image.Q.width` | `Integer` | `.eq()`, `.neq()`, `.gt()`, `.gte()`, `.lt()`, `.lte()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, Image

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for image data based on width
            qresponse = client.query(
                QueryOntologyCatalog(Image.Q.width.between(0, 100))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    height: int
    """
    The height of the image in pixels.

    ### Querying with the **`.Q` Proxy**
    The height is queryable via the `height` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Image.Q.height` | `Integer` | `.eq()`, `.neq()`, `.gt()`, `.gte()`, `.lt()`, `.lte()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, Image

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for image data based on height
            qresponse = client.query(
                QueryOntologyCatalog(Image.Q.height.between(0, 100))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    stride: int
    """
    The number of bytes per row of the image.

    ### Querying with the **`.Q` Proxy**
    The stride is queryable via the `stride` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Image.Q.stride` | `Integer` | `.eq()`, `.neq()`, `.gt()`, `.gte()`, `.lt()`, `.lte()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, Image

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for image data based on stride
            qresponse = client.query(
                QueryOntologyCatalog(Image.Q.stride.between(0, 100))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    encoding: str
    """
    The pixel encoding (e.g., 'bgr8', 'mono16'). Optional field.

    ### Querying with the **`.Q` Proxy**
    The encoding is queryable via the `encoding` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Image.Q.encoding` | `String` | `.eq()`, `.neq()`, `.match()`, `.in_()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, Image

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for image data based on encoding
            qresponse = client.query(
                QueryOntologyCatalog(Image.Q.encoding.eq("bgr8"))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    is_bigendian: Optional[bool] = None
    """
    Store if the original data is Big-Endian. Optional field.

    ### Querying with the **`.Q` Proxy**
    The is_bigendian is queryable via the `is_bigendian` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Image.Q.is_bigendian` | `Boolean` | `.eq()`, `.neq()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, Image

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for image data based on is_bigendian
            qresponse = client.query(
                QueryOntologyCatalog(Image.Q.is_bigendian.eq(True))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    @classmethod
    def from_linear_pixels(
        cls,
        data: List[int],
        stride: int,
        height: int,
        width: int,
        encoding: str,
        header: Optional[Header] = None,
        is_bigendian: Optional[bool] = None,
        format: Optional[ImageFormat] = _DEFAULT_IMG_FORMAT,
    ) -> "Image":
        """
        Encodes linear pixel uint8 data into the storage container.

        **The "Wide Grayscale" Trick:**
        When saving complex types (like `float32` depth or `uint16` raw) into standard
        image containers like PNG, we cannot rely on standard RGB encoders as they might
        apply color corrections or bit-depth reductions.

        Instead, this method treats the data as a raw byte stream. It reshapes the
        stream into a 2D "Grayscale" image where:
        - `Image_Height` = `Original_Height`
        - `Image_Width` = `Stride` (The full row stride in bytes)

        This guarantees that every bit of the original memory (including padding) is
        preserved losslessly.

        Args:
            data (List[int]): Flattened list of bytes (uint8).
            stride (int): Row stride in bytes.
            height (int): Image height.
            width (int): Image width.
            encoding (str): Pixel format string.
            format (ImageFormat): Target container ('raw' or 'png').

        Returns:
            Image: An instantiated object.
        """
        if not format:
            format = _DEFAULT_IMG_FORMAT

        if format not in cls.__supported_image_formats__:
            raise ValueError(
                f"Invalid image format '{format}'. Supported formats {cls.__supported_image_formats__}"
            )

        raw_bytes = bytes(data)

        if format == ImageFormat.RAW:
            img_bytes = raw_bytes
        else:
            try:
                # View as uint8
                arr_uint8 = np.frombuffer(raw_bytes, dtype=np.uint8)

                # Reshape based on physical memory layout (Height x Stride)
                # ignoring logical width to preserve padding.
                matrix_shape = (height, stride)
                arr_reshaped = arr_uint8.reshape(matrix_shape)

                # Save as Mode 'L' (8-bit grayscale)
                pil_image = PILImage.fromarray(
                    arr_reshaped
                )  # avoid mode ='L' because is deprecated
                buf = io.BytesIO()
                pil_image.save(buf, format=format.value.upper())
                img_bytes = buf.getvalue()

            except Exception as e:
                logger.error(f"Encoding failed ('{e}'). Falling back to RAW.")
                img_bytes = raw_bytes
                format = ImageFormat.RAW

        return cls(
            header=header,
            data=img_bytes,
            format=format,
            width=width,
            height=height,
            stride=stride,
            is_bigendian=is_bigendian,
            encoding=encoding,
        )

    def to_linear_pixels(self) -> List[int]:
        """
        Decodes the storage container back to a linear byte list.

        Reverses the "Wide Grayscale" encoding to return the original,
        flattened memory buffer.

        Returns:
            List[int]: A list of uint8 integers representing the raw memory.
        """
        if self.format == ImageFormat.RAW:
            return list(self.data)

        try:
            # PIL reads the header to get dimensions (Height, Step)
            with PILImage.open(io.BytesIO(self.data)) as img:
                arr_uint8 = np.array(img)

            # Flatten back to 1D
            raw_bytes = arr_uint8.tobytes()
            return list(raw_bytes)

        except Exception:
            return list(self.data)

    def to_pillow(self) -> PILImage.Image:
        """
        Converts the raw binary data into a standard PIL Image.

        This method performs the heavy lifting of interpretation:
        1.  **Decoding**: Unpacks the transport container (e.g., PNG -> bytes).
        2.  **Casting**: Interprets bytes according to `self.encoding` (e.g., as float32).
        3.  **Endianness**: Swaps bytes if the source endianness differs from the local CPU.
        4.  **Color Swap**: Converts BGR (common in OpenCV/Robotics) to RGB (required by PIL).

        Returns:
            PILImage.Image: A visualizable image object.

        Raises:
            NotImplementedError: If the encoding is unknown.
            ValueError: If data size doesn't match dimensions.
        """
        if self.encoding not in _IMG_ENCODING_MAP:
            raise NotImplementedError(
                f"Encoding '{self.encoding}' not supported for PIL conversion."
            )

        dtype, channels, mode = _IMG_ENCODING_MAP[self.encoding]
        decoded_uint8_list = self.to_linear_pixels()
        raw_bytes = bytes(decoded_uint8_list)

        # Attempt to interpret raw_bytes with the given dtype. If any error
        # (like the data cannot be evenly divided into the required number of elements)
        # convert to uint8 and then interpret (view) as dtype
        try:
            arr = np.frombuffer(raw_bytes, dtype=dtype)
        except ValueError:
            # here we only need memory contiguity
            arr = np.frombuffer(raw_bytes, dtype=np.uint8).view(dtype)

        # Handle Endianness
        system_is_little = sys.byteorder == "little"
        source_is_big = (
            self.is_bigendian
            if self.is_bigendian is not None
            else (not system_is_little)
        )

        if (source_is_big and system_is_little) or (
            not source_is_big and not system_is_little
        ):
            arr = arr.byteswap()

        # Reshape and Validate
        expected_items = self.width * self.height * channels
        if arr.size != expected_items:
            # Handle strided padding by truncation
            if arr.size > expected_items:
                arr = arr[:expected_items]
            else:
                raise ValueError(
                    f"Data size mismatch. Expected {expected_items}, got {arr.size}"
                )

        shape = (
            (self.height, self.width, channels)
            if channels > 1
            else (self.height, self.width)
        )
        arr = arr.reshape(shape)

        # Handle BGR -> RGB
        if self.encoding in ["bgr8", "bgra8", "bgr16", "bgra16"]:
            arr = arr[..., ::-1]

        return PILImage.fromarray(arr, mode=mode)

    @classmethod
    def from_pillow(
        cls,
        pil_image: PILImage.Image,
        header: Optional[Header] = None,
        target_encoding: Optional[str] = None,
        output_format: Optional[ImageFormat] = None,
    ) -> "Image":
        """
        Factory method to create an Image from a PIL object.

        Automatically handles:

        - Data flattening (row-major).
        - Stride calculation.
        - RGB to BGR conversion (if target_encoding requires it).
        - Type casting (e.g., float -> uint8).

        Args:
            pil_image (PILImage.Image): Source image.
            header (Optional[Header]): Metadata.
            target_encoding (Optional[str]): Target pixel format (e.g., "bgr8").
            output_format (Optional[ImageFormat]): ('raw' or 'png').

        Returns:
            Image: Populated data object.
        """

        if output_format not in cls.__supported_image_formats__:
            raise ValueError(
                f"Invalid image format '{output_format}'. Supported formats {cls.__supported_image_formats__}"
            )

        arr = np.array(pil_image)

        # Default encoding inference
        if target_encoding is None:
            mode_map = {
                "L": "mono8",
                "RGB": "rgb8",
                "RGBA": "rgba8",
                "F": "32FC1",
                "I;16": "mono16",
            }
            target_encoding = mode_map.get(pil_image.mode, "rgb8")

        expected_dtype, _, _ = _IMG_ENCODING_MAP.get(
            target_encoding, (np.uint8, 1, None)
        )

        # Enforce Type
        if arr.dtype != expected_dtype:
            arr = arr.astype(expected_dtype)

        # Handle RGB -> BGR
        if target_encoding in ["bgr8", "bgra8", "bgr16", "bgra16"]:
            if arr.ndim == 3:
                arr = arr[..., ::-1]

        # Ensure contiguous memory for correct stride calc
        arr = np.ascontiguousarray(arr)

        raw_bytes = arr.tobytes()
        data_list = list(raw_bytes)
        stride = arr.strides[0]
        height, width = arr.shape[:2]

        return cls.from_linear_pixels(
            data=data_list,
            stride=stride,
            width=width,
            format=output_format,
            encoding=target_encoding,
            height=height,
            is_bigendian=sys.byteorder == "big",
            header=header,
        )


class StatefulDecodingSession:
    """
    Manages the stateful decoding of video streams for a specific reading session.

    NOTE: The image formats supported are: [h264 and hevc]
    """

    __suppported_formats__ = [ImageFormat.H264, ImageFormat.HEVC]

    def __init__(self):
        # Key: topic_name (str) -> Value: av.CodecContext
        self._decoders: Dict[str, av.CodecContext] = {}

    def decode(
        self,
        img_data: bytes,
        format: ImageFormat,
        context: str,
    ) -> Optional[PILImage.Image]:
        """
        Decodes a CompressedImage message into a PIL Image using the
        persistent state associated with 'topic_name'.
        """
        if format not in self.__suppported_formats__:
            logger.error(
                f"Input format '{format.value}' not among the supported formats: {[f"'{fmt.value}'" for fmt in self.__suppported_formats__]}"
            )
            return None

        return self._decode_video_frame(img_data, format, context)

    def _decode_video_frame(
        self,
        img_data: bytes,
        format: ImageFormat,
        context: str,
    ) -> Optional[PILImage.Image]:
        # Lazy initialization of the decoder for this specific topic
        if context not in self._decoders:
            try:
                self._decoders[context] = av.CodecContext.create(format, "r")
                logger.debug(f"Created new decoder context for context: '{context}'")
            except Exception as e:
                logger.error(f"Failed to create decoder for context '{context}': '{e}'")
                return None

        decoder = self._decoders[context]

        try:
            packet = av.Packet(img_data)
            frames: List[av.VideoFrame] = decoder.decode(packet)

            # Return the first available frame
            if frames:
                return frames[0].to_image()  # PyAV >= 0.5.0 supports .to_image() (PIL)

        except Exception as e:
            logger.warning(f"Decoding error on '{context}': '{e}'")
            # Optional: Implement reset logic here if the stream is truly corrupted

        return None

    def close(self):
        """Explicitly release resources (optional, GC usually handles this)."""
        self._decoders.clear()


class _StatelessDefaultCodec:
    """
    Standard codec implementation using the Pillow (PIL) library.

    Does not make any check on format values: if encoding/deconding fails,
    the function returns None
    """

    def decode(
        self, data_bytes: bytes, format: ImageFormat
    ) -> Optional[PILImage.Image]:
        """Decodes bytes using PIL.Image.open."""
        try:
            image = PILImage.open(io.BytesIO(data_bytes))
            image.load()
            return image
        except Exception as e:
            logger.error(f"_DefaultCodec decode error: '{e}'")
            return None

    def encode(
        self, image: PILImage.Image, format: ImageFormat, **kwargs
    ) -> Optional[bytes]:
        """Encodes image using PIL.Image.save."""
        buf = io.BytesIO()
        try:
            image.save(buf, format=format.value.upper(), **kwargs)
            return buf.getvalue()
        except Exception as e:
            logger.error(f"_DefaultCodec encode error: '{e}'")
            return None


# --- Data Structure ---


class CompressedImage(Serializable, HeaderMixin):
    """
    Represents image data stored as a compressed binary blob (e.g. JPEG, PNG, H264, ...).

    This class acts as a data container. It delegates the complex logic of
    decoding (bytes -> Image) and encoding (Image -> bytes) to the registered
    codecs in `_IMG_CODECS_FACTORY`.

    Attributes:
        data (bytes): The compressed binary payload.
        format (str): The format identifier string (e.g., 'jpeg', 'png').

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter image data based
    on image parameters within a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, CompressedImage, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for image data based on image parameters
            qresponse = client.query(
                QueryOntologyCatalog(CompressedImage.Q.format.eq("jpeg"))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.binary(),
                nullable=False,
                metadata={
                    "description": "The serialized (compressed) image data as bytes."
                },
            ),
            pa.field(
                "format",
                pa.string(),
                nullable=False,
                metadata={
                    "description": "The compression format (e.g., 'jpeg', 'png')."
                },
            ),
        ]
    )

    __serialization_format__ = SerializationFormat.Image

    data: bytes
    """
    The serialized (compressed) image data as bytes.

    ### Querying with the **`.Q` Proxy**
    The data is not queryable via the `data` field (bytes are not comparable).
    """

    format: ImageFormat
    """
    The compression format (e.g., 'jpeg', 'png').

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `CompressedImage.Q.format` | `String` | `.eq()`, `.neq()`, `.match()`, `.in_()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, CompressedImage, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for image data based on image parameters
            qresponse = client.query(
                QueryOntologyCatalog(CompressedImage.Q.format.eq("jpeg"))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    def to_image(
        self,
        # TODO: enable param when allowing generic formats (not via Enum)
        # codec: Optional[Any] = None,
    ) -> Optional[PILImage.Image]:
        """
        Decompresses the stored binary data into a usable PIL Image object.

        NOTE: The function use the _DefaultCodec which is valid for stateless formats
        only ('png', 'jpeg', ...). If dealing with a stateful compressed image,
        the conversion must be made via explicit instantiation of a StatefulDecodingSession
        class.

        Returns:
            PILImage.Image: A ready-to-use Pillow image object.
            None: If the data is empty or decoding fails.
        """
        if not self.data:
            return None
        _codec = _StatelessDefaultCodec()
        return _codec.decode(self.data, self.format)

    @classmethod
    def from_image(
        cls,
        image: PILImage.Image,
        format: ImageFormat = ImageFormat.PNG,
        header: Optional[Header] = None,
        # TODO: enable param when allowing generic formats (not via Enum)
        # codec: Optional[CompressedImageCodec] = None,
        **kwargs,
    ) -> "CompressedImage":
        """
        Factory method to create a CompressedImage from a PIL Image.

        NOTE: The function use the _DefaultCodec which is valid for stateless formats
        only ('png', 'jpeg', ...). If dealing with a stateful compressed image,
        the conversion must be made via user defined encoding algorithms.

        Args:
            image: The source Pillow image.
            format: The target compression format (default: 'jpeg').
            header: Optional Header metadata.
            **kwargs: Additional arguments passed to the codec's encode method
                      (e.g., quality=90).

        Returns:
            CompressedImage: A new instance containing the compressed bytes.

        Raises:
            ValueError: If no codec is found or encoding fails.
        """
        fmt_lower = format.value.lower()
        _codec = _StatelessDefaultCodec()
        compressed_bytes = _codec.encode(image, format, **kwargs)
        if compressed_bytes is None:
            raise RuntimeError(
                f"Failed to create CompressedImage (format: '{fmt_lower}')"
            )
        return cls(data=compressed_bytes, format=format, header=header)
