import logging as log
from typing import List, Tuple

import numpy as np
import pytest

# Import your classes (adjust the import path to match your project structure)
from mosaicolabs.models.sensors import Image, ImageFormat

# import private (not exported) variable for testing purposes
from mosaicolabs.models.sensors.image import _IMG_ENCODING_MAP


def generate_test_data(
    width: int, height: int, encoding: str
) -> Tuple[List[int], int, np.ndarray]:
    """
    Helper to generate random synthetic data mimicking ROS memory layouts.
    Returns:
        Tuple[List[int], int, np.ndarray]: A tuple containing:

        - flat_byte_list: The list[int] expected by Image.encode
        - stride: The calculated stride (row step)
        - original_arr: The numpy source (for debugging if needed)
    """
    dtype, channels, _ = _IMG_ENCODING_MAP[encoding]

    # Generate random data in the semantic type (e.g., float32 or uint16)
    if np.issubdtype(dtype, np.floating):
        # Random floats
        arr = np.random.rand(height, width, channels).astype(dtype)
    else:
        # Random integers (full range of the type)
        info = np.iinfo(dtype)
        arr = np.random.randint(
            info.min, info.max, (height, width, channels), dtype=dtype
        )

    # Reshape to flat C-contiguous memory and Convert to raw bytes list (List[uint8])
    # This simulates how ROS stores data in the 'data' field
    flat_arr = arr.flatten()
    raw_bytes = flat_arr.tobytes()
    byte_list = list(raw_bytes)

    # Calculate Stride (Width * Channels * ItemSize)
    stride = width * channels * np.dtype(dtype).itemsize

    return byte_list, stride, arr


# -----------------------------------------------------------------------------
# Test Cases
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("format", [ImageFormat.PNG, ImageFormat.RAW])
@pytest.mark.parametrize(
    "encoding",
    list(_IMG_ENCODING_MAP.keys()),
)
def test_image_round_trip_integrity(format, encoding):
    """
    Verifies that bytes encoded -> decoded are bit-to-bit identical to the source.
    This proves the "Wide Grayscale" strategy works for floats and 16-bit ints.
    """
    width, height = 640, 480

    log.debug("generating test img data")
    # Generate Input Data
    original_bytes, stride, _ = generate_test_data(width, height, encoding)

    log.debug("encoding test img data")
    # Encode (Compress)
    img_obj = Image.from_linear_pixels(
        data=original_bytes,
        stride=stride,
        height=height,
        width=width,
        encoding=encoding,
        format=format,
    )

    # Sanity Checks on the Object
    assert img_obj.width == width
    assert img_obj.height == height
    assert img_obj.encoding == encoding
    assert img_obj.format == format

    # For PNG, the stored data size should generally be smaller (unless random noise)
    # or at least formatted as a PNG file header.
    if format == ImageFormat.PNG:
        assert img_obj.data.startswith(b"\x89PNG"), "Data is not a valid PNG blob"

    # Decode (Decompress)
    decoded_bytes = img_obj.to_linear_pixels()

    # Assert Equality
    # The length must match exactly
    assert len(decoded_bytes) == len(original_bytes), (
        f"Length mismatch! Orig: {len(original_bytes)}, Decoded: {len(decoded_bytes)}"
    )

    # The content must match exactly
    if format not in [ImageFormat.PNG, ImageFormat.RAW]:
        # the following check will certainly fail for non lossless conversions
        return

    if decoded_bytes != original_bytes:
        pytest.fail(f"Content mismatch for {encoding} in {format} format!")


@pytest.mark.parametrize(
    "format", [f for f in ImageFormat if f not in Image.__supported_image_formats__]
)
@pytest.mark.parametrize(
    "encoding",
    list(_IMG_ENCODING_MAP.keys()),
)
def test_image_invalid_format(format, encoding):
    """
    Verifies that bytes encoded -> decoded are bit-to-bit identical to the source.
    This proves the "Wide Grayscale" strategy works for floats and 16-bit ints.
    """
    width, height = 640, 480

    log.debug("generating test img data")
    # Generate Input Data
    original_bytes, stride, _ = generate_test_data(width, height, encoding)

    log.debug("encoding test img data")
    # Encode (Compress)
    with pytest.raises(ValueError, match=f"Invalid image format '{format}'"):
        _ = Image.from_linear_pixels(
            data=original_bytes,
            stride=stride,
            height=height,
            width=width,
            encoding=encoding,
            format=format,
        )


def test_stride_padding_preservation():
    """
    Tests if the encoder preserves 'padding' bytes at the end of rows.
    Real hardware often aligns rows to 4-byte or 64-byte boundaries, leaving
    junk/padding at the end of every row.
    The encoder MUST save this padding, not discard it based on Width.
    """
    height = 10  # 10 pixels of mono8
    logical_width_bytes = 10  # 10 pixels of mono8
    stride = 12  # 10 bytes of data + 2 bytes of padding per row

    # Create data: [Row Data (10) | Padding (2)] * Height
    total_size = stride * height

    # Fill with specific pattern
    # 0-9 are data, 255 is padding
    original_data = []
    for _ in range(height):
        row = [i for i in range(10)] + [255, 255]
        original_data.extend(row)

    # Encode as PNG (This relies on the encoder using 'stride' for width, not 'width')
    img_obj = Image.from_linear_pixels(
        data=original_data,
        stride=stride,
        height=height,
        width=logical_width_bytes,  # Logical width is smaller than stride
        encoding="mono8",
        format=ImageFormat.PNG,
    )

    # Decode
    decoded_data = img_obj.to_linear_pixels()

    # Assert
    assert len(decoded_data) == total_size
    assert decoded_data == original_data
    # Explicitly check padding bytes exist in result
    assert decoded_data[10] == 255
    assert decoded_data[11] == 255


def test_endianness_metadata_passthrough():
    """
    The Image class is a transport container. It should not modify the
    is_bigendian flag during raw transport logic.
    """
    data = [1, 2, 3, 4]

    # Case 1: Big Endian Source
    img_be = Image.from_linear_pixels(
        data=data, stride=4, height=1, width=1, encoding="32FC1", is_bigendian=True
    )
    assert img_be.is_bigendian is True

    # Case 2: Little Endian Source
    img_le = Image.from_linear_pixels(
        data=data, stride=4, height=1, width=1, encoding="32FC1", is_bigendian=False
    )
    assert img_le.is_bigendian is False


def test_invalid_input_fallback():
    """
    If encoding fails (e.g. dimensions don't match data length),
    it should fallback to RAW without crashing.
    """
    # Create mismatching data
    # Stride=10, Height=10 -> Expected 100 bytes. We provide 5.
    bad_data = [255, 255, 255, 255, 255]

    img_obj = Image.from_linear_pixels(
        data=bad_data,
        stride=10,
        height=10,
        width=10,
        encoding="mono8",
        format=ImageFormat.PNG,
    )

    # It should have gracefully fallen back to RAW
    assert img_obj.format == ImageFormat.RAW
    assert list(img_obj.data) == bad_data


# -----------------------------------------------------------------------------
# PIL Conversion: channel ordering
# -----------------------------------------------------------------------------

# A single pixel with asymmetric channel values (B, G, R, A), so that any wrong
# permutation (e.g. a full reversal) produces a detectable result.
_B, _G, _R, _A = 10, 20, 30, 200


def _single_pixel_image(values: List[int], encoding: str, format: ImageFormat):
    dtype, channels, _ = _IMG_ENCODING_MAP[encoding]
    arr = np.array([[values]], dtype=dtype)
    return Image.from_linear_pixels(
        data=list(arr.tobytes()),
        stride=channels * np.dtype(dtype).itemsize,
        height=1,
        width=1,
        encoding=encoding,
        format=format,
    )


@pytest.mark.parametrize("format", [ImageFormat.PNG, ImageFormat.RAW])
@pytest.mark.parametrize(
    "encoding, values, expected_mode, expected_pixel",
    [
        ("bgr8", [_B, _G, _R], "RGB", (_R, _G, _B)),
        ("bgra8", [_B, _G, _R, _A], "RGBA", (_R, _G, _B, _A)),
        ("rgb8", [_R, _G, _B], "RGB", (_R, _G, _B)),
        ("rgba8", [_R, _G, _B, _A], "RGBA", (_R, _G, _B, _A)),
        # 16-bit: Pillow only keeps the most significant byte
        ("bgr16", [_B << 8, _G << 8, _R << 8], "RGB", (_R, _G, _B)),
        ("bgra16", [_B << 8, _G << 8, _R << 8, _A << 8], "RGBA", (_R, _G, _B, _A)),
        ("rgb16", [_R << 8, _G << 8, _B << 8], "RGB", (_R, _G, _B)),
        ("rgba16", [_R << 8, _G << 8, _B << 8, _A << 8], "RGBA", (_R, _G, _B, _A)),
    ],
)
def test_to_pillow_channel_order(
    format, encoding, values, expected_mode, expected_pixel
):
    """
    Color channels must be swapped for BGR(A) sources, while alpha stays last.
    """
    pil_img = _single_pixel_image(values, encoding, format).to_pillow()

    assert pil_img.mode == expected_mode
    assert pil_img.getpixel((0, 0)) == expected_pixel


@pytest.mark.parametrize("encoding", ["bgra8", "bgra16"])
def test_to_pillow_preserves_alpha(encoding):
    """
    Alpha must be read from the 4th channel of the source, not from the blue one.
    """
    dtype, _, _ = _IMG_ENCODING_MAP[encoding]
    scale = 1 << 8 if dtype == np.uint16 else 1
    # Fully opaque pixel with a blue value that would be misread as alpha
    values = [v * scale for v in (5, 0, 0, 255)]

    pil_img = _single_pixel_image(values, encoding, ImageFormat.RAW).to_pillow()

    assert pil_img.getchannel("A").getpixel((0, 0)) == 255
    assert pil_img.getchannel("B").getpixel((0, 0)) == 5


@pytest.mark.parametrize("encoding", ["bgr8", "bgra8"])
def test_from_pillow_channel_order(encoding):
    """
    RGB(A) -> BGR(A) must swap only the color channels, and to_pillow must
    restore the original PIL image.
    """
    from PIL import Image as PILImage

    rgba = (_R, _G, _B, _A)
    mode = "RGBA" if encoding == "bgra8" else "RGB"
    src = PILImage.new(mode, (1, 1), rgba[: len(mode)])

    img = Image.from_pillow(
        src, target_encoding=encoding, output_format=ImageFormat.RAW
    )

    expected_raw = [_B, _G, _R, _A][: len(mode)]
    assert list(img.to_linear_pixels()) == expected_raw
    assert img.to_pillow().getpixel((0, 0)) == src.getpixel((0, 0))


@pytest.mark.parametrize("format", [ImageFormat.PNG, ImageFormat.RAW])
@pytest.mark.parametrize(
    "encoding, mode",
    [
        ("rgb16", "RGB"),
        ("bgr16", "RGB"),
        ("rgba16", "RGBA"),
        ("bgra16", "RGBA"),
        ("16UC3", "RGB"),
        ("16UC4", "RGBA"),
    ],
)
def test_from_pillow_16bit_scaling_round_trip(format, encoding, mode):
    """
    8-bit PIL color must be expanded to the full 16-bit range, so that
    to_pillow(from_pillow(x)) == x for 16-bit multi-channel encodings.
    """
    from PIL import Image as PILImage

    src = PILImage.new(mode, (1, 1), (_R, _G, _B, _A)[: len(mode)])

    img = Image.from_pillow(src, target_encoding=encoding, output_format=format)

    # Stored values span the full 16-bit range (255 -> 65535)
    stored = np.frombuffer(bytes(img.to_linear_pixels()), dtype=np.uint16)
    if encoding in ("bgr16", "bgra16"):
        expected = [_B, _G, _R, _A]
    else:
        expected = [_R, _G, _B, _A]
    assert stored.tolist() == [v * 257 for v in expected[: len(mode)]]

    assert img.to_pillow().getpixel((0, 0)) == src.getpixel((0, 0))


def test_from_pillow_16bit_full_range():
    """
    The extremes of the 8-bit range map to the extremes of the 16-bit range.
    """
    from PIL import Image as PILImage

    src = PILImage.new("RGBA", (2, 1))
    src.putpixel((0, 0), (0, 0, 0, 0))
    src.putpixel((1, 0), (255, 255, 255, 255))

    img = Image.from_pillow(
        src, target_encoding="rgba16", output_format=ImageFormat.RAW
    )

    stored = np.frombuffer(bytes(img.to_linear_pixels()), dtype=np.uint16)
    assert stored.tolist() == [0] * 4 + [65535] * 4
