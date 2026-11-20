import numpy as np
import pytest
from PIL import Image as PILImage

# Import your classes
from mosaicolabs.models.sensors import CompressedImage, ImageFormat


# Helper to create a dummy image
def create_test_image(width=100, height=100, color=(255, 0, 0)):
    """Creates a solid color RGB image."""
    img = PILImage.new("RGB", (width, height), color)
    return img


# -----------------------------------------------------------------------------
# Standard Codec Tests (JPEG, PNG) - Real Execution
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "format", [ImageFormat.JPEG, ImageFormat.PNG, ImageFormat.TIFF]
)
def test_compressed_image_round_trip_standard(format):
    """
    Verifies that we can encode -> decode standard formats using the default codec.
    """
    original_img = create_test_image()

    # Encode (from_image)
    compressed_msg = CompressedImage.from_image(image=original_img, format=format)

    assert compressed_msg.format == format
    assert len(compressed_msg.data) > 0

    # Verify the bytes start with correct magic numbers (basic check)
    if format == ImageFormat.PNG:
        assert compressed_msg.data.startswith(b"\x89PNG")
    elif format == ImageFormat.JPEG:
        # JPEG start of image marker FF D8
        assert compressed_msg.data.startswith(b"\xff\xd8")
    elif format == ImageFormat.TIFF:
        assert compressed_msg.data.startswith(
            b"II\x2a\x00"
        ) or compressed_msg.data.startswith(b"MM\x00\x2a")

    # 2. Decode (to_image)
    decoded_img = compressed_msg.to_image()

    assert decoded_img is not None
    assert decoded_img.size == original_img.size
    assert decoded_img.mode == original_img.mode

    # 3. Content Check
    # For lossless (PNG/TIFF), pixels should be identical.
    # For lossy (JPEG), they should be close.
    original_pixels = np.array(original_img)
    decoded_pixels = np.array(decoded_img)

    if format in [ImageFormat.PNG, ImageFormat.TIFF]:
        np.testing.assert_array_equal(original_pixels, decoded_pixels)
    else:
        # Allow small deviation for JPEG compression artifacts
        # Mean Squared Error should be low
        mse = np.mean((original_pixels - decoded_pixels) ** 2)
        assert mse < 10.0, f"JPEG compression artifacting too high (MSE={mse})"


# -----------------------------------------------------------------------------
# Edge Cases & Error Handling
# -----------------------------------------------------------------------------


def test_corrupted_data_decoding():
    """
    If the binary data is garbage, to_image should return None (not crash).
    """
    msg = CompressedImage(
        data=b"garbage_data_not_an_image_12345", format=ImageFormat.JPEG
    )

    # Should log an error but return None safely
    result = msg.to_image()
    assert result is None
