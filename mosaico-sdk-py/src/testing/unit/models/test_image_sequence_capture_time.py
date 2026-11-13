"""`ImageSequence` and `ImageSequenceRaw` need a capture time, which orders the frames of
their video: an image whose capture time is not positive is refused, whatever the client."""

import pytest
from pydantic import ValidationError

from mosaicolabs import ImageSequence, ImageSequenceFormat, ImageSequenceRaw


def _image_sequence(capture_timestamp_ns: int) -> ImageSequence:
    return ImageSequence(
        format=ImageSequenceFormat.PNG,
        capture_timestamp_ns=capture_timestamp_ns,
        data=b"\x89PNG\r\n\x1a\n",
    )


def _image_sequence_raw(capture_timestamp_ns: int) -> ImageSequenceRaw:
    return ImageSequenceRaw(
        capture_timestamp_ns=capture_timestamp_ns,
        width=1,
        height=1,
        stride=1,
        encoding="mono8",
        is_bigendian=False,
        data=b"\x00",
    )


@pytest.mark.parametrize("make", [_image_sequence, _image_sequence_raw])
def test_a_positive_capture_time_is_accepted(make):
    assert make(1).capture_timestamp_ns == 1


@pytest.mark.parametrize("make", [_image_sequence, _image_sequence_raw])
@pytest.mark.parametrize("capture_timestamp_ns", [0, -1])
def test_no_capture_time_is_refused(make, capture_timestamp_ns):
    with pytest.raises(ValidationError, match="no capture time"):
        make(capture_timestamp_ns)
