from mosaicolabs import ImageSequenceRaw


def test_to_pillow_swaps_bgr_into_rgb():
    image = ImageSequenceRaw(
        capture_timestamp_ns=1,
        width=2,
        height=1,
        stride=6,
        encoding="bgr8",
        data=bytes([1, 2, 3, 4, 5, 6]),
    )
    pillow = image.to_pillow()

    assert (pillow.mode, pillow.size) == ("RGB", (2, 1))
    assert [pillow.getpixel((x, 0)) for x in range(2)] == [(3, 2, 1), (6, 5, 4)]


def test_to_pillow_reads_16_bits_in_their_byte_order():
    image = ImageSequenceRaw(
        capture_timestamp_ns=1,
        width=1,
        height=1,
        stride=2,
        encoding="mono16",
        is_bigendian=True,
        data=bytes([1, 2]),
    )

    assert image.to_pillow().getpixel((0, 0)) == 0x0102
