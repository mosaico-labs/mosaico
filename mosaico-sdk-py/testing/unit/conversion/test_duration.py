import pytest

from mosaicolabs import Duration

# -----------------------------------------------------------------------------
# Factory Method Tests
# -----------------------------------------------------------------------------


def test_duration_from_float_basic():
    d = Duration.from_float(10.0)
    assert d.seconds == 10
    assert d.nanoseconds == 0

    d = Duration.from_float(1.5)
    assert d.seconds == 1
    assert d.nanoseconds == 500_000_000

    d = Duration.from_float(0.000000001)  # 1ns
    assert d.seconds == 0
    assert d.nanoseconds == 1


def test_duration_from_float_returns_duration():
    d = Duration.from_float(1.5)
    assert type(d) is Duration


def test_duration_from_milliseconds():
    d = Duration.from_milliseconds(1500)
    assert d.seconds == 1
    assert d.nanoseconds == 500_000_000


def test_duration_from_nanoseconds():
    d = Duration.from_nanoseconds(1_500_000_005)
    assert d.seconds == 1
    assert d.nanoseconds == 500_000_005


# -----------------------------------------------------------------------------
# Converter Method Tests
# -----------------------------------------------------------------------------


def test_duration_to_float():
    d = Duration(seconds=1, nanoseconds=500_000_000)
    assert d.to_float() == 1.5


def test_duration_to_nanoseconds():
    d = Duration(seconds=2, nanoseconds=5)
    assert d.to_nanoseconds() == 2_000_000_005


def test_duration_to_milliseconds():
    d = Duration(seconds=1, nanoseconds=500_000_000)
    assert d.to_milliseconds() == 1500

    d2 = Duration(seconds=0, nanoseconds=1_500_000)  # 1.5ms
    assert d2.to_milliseconds() == 1


# -----------------------------------------------------------------------------
# Validation Tests
# -----------------------------------------------------------------------------


def test_duration_rejects_invalid_nanoseconds():
    with pytest.raises(ValueError, match="Nanoseconds must be in"):
        Duration(seconds=0, nanoseconds=1_000_000_000)

    with pytest.raises(ValueError, match="Nanoseconds must be in"):
        Duration(seconds=0, nanoseconds=-1)
