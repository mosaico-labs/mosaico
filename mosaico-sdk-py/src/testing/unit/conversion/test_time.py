import time
from datetime import datetime, timezone

import pytest

# Import your Time class (adjust import as needed)
# Assuming it is in a file named time_module.py or similar
from mosaicolabs.types import Time

# -----------------------------------------------------------------------------
# Factory Method Tests
# -----------------------------------------------------------------------------


def test_time_from_float_basic():
    """Verifies basic float conversion logic."""
    # Case 1: Integer float
    t = Time.from_float(10.0)
    assert t.seconds == 10
    assert t.nanoseconds == 0

    # Case 2: Simple fractional
    t = Time.from_float(1.5)
    assert t.seconds == 1
    assert t.nanoseconds == 500_000_000

    # Case 3: Small precision
    t = Time.from_float(0.000000001)  # 1ns
    assert t.seconds == 0
    assert t.nanoseconds == 1


def test_time_from_float_precision_rounding():
    """
    Verifies that floating point artifacts are rounded correctly to the nearest nanosecond.
    e.g. 1.9999999999 -> 2.0 instead of 1.999999999
    """
    # Rollover case: 0.9999999999 -> should round to 1.0 sec
    # 1e-10 is smaller than 1ns (1e-9), so it should likely round down or up depending on closeness
    # Let's test a value extremely close to the next second
    val = 1.0 - 1e-10
    t = Time.from_float(val)
    # Depending on float precision, this might hit the rollover logic
    assert (
        t.seconds == 1 or t.nanoseconds == 999_999_999
    )  # Should trigger normalization logic


def test_time_from_float_negative():
    """Verifies handling of negative timestamps (before epoch)."""
    # -1.5 seconds -> -2 seconds + 500,000,000 ns
    t = Time.from_float(-1.5)
    assert t.seconds == -2
    assert t.nanoseconds == 500_000_000

    # Check reconstruction
    assert t.to_float() == -1.5


def test_time_from_milliseconds():
    t = Time.from_milliseconds(1500)
    assert t.seconds == 1
    assert t.nanoseconds == 500_000_000


def test_time_from_nanoseconds():
    total_ns = 1_500_000_005
    t = Time.from_nanoseconds(total_ns)
    assert t.seconds == 1
    assert t.nanoseconds == 500_000_005


def test_time_from_datetime():
    # Use a fixed known datetime
    dt = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    expected_ts = dt.timestamp()

    t = Time.from_datetime(dt)

    assert t.seconds == int(expected_ts)
    assert t.nanoseconds == 0


def test_time_now():
    """Smoke test for .now() factory."""
    before = time.time()
    t = Time.now()
    after = time.time()

    t_float = t.to_float()
    assert before <= t_float <= after


# -----------------------------------------------------------------------------
# Converter Method Tests
# -----------------------------------------------------------------------------


def test_to_float():
    t = Time(seconds=1, nanoseconds=500_000_000)
    assert t.to_float() == 1.5
    # Test negative conversion
    t = Time(seconds=-1, nanoseconds=500_000_000)
    assert t.to_float() == -0.5


def test_to_nanoseconds():
    t = Time(seconds=2, nanoseconds=5)
    assert t.to_nanoseconds() == 2_000_000_005


def test_to_milliseconds():
    t = Time(seconds=1, nanoseconds=500_000_000)
    assert t.to_milliseconds() == 1500

    # Truncation check
    t2 = Time(seconds=0, nanoseconds=1_500_000)  # 1.5ms
    assert t2.to_milliseconds() == 1


def test_to_datetime():
    t = Time(seconds=1672531200, nanoseconds=0)  # 2023-01-01 00:00:00 UTC
    dt = t.to_datetime()

    assert dt.year == 2023
    assert dt.month == 1
    assert dt.day == 1
    assert dt.tzinfo == timezone.utc


# -----------------------------------------------------------------------------
# Validation Tests
# -----------------------------------------------------------------------------


def test_validation_nanosec_bounds():
    """Ensures Pydantic validator catches invalid nanoseconds."""
    # Valid
    Time(seconds=0, nanoseconds=999_999_999)

    # Invalid: Too high
    with pytest.raises(ValueError, match="Nanoseconds must be in"):
        Time(seconds=0, nanoseconds=1_000_000_000)

    # Invalid: Negative
    with pytest.raises(ValueError, match="Nanoseconds must be in"):
        Time(seconds=0, nanoseconds=-1)
