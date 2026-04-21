import pytest

from mosaicolabs.ros_bridge.loader import _filter_topics


@pytest.fixture
def available_topics():
    # Mokup for topics in a rosbag
    return {
        "/gps/a": object(),
        "/gps/b": object(),
        "/gps/vendor/time_reference": object(),
        "/cam/front/image": object(),
        "/cam/front/camera_info": object(),
    }


def test_no_patterns_returns_all(available_topics):
    """Verify that when no patterns are provided, all available topics are returned unchanged."""
    requested_topics = None

    result = _filter_topics(available_topics, requested_topics)

    assert set(result.keys()) == set(available_topics.keys())

    requested_topics = []

    result = _filter_topics(available_topics, requested_topics)

    assert set(result.keys()) == set(available_topics.keys())


def test_unmatched_patterns_returns_empty(available_topics):
    """Verify that when no patterns are provided, all available topics are returned unchanged."""
    requested_topics = ["unmatched/topic*"]

    result = _filter_topics(available_topics, requested_topics)

    assert not result


def test_exclude_all_returns_empty(available_topics):
    """Verify that when no patterns are provided, all available topics are returned unchanged."""
    requested_topics = ["!*"]

    result = _filter_topics(available_topics, requested_topics)

    assert not result


def test_include_all_returns_all(available_topics):
    """Verify that when no patterns are provided, all available topics are returned unchanged."""
    requested_topics = ["*"]

    result = _filter_topics(available_topics, requested_topics)

    assert set(result.keys()) == set(available_topics.keys())


def test_include_only(available_topics):
    """Verify that a single include glob pattern selects only matching topics."""
    requested_topics = ["/gps/*"]

    result = _filter_topics(available_topics, requested_topics)

    assert set(result.keys()) == {
        "/gps/a",
        "/gps/b",
        "/gps/vendor/time_reference",
    }


def test_exclude_only(available_topics):
    """Verify that a single exclusion-only pattern removes matching topics from the full set."""
    requested_topics = ["!/gps/*"]

    result = _filter_topics(available_topics, requested_topics)

    assert "/gps/a" not in result
    assert "/gps/b" not in result
    assert "/gps/vendor/time_reference" not in result
    assert "/cam/front/image" in result


def test_include_then_exclude(available_topics):
    """Verify that an include pattern followed by an exclusion pattern removes excluded matches from the included set."""
    requested_topics = [
        "/gps/*",
        "!/gps/vendor/*",
    ]

    result = _filter_topics(available_topics, requested_topics)

    assert set(result.keys()) == {
        "/gps/a",
        "/gps/b",
    }


def test_exclude_then_reinclude_override(available_topics):
    """Verify that an exclusion pattern followed by an inclusion pattern re-includes previously excluded topics (order-dependent behavior)."""
    # Exclude all the gps, but the vendor
    requested_topics = [
        "!/gps/*",
        "/gps/vendor/time_reference",
    ]

    result = _filter_topics(available_topics, requested_topics)

    assert set(result.keys()) == {
        "/gps/vendor/time_reference",
    }


def test_include_then_reexclude_override(available_topics):
    """Verify that an inclusion pattern followed by an exclusion pattern clears previous inclusion (order-dependent behavior)."""
    requested_topics = [
        "/gps/vendor/time_reference",
        "!/gps/*",  # exclude '/gps/vendor/time_reference' also
    ]

    result = _filter_topics(available_topics, requested_topics)

    assert set(result) == set()


def test_multiple_includes_union(available_topics):
    """Verify that multiple include patterns are combined using union semantics."""
    requested_topics = [
        "/gps/*",
        "/cam/front/image",
    ]

    result = _filter_topics(available_topics, requested_topics)

    assert "/gps/a" in result
    assert "/gps/b" in result
    assert "/gps/vendor/time_reference" in result
    assert "/cam/front/image" in result
    assert "/cam/front/camera_info" not in result


def test_global_include_then_exclude(available_topics):
    """Verify that a global include pattern followed by an exclusion pattern correctly filters out excluded topic groups."""
    requested_topics = [
        "/*",
        "!/cam/*",
    ]

    result = _filter_topics(available_topics, requested_topics)

    assert "/cam/front/image" not in result
    assert "/cam/front/camera_info" not in result
    assert "/gps/a" in result
    assert "/gps/b" in result
    assert "/gps/vendor/time_reference" in result

    # This must be equivalent
    requested_topics = [
        "!/cam/*",
    ]

    assert set(result) == set(_filter_topics(available_topics, requested_topics))
