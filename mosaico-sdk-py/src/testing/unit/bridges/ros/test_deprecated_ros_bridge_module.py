import sys

import pytest

import mosaicolabs.bridges.ros
from mosaicolabs.bridges.ros import ROSSequenceExtractor


@pytest.fixture
def _fresh_alias_import():
    """Undo any caching so the deprecated import path is re-resolved (and its
    DeprecationWarning re-emitted) on every test, regardless of import order."""
    stale = [name for name in sys.modules if name.startswith("mosaicolabs.ros_bridge")]
    for name in stale:
        del sys.modules[name]
    yield
    for name in list(sys.modules):
        if name.startswith("mosaicolabs.ros_bridge"):
            del sys.modules[name]


def test_deprecated_package_import_warns_and_aliases(_fresh_alias_import):
    with pytest.deprecated_call(
        match="'mosaicolabs.ros_bridge' is deprecated and will be removed in a future release; import from 'mosaicolabs.bridges.ros' instead."
    ):
        import mosaicolabs.ros_bridge as old_bridge  # type: ignore

    assert old_bridge is mosaicolabs.bridges.ros


def test_deprecated_submodule_import_warns_and_aliases(_fresh_alias_import):
    with pytest.deprecated_call(
        match="mosaicolabs.ros_bridge.sequence_extractor' is deprecated and will be removed in a future release; import from 'mosaicolabs.bridges.ros.sequence_extractor' instead."
    ):
        from mosaicolabs.ros_bridge.sequence_extractor import (  # type: ignore
            ROSSequenceExtractor as OldSequenceExtractor,
        )

    assert OldSequenceExtractor is ROSSequenceExtractor
    assert (
        sys.modules["mosaicolabs.ros_bridge.sequence_extractor"]
        is sys.modules["mosaicolabs.bridges.ros.sequence_extractor"]
    )


def test_deprecated_import_reload_is_idempotent(_fresh_alias_import):
    with pytest.deprecated_call():
        import mosaicolabs.ros_bridge as old_bridge_1  # type: ignore

    # Once cached in sys.modules, re-importing doesn't re-warn (same as any
    # normal module) and keeps returning the same aliased object.
    import mosaicolabs.ros_bridge as old_bridge_2  # type: ignore

    assert old_bridge_1 is old_bridge_2 is mosaicolabs.bridges.ros
