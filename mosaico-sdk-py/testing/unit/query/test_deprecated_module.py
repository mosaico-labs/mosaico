import sys

import pytest

import mosaicolabs.query
from mosaicolabs.query.builders import QueryTopic


@pytest.fixture
def _fresh_alias_import():
    """Undo any caching so the deprecated import path is re-resolved (and its
    DeprecationWarning re-emitted) on every test, regardless of import order."""
    stale = [
        name for name in sys.modules if name.startswith("mosaicolabs.models.query")
    ]
    for name in stale:
        del sys.modules[name]
    yield
    for name in list(sys.modules):
        if name.startswith("mosaicolabs.models.query"):
            del sys.modules[name]


def test_deprecated_package_import_warns_and_aliases(_fresh_alias_import):
    with pytest.deprecated_call(match="mosaicolabs.models.query.*mosaicolabs.query"):
        import mosaicolabs.models.query as old_query  # type: ignore

    assert old_query is mosaicolabs.query


def test_deprecated_submodule_import_warns_and_aliases(_fresh_alias_import):
    with pytest.deprecated_call(
        match="mosaicolabs.models.query.builders.*mosaicolabs.query.builders"
    ):
        from mosaicolabs.models.query.builders import (  # type: ignore
            QueryTopic as OldQueryTopic,
        )

    assert OldQueryTopic is QueryTopic
    assert (
        sys.modules["mosaicolabs.models.query.builders"]
        is sys.modules["mosaicolabs.query.builders"]
    )


def test_deprecated_import_reload_is_idempotent(_fresh_alias_import):
    with pytest.deprecated_call():
        import mosaicolabs.models.query as old_query_1  # type: ignore

    # Once cached in sys.modules, re-importing doesn't re-warn (same as any
    # normal module) and keeps returning the same aliased object.
    import mosaicolabs.models.query as old_query_2  # type: ignore

    assert old_query_1 is old_query_2 is mosaicolabs.query
