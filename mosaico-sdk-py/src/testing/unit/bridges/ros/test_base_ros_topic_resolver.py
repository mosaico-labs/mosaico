from mosaicolabs.bridges.ros.loader import BaseLoader
from mosaicolabs.bridges.topic_status import CommonTopicStatus, ROSTopicStatus


class _FakeAdapter:
    """Stand-in for a resolved adapter type; identity is all that matters here."""


class _FakeLoader(BaseLoader):
    """
    Minimal concrete subclass exercising the base class's bookkeeping without needing
    a real bag file or Mosaico sequence. The accepted/resolved containers are passed in
    as either dicts (mimicking ROSLoader/MCAPLoader) or lists (mimicking MosaicoLoader),
    to prove the base class properties work uniformly regardless of which container a
    subclass uses. Rejections always live in a single `{topic: status}` dict.
    """

    def __init__(
        self,
        resolved,
        accepted,
        adapters,
        rejections=None,
    ):
        self._resolved_topics = resolved
        self._accepted_topics = accepted
        self._topic_cached_adapters = adapters
        self._rejections = dict(rejections or {})
        self.ensure_resolved_calls = 0

    def _ensure_resolved(self) -> None:
        self.ensure_resolved_calls += 1


def _dict_backed_resolver(**rejection_kwargs):
    adapter = _FakeAdapter()
    rejection_kwargs.setdefault(
        "rejections", {"/debug": CommonTopicStatus.UNRESOLVED_ADAPTER}
    )
    return _FakeLoader(
        resolved={"/imu": None, "/gps": None, "/debug": None},
        accepted={"/imu": None, "/gps": None},
        adapters={"/imu": adapter, "/gps": adapter},
        **rejection_kwargs,
    )


def _list_backed_resolver(**rejection_kwargs):
    adapter = _FakeAdapter()
    rejection_kwargs.setdefault(
        "rejections", {"/debug": CommonTopicStatus.UNRESOLVED_ADAPTER}
    )
    return _FakeLoader(
        resolved=["/imu", "/gps", "/debug"],
        accepted=["/imu", "/gps"],
        adapters={"/imu": adapter, "/gps": adapter},
        **rejection_kwargs,
    )


def test_topics_returns_accepted_keys_for_dict_backed_storage():
    resolver = _dict_backed_resolver()

    assert set(resolver.topics) == {"/imu", "/gps"}


def test_topics_returns_accepted_items_for_list_backed_storage():
    resolver = _list_backed_resolver()

    assert set(resolver.topics) == {"/imu", "/gps"}


def test_resolved_topics_returns_everything_regardless_of_filtering():
    for resolver in (_dict_backed_resolver(), _list_backed_resolver()):
        assert set(resolver.resolved_topics) == {"/imu", "/gps", "/debug"}


def test_unresolved_adapter_topics():
    for resolver in (_dict_backed_resolver(), _list_backed_resolver()):
        assert list(resolver.unresolved_adapter_topics) == ["/debug"]


def test_filtered_topics_empty_when_no_filter_applied():
    for resolver in (_dict_backed_resolver(), _list_backed_resolver()):
        assert resolver.filtered_topics == []


def test_filtered_topics_reflects_excluded_topics():
    resolver = _FakeLoader(
        resolved={"/imu": None, "/cam": None},
        accepted={"/imu": None},
        adapters={"/imu": _FakeAdapter()},
        rejections={"/cam": CommonTopicStatus.FILTERED},
    )

    assert resolver.filtered_topics == ["/cam"]


def test_reject_records_the_reason():
    resolver = _dict_backed_resolver(rejections={})
    assert resolver.rejected_topics == []

    resolver._reject("/cam", CommonTopicStatus.FILTERED)

    assert resolver.rejected_topics == [("/cam", CommonTopicStatus.FILTERED)]
    assert resolver.filtered_topics == ["/cam"]


def test_rejected_topics_combines_filtered_and_unresolved():
    resolver = _dict_backed_resolver(
        rejections={
            "/debug": CommonTopicStatus.UNRESOLVED_ADAPTER,
            "/cam": CommonTopicStatus.FILTERED,
        }
    )

    rejected = dict(resolver.rejected_topics)

    assert rejected == {
        "/debug": CommonTopicStatus.UNRESOLVED_ADAPTER,
        "/cam": CommonTopicStatus.FILTERED,
    }


def test_rejected_topics_includes_source_specific_rejections():
    """A subclass's own TopicStatus members reach `rejected_topics` with no extra
    plumbing -- this is what the deleted `_extra_rejected_topics()` hook used to do."""
    resolver = _dict_backed_resolver(
        rejections={
            "/debug": CommonTopicStatus.UNRESOLVED_ADAPTER,
            "/malformed": ROSTopicStatus.MALFORMED_METADATA,
            "/missing_type": ROSTopicStatus.NOT_IN_TYPESTORE,
        }
    )

    rejected = dict(resolver.rejected_topics)

    assert rejected == {
        "/debug": CommonTopicStatus.UNRESOLVED_ADAPTER,
        "/malformed": ROSTopicStatus.MALFORMED_METADATA,
        "/missing_type": ROSTopicStatus.NOT_IN_TYPESTORE,
    }


def test_every_topic_is_either_accepted_or_rejected():
    """The invariant `ProgressManager.setup()` relies on: a topic that is in neither
    bucket renders as a blank, never-completing progress bar."""
    resolver = _dict_backed_resolver(
        rejections={
            "/debug": CommonTopicStatus.UNRESOLVED_ADAPTER,
        }
    )

    accounted = set(resolver.topics) | {t for t, _ in resolver.rejected_topics}

    assert accounted == set(resolver.resolved_topics)


def test_resolve_adapter_returns_cached_adapter_for_accepted_topic():
    resolver = _dict_backed_resolver()

    assert resolver.resolve_adapter("/imu") is resolver._topic_cached_adapters["/imu"]


def test_resolve_adapter_returns_none_for_rejected_topic():
    resolver = _dict_backed_resolver()

    assert resolver.resolve_adapter("/debug") is None


def test_resolve_adapter_returns_none_for_unknown_topic():
    resolver = _dict_backed_resolver()

    assert resolver.resolve_adapter("/does-not-exist") is None


def test_properties_trigger_ensure_resolved():
    resolver = _dict_backed_resolver()
    assert resolver.ensure_resolved_calls == 0

    resolver.topics
    resolver.resolved_topics
    resolver.filtered_topics
    resolver.unresolved_adapter_topics
    resolver.rejected_topics
    resolver.resolve_adapter("/imu")

    assert resolver.ensure_resolved_calls >= 6
