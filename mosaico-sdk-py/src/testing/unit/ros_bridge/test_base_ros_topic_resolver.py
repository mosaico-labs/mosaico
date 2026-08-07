from mosaicolabs.ros_bridge.loader import TopicStatus, _BaseROSTopicResolver


class _FakeAdapter:
    """Stand-in for a resolved adapter type; identity is all that matters here."""


class _FakeResolver(_BaseROSTopicResolver):
    """
    Minimal concrete subclass exercising the base class's bookkeeping without needing
    a real bag file or Mosaico sequence. Storage containers are passed in as either
    dicts (mimicking ROSLoader) or lists (mimicking MosaicoLoader), to prove the base
    class properties work uniformly regardless of which container a subclass uses.
    """

    def __init__(
        self,
        resolved,
        accepted,
        unresolved,
        filtered,
        adapters,
        extra_rejections=None,
    ):
        self._resolved_topics = resolved
        self._accepted_topics = accepted
        self._unresolved_adapter_topics = unresolved
        self._filtered_topics = filtered
        self._topic_cached_adapters = adapters
        self._extra_rejections = extra_rejections or []
        self.ensure_resolved_calls = 0

    def _ensure_resolved(self) -> None:
        self.ensure_resolved_calls += 1

    def _extra_rejected_topics(self):
        return self._extra_rejections


def _dict_backed_resolver(**extra_rejections_kwargs):
    adapter = _FakeAdapter()
    return _FakeResolver(
        resolved={"/imu": None, "/gps": None, "/debug": None},
        accepted={"/imu": None, "/gps": None},
        unresolved={"/debug": None},
        filtered={},
        adapters={"/imu": adapter, "/gps": adapter},
        **extra_rejections_kwargs,
    )


def _list_backed_resolver(**extra_rejections_kwargs):
    adapter = _FakeAdapter()
    return _FakeResolver(
        resolved=["/imu", "/gps", "/debug"],
        accepted=["/imu", "/gps"],
        unresolved=["/debug"],
        filtered=[],
        adapters={"/imu": adapter, "/gps": adapter},
        **extra_rejections_kwargs,
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


def test_unresolved_adapted_topics():
    for resolver in (_dict_backed_resolver(), _list_backed_resolver()):
        assert list(resolver.unresolved_adapted_topics) == ["/debug"]


def test_filtered_topics_empty_when_no_filter_applied():
    for resolver in (_dict_backed_resolver(), _list_backed_resolver()):
        assert resolver.filtered_topics == []


def test_filtered_topics_reflects_excluded_topics():
    resolver = _FakeResolver(
        resolved={"/imu": None, "/cam": None},
        accepted={"/imu": None},
        unresolved={},
        filtered={"/cam": None},
        adapters={"/imu": _FakeAdapter()},
    )

    assert resolver.filtered_topics == ["/cam"]


def test_rejected_topics_combines_filtered_and_unresolved():
    resolver = _dict_backed_resolver()

    rejected = dict(resolver.rejected_topics)

    assert rejected == {"/debug": TopicStatus.UNRESOLVED_ADAPTED}


def test_rejected_topics_includes_source_specific_extra_rejections():
    resolver = _dict_backed_resolver(
        extra_rejections=[
            ("/malformed", TopicStatus.MALFORMED_METADATA),
            ("/missing_type", TopicStatus.NOT_IN_TYPESTORE),
        ]
    )

    rejected = dict(resolver.rejected_topics)

    assert rejected == {
        "/debug": TopicStatus.UNRESOLVED_ADAPTED,
        "/malformed": TopicStatus.MALFORMED_METADATA,
        "/missing_type": TopicStatus.NOT_IN_TYPESTORE,
    }


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
    resolver.unresolved_adapted_topics
    resolver.rejected_topics
    resolver.resolve_adapter("/imu")

    # rejected_topics composes filtered_topics/unresolved_adapted_topics internally,
    # so it triggers _ensure_resolved more than once by itself; assert it fires at
    # least once per accessed property rather than pin an exact, implementation-tied count.
    assert resolver.ensure_resolved_calls >= 6
