"""
Events sent by the running mosaicod to a `HookServer` registered as a hook.

Registering a hook takes the `mosaicod` CLI, so these tests are skipped without
`--mosaicod-bin`, which `scripts/tests` passes.
"""

import os
import queue
import subprocess
import threading
import time

import pytest

from mosaicolabs import Message
from mosaicolabs.hook import Event, HookServer, SessionUploaded, TopicUploaded
from mosaicolabs.models.data import String

SEQUENCE = "test_hook_sequence"
TOPICS = ["/hook/first", "/hook/second"]

# mosaicod delivers events in the background, after the requests that cause them.
TIMEOUT = 15


def mosaicod(mosaicod_bin: str, *args: str) -> str:
    """Runs a `mosaicod` command and returns its output."""
    # Without the script's `RUST_LOG`, so the output is only the command's.
    env = os.environ | {"RUST_LOG": ""}
    result = subprocess.run(
        [mosaicod_bin, *args], env=env, capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


@pytest.fixture
def events(mosaicod_bin):
    """The events a `HookServer`, registered as a hook for the test only, receives."""
    if mosaicod_bin is None:
        pytest.skip("Tests run without '--mosaicod-bin'")

    received: queue.Queue[Event] = queue.Queue()
    hook = HookServer()
    hook(received.put)

    server = hook._bind("127.0.0.1", 0)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()

    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/"
        hook_id = mosaicod(mosaicod_bin, "hook", "create", url)
        try:
            yield received
        finally:
            # Left registered, the hook would get the events of every later test.
            mosaicod(mosaicod_bin, "hook", "rm", hook_id)
    finally:
        server.shutdown()
        server.close()
        thread.join(TIMEOUT)


def wait_for_events(received: queue.Queue[Event], n: int) -> list[Event]:
    """Waits for `n` events about `SEQUENCE`, skipping any other."""
    events: list[Event] = []
    deadline = time.monotonic() + TIMEOUT

    while len(events) < n:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            pytest.fail(f"got {len(events)} hook events out of {n} in {TIMEOUT}s")

        try:
            event = received.get(timeout=remaining)
        except queue.Empty:
            continue

        if (
            isinstance(event, TopicUploaded | SessionUploaded)
            and event.sequence_name == SEQUENCE
        ):
            events.append(event)

    return events


def test_upload_sends_events(events, mosaico_client):
    with mosaico_client.sequence_create(SEQUENCE, {}) as seqw:
        for topic in TOPICS:
            tw = seqw.topic_create(topic, {"source": topic}, String)
            tw.push(Message(timestamp_ns=12345678, data=String(data="hook")))

    try:
        # One `TopicUploaded` per topic and one `SessionUploaded`, in any order:
        # deliveries run concurrently.
        received = wait_for_events(events, len(TOPICS) + 1)

        topic_events = sorted(
            (e for e in received if isinstance(e, TopicUploaded)),
            key=lambda e: e.topic_name,
        )
        session_events = [e for e in received if isinstance(e, SessionUploaded)]

        assert [e.topic_name for e in topic_events] == sorted(TOPICS)
        assert len(session_events) == 1

        session = session_events[0]
        assert session.session_locator.startswith(f"{SEQUENCE}:")
        assert sorted(session.topics) == sorted(TOPICS)

        for e in topic_events:
            assert e.session_locator == session.session_locator
            assert e.ontology_tag == String.ontology_tag()
            assert e.user_metadata == {"source": e.topic_name}
            # The event's names go straight to the client.
            assert mosaico_client.topic_handler(e.sequence_name, e.topic_name)
    finally:
        mosaico_client.sequence_delete(SEQUENCE)
