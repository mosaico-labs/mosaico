import http.client
import json
import queue
import threading

import pytest

from mosaicolabs.hook import Event, HookServer, SessionUploaded, TopicUploaded
from mosaicolabs.hook.server import MAX_BODY_SIZE, _listening_message

from .test_events import SESSION_UPLOADED, TOPIC_UPLOADED

TIMEOUT = 5


@pytest.fixture
def hook():
    """A `HookServer` listening on a free port, as `(hook, port)`."""
    hook = HookServer()
    server = hook._bind("127.0.0.1", 0)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()

    yield hook, server.server_address[1]

    server.shutdown()
    server.close()
    thread.join(TIMEOUT)


def post(port: int, payload=None, headers=None, body: bytes | None = None) -> int:
    """Sends a request and returns the response status."""
    if body is None:
        body = json.dumps(payload).encode()

    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=TIMEOUT)
    try:
        connection.putrequest("POST", "/")
        if headers is None:
            headers = {"Content-Length": str(len(body))}
        for name, value in headers.items():
            connection.putheader(name, value)
        connection.endheaders(body)
        return connection.getresponse().status
    finally:
        connection.close()


def event(payload: dict, event_id: str) -> dict:
    return {**payload, "id": event_id}


def collect(hook: HookServer) -> "queue.Queue[Event]":
    received: "queue.Queue[Event]" = queue.Queue()
    hook(received.put)
    return received


def test_every_receiver_gets_every_event(hook):
    hook, port = hook
    first, second = collect(hook), collect(hook)

    assert post(port, event(TOPIC_UPLOADED, "1")) == 200
    assert post(port, event(SESSION_UPLOADED, "2")) == 200

    for received in (first, second):
        topic = received.get(timeout=TIMEOUT)
        session = received.get(timeout=TIMEOUT)
        assert isinstance(topic, TopicUploaded) and topic.id == "1"
        assert topic.topic == "drive_042/front_camera"
        assert isinstance(session, SessionUploaded) and session.id == "2"
        assert session.topics == ["drive_042/front_camera", "drive_042/imu"]


def test_receivers_run_in_registration_order(hook):
    hook, port = hook
    calls = []
    done = threading.Event()
    hook(lambda e: calls.append("first"))
    hook(lambda e: (calls.append("second"), done.set()))

    assert post(port, event(TOPIC_UPLOADED, "1")) == 200

    assert done.wait(TIMEOUT)
    assert calls == ["first", "second"]


def test_decorator_returns_the_function():
    hook = HookServer()

    def receiver(e: Event) -> str:
        return "called"

    assert hook(receiver) is receiver
    assert receiver(None) == "called"


def test_decorator_rejects_non_callables():
    with pytest.raises(TypeError):
        HookServer()("not a function")


def test_replies_before_a_slow_receiver_finishes(hook):
    hook, port = hook
    release = threading.Event()
    finished = threading.Event()

    @hook
    def slow(e: Event):
        release.wait(TIMEOUT)
        finished.set()

    assert post(port, event(TOPIC_UPLOADED, "1")) == 200
    assert not finished.is_set()

    release.set()
    assert finished.wait(TIMEOUT)


def test_a_raising_receiver_does_not_stop_the_others(hook):
    hook, port = hook

    @hook
    def broken(e: Event):
        raise RuntimeError("boom")

    received = collect(hook)

    assert post(port, event(TOPIC_UPLOADED, "1")) == 200
    assert post(port, event(TOPIC_UPLOADED, "2")) == 200

    assert received.get(timeout=TIMEOUT).id == "1"
    assert received.get(timeout=TIMEOUT).id == "2"


def test_an_unknown_type_runs_nothing(hook):
    hook, port = hook
    received = collect(hook)

    unknown = {"id": "1", "type": "sequence_deleted", "data": {}}
    assert post(port, unknown) == 200
    assert post(port, event(TOPIC_UPLOADED, "2")) == 200

    assert received.get(timeout=TIMEOUT).id == "2"
    assert received.empty()


def test_any_path_is_accepted(hook):
    hook, port = hook
    received = collect(hook)
    body = json.dumps(event(TOPIC_UPLOADED, "1")).encode()

    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=TIMEOUT)
    connection.request("POST", "/mosaico/hook", body=body)
    assert connection.getresponse().status == 200
    connection.close()

    assert received.get(timeout=TIMEOUT).id == "1"


@pytest.mark.parametrize(
    ("headers", "body", "status"),
    [
        ({}, b"", 411),
        ({"Content-Length": "many"}, b"", 400),
        ({"Content-Length": str(MAX_BODY_SIZE + 1)}, b"", 413),
        ({"Content-Length": "8"}, b"not json", 400),
    ],
)
def test_bad_requests_are_rejected(hook, headers, body, status):
    hook, port = hook
    received = collect(hook)

    assert post(port, headers=headers, body=body) == status
    assert received.empty()


def test_listening_message():
    assert (
        _listening_message("127.0.0.1", 8080) == "Listening on http://127.0.0.1:8080/"
    )

    everywhere = _listening_message("0.0.0.0", 8080)
    assert "all interfaces, port 8080" in everywhere
    assert "mosaicod hook create http://" in everywhere
