"""
A small HTTP server that receives the events mosaicod sends to a hook.
"""

import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, TypeVar

from mosaicolabs.logging_config import get_logger

from .events import Event, _parse_event

logger = get_logger(__name__)

Receiver = Callable[[Event], Any]
R = TypeVar("R", bound=Receiver)

DEFAULT_PORT = 8080
"""The port `HookServer.start` listens on by default."""

MAX_BODY_SIZE = 10 * 1024 * 1024
"""Bodies larger than this, in bytes, are rejected."""


class HookServer:
    """
    Receives the events mosaicod sends to a hook, and passes each one to every receiver.

    Mosaicod sends an event when a topic finishes uploading and when a session is
    finalized. Register the server's URL with `mosaicod hook create <URL>`.

    Delivery is best effort: an event can be lost, for example if this server is down
    longer than mosaicod keeps retrying, and a retry can deliver an event twice, with
    the same `id`. Requests aren't authenticated, so anything that can reach the
    server can send it events. Keep it on a network only mosaicod can reach.

    Example:
        ```python
        from mosaicolabs.hook import Event, HookServer, TopicUploaded

        hook = HookServer()

        @hook
        def on_event(e: Event):
            if isinstance(e, TopicUploaded):
                print(f"{e.topic_name} uploaded to {e.sequence_name}")

        if __name__ == "__main__":
            hook.start(host="0.0.0.0", port=8080)
        ```
    """

    def __init__(self) -> None:
        self._receivers: list[Receiver] = []
        self._lock = threading.Lock()

    def __call__(self, receiver: R) -> R:
        """
        Registers `receiver` to get every event, and returns it unchanged.

        With several receivers, each gets every event, in the order they were
        registered. A receiver that raises is logged, and the next one still runs.

        Raises:
            TypeError: If `receiver` isn't callable.
        """
        if not callable(receiver):
            raise TypeError(f"a hook receiver must be callable, not {receiver!r}")

        with self._lock:
            self._receivers.append(receiver)

        return receiver

    def start(self, host: str = "127.0.0.1", port: int = DEFAULT_PORT) -> None:
        """
        Listens for events until interrupted, for example with Ctrl-C.

        `host` and `port` are where this server listens, which isn't always the URL
        mosaicod reaches it at, for example behind NAT or Docker port mapping. Register
        that URL with `mosaicod hook create`.

        Args:
            host: The address to listen on. The default, `127.0.0.1`, is only
                reachable from this machine. Use `"0.0.0.0"` when mosaicod runs on
                another machine or container.
            port: The port to listen on. Keep it fixed, so the URL registered in
                mosaicod stays valid across restarts.
        """
        server = self._bind(host, port)
        print(_listening_message(host, server.server_address[1]), flush=True)

        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.close()

    def _bind(self, host: str, port: int) -> "_Server":
        return _Server((host, port), self)

    def _run_receivers(self, event: Event) -> None:
        with self._lock:
            receivers = list(self._receivers)

        for receiver in receivers:
            try:
                receiver(event)
            except Exception:
                logger.exception(
                    "hook receiver %s failed on %s %s",
                    getattr(receiver, "__name__", repr(receiver)),
                    type(event).__name__,
                    event.id,
                )


class _Server(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], hook: HookServer) -> None:
        self.hook = hook
        # Receivers run here, after the request is answered.
        self.pool = ThreadPoolExecutor(thread_name_prefix="mosaico-hook")
        super().__init__(address, _Handler)

    def close(self) -> None:
        """Stops listening, drops the events not started yet, and waits for the rest."""
        self.server_close()
        self.pool.shutdown(wait=True, cancel_futures=True)


class _Handler(BaseHTTPRequestHandler):
    server: _Server

    def do_POST(self) -> None:
        # Any path is accepted, so a URL registered with a path still works.
        length = self.headers.get("Content-Length")
        if length is None:
            self._reply(HTTPStatus.LENGTH_REQUIRED)
            return

        try:
            length = int(length)
        except ValueError:
            self._reply(HTTPStatus.BAD_REQUEST)
            return

        if length < 0:
            self._reply(HTTPStatus.BAD_REQUEST)
            return

        if length > MAX_BODY_SIZE:
            self._reply(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
            return

        body = self.rfile.read(length)

        try:
            event = _parse_event(body)
        except ValueError as e:
            logger.warning("rejected a hook request: %s", e)
            self._reply(HTTPStatus.BAD_REQUEST)
            return

        self._reply(HTTPStatus.OK)

        if event is None:
            logger.debug("ignored an event of a type this SDK doesn't know")
        else:
            self.server.pool.submit(self.server.hook._run_receivers, event)

    def _reply(self, status: HTTPStatus) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()
        self.wfile.flush()

    def log_message(self, format: str, *args: Any) -> None:
        logger.debug("%s - %s", self.address_string(), format % args)


def _listening_message(host: str, port: int) -> str:
    if host in ("", "0.0.0.0"):
        return (
            f"Listening on all interfaces, port {port}. Register it with "
            f"`mosaicod hook create http://{socket.gethostname()}:{port}/`, or with "
            f"the address mosaicod reaches this machine at."
        )

    return f"Listening on http://{host}:{port}/"
