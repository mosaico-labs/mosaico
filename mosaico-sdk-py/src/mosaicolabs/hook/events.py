"""
The events mosaicod sends to hooks, parsed from the JSON body of its requests.
"""

import datetime
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def _datetime_from_ns(value: Any) -> Any:
    """
    Converts Unix nanoseconds, as mosaicod sends them, to a UTC datetime exact to the
    microsecond. A datetime passes through, so events can be built by hand, for
    example to test a receiver.
    """
    if isinstance(value, datetime.datetime):
        return value

    # `bool` is a subclass of `int`, but never a valid timestamp.
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError("must be an integer number of Unix nanoseconds")

    return _EPOCH + datetime.timedelta(microseconds=value // 1_000)


_UnixNanos = Annotated[datetime.datetime, BeforeValidator(_datetime_from_ns)]


class Event(BaseModel):
    """
    Base class of the events mosaicod sends to hooks.

    A receiver gets every event, and tells them apart by their class:

    Example:
        ```python
        @hook
        def on_event(e: Event):
            match e:
                case TopicUploaded():
                    print(f"{e.topic} uploaded")
                case SessionUploaded():
                    print(f"{len(e.topics)} topics in {e.session}")
        ```

    Attributes:
        id: Identifies the event. A retry delivers the same event with the same id.
    """

    model_config = ConfigDict(frozen=True, strict=True)

    id: str


class TopicUploaded(Event):
    """
    A topic's upload finished, and its data can't change anymore.

    Attributes:
        topic: The topic, as `sequence/topic`.
        sequence: The topic's sequence.
        session: The session the topic was uploaded in, as `sequence:session`.
        ontology_tag: The ontology tag of the topic's data.
        user_metadata: The metadata given when the topic was created, if any.
        created_at: When the topic was created.
    """

    topic: str
    sequence: str
    session: str
    ontology_tag: str
    user_metadata: dict[str, Any] | None
    created_at: _UnixNanos


class SessionUploaded(Event):
    """
    A session was finalized, and all its topics are uploaded.

    Attributes:
        session: The session, as `sequence:session`.
        sequence: The session's sequence.
        topics: The session's topics, each as `sequence/topic`.
        created_at: When the session was created.
    """

    session: str
    sequence: str
    topics: list[str]
    created_at: _UnixNanos


class _Envelope(BaseModel):
    """The JSON body of a request from mosaicod: `{"id", "type", "data"}`."""

    model_config = ConfigDict(strict=True)

    id: str
    type: str
    data: dict[str, Any]


def _parse_event(body: bytes) -> Event | None:
    """
    Parses the body of a request from mosaicod.

    Returns `None` for an event type this SDK doesn't know, so a newer mosaicod doesn't
    break an older hook. Fields this SDK doesn't know are ignored.

    Raises:
        pydantic.ValidationError: If the body isn't a valid event. It's a `ValueError`.
    """
    envelope = _Envelope.model_validate_json(body)

    match envelope.type:
        case "topic_uploaded":
            event_type = TopicUploaded
        case "session_uploaded":
            event_type = SessionUploaded
        case _:
            return None

    return event_type.model_validate({**envelope.data, "id": envelope.id})
