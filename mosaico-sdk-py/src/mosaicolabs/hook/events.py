"""
The events mosaicod sends to hooks, parsed from the JSON body of its requests.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict

from mosaicolabs.helpers.helpers import unpack_topic_full_path


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
                    print(f"{e.topic_name} uploaded to {e.sequence_name}")
                case SessionUploaded():
                    print(f"{len(e.topics)} topics uploaded to {e.sequence_name}")
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
        sequence_name: The topic's sequence.
        topic_name: The topic.
        session_locator: The session the topic was uploaded in, as
            `sequence_name:session_identifier`.
        ontology_tag: The ontology tag of the topic's data.
        user_metadata: The metadata given when the topic was created, if any.
        created_timestamp: When the topic was created, in Unix nanoseconds.
    """

    sequence_name: str
    topic_name: str
    session_locator: str
    ontology_tag: str
    user_metadata: dict[str, Any] | None
    created_timestamp: int


class SessionUploaded(Event):
    """
    A session was finalized, and all its topics are uploaded.

    Attributes:
        sequence_name: The session's sequence.
        session_locator: The session, as `sequence_name:session_identifier`.
        topics: The names of the session's topics.
        created_timestamp: When the session was created, in Unix nanoseconds.
    """

    sequence_name: str
    session_locator: str
    topics: list[str]
    created_timestamp: int


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
    data = envelope.data

    # mosaicod's names on the left, the SDK's on the right.
    match envelope.type:
        case "topic_uploaded":
            return TopicUploaded.model_validate(
                {
                    "id": envelope.id,
                    "sequence_name": data.get("sequence"),
                    "topic_name": _topic_name(data.get("topic")),
                    "session_locator": data.get("session"),
                    "ontology_tag": data.get("ontology_tag"),
                    "user_metadata": data.get("user_metadata"),
                    "created_timestamp": data.get("created_at"),
                }
            )
        case "session_uploaded":
            topics = data.get("topics")
            return SessionUploaded.model_validate(
                {
                    "id": envelope.id,
                    "sequence_name": data.get("sequence"),
                    "session_locator": data.get("session"),
                    "topics": (
                        [_topic_name(topic) for topic in topics]
                        if isinstance(topics, list)
                        else topics
                    ),
                    "created_timestamp": data.get("created_at"),
                }
            )
        case _:
            return None


def _topic_name(locator: Any) -> Any:
    """
    Turns mosaicod's `sequence/topic` into the topic's name, `/topic`, as the rest of
    the SDK names topics. Anything else is returned as is, for validation to reject.
    """
    unpacked = unpack_topic_full_path(locator) if isinstance(locator, str) else None
    return unpacked[1] if unpacked else locator
