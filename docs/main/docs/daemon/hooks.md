---
title: Hooks
sidebar_position: 9
description: "How mosaicod notifies your own HTTP endpoints when a topic is uploaded or a session is finalized. Covers registering hooks, the events and their JSON format, what delivery does and doesn't promise, and security."
---

A **hook** is an HTTP endpoint that `mosaicod` sends an event to when data lands: when a topic finishes uploading, and when a session is finalized. Use hooks to run your own code, such as sending a notification or starting a processing job, instead of polling `mosaicod`.

## Registering a hook

Hooks are managed with the [`mosaicod hook`](cli.md#mosaicod-hook) command:

```bash
$ mosaicod hook create http://hooks.internal:8080/
3
$ mosaicod hook ls
ID     CREATED                URL
3      2026-11-03 11:00 UTC   http://hooks.internal:8080/
$ mosaicod hook rm 3
```

A change applies from the next event on, without restarting `mosaicod server`.

## Events

Every hook receives every event, as an HTTP `POST` with a JSON body:

```json
{
  "id": "0b6f2c1e-4f1a-4c8e-9a43-2d7e5b1f6a90",
  "type": "topic_uploaded",
  "data": { … }
}
```

| Field | Description |
| :--- | :--- |
| `id` | A UUID identifying the event. A retry sends the same event with the same `id`. |
| `type` | The event type: `topic_uploaded` or `session_uploaded`. |
| `data` | The event's fields, which depend on its type. |

Timestamps are in Unix nanoseconds. New event types and fields may be added, so a receiver should ignore the ones it doesn't know.

### `topic_uploaded`

Sent when a topic's upload finishes. The topic's data can't change anymore.

| Field | Description |
| :--- | :--- |
| `topic` | The topic's locator, `sequence/topic`. |
| `sequence` | The topic's sequence. |
| `session` | The session the topic was uploaded in, `sequence:session`. |
| `ontology_tag` | The ontology tag of the topic's data. |
| `user_metadata` | The metadata given when the topic was created, or `null`. |
| `created_at` | When the topic was created. |

```json
{
  "id": "0b6f2c1e-4f1a-4c8e-9a43-2d7e5b1f6a90",
  "type": "topic_uploaded",
  "data": {
    "topic": "drive_042/front_camera",
    "sequence": "drive_042",
    "session": "drive_042:01JBX5Q3ZK8V7M2N4P6R8T0W2Y",
    "ontology_tag": "image",
    "user_metadata": { "driver": "Ada" },
    "created_at": 1793707200123456789
  }
}
```

### `session_uploaded`

Sent when a session is finalized, after all its topics are uploaded.

| Field | Description |
| :--- | :--- |
| `session` | The session's locator, `sequence:session`. |
| `sequence` | The session's sequence. |
| `topics` | The locators of the session's topics. |
| `created_at` | When the session was created. |

```json
{
  "id": "6c1d0a52-93e4-4b7f-8d21-5a0e7c3b9f14",
  "type": "session_uploaded",
  "data": {
    "session": "drive_042:01JBX5Q3ZK8V7M2N4P6R8T0W2Y",
    "sequence": "drive_042",
    "topics": ["drive_042/front_camera", "drive_042/imu"],
    "created_at": 1793707200098765432
  }
}
```

An upload or a finalize that fails sends no event.

## Delivery

Delivery is **best effort**. A request counts as delivered when the hook answers with a `2xx` status. Otherwise:

| Outcome | What `mosaicod` does |
| :--- | :--- |
| Connection error, no answer within 5 seconds, `429`, or `5xx` | Retries, up to 3 attempts in all, waiting 1 second and then 10 seconds. |
| Any other status | Gives up. Redirects aren't followed. |

When it gives up, `mosaicod` logs a warning with the hook's id, as listed by `mosaicod hook ls`, the event `id` and the error.

What that means for a receiver:

- **Answer quickly.** Do slow work after answering, or the 5 second timeout makes `mosaicod` send the event again.
- **An event can be lost.** That happens when a hook is down longer than the retries, when `mosaicod` stops before an event is delivered, and when more than 1024 events are waiting because hooks are slow to answer.
- **An event can arrive twice.** If a hook handles a request but its answer doesn't reach `mosaicod`, the retry sends the same event again, with the same `id`. Use `id` to skip duplicates when they matter.
- **Events can arrive out of order.** Each `mosaicod server` sends up to 16 requests to hooks at once.

## Security

:::warning
    Requests to hooks aren't authenticated: anything that can reach a hook can send it fake events. Keep hooks on a network only `mosaicod` can reach.
:::

Over an `http://` URL the request body, `user_metadata` included, crosses the network in clear. Use `https://` when the network between `mosaicod` and the hook isn't trusted.

## Receiving events in Python

The Python SDK's [`HookServer`](https://docs.mosaico.dev/python-sdk/SDK/API_reference/hook/) receives events and passes each one to every function decorated with it:

```python
from mosaicolabs.hook import Event, HookServer, SessionUploaded, TopicUploaded

hook = HookServer()

@hook
def on_event(e: Event):
    match e:
        case TopicUploaded():
            print(f"{e.topic_name} uploaded to {e.sequence_name}")
        case SessionUploaded():
            print(f"{len(e.topics)} topics uploaded to {e.sequence_name}")

if __name__ == "__main__":
    hook.start(host="0.0.0.0", port=8080)
```

The events use the SDK's names rather than the JSON ones: `sequence_name`, `topic_name` (such as `/front_camera`), `session_locator` and `created_timestamp`, and a session's `topics` are topic names. So an event can go straight to the client, for example `client.topic_handler(e.sequence_name, e.topic_name)`.

`HookServer` answers each request before running the decorated functions, so a slow function doesn't cause a retry. By default it listens on `127.0.0.1`, which only this machine can reach; pass `host="0.0.0.0"` when `mosaicod` runs on another machine or container. It accepts events on any path, so a hook registered as `http://host:8080/mosaico` works too.

`start()` logs where it listens, and any decorated function that raises. Unless logging is already set up, for example with `setup_sdk_logging()` or `logging.basicConfig()`, it calls `setup_sdk_logging()` first, which logs to stderr.
