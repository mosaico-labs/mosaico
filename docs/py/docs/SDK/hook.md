---
title: Hooks
description: Receiving the events mosaicod sends to hooks
---

The **Hook** module runs your own code on the events `mosaicod` sends to [hooks](https://docs.mosaico.dev/daemon/hooks), the HTTP endpoints registered with `mosaicod hook create`. A [`HookServer`][mosaicolabs.hook.HookServer] receives each event and passes it to every function decorated with it.

!!! info "Hooks in `mosaicod`"
    Registering hooks, the events' JSON format and what delivery promises are covered in the daemon's [Hooks](https://docs.mosaico.dev/daemon/hooks) page. This page covers receiving the events in Python.

!!! example "Learn How-To"
    See the **[Reacting to New Data with Hooks](https://docs.mosaico.dev/learn/hooks)** guide for a walkthrough and typical use cases.

## Quick Start

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

Then register the server's URL, as `mosaicod` reaches it:

```bash
mosaicod hook create http://hooks.internal:8080/
```

## Events

Every event is an [`Event`][mosaicolabs.hook.Event], with an `id`, and functions tell them apart by their class:

| Class | Fields |
| --- | --- |
| [`TopicUploaded`][mosaicolabs.hook.TopicUploaded] | `sequence_name`, `topic_name`, `session_locator`, `ontology_tag`, `user_metadata`, `created_timestamp` |
| [`SessionUploaded`][mosaicolabs.hook.SessionUploaded] | `sequence_name`, `session_locator`, `topics`, `created_timestamp` |

An event can go straight to the client, for example `client.topic_handler(e.sequence_name, e.topic_name)`.

A `mosaicod` newer than the SDK can send event types this SDK version doesn't know yet. `HookServer` accepts them, so `mosaicod` doesn't retry them, but ignores them: your functions only receive the classes above. To receive the new types, update the SDK.

## How Events Are Handled

* **Answer first**: the server answers each request before running the decorated functions, so a slow function doesn't make `mosaicod` send the event again. As a result, a function that fails isn't retried.
* **In order, per event**: the functions get each event one after another, in the order they were decorated. A function that raises is logged, and the next one still runs.
* **Concurrently, across events**: events are handled on a thread pool, so functions can run for several events at once. Protect any state they share, for example with a lock.
* **Best effort**: an event can be lost, arrive twice or arrive out of order (see [Delivery](https://docs.mosaico.dev/daemon/hooks#delivery)). A repeated event keeps its `id`, so use `e.id` to skip duplicates when they matter.

## Listening

[`start()`][mosaicolabs.hook.HookServer.start] listens until interrupted, for example with Ctrl-C.

* **Host**: by default it listens on `127.0.0.1`, which only this machine can reach. Pass `host="0.0.0.0"` when `mosaicod` runs on another machine or container.
* **Port**: `8080` by default. Keep it fixed, so the URL registered in `mosaicod` stays valid across restarts.
* **URL**: register the URL `mosaicod` reaches the server at. Any path is accepted, so a hook registered as `http://hooks.internal:8080/mosaico` works too.
* **HTTPS**: `HookServer` serves plain HTTP. To register an `https://` URL, put it behind a reverse proxy that handles TLS.
* **Stopping**: on Ctrl-C it stops listening, drops the events whose functions haven't started, and waits for the ones running.

### Logging

`start()` logs where it listens, and any decorated function that raises. Unless logging is already set up, for example with [`setup_sdk_logging()`][mosaicolabs.logging_config.setup_sdk_logging] or `logging.basicConfig()`, it calls `setup_sdk_logging()` first, which logs to stderr.
