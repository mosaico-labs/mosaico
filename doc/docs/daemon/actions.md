# Custom Actions

Mosaico implements its own administrative protocols directly on top of Apache Arrow Flight. Rather than relying on a separate control channel abstraction, Mosaico leverages the Flight `DoAction` RPC mechanism to handle discrete lifecycle events, administrative interfaces, and resource management.

Unlike streaming endpoints designed for continuous data throughput, these custom actions manage the platform's overarching state. While individual calls are synchronous, they often initiate or conclude multi-step processes, such as topic upload, that govern the long-term integrity of data within the platform.

All custom actions follow a standardized pattern: they expect a JSON-serialized payload defining the request parameters and return a JSON-serialized response containing the result.

## Sequence Management

Sequences are the fundamental containers for data recordings in Mosaico. These custom actions enforce a strict lifecycle state machine to guarantee data integrity.

| Action | Description | Permission |
| --- | ---- | --- |
| `sequence_create` | Initializes a new, empty sequence. | `write` |
| `sequence_delete` | Permanently removes a sequence from the platform. | `delete` |

## Topic Management

Topics represent the individual sensor streams (e.g., `camera/front`, `gps`) contained within a sequence.

| Action | Description | Permission |
| --- | --- | --- |
| `topic_create` | Registers a new topic. | `write` |
| `topic_delete` | Removes a specific topic from a sequence. | `delete` |

## Session Management

Uploading data to the platform is made through sessions. Within a session it is possible to load one or more topics. Once closed, it becomes immutable.

| Action             | Description                                                                                                                                                                | Permission |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------| ---------- |
| `session_create`   | Start a new upload session.                                                                                                                                                | `write`    |
| `session_finalize` | Moves the session status from *uploading* to *archived*. This action locks the session, marking it as immutable. Once finalized, no further data can be added or modified. | `write`    |
| `session_delete`   | Removes a specific session and all its data.                                                                                                                               | `delete`   |

## Notification System

The platform includes a tagging mechanism to attach alerts or informational messages to resources. For example, if an exception is raised during an upload, the notification system automatically registers the event, ensuring the failure is logged and visible for troubleshooting.

| Action                  | Description | Permission |
|-------------------------| --- | --- |
| `*_notification_create` | Attaches a notification to a Sequence or Topic, such as logging an error or status update. | `write` |
| `*_notification_list`   | Retrieves the history of active notifications for a resource, allowing clients to review alerts. | `read` |
| `*_notification_purge`  | Clears the notification history for a resource, useful for cleanup after resolution. | `delete` |

Here, `*` can be either `sequence` or `topic`.

## Query

| Action | Description | Permission |
| --- | --- | --- |
| `query` | This action serves as the gateway to the query system. It accepts a complex filter object and returns a list of resources that match the criteria. | `read` |

## Misc

| Action | Description | Permission |
| --- | --- | --- | 
| `version` | Retrieves the current daemon version. | `read` |
