# Custom Actions

Mosaico implements its own administrative protocols directly on top of Apache Arrow Flight. Rather than relying on a separate control channel abstraction, Mosaico leverages the Flight `DoAction` RPC mechanism to handle discrete lifecycle events, administrative interfaces, and resource management.

Unlike streaming endpoints designed for continuous data throughput, these custom actions manage the platform's overarching state. While individual calls are synchronous, they often initiate or conclude multi-step processes, such as topic upload, that govern the long-term integrity of data within the platform.

All custom actions follow a standardized pattern: they expect a JSON-serialized payload defining the request parameters and return a JSON-serialized response containing the result.

## Sequence Management

Sequences are the fundamental containers for data recordings in Mosaico. These custom actions enforce a strict lifecycle state machine to guarantee data integrity.

| Action | Description |
| --- | --- |
| `sequence_create` | Initializes a new, empty sequence. It generates and returns a unique key (UUID). This key acts as a write token, authorizing subsequent data ingestion into this specific sequence. This avoids concurrent access and creation issues when multiple clients attempt to create sequences simultaneously. |
| `sequence_finalize` | Transitions a sequence from *uploading* to *archived*. This action locks the sequence, marking it as immutable. Once finalized, no further data can be added or modified, ensuring a perfect audit trail. |
| `sequence_abort` | A cleanup operation for failed uploads. It discards a sequence that is currently being uploaded, purging any partial data from the storage to prevent *zombie* records. |
| `sequence_delete` | Permanently removes a sequence from the platform. To protect data lineage, this is typically permitted only on unlocked (incomplete) sequences. |

## Topic Management

Topics represent the individual sensor streams (e.g., `camera/front`, `gps`) contained within a sequence.

| Action | Description |
| --- | --- |
| `topic_create` | Registers a new topic. |
| `topic_delete` | Removes a specific topic from a sequence, permitted only if the parent sequence is still unlocked. |

## Notification System

The platform includes a tagging mechanism to attach alerts or informational messages to resources. For example, if an exception is raised during an upload, the notification system automatically registers the event, ensuring the failure is logged and visible for troubleshooting.

| Action | Description |
| --- | --- |
| `*_notify_create` | Attaches a notification to a Sequence or Topic, such as logging an error or status update. |
| `*_notify_list` | Retrieves the history of active notifications for a resource, allowing clients to review alerts. |
| `*_notify_purge` | Clears the notification history for a resource, useful for cleanup after resolution. |

Here, `*` can be either `sequence` or `topic`.

## Query

| Action | Description |
| --- | --- |
| `query` | This action serves as the gateway to the query system. It accepts a complex filter object and returns a list of resources that match the criteria. |