---
sidebar_position: 8
---

# Background Cleanup Routine

To ensure high performance and low latency during user operations, Mosaico does not perform physical deletions in object storage in real-time. When a **Sequence** or **Topic** is deleted via the [Management Actions](actions.md#sequence-management), the database record is removed immediately, but the associated storage files remain.

The **Cleanup Routine** is a background process responsible for identifying and permanently purging these orphaned files.

## Deletion Lifecycle

The cleanup process operates in two distinct phases to prevent accidental data loss and optimize system resources:

*  **Marking (Soft Deletion):** The routine identifies folders in object storage that no longer have a corresponding entry in the database. These folders are marked for deletion by creating a `TO_DELETE` marker file inside the directory.
*  **Purging (Permanent Deletion):** Once a folder has been marked and the configured retention period has elapsed, the routine permanently removes the folder and all its contents from the object store.

## Configuration

The routine behavior can be configured via the following [environment variables](env.md#cleanup-routine):

| Variable | Description                                                                                 |
| :--- |:--------------------------------------------------------------------------------------------|
| `MOSAICOD_CLEANUP_TIME_INTERVAL` | The frequency at which the routine checks whether it should run or not (e.g., `1h`, `24h`). |
| `MOSAICOD_CLEANUP_RETENTION_DURATION` | The minimum age of a TO_DELETE marker before the folder is eligible for permanent removal. |


## Distributed Coordination

In a multi-instance environment, every `mosaicod` daemon runs its own background cleanup routine. To prevent race conditions and redundant resource consumption, the instances coordinate via the database:

* **Concurrency Control:** A centralized log history in the database tracks active cleanup sessions. If one instance is already performing a cleanup, other instances will remain idle.
* **Execution Logic:** When an instance wakes up, it checks the timestamp of the last successful cleanup. A new cycle begins only if:
    1.  No other cleanup is currently "In Progress."
    2.  The time elapsed since the last completion exceeds the `MOSAICOD_CLEANUP_TIME_INTERVAL`.

:::note
This cooperative model ensures that even with dozens of daemon instances, the object storage is never overwhelmed by simultaneous deletion requests.
:::
