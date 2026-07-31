---
sidebar_position: 9
---

# Background Cleanup Routine

To ensure high performance and low latency during user operations, Mosaico does not perform physical deletions in object storage in real-time. When a **Sequence** or **Topic** is deleted via the [Management Actions](actions.md#sequence-management), the database record is removed immediately, but the associated storage files remain.

The **Cleanup Routine** is a dedicated process responsible for identifying and permanently purging these orphaned files. It is run through the `mosaicod cleanup` subcommand, independently from the `mosaicod server` process.

## Deletion Lifecycle

The cleanup process operates in two distinct phases to prevent accidental data loss and optimize system resources:

*  **Marking (Soft Deletion):** The routine identifies folders in object storage that no longer have a corresponding entry in the database. These folders are marked for deletion by creating a `TO_DELETE` marker file inside the directory.
*  **Purging (Permanent Deletion):** Once a folder has been marked and the configured retention period has elapsed, the routine permanently removes the folder and all its contents from the object store.

## Running the routine

The routine is started with the `mosaicod cleanup` subcommand and configured through its options:

| Option | Default | Description                                                                                 |
| :--- | :--- |:--------------------------------------------------------------------------------------------|
| `--time-interval <SECONDS>` | `0` | Minimum interval between a cleanup run and the next one. When set to `0` a single cleanup is performed and then the process terminates. Any value greater than `0` runs the routine in a loop, sleeping `time_interval` seconds between runs. |
| `--retention-duration <SECONDS>` | `86400` | The minimum age of a `TO_DELETE` marker before the folder is eligible for permanent removal. Set it to `0` to delete obsolete files right away. |

Run a single cleanup and exit:

```bash
mosaicod cleanup
```

Run the routine continuously, e.g. once a day:

```bash
mosaicod cleanup --time-interval 86400
```

See the [CLI reference](cli.md#mosaicod-cleanup) for more examples.


## Distributed Coordination

In a multi-instance environment you may run several `mosaicod cleanup` processes at once. To prevent race conditions and redundant resource consumption, the instances coordinate via the database:

* **Concurrency Control:** A centralized log history in the database tracks active cleanup sessions. If one instance is already performing a cleanup, other instances will remain idle.
* **Execution Logic:** When an instance wakes up, it checks the timestamp of the last successful cleanup. A new cycle begins only if:
    1.  No other cleanup is currently "In Progress."
    2.  The time elapsed since the last completion exceeds the configured `--time-interval`.

:::note
This cooperative model ensures that even with dozens of cleanup instances, the object storage is never overwhelmed by simultaneous deletion requests.
:::
