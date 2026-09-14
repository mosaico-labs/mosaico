---
title: Store Optimizer
sidebar_position: 10
description: "How mosaicod's store optimization routine rewrites a topic's small chunks into fewer, larger ones. Covers the mosaicod store-optimizer subcommand, its options, the memory pool environment variable, distributed coordination, and the internal rewrite mechanics."
---

# Store Optimization Routine

Data streamed into `mosaicod` is written incrementally: as a session progresses, a topic accumulates many small Parquet chunks in the object store rather than a single large file. This keeps ingestion latency low, but over time it leaves topics fragmented across a large number of small files, which hurts query and retrieval performance.

The **Store Optimization Routine** rewrites a topic's small chunks into fewer, larger ones, without ever touching the original files until the rewrite is complete. It is run through the `mosaicod store-optimizer` subcommand, independently from the `mosaicod server` process.

## Running the routine

The routine is started with the `mosaicod store-optimizer` subcommand and configured through its options:

| Option | Default | Description |
| :--- | :--- | :--- |
| `--time-interval <SECONDS>` | `0` | Minimum interval between an optimization run and the next one. When set to `0` a single run is performed and then the process terminates. Any value greater than `0` runs the routine in a loop, sleeping `time_interval` seconds between runs. |
| `--max-chunk-size <BYTES>` | `256000000` | Maximum size (in bytes) for an output chunk after optimization. This is a soft limit: actual files in the store may exceed this value slightly. |

Run a single optimization pass and exit:

```bash
mosaicod store-optimizer
```

Run the routine continuously, e.g. once a day:

```bash
mosaicod store-optimizer --time-interval 86400
```

Cap rewritten chunks to 128 MB:

```bash
mosaicod store-optimizer --max-chunk-size 134217728
```

Memory used while rewriting chunks can be capped with the [`MOSAICOD_STORE_OPTIMIZER_MEMORY_POOL_SIZE`](env.md#store-optimizer) environment variable.

On completion, the routine logs how many topics were optimized successfully and how many failed, along with the locator and error of each failure.

See the [CLI reference](cli.md#mosaicod-store-optimizer) for more examples.

## Distributed Coordination

As with the [cleanup routine](cleanup.md#distributed-coordination), you may run several `mosaicod store-optimizer` processes at once. Leasing a topic before rewriting it ensures two instances never optimize the same topic concurrently: an instance that picks up an already-leased topic simply skips it and moves on to the next one in the queue.

## How it works

Each optimization run:

1. Reclaims topics whose optimization lease has expired (a previous run likely crashed or was killed mid-optimization) and re-scans the database for topics that need optimizing, queuing them for processing.
2. Acquires queued topics one at a time. Acquiring a topic leases it, so concurrent `store-optimizer` instances don't race on the same topic, and allocates a fresh location that the rewritten data will be written to, leaving the topic's current files untouched until the new ones are ready.
3. Reads all of the topic's existing chunks, sorts the rows chronologically, and re-encodes them into new chunks, flushed once they reach roughly `--max-chunk-size` bytes. Schema and field-level metadata attached by the client at ingestion time is preserved verbatim across the rewrite.
4. Once every chunk is written, the topic's database record is atomically updated: old chunk stats are replaced with the new ones, the topic is switched over to the freshly written data, and it is removed from the optimization queue.

If optimizing a topic fails, it is simply dropped from the queue, rather than left leased, so it is picked up again on a future run; the topic's original data is never modified, so a failed run is always safe to retry.
