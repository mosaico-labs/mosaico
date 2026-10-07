---
title: Watchdog
description: Automatic ingestion of MCAP and ROS bag files
---

The **Watchdog** module ingests MCAP and ROS bag files into Mosaico automatically. It watches a folder, subfolders included, and loads every new `.mcap`, `.bag` or `.db3` file it finds through the [ROS Bridge](bridges/ros.md) or the [MCAP Bridge](bridges/mcap.md), with no need to launch an injector by hand for each file.

!!! info "API-Keys"
    The Watchdog relies on the bridges' injectors, which employ the mosaico [Writing Workflow](handling/writing.md): when the connection uses an [API-Key](client.md#2-authentication-api-key), the key must have the `write` permission.

!!! example "Try-It Out"
    See the **[Automatic Ingestion with the Watchdog](https://docs.mosaico.dev/learn/watchdog)** guide for an overview and typical use cases.

## Architecture

The Watchdog is an orchestrator built around a **Producer** and a **Consumer** sharing a queue:

```text
File Source → FileStateStore → Producer → Queue → Consumer → InjectorDispatcher → Injector → Mosaico
```

1. The **Producer** asks the **File Source** for the candidate files, and drops the ones the **FileStateStore** already knows: loaded, quarantined, or already in the queue.
2. Each remaining file is marked `in_queue` and appended to the queue.
3. The **Consumer** takes one file at a time, asks the **InjectorDispatcher** for the right injector, runs it against the Mosaico server, and records the outcome (loaded or quarantined) in the FileStateStore.

### Execution Modes

| Mode | Behavior | Threads |
| --- | --- | --- |
| `single-pass` (default) | Scans the folder once, injects every new file, then returns. | None: scan and injections run one after the other in the calling thread. |
| `daemon` | Scans the folder every `polling_interval_s` seconds and injects new files as they appear, until Ctrl-C or an error stops it. | The Producer runs in its own thread, the Consumer in the calling thread. |

* **One injection at a time**: there is exactly one Consumer.
* **Bounded queue**: in `daemon` mode the queue holds at most `max_queue_size` files, and the remaining new files wait for the next scan. In `single-pass` mode the queue is unbounded.
* **Shared state**: the FileStateStore is used by both threads and protected by a lock. A failed scan is logged and does not stop the Producer.
* **Ctrl-C**: in both modes, the injector cancels the file being loaded (its partial sequence is removed), the file is not marked as loaded, and `run()` raises `KeyboardInterrupt` once the Producer thread is joined.

### The Components

* **File Source** (`FileSource` protocol): lists the candidate files of a location, and gives a local path to read each of them (`get_local_path()`). Each file is described by an immutable `FileRef` (`relative_path`, `uri`, `size`, `mtime_ns`), identified by its path relative to the watched folder. `LocalFileSource` scans a local folder recursively and keeps only the files that:
    * have a supported extension (`.mcap`, `.bag`, `.db3`);
    * match `glob_pattern`, if set;
    * were not modified in the last `min_file_age_s` seconds, if set, since they may still be written.
* **FileStateStore**: remembers what happened to each file (see [State Files](#state-files)).
* **InjectorDispatcher**: chooses the injector of each file (see [Injector Selection](#injector-selection)).
* **Watchdog**: the orchestrator. It runs the Producer and the Consumer, builds the configuration of each injector, and applies the `on_error` policy.

### State Files

The FileStateStore keeps two plain text files inside the watched folder, with one path per line, relative to the folder (e.g. `run_01/drive.mcap`):

* `.already_loaded.txt`: the files loaded successfully. A line is appended right after each successful injection, so a crash loses at most one file.
* `.quarantine_file.txt`: the files whose injection failed. Removing a line makes the Watchdog try that file again.

Both files are read again at every scan, so manual edits take effect while a daemon is running. The `in_queue` state (files queued or being loaded) lives in memory only. Since the Watchdog writes these files, the watched folder must be writable, and an unreadable state file makes the Watchdog refuse to start rather than reload every file.

!!! warning "One folder, one server"
    The state files do not record the server address: a folder is tracked for one Mosaico server only, and loading it into another server would skip the files already listed.

### Injector Selection

| Extension | Injector |
| --- | --- |
| `.bag`, `.db3` | [`RosbagInjector`][mosaicolabs.bridges.ros.RosbagInjector] |
| `.mcap` | [`RosbagInjector`][mosaicolabs.bridges.ros.RosbagInjector] if the file has at least one channel and every channel has a ROS 2 schema (`ros2msg` or `ros2idl`), [`MCAPInjector`][mosaicolabs.bridges.mcap.MCAPInjector] otherwise |

`MCAPInjector` skips the channels it cannot decode, so an `.mcap` file mixing encodings may be loaded only partially.

### Sequence Naming

Each file is ingested as a new sequence named after its path relative to the watched folder: folders are joined with `-`, followed by `__` and the extension, so `run_01/drive.mcap` becomes `run_01-drive__mcap`. Characters not accepted in a sequence name become `_`. Since the extension is part of the name, `drive.mcap` and `drive.bag` do not collide. Different paths can still produce the same name (e.g. `a-b/c.mcap` and `a/b-c.mcap`): the second file then fails because the sequence already exists, and is handled by `on_error`.

## Configuration

The Watchdog is driven by `WatchdogConfig`. Note that `mode` and `on_error` must be passed as enums (`WatchdogMode`, `WatchdogError`): plain strings are not converted.

| Parameter | Default | Description |
| --- | --- | --- |
| `path_to_monitor` | required | Folder to watch, subfolders included. It must exist and be writable. |
| `mode` | `WatchdogMode.SINGLEPASS` | `SINGLEPASS` or `DAEMON`. |
| `polling_interval_s` | `5.0` | Seconds between the start of two scans (`daemon` only). |
| `glob_pattern` | `None` | Only files matching this pattern, at any depth (e.g. `"run_*/*.mcap"`). |
| `min_file_age_s` | `None` | Skip the files modified less than this many seconds ago. |
| `on_error` | `WatchdogError.QUARANTINE` | What to do when an injection fails (see below). |
| `retry_number` | `2` | Retries before quarantining a file, with `WatchdogError.RETRY`. |
| `max_queue_size` | `100` | Maximum number of files waiting to be injected (`daemon` only). |
| `host`, `port` | `"localhost"`, `6726` | Address of the Mosaico server. |
| `mosaico_api_key`, `tls_cert_path`, `enable_tls` | `None`, `None`, `False` | Authentication and TLS settings. |
| `log_level` | `"INFO"` | Logging verbosity, also passed to the injectors. |

### Error Handling (`on_error`)

| Policy | Behavior |
| --- | --- |
| `WatchdogError.QUARANTINE` (default) | Quarantine the file at the first failure and move on. |
| `WatchdogError.RETRY` | Try the file again up to `retry_number` more times, then quarantine it. |
| `WatchdogError.RAISE` | Stop the Watchdog and re-raise the error from `run()`. |

Each injection runs with [`SessionLevelErrorPolicy.Delete`][mosaicolabs.enum.SessionLevelErrorPolicy.Delete], so a failed injection leaves no partial sequence behind and a retry starts clean.

### Practical Example: Programmatic Usage

```python
from pathlib import Path

from mosaicolabs.watchdog.watchdog import (
    Watchdog,
    WatchdogConfig,
    WatchdogError,
    WatchdogMode,
)

config = WatchdogConfig(
    path_to_monitor=Path("/data/recordings"),
    mode=WatchdogMode.DAEMON,
    glob_pattern="*.mcap",  # Only MCAP files, at any depth
    min_file_age_s=30.0,  # Skip the files that may still be written
    on_error=WatchdogError.RETRY,
    retry_number=2,
    host="localhost",
    port=6726,
)

watchdog = Watchdog(config)
watchdog.run()  # Blocks until Ctrl-C or an error (daemon mode)
```

### CLI Usage

The Watchdog is installed with the SDK as the `mosaicolabs.watchdog` command. The full list of options can be retrieved by running `mosaicolabs.watchdog -h`.

```bash
# Single Pass: ingest every new file, then exit
mosaicolabs.watchdog /data/recordings

# Daemon: keep watching the folder, retrying failed files twice before quarantining them
mosaicolabs.watchdog /data/recordings \
  --mode daemon \
  --polling-interval 10 \
  --glob-pattern "run_*/*.mcap" \
  --on-error retry \
  --retry-number 2 \
  --host localhost \
  --port 6726
```

Unlike `WatchdogConfig`, the CLI skips by default the files modified in the last 30 seconds (`--min-file-age`). The API key can be passed with `--api-key` or, to keep it out of the shell history, through the `MOSAICO_API_KEY` environment variable. The command exits with code `130` on Ctrl-C, and with code `1` if the Watchdog cannot start or stops on an error with `--on-error raise`.

## Known Issues & Limitations

* **Local folders only**: files stored in an object store cannot be ingested.
* **One Watchdog per folder**: two Watchdogs running on the same folder are not prevented from loading the same file.
* **Server unreachable**: every injection fails, and the files are handled by `on_error` (quarantined with the default policy).
* **Existing sequences**: a file whose sequence already exists on the server fails, and is handled by `on_error`.
* **SIGTERM**: unlike Ctrl-C, SIGTERM (e.g. `docker stop`) terminates the process at once, possibly in the middle of an upload.
* **ROS 1 MCAP files**: not supported, since `MCAPInjector` cannot decode their channels.
* **File age heuristic**: a copy preserving the original modification time (`cp -p`, `rsync -t`) looks old, and a writer pausing for longer than `min_file_age_s` can be read half written.
* **No run report**: `run()` returns nothing; the outcome of each file is in the state files and in the logs.
