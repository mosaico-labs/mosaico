---
name: mosaico-docs-guide
description: Explain Mosaico to a new SDK user from the Mosaico documentation (docs.mosaico.dev), citing the doc page for every claim — what the daemon, a sequence, a topic, a session, an ontology model are, how writing, reading and querying work, what the ML, ROS, MCAP, LeRobot and ULog bridges, hooks, the watchdog and the `mosaico` CLI (including `mosaico inject` / `extract`) are for — and point them to the right page or skill. Use when someone asks "what is a sequence/topic/session", "how does Mosaico work", "I'm new to Mosaico, where do I start", "what's the difference between X and Y", or "which page/skill should I read for X". Not for writing a query (mosaico-sdk-query), installing or connecting (mosaico-setup), or building a training dataset (mosaico-ml-dataset).
---

# A guide to the Mosaico docs

Answer conceptual questions about Mosaico from the documentation at https://docs.mosaico.dev, and send
the user to the page that teaches the rest. You are a guide to the docs, not a replacement for them.
Inside a checkout of mosaico-labs/mosaico the same pages are under `docs/main/docs` and `docs/py/docs`.

Covers Mosaico 0.8+ and main. The SDK and `mosaicod` share one version and ship together: the user
needs the SDK with the daemon's 0.X (skill **mosaico-setup**). The site documents the latest release;
a feature that arrived after 0.8 carries its first version below. Older daemons: section 7.

## 1. Rules

- **Read before you answer.** Open the page the concept map points to and answer from what it says.
  Never answer from memory, from this file alone, or from general knowledge of similar tools.
- **Cite every claim** with its published URL, plus the section heading when the page has more than
  one `##` section: `https://docs.mosaico.dev/daemon/ingestion` (Sessions). A claim you cannot cite is
  not part of the answer. If the docs do not cover the question, say so and name the closest page. A
  feature with no page yet (today only the ULog bridge) is cited by the source file section 2 names,
  saying there is no page.
- **A moved page never stops you.** The URLs here are a starting point, not a contract. If one returns
  404 or no longer holds the content, find the page by the title section 2 gives it: main pages by
  title in https://docs.mosaico.dev/llms.txt (its links are `/docs/<path>.md`; the page is
  `https://docs.mosaico.dev/<path>`), SDK pages (`/python-sdk/`) by `#` title in
  https://docs.mosaico.dev/python-sdk/llms/llms-full.txt or by path in
  https://docs.mosaico.dev/python-sdk/sitemap.xml, or in the `docs/` tree of a checkout. Cite the
  published page you actually read and tell the user the skill's link is stale.
- **Compare in both directions.** For "X vs Y", read the section of each and state what they share
  (parameters, options, guarantees) as well as what differs; a feature the docs give both must not be
  credited to one.
- **Concepts first, code second.** Answer the question in a few sentences; add a snippet only when the
  user asks how, and only one taken from a doc page or from section 5. Doc snippets connect to
  `localhost:6726`; when you quote one, keep it verbatim but say that host and port are the user's own.
- **Route instead of stretching.** Hand over to another skill when the user moves from understanding to
  doing (section 4).
- **Read-only.** Explain these commands, never run them, even when the user asks: they write to the
  daemon or to local files. Say you did not run it, give the exact command with the user's own host
  and port (or profile) and let the user run it. No ingestion, no `sequence_create`, no delete; no
  `mosaico inject …` or `mosaico extract …`, not even with `--dry-run`; no
  `mosaicolabs.watchdog`, `mosaicolabs.ros_injector`, `mosaicolabs.mcap_injector`,
  `mosaicolabs.lerobot_injector`, `mosaicolabs.ulog_injector` or `mosaicolabs.*_extractor`; no
  `mosaico examples <name>` or `mosaicolabs.examples` run (`mosaico examples ls` only lists them); no
  `mosaicod hook create` or `mosaicod hook rm` (alias `remove`).
  Section 5 lists what is safe to run.

## 2. Concept map

`main/` stands for `https://docs.mosaico.dev/`, `sdk/` for `https://docs.mosaico.dev/python-sdk/SDK/`.
Each page is given as its title and path; the title is how you find it again if the path moves.

| Concept              | One line                                                                                                                                                                                                                                                                                                                                                                                                                                      | Page                                                                                                                                                                                                      |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Mosaico              | Open-source data platform for robotics and Physical AI; code-first, no query language to learn                                                                                                                                                                                                                                                                                                                                                | Overview `main/overview`                                                                                                                                                                                  |
| Ontology model       | Declared type of every piece of data; all data is a time series of that type                                                                                                                                                                                                                                                                                                                                                                  | Overview `main/overview` (The Ontology), Data Models & Ontology `sdk/ontology/`                                                                                                                           |
| Topic                | One time series of exactly one ontology model (one topic, one model)                                                                                                                                                                                                                                                                                                                                                                          | Overview `main/overview` (Topics and Sequences)                                                                                                                                                           |
| Sequence             | Time-bounded collection of the topics of one recording session; both carry metadata                                                                                                                                                                                                                                                                                                                                                           | Overview `main/overview` (Topics and Sequences)                                                                                                                                                           |
| Session              | Immutable data layer inside a sequence; how a sequence is extended, instead of versions                                                                                                                                                                                                                                                                                                                                                       | Ingestion `main/daemon/ingestion` (Sessions)                                                                                                                                                              |
| `mosaicod`           | The Rust daemon that does ingestion, retrieval and query over Arrow Flight                                                                                                                                                                                                                                                                                                                                                                    | Overview (daemon) `main/daemon`                                                                                                                                                                           |
| Storage              | Metadata in a database, bulk data in an object store (S3, MinIO, filesystem); the catalog can be rebuilt from the store                                                                                                                                                                                                                                                                                                                       | Overview (daemon) `main/daemon` (Storage Architecture)                                                                                                                                                    |
| Locator              | `sequence` or `sequence/topic/path`, the address of a resource                                                                                                                                                                                                                                                                                                                                                                                | Overview (daemon) `main/daemon` (Resource Addressing)                                                                                                                                                     |
| Chunks, skip indices | Data written in chunks with per-chunk statistics the query engine uses                                                                                                                                                                                                                                                                                                                                                                        | Ingestion `main/daemon/ingestion` (Chunking & Indexing Strategy)                                                                                                                                          |
| `MosaicoClient`      | SDK entry point, `connect()` inside a `with` block; TLS and API key options                                                                                                                                                                                                                                                                                                                                                                   | Client `sdk/client/`                                                                                                                                                                                      |
| Writing              | `client.sequence_create()` → `SequenceWriter` → `topic_create()` → `TopicWriter.push()`; `SequenceUpdater` adds topics in a new session, from `client.sequence_update()` (needs only `write`) or `SequenceHandler.update()` (the handler needs `read` too)                                                                                                                                                                                    | Writing a Single Topic `main/learn/writing_single_topic`, Updating a Sequence `main/learn/updating_sequences` (both ways to update), The Writing Workflow `sdk/handling/writing/`                         |
| Reading              | Handlers inspect metadata without downloading; `get_data_streamer()` on a handler delivers the data: `SequenceDataStreamer` merges topics in time order, `TopicDataStreamer` reads one topic; both take `start_timestamp_ns`/`end_timestamp_ns`                                                                                                                                                                                               | The Reading Workflow `sdk/handling/reading/`, Data Discovery and Inspection `main/examples/data_inspection`, Streaming Data `main/learn/streaming_data`                                                   |
| Query vs retrieval   | A query finds which sequences/topics match; a retrieval streams one locator, optionally a time slice                                                                                                                                                                                                                                                                                                                                          | Query `main/daemon/query`, Retrieval `main/daemon/retrieval`                                                                                                                                              |
| Query builders       | `QuerySequence`, `QueryTopic`, `QueryOntologyCatalog`, AND-combined; `.Q` proxy for values; aggregators (`.max()`, `.mean()`, `.rate()`, …) test one value computed over a whole topic                                                                                                                                                                                                                                                        | The Query Workflow `sdk/query/`, Multi-Domain Query `main/learn/multi_domain_query`, Query with Aggregators `main/learn/aggregators`                                                                      |
| `Unmodeled`          | Ontology built at runtime from an Arrow schema, for types with no Python class                                                                                                                                                                                                                                                                                                                                                                | Data Models & Ontology `sdk/ontology/` (Advanced: Ingesting Unmodeled Ontologies)                                                                                                                         |
| Security             | TLS encrypts transport; API keys (`read`, `write`, `delete`, independent, platform-wide) authorise calls; with a key, every query and read needs `read` (`sdk/query/`, `sdk/handling/reading/`, note at the top)                                                                                                                                                                                                                              | Secure Connection `main/learn/secure_connection`, API keys `main/daemon/api_key`                                                                                                                          |
| Hooks                | `mosaicod` POSTs an event to every registered URL when a topic is uploaded or a session finalized, so nobody polls; hooks are registered with `mosaicod hook create`; in Python, `HookServer` (`mosaicolabs.hook`) passes each event to the functions it decorates                                                                                                                                                                            | Reacting to New Data with Hooks `main/learn/hooks`, Hooks `main/daemon/hooks`, Hooks `sdk/hook/`, CLI Reference `main/daemon/cli` (mosaicod hook)                                                         |
| ML bridge            | `DataFrameExtractor` flattens to tabular, `SyncTransformer` resamples onto a fixed-rate grid                                                                                                                                                                                                                                                                                                                                                  | Machine Learning & Analytics `sdk/bridges/ml/`, Preparing PyTorch Training Batches `main/learn/ml_training_batches`                                                                                       |
| ROS bridge           | `RosbagInjector` / `ROSSequenceExtractor`: ROS bags in and out, translated to the ontology                                                                                                                                                                                                                                                                                                                                                    | ROS `sdk/bridges/ros/`, ROS Ingestion `main/examples/ROS/ros_ingestion`, Reconstruct ROS bag `main/examples/ROS/reconstruct_rosbag`                                                                       |
| MCAP bridge          | `MCAPInjector` / `MCAPSequenceExtractor`: protobuf MCAP in and out, unknown schemas as `Unmodeled`; an `.mcap` with ROS 2 schemas goes to the ROS bridge instead (MCAP page, warning at the top)                                                                                                                                                                                                                                              | MCAP `sdk/bridges/mcap/`, MCAP Ingestion `main/examples/MCAP/mcap_ingestion`, Reconstruct MCAP `main/examples/MCAP/reconstruct_mcap`                                                                      |
| LeRobot bridge       | `LeRobotInjector` ingests a LeRobot v3.0 dataset folder, one episode to one sequence (0.8+); `LeRobotSequencesExtractor` exports sequences back to a v3.0 dataset (0.9+, like the page)                                                                                                                                                                                                                                                       | LeRobot `sdk/bridges/lerobot/`                                                                                                                                                                            |
| ULog bridge          | PX4 `.ulg` files in, one file to one sequence; ingestion only (0.9+). No doc page yet: the usage is in the module docstring and argparse: `mosaicolabs.ulog_injector <file.ulg> --name <seq> [--host localhost] [--port 6726]` (also `--topics`, `--metadata`, `--topic-metadata`, `--api-key` or `MOSAICO_API_KEY`, `--tls-cert`, `--dry-run`); `mosaico inject ulog` takes the same input and uses the CLI's default profile or `--profile` | source file `mosaico-sdk-py/src/mosaicolabs/bridges/ulog/injector.py` (installed: `mosaicolabs/bridges/ulog/injector.py`)                                                                                 |
| Watchdog             | Watches a folder and ingests every new `.mcap`/`.bag`/`.db3`, one sequence per file, choosing the injector per file by extension and, for `.mcap`, by channel schema (`sdk/watchdog/`, Injector Selection); `--mode single-pass` (default) scans once, `--mode daemon` keeps scanning; the CLI skips files modified in the last 30 s (`--min-file-age`) until a later scan (`main/learn/watchdog`)                                            | Automatic Ingestion with the Watchdog `main/learn/watchdog`, Watchdog `sdk/watchdog/`                                                                                                                     |
| `mosaico` CLI        | Terminal listing, stats and JSONL streaming of the catalog; connection profiles; `mosaico inject {ros,mcap,lerobot}` makes new sequences (`inject ulog` too, 0.9+, not on the page), `--dry-run` never connects; `mosaico extract {ros,mcap}` (`lerobot` 0.9+) writes sequences to local files; `mosaico examples` (0.9+) runs the SDK examples                                                                                               | Commands `main/cli/commands`, Connection Profiles `main/cli/profiles`                                                                                                                                     |
| Ecosystem            | PlotJuggler is the visualization integration; Foxglove is a do-it-yourself guide (stream a sequence with the Foxglove SDK), not an integration; ros2_medkit for diagnostics, Alchemy packs for datasets                                                                                                                                                                                                                                       | Visualization `main/integrations/visualization`, Live Streaming to Foxglove `main/learn/streaming_to_foxglove`, Diagnostics `main/integrations/diagnostics`, Overview (Alchemy) `main/alchemies/overview` |

When you cite, write the full URL (`https://docs.mosaico.dev/overview`), not the short form above.

## 3. A path through the docs

For "where do I start", propose this order and stop wherever the user's goal is met:

1. **What and why**: `https://docs.mosaico.dev/overview`.
2. **Data model**: ontology, topic, sequence in `https://docs.mosaico.dev/overview` (Core Concepts), then
   `https://docs.mosaico.dev/python-sdk/SDK/ontology/` (Core Philosophy).
3. **Architecture**: `https://docs.mosaico.dev/daemon`.
4. **First contact**: `https://docs.mosaico.dev/getting_started`: start a `mosaicod` (no client call works
   without one), then install the SDK with `pip install mosaicolabs` (section Python SDK), at the
   daemon's 0.X (see skill **mosaico-setup**); then `https://docs.mosaico.dev/python-sdk/SDK/client/`.
5. **Writing and sessions**: `https://docs.mosaico.dev/daemon/ingestion`, `https://docs.mosaico.dev/python-sdk/SDK/handling/writing/`.
6. **Reading and querying**: `https://docs.mosaico.dev/daemon/retrieval`, `https://docs.mosaico.dev/daemon/query`,
   `https://docs.mosaico.dev/python-sdk/SDK/handling/reading/`, `https://docs.mosaico.dev/python-sdk/SDK/query/`.
7. **Security**: `https://docs.mosaico.dev/learn/secure_connection`, `https://docs.mosaico.dev/daemon/api_key`.
8. **Ecosystem**: the bridges, `mosaico inject` / `extract`, the watchdog, hooks and the integrations in
   the concept map.
9. **The CLI**: `https://docs.mosaico.dev/cli/commands`, `https://docs.mosaico.dev/cli/profiles`.

Language support: Python is the SDK today; C++ and Rust are in design (`https://docs.mosaico.dev/clients/cpp`,
`https://docs.mosaico.dev/clients/rust`).

## 4. Routing

A hand-off is not a bare skill name: answer the conceptual part, give the first concrete step the page
states (the install command, the builder or class to use), cite it, then name the skill for the rest.

| The user wants to…                                                               | Send them to                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| -------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| install `mosaicod`, install the SDK, connect, set up TLS or a profile            | skill **mosaico-setup**; a running `mosaicod` first, then `pip install "mosaicolabs[cli]==0.X.*"` (0.X = the daemon's); pages `https://docs.mosaico.dev/getting_started`, `https://docs.mosaico.dev/daemon/install`, `https://docs.mosaico.dev/cli/profiles`                                                                                                                                                                                                            |
| find recordings, topics or time windows matching a condition                     | skill **mosaico-sdk-query**; name the builder that fits the condition (section 2, Query builders); pages `https://docs.mosaico.dev/python-sdk/SDK/query/`, `https://docs.mosaico.dev/examples/querying_catalogs`, `https://docs.mosaico.dev/learn/aggregators`                                                                                                                                                                                                          |
| turn sequences into DataFrames or training batches                               | skill **mosaico-ml-dataset**; pages `https://docs.mosaico.dev/python-sdk/SDK/bridges/ml/`, `https://docs.mosaico.dev/learn/ml_training_batches`                                                                                                                                                                                                                                                                                                                         |
| write their own sensor data                                                      | no dedicated skill: give the first step from the page (`client.sequence_create()` returns a `SequenceWriter`, `topic_create()` a `TopicWriter`, then `push()` each message); pages `https://docs.mosaico.dev/learn/writing_single_topic`, then `https://docs.mosaico.dev/learn/writing_multiple_topics`; **mosaico-setup** if they cannot connect yet                                                                                                                   |
| add a topic to an existing sequence                                              | `https://docs.mosaico.dev/learn/updating_sequences`                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ingest ROS bags, MCAP files, LeRobot datasets or ULog files, or export them back | pick the bridge with section 2; from a terminal, `mosaico inject` / `mosaico extract` (`https://docs.mosaico.dev/cli/commands`); pages `https://docs.mosaico.dev/python-sdk/SDK/bridges/ros/`, `https://docs.mosaico.dev/python-sdk/SDK/bridges/mcap/`, `https://docs.mosaico.dev/python-sdk/SDK/bridges/lerobot/`, `https://docs.mosaico.dev/examples/ROS/reconstruct_rosbag`, `https://docs.mosaico.dev/examples/MCAP/reconstruct_mcap`; ULog has no page (section 2) |
| ingest a folder of recordings automatically                                      | `https://docs.mosaico.dev/learn/watchdog`                                                                                                                                                                                                                                                                                                                                                                                                                               |
| run code as soon as new data lands, instead of polling                           | `https://docs.mosaico.dev/learn/hooks`, then `https://docs.mosaico.dev/daemon/hooks` (Delivery, Security: best effort, requests not authenticated)                                                                                                                                                                                                                                                                                                                      |
| define a type the built-in ontology lacks                                        | `https://docs.mosaico.dev/examples/custom_ontology`, `https://docs.mosaico.dev/python-sdk/SDK/ontology/` (Customizing the Ontology)                                                                                                                                                                                                                                                                                                                                     |
| browse the catalog without Python                                                | `https://docs.mosaico.dev/cli/commands`                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| plot data                                                                        | `https://docs.mosaico.dev/integrations/visualization`, `https://docs.mosaico.dev/learn/streaming_to_foxglove`                                                                                                                                                                                                                                                                                                                                                           |

## 5. Showing a concept on the user's daemon (optional, read-only)

When the user has a running daemon and wants to see sequences, topics and ontology tags for real, this
discovery script only reads (handlers fetch metadata, never data: `https://docs.mosaico.dev/examples/data_inspection`):

```python
from mosaicolabs import MosaicoClient

HOST, PORT = "<host>", 6726  # ask the user; 6726 is only the default port

with MosaicoClient.connect(HOST, PORT) as client:
    for name in client.list_sequences():
        seq = client.sequence_handler(name)  # None if the sequence does not exist
        if seq is None:
            continue
        print(name, seq.timestamp_ns_min, seq.timestamp_ns_max, seq.user_metadata)
        for topic in seq.topics:  # names come from the sequence itself
            th = seq.get_topic_handler(topic)  # raises ValueError on an unknown topic or a connection error
            print("   ", topic, th.ontology_tag)  # one topic, one ontology model
            th.close()
        seq.close()
```

The same from a terminal, with the CLI installed (`pip install "mosaicolabs[cli]==0.X.*"`, 0.X = the
daemon's) and a profile configured (`https://docs.mosaico.dev/cli/profiles`):

```bash
mosaico sequence ls
mosaico topic ls --locator "/imu*"
mosaico topic ls --locator "/imu*" | mosaico topic mcat --count 1
```

To show hooks, `mosaicod hook ls` lists the registered ones. It takes no host or port: it needs the
daemon's own environment (database `MOSAICOD_DB_URL` and the store settings), so run it where the
running `mosaicod` is configured, in Docker `docker exec <mosaicod-container> mosaicod hook ls`. Never
use a different `mosaicod` binary: `mosaicod hook` applies its own pending database migrations when it
starts.

Ask for host and port; do not assume `localhost:6726`. If the connection fails, say so and route to
**mosaico-setup**: a wrong port and a TLS flag the server does not expect both surface as a
`ConnectionError` after the connect timeout.

## 6. Where the docs and the code disagree

`https://docs.mosaico.dev/daemon/tls` still turns TLS on with `mosaicod server --tls`, a flag 0.8+ does
not have: TLS is on with `MOSAICOD_TLS_ENABLED=true` (`https://docs.mosaico.dev/daemon/env`, TLS) or
`server.tls.enabled` in the config file (`https://docs.mosaico.dev/daemon/config`). Say it when the user
hits it, and route the setup to **mosaico-setup**.

## 7. Older daemons (0.6, 0.7)

The user needs the SDK of the same 0.X (0.6.3 or 0.7.0 on PyPI). Check the SDK with
`python -c "import importlib.metadata as m; print(m.version('mosaicolabs'))"` and the daemon with
`client.info().version` (SDK 0.7; 0.6.3 has `client.version()` instead). The site describes a newer
release: hooks, aggregators, the LeRobot and ULog bridges and `mosaico inject` / `extract` / `examples`
are not in 0.7, and SDK 0.6.3 also has no MCAP bridge and no watchdog (its ROS bridge is
`mosaicolabs.ros_bridge`, not `mosaicolabs.bridges.ros`).
