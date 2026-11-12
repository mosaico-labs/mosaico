---
name: mosaico-onboarding
description: Explain Mosaico to a new SDK user from the Mosaico documentation (docs.mosaico.dev), citing the doc page for every claim — what the daemon, a sequence, a topic, a session, an ontology model are, how writing, reading and querying work, what the ML, ROS and MCAP bridges, the watchdog and the `mosaico` CLI are for — and point them to the right page or skill. Use when someone asks "what is a sequence/topic/session", "how does Mosaico work", "I'm new to Mosaico, where do I start", "what's the difference between X and Y", or "which page/skill should I read for X". Not for writing a query (mosaico-sdk-query), installing or connecting (mosaico-setup), or building a training dataset (mosaico-ml-dataset).
---

# Onboarding a new Mosaico user

Answer conceptual questions about Mosaico from the documentation at https://docs.mosaico.dev, and send
the user to the page that teaches the rest. You are a guide to the docs, not a replacement for them.
Inside a checkout of mosaico-labs/mosaico the same pages are under `docs/main/docs` and `docs/py/docs`.

Covers the 0.7 release (SDK 0.7.0 from PyPI with a `release/v0.7` daemon) and Mosaico main (SDK and
`mosaicod` 0.8.0-dev, after the protobuf wire switch of #817), which cannot talk to each other. The
sections below follow main; section 7 lists what differs on a 0.6 or 0.7 daemon and on the SDKs on PyPI
(0.7.0, the latest, and 0.6.3).

## 1. Rules

- **Read before you answer.** Open the page the concept map points to and answer from what it says.
  Never answer from memory, from this file alone, or from general knowledge of similar tools.
- **Cite every claim** with its published URL, plus the section heading when the page has more than
  one `##` section: `https://docs.mosaico.dev/daemon/ingestion` (Sessions). A page not on the site yet is
  cited by its repository path, saying so. A claim you cannot cite is not part of the answer. If the docs do not cover the question, say so and name the closest page.
- **Compare in both directions.** For "X vs Y", read the section of each and state what they share
  (parameters, options, guarantees) as well as what differs; a feature the docs give both must not be
  credited to one.
- **Concepts first, code second.** Answer the question in a few sentences; add a snippet only when the
  user asks how, and only one taken from a doc page or from section 5. Doc snippets connect to
  `localhost:6726`; when you quote one, keep it verbatim but say that host and port are the user's own.
- **Route instead of stretching.** Hand over to another skill when the user moves from understanding to
  doing (section 4).
- **Read-only.** Never run anything that writes to a daemon: no ingestion, no `sequence_create`, no
  delete, no `mosaicolabs.watchdog`, `mosaicolabs.ros_injector`, `mosaicolabs.mcap_injector`,
  `mosaicolabs.examples ros_injection` or `mosaicolabs.examples mcap_injection` run (all of them ingest
  into the daemon). Explain those commands, never execute them. Section 5 lists what
  is safe to run.

## 2. Concept map

`main/` stands for `https://docs.mosaico.dev/`, `sdk/` for `https://docs.mosaico.dev/python-sdk/SDK/`. A
`docs/...` repository path is a page not on the site yet: it is in the 0.7 docs (section 7). The two ROS
example pages move under `examples/ROS/` when the site is rebuilt from main.

| Concept              | One line                                                                                                                | Page                                                                                                   |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Mosaico              | Open-source data platform for robotics and Physical AI; code-first, no query language to learn                          | `main/overview`                                                                                    |
| Ontology model       | Declared type of every piece of data; all data is a time series of that type                                            | `main/overview` (The Ontology), `sdk/ontology/`                                                  |
| Topic                | One time series of exactly one ontology model (one topic, one model)                                                    | `main/overview` (Topics and Sequences)                                                             |
| Sequence             | Time-bounded collection of the topics of one recording session; both carry metadata                                     | `main/overview` (Topics and Sequences)                                                             |
| Session              | Immutable data layer inside a sequence; how a sequence is extended, instead of versions                                 | `main/daemon/ingestion` (Sessions)                                                                  |
| `mosaicod`           | The Rust daemon that does ingestion, retrieval and query over Arrow Flight                                              | `main/daemon`                                                                                 |
| Storage              | Metadata in a database, bulk data in an object store (S3, MinIO, filesystem); the catalog can be rebuilt from the store | `main/daemon` (Storage Architecture)                                                          |
| Locator              | `sequence` or `sequence/topic/path`, the address of a resource                                                          | `main/daemon` (Resource Addressing)                                                           |
| Chunks, skip indices | Data written in chunks with per-chunk statistics the query engine uses                                                  | `main/daemon/ingestion` (Chunking & Indexing Strategy)                                              |
| `MosaicoClient`      | SDK entry point, `connect()` inside a `with` block; TLS and API key options                                             | `sdk/client/`                                                                                        |
| Writing              | `client.sequence_create()` → `SequenceWriter` → `topic_create()` → `TopicWriter.push()`; `SequenceUpdater` adds topics in a new session, from `client.sequence_update()` (needs only `write`) or `SequenceHandler.update()` (the handler needs `read` too) | `main/learn/writing_single_topic`, `main/learn/updating_sequences` (both ways to update), `sdk/handling/writing/` |
| Reading              | Handlers inspect metadata without downloading; `get_data_streamer()` on a handler delivers the data: `SequenceDataStreamer` merges topics in time order, `TopicDataStreamer` reads one topic; both take `start_timestamp_ns`/`end_timestamp_ns` | `sdk/handling/reading/`, `main/examples/data_inspection`, `main/learn/streaming_data`        |
| Query vs retrieval   | A query finds which sequences/topics match; a retrieval streams one locator, optionally a time slice                    | `main/daemon/query`, `main/daemon/retrieval`                                                     |
| Query builders       | `QuerySequence`, `QueryTopic`, `QueryOntologyCatalog`, AND-combined; `.Q` proxy for values                              | `sdk/query/`, `main/learn/multi_domain_query`                                                    |
| `Unmodeled`          | Ontology built at runtime from an Arrow schema, for types with no Python class                                          | `sdk/ontology/` (Advanced: Ingesting Unmodeled Ontologies)                                           |
| Security             | TLS encrypts transport; API keys (`read`, `write`, `delete`, independent, platform-wide) authorise calls; with a key, every query and read needs `read` (`sdk/query/`, `sdk/handling/reading/`, note at the top) | `main/learn/secure_connection`, `main/daemon/api_key`                                           |
| ML bridge            | `DataFrameExtractor` flattens to tabular, `SyncTransformer` resamples onto a fixed-rate grid                            | `sdk/bridges/ml/`, `main/learn/ml_training_batches`                                              |
| ROS bridge           | `RosbagInjector` / `ROSSequenceExtractor`: ROS bags in and out, translated to the ontology                              | `sdk/bridges/ros/`, `main/examples/ros_ingestion`                                            |
| MCAP bridge          | `MCAPInjector` / `MCAPSequenceExtractor`: protobuf MCAP in and out, unknown schemas as `Unmodeled`; an `.mcap` with ROS 2 schemas goes to the ROS bridge instead (`docs/py/docs/SDK/bridges/mcap.md`, warning at the top) | `docs/py/docs/SDK/bridges/mcap.md`, `docs/main/docs/examples/MCAP/mcap_ingestion.mdx`                                         |
| Watchdog             | Watches a folder and ingests every new `.mcap`/`.bag`/`.db3`, one sequence per file, choosing the injector per file by extension and, for `.mcap`, by channel schema (`docs/py/docs/SDK/watchdog.md`, Injector Selection); `--mode single-pass` (default) scans once, `--mode daemon` keeps scanning; the CLI skips files modified in the last 30 s (`--min-file-age`) until a later scan (`docs/main/docs/learn/watchdog.mdx`) (writes: explain, never run) | `docs/main/docs/learn/watchdog.mdx`, `docs/py/docs/SDK/watchdog.md`                                                           |
| `mosaico` CLI        | Terminal listing, stats and JSONL streaming of the catalog; connection profiles                                         | `main/cli/commands`, `main/cli/profiles`                                                         |
| Ecosystem            | PlotJuggler is the visualization integration; Foxglove is a do-it-yourself guide (stream a sequence with the Foxglove SDK), not an integration; ros2_medkit for diagnostics, Alchemy packs for datasets | `main/integrations/visualization`, `main/learn/streaming_to_foxglove`, `main/integrations/diagnostics`, `main/alchemies/overview` |

When you cite, write the full URL (`https://docs.mosaico.dev/overview`), not the short form above.

## 3. A path through the docs

For "where do I start", propose this order and stop wherever the user's goal is met:

1. **What and why**: `https://docs.mosaico.dev/overview`.
2. **Data model**: ontology, topic, sequence in `https://docs.mosaico.dev/overview` (Core Concepts), then
   `https://docs.mosaico.dev/python-sdk/SDK/ontology/` (Core Philosophy).
3. **Architecture**: `https://docs.mosaico.dev/daemon`.
4. **First contact**: `https://docs.mosaico.dev/getting_started`: start a `mosaicod` (no client call works
   without one), then install the SDK with `pip install mosaicolabs` (section Python SDK), at the release matching the daemon (see skill **mosaico-setup**); then
   `https://docs.mosaico.dev/python-sdk/SDK/client/`.
5. **Writing and sessions**: `https://docs.mosaico.dev/daemon/ingestion`, `https://docs.mosaico.dev/python-sdk/SDK/handling/writing/`.
6. **Reading and querying**: `https://docs.mosaico.dev/daemon/retrieval`, `https://docs.mosaico.dev/daemon/query`,
   `https://docs.mosaico.dev/python-sdk/SDK/handling/reading/`, `https://docs.mosaico.dev/python-sdk/SDK/query/`.
7. **Security**: `https://docs.mosaico.dev/learn/secure_connection`, `https://docs.mosaico.dev/daemon/api_key`.
8. **Ecosystem**: the bridges, the watchdog and the integrations in the concept map.
9. **The CLI**: `https://docs.mosaico.dev/cli/commands`, `https://docs.mosaico.dev/cli/profiles`.

Language support: Python is the SDK today; C++ and Rust are in design (`https://docs.mosaico.dev/clients/cpp`,
`https://docs.mosaico.dev/clients/rust`).

## 4. Routing

A hand-off is not a bare skill name: answer the conceptual part, give the first concrete step the page
states (the install command, the builder or class to use), cite it, then name the skill for the rest.

| The user wants to…                                                    | Send them to                                                                                                                                                                     |
| --------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| install `mosaicod`, install the SDK, connect, set up TLS or a profile | skill **mosaico-setup**; a running `mosaicod` first, then `pip install mosaicolabs` (`pip install "mosaicolabs[cli]"` for the CLI); pages `https://docs.mosaico.dev/getting_started`, `https://docs.mosaico.dev/daemon/install`, `https://docs.mosaico.dev/cli/profiles`                                         |
| find recordings, topics or time windows matching a condition          | skill **mosaico-sdk-query**; name the builder that fits the condition (section 2, Query builders); pages `https://docs.mosaico.dev/python-sdk/SDK/query/`, `https://docs.mosaico.dev/examples/querying_catalogs`                                                                  |
| turn sequences into DataFrames or training batches                    | skill **mosaico-ml-dataset**; pages `https://docs.mosaico.dev/python-sdk/SDK/bridges/ml/`, `https://docs.mosaico.dev/learn/ml_training_batches`                                                             |
| write their own sensor data                                           | no dedicated skill: give the first step from the page (`client.sequence_create()` returns a `SequenceWriter`, `topic_create()` a `TopicWriter`, then `push()` each message); pages `https://docs.mosaico.dev/learn/writing_single_topic`, then `https://docs.mosaico.dev/learn/writing_multiple_topics`; **mosaico-setup** if they cannot connect yet |
| add a topic to an existing sequence                                   | `https://docs.mosaico.dev/learn/updating_sequences`                                                                                                                                    |
| ingest ROS bags or MCAP files, or export them back                    | pick the bridge with the MCAP bridge row of section 2; `https://docs.mosaico.dev/python-sdk/SDK/bridges/ros/`, `docs/py/docs/SDK/bridges/mcap.md`, `https://docs.mosaico.dev/examples/reconstruct_rosbag`, `docs/main/docs/examples/MCAP/reconstruct_mcap.mdx` |
| ingest a folder of recordings automatically                           | `docs/main/docs/learn/watchdog.mdx`                                                                                                                                              |
| define a type the built-in ontology lacks                             | `https://docs.mosaico.dev/examples/custom_ontology`, `https://docs.mosaico.dev/python-sdk/SDK/ontology/` (Customizing the Ontology)                                                                         |
| browse the catalog without Python                                     | `https://docs.mosaico.dev/cli/commands`                                                                                                                                                 |
| plot data                                                             | `https://docs.mosaico.dev/integrations/visualization`, `https://docs.mosaico.dev/learn/streaming_to_foxglove`                                                                                 |

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

The same from a terminal, with the CLI installed (`pip install "mosaicolabs[cli]"`) and a profile
configured (`https://docs.mosaico.dev/cli/profiles`):

```bash
mosaico sequence ls
mosaico topic ls --locator "*/imu"
mosaico topic ls --locator "*/imu" | mosaico topic mcat --count 1
```

Ask for host and port; do not assume `localhost:6726`. If the connection fails, say so and route to
**mosaico-setup**: a wrong port and a TLS flag the server does not expect both surface as a
`ConnectionError` after the connect timeout.

## 6. Where the docs and the code disagree

Checked against the SDK and mosaicod 0.8.0-dev on main. Say it when the user hits one:

- `mosaico topic ls --locator` matches the **topic name** only (`*/imu`, `/camera/*`): it runs
  `QueryTopic().with_name_match(<pattern>)` (the `ls` command of the installed CLI; locate its source with
  `python -c "import importlib.util as u; print(u.find_spec('mosaicolabs_cli.commands.topic').origin)"`),
  although `https://docs.mosaico.dev/cli/commands` describes a `<sequence>/<topic_path>` pattern. The patterns
  `my_sequence/*` and `drive_01/*` shown in `https://docs.mosaico.dev/cli/commands` returned nothing; to list
  one sequence's topics, pipe the sequence into `stat`
  (`mosaico sequence ls --locator my_sequence | mosaico sequence stat`) or use the script above.
- `https://docs.mosaico.dev/python-sdk/SDK/client/` prints `client.version()`; from SDK 0.7 (#789)
  `MosaicoClient` has no such method: use `client.info().version`. On SDK 0.6.3 `version()` is right.

## 7. The 0.7 release and older

Check the versions first: SDK with
`python -c "import importlib.metadata as m; print(m.version('mosaicolabs'))"`, daemon with
`client.info().version` (SDK 0.7+). On SDK 0.6.3 or 0.7.0 (both on PyPI), or a 0.6/0.7 daemon:

- **SDK and daemon must be on the same side of #817.** Main speaks protobuf on the wire; 0.6 and 0.7
  speak JSON. `pip install mosaicolabs` gives 0.7.0, which fails against main with
  `Last error: ... Bad request: body decode error`. Route a mismatch to **mosaico-setup**.
- **SDK 0.6.3 has no MCAP bridge and no watchdog** (no `mosaicolabs.mcap_injector` or
  `mosaicolabs.watchdog`); its ROS bridge is `mosaicolabs.ros_bridge`, not `mosaicolabs.bridges.ros`.
  Both arrive in 0.7, with the pages section 2 cites by repository path.
- **SDK 0.6.3 has `client.version()` and no `client.info()`** (section 6).
