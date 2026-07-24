# Changelog


## [0.6.0] - 2026-24-07

This release substantially expands the **query engine** (new operators, regex, complex column and list support, glob patterns on user metadata), introduces the **topic clustering and intersection actions**, and completes a major effort to **modularize the server into separate crates**. It also includes the switch to the **Apache-2.0** license and several fixes to critical race conditions.

### Breaking Changes

- **Granular API key permissions**: API key permissions are now granular and the `Manage` permission has been removed. ([#615](https://github.com/mosaico-labs/mosaico/pull/615))
- **`name` replaces `locator` in queries**: the `locator` field has been renamed to `name` inside queries. Update any query that referenced it. ([#565](https://github.com/mosaico-labs/mosaico/pull/565))
- **Removed the `include_timestamp_range` option** from the query request. ([#604](https://github.com/mosaico-labs/mosaico/pull/604))
- **`match` operator now accepts only a simplified regex**: previously any regex was accepted; the operator now supports a restricted, custom simplified regex syntax. Queries relying on full regex features must be rewritten. Applies to sequence name and topic name as well. ([#622](https://github.com/mosaico-labs/mosaico/pull/622), [#543](https://github.com/mosaico-labs/mosaico/pull/543))
- **Glob patterns `*` and `**` in user metadata queries**: `*` and `**` are now interpreted as wildcards when querying user metadata, changing the matching behavior for keys that contain these characters. ([#547](https://github.com/mosaico-labs/mosaico/pull/547))
- **Input validation for user metadata keys**: user metadata keys are now validated on write, so keys that were previously accepted may now be rejected. ([#560](https://github.com/mosaico-labs/mosaico/pull/560))
- **`cleanup` and `server` split into separate subcommands**: the server is no longer started via the top-level command; use the dedicated `server` and `cleanup` subcommands. Update any scripts or service units accordingly. ([#639](https://github.com/mosaico-labs/mosaico/pull/639))
- **Ontology tag is now validated in queries**: the filter previously ignored the ontology tag prefix, so any prefix matched valid fields. Filter tags that do not match the topic's registered ontology tag are now rejected. Queries that relied on the previous lenient behavior will now return an error. ([#539](https://github.com/mosaico-labs/mosaico/pull/539))
- **`session_uuid` renamed to `session_locator` in topic metadata**: the serialized topic metadata (`TopicMetadataProps`/`JsonTopicProperties`) now exposes `session_locator` instead of `session_uuid`, and its value is a locator rather than a UUID. Update any consumer that reads this field. ([#584](https://github.com/mosaico-labs/mosaico/pull/584))

### Features

**Query engine**
- New `outside([a, b])` operator. ([#659](https://github.com/mosaico-labs/mosaico/pull/659))
- Implemented the **missing operators** for ontology queries and for `user_metadata` fields. ([#548](https://github.com/mosaico-labs/mosaico/pull/548), [#530](https://github.com/mosaico-labs/mosaico/pull/530))
- Queries over **complex column types** and over **lists of structs**. ([#578](https://github.com/mosaico-labs/mosaico/pull/578), [#581](https://github.com/mosaico-labs/mosaico/pull/581))

**Clustering & Intersection**
- New **`topic_filter_clusterize`** action: core clustering routine, API and endpoint, with support for **multiple ontology fields**. ([#514](https://github.com/mosaico-labs/mosaico/pull/514), [#517](https://github.com/mosaico-labs/mosaico/pull/517), [#524](https://github.com/mosaico-labs/mosaico/pull/524), [#593](https://github.com/mosaico-labs/mosaico/pull/593))
- New **`topic_filter_intersect`** action. ([#563](https://github.com/mosaico-labs/mosaico/pull/563))

**Response metadata & CLI**
- The `do_get` response now includes **message count** and **timestamp range** in its metadata. ([#594](https://github.com/mosaico-labs/mosaico/pull/594))
- The **ontology tag** is now attached to topics in the query response. ([#598](https://github.com/mosaico-labs/mosaico/pull/598))
- New **CLI** to interact with Mosaico. ([#556](https://github.com/mosaico-labs/mosaico/pull/556))
- Missing query error mapping added to `PublicError`. ([#534](https://github.com/mosaico-labs/mosaico/pull/534))

### Bug Fixes

- Fixed a server error when closing topics with the **`MultiEchoLaserScan`** ontology. ([#654](https://github.com/mosaico-labs/mosaico/pull/654))
- `get_flight_info` and `do_get` now return the **same schema**. ([#662](https://github.com/mosaico-labs/mosaico/pull/662))
- `Utf8View` is now recognized as a **textual type**. ([#669](https://github.com/mosaico-labs/mosaico/pull/669))
- Fixed a **critical race** in `try_cleanup`. ([#632](https://github.com/mosaico-labs/mosaico/pull/632))
- Fixed compilation without a git tag. ([#531](https://github.com/mosaico-labs/mosaico/pull/531))
- Fixed cleanup unit tests getting stuck. ([#515](https://github.com/mosaico-labs/mosaico/pull/515))

### Refactoring & Performance

- **Modularization**: `mosaicod-server` split into multiple crates, the gRPC server extracted into `mosaicod-grpc`, and CLI commands extracted into the `mosaicod-commands` crate. ([#546](https://github.com/mosaico-labs/mosaico/pull/546), [#561](https://github.com/mosaico-labs/mosaico/pull/561), [#643](https://github.com/mosaico-labs/mosaico/pull/643))
- Facade functions and DB queries reworked to **prevent race conditions**; server shuts down on the first service failure. ([#583](https://github.com/mosaico-labs/mosaico/pull/583), [#636](https://github.com/mosaico-labs/mosaico/pull/636))
- `InvalidArgument` returned for invalid keys in user metadata. ([#620](https://github.com/mosaico-labs/mosaico/pull/620))
- Lint fixes and cleanup. ([#618](https://github.com/mosaico-labs/mosaico/pull/618))
