# mosaicod wire-protocol bindings

`v1/*_pb2.py`/`*_pb2.pyi` are generated from the schema shared with the
`mosaicod` server at the repo root (`proto/mosaico/v1/*.proto` — the same
files `mosaicod-proto`'s Rust build.rs consumes). They define the `do_action`
request/response messages and the Arrow Flight `cmd`/`app_metadata` payloads
this SDK speaks on the wire.

They're **not** committed to the repo: `../../../build.py` regenerates them
automatically as part of `poetry install`/`poetry sync` (see
`[tool.poetry.build]` in `pyproject.toml`), using `grpcio-tools`'s bundled
`protoc`.
Poetry runs this build step in an isolated build environment, which is why
`grpcio-tools` is also listed under `[build-system].requires`, not just the
dev dependency group.

## Regenerating

Just run `poetry install` (or `poetry run python build.py` directly to
regenerate without a full install).

Note: since the generated files aren't committed, building this package from
an sdist (rather than a prebuilt wheel) requires network access to fetch
`grpcio-tools` during the build.
