# mosaicod wire-protocol bindings

`v1/*_pb2.py`/`*_pb2.pyi` are generated from the schema shared with the
`mosaicod` server at the repo root (`proto/mosaico/v1/*.proto` — the same
files `mosaicod-proto`'s Rust build.rs consumes). They define the `do_action`
request/response messages and the Arrow Flight `cmd`/`app_metadata` payloads
this SDK speaks on the wire.

They're committed rather than generated at install/build time so that
installing this SDK (e.g. via `pip`/`poetry`) never requires `protoc` on the
end user's machine — only SDK maintainers regenerating the bindings need it
installed locally.

## Regenerating

With `protoc` installed (e.g. `apt install protobuf-compiler`):

```sh
./scripts/compile_protos.sh
```

Run this after any change to `../../../proto/mosaico/v1/*.proto`, and commit
the resulting diff under `v1/`.
