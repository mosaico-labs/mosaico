# MCAP - creating supporting schemas for unit tests

## Regenerating the `.desc` files

With `protoc` installed (e.g. `apt install protobuf-compiler`), generate `Python` all `.proto` files at once:

```sh
src/testing/unit/bridges/mcap/utils/compile_protos.sh
```