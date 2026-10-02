#!/usr/bin/env bash
# Compiles the shared mosaicod wire-protocol schema (repo-root `proto/mosaico/v1/*.proto`,
# the same schema `mosaicod-proto`'s Rust build.rs consumes) into Python bindings under
# `src/mosaicolabs/proto/v1/*_pb2.py`. See src/mosaicolabs/proto/README.md for why these
# are committed.
#
# Usage: ./scripts/compile_protos.sh
# Requires `protoc` on PATH (e.g. `apt install protobuf-compiler`).
#
# Note: `protoc`'s Python codegen writes cross-file imports as absolute imports matching
# the proto file's path (`from mosaico.v1 import time_pb2 as ...`), derived from the `-I`
# include root used to resolve `import "mosaico/v1/time.proto";` statements inside the
# .proto files themselves (shared with the Rust side, so that path can't change). Since we
# want the generated files to live at `mosaicolabs.proto.v1`, not a top-level `mosaico`
# package, this script generates into a scratch directory and rewrites those imports before
# moving the files into place.

set -euo pipefail

if ! command -v protoc >/dev/null 2>&1; then
  echo "error: protoc not found on PATH. Install it, e.g. 'apt install protobuf-compiler'." >&2
  exit 1
fi

SDK_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_ROOT="$(cd "${SDK_ROOT}/../proto" && pwd)"
PROTO_DIR="${PROTO_ROOT}/mosaico/v1"
OUT_DIR="${SDK_ROOT}/src/mosaicolabs/proto/v1"

SCRATCH_DIR="$(mktemp -d)"
trap 'rm -rf "${SCRATCH_DIR}"' EXIT

mkdir -p "${OUT_DIR}"

proto_files=("${PROTO_DIR}"/*.proto)

protoc -I "${PROTO_ROOT}" \
  --python_out="${SCRATCH_DIR}" \
  --pyi_out="${SCRATCH_DIR}" \
  "${proto_files[@]}"

for generated_file in "${SCRATCH_DIR}"/mosaico/v1/*_pb2.py "${SCRATCH_DIR}"/mosaico/v1/*_pb2.pyi; do
  [ -e "${generated_file}" ] || continue
  # Rewrite the absolute `mosaico.v1` package imports protoc emits for cross-file
  # references into the flat `mosaicolabs.proto.v1` layout this SDK actually uses.
  sed -i 's/^from mosaico\.v1 import/from mosaicolabs.proto.v1 import/' "${generated_file}"
  cp "${generated_file}" "${OUT_DIR}/"
done

for proto_file in "${proto_files[@]}"; do
  name="$(basename "${proto_file}" .proto)"
  echo "compiled ${name}.proto -> src/mosaicolabs/proto/v1/${name}_pb2.py, ${name}_pb2.pyi"
done
