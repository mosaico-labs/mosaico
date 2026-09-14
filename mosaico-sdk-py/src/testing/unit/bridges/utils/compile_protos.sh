#!/usr/bin/env bash
# Compiles every .proto file in proto/ into its associated generated Python module
# under generated/<name>_pb2.py. See README.md for why these are committed.
#
# Usage: ./compile_protos.sh
# Requires `protoc` on PATH (see README.md for install options).

set -euo pipefail

if ! command -v protoc >/dev/null 2>&1; then
  echo "error: protoc not found on PATH. See README.md for install options." >&2
  exit 1
fi

UTILS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="${UTILS_DIR}/proto"
GENERATED_DIR="${UTILS_DIR}/generated"

mkdir -p "${GENERATED_DIR}"

proto_files=("${PROTO_DIR}"/*.proto)

protoc -I "${PROTO_DIR}" \
  --python_out="${GENERATED_DIR}" \
  --pyi_out="${GENERATED_DIR}" \
  "${proto_files[@]}"

for proto_file in "${proto_files[@]}"; do
  name="$(basename "${proto_file}" .proto)"
  echo "compiled ${name}.proto -> generated/${name}_pb2.py, generated/${name}_pb2.pyi"
done
