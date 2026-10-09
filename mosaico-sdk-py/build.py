"""
Generates the `mosaicolabs.proto.v1` protobuf bindings from the schema shared
with the `mosaicod` server at the repo root (`proto/mosaico/v1/*.proto`).

Run automatically by `poetry install`/`poetry sync` (see `[tool.poetry.build]`
in pyproject.toml), using `grpcio-tools`'s bundled `protoc` -- no system
`protoc` binary required. The generated `_pb2.py`/`_pb2.pyi` files are not
committed to the repo; see src/mosaicolabs/proto/README.md.

Note: `protoc`'s Python codegen writes cross-file imports as absolute imports
matching the proto file's path (`from mosaico.v1 import time_pb2 as ...`),
derived from the `-I` include root used to resolve `import "mosaico/v1/time.proto";`
statements inside the .proto files themselves (shared with the Rust side, so
that path can't change). Since we want the generated files to live at
`mosaicolabs.proto.v1`, not a top-level `mosaico` package, this script
generates into a scratch directory and rewrites those imports before moving
the files into place.
"""

import re
import sys
import tempfile
from pathlib import Path

from grpc_tools import protoc

SDK_ROOT = Path(__file__).resolve().parent
PROTO_ROOT = (SDK_ROOT / ".." / "proto").resolve()
PROTO_DIR = PROTO_ROOT / "mosaico" / "v1"
OUT_DIR = SDK_ROOT / "src" / "mosaicolabs" / "proto" / "v1"

IMPORT_RE = re.compile(r"^from mosaico\.v1 import", re.MULTILINE)


def main() -> None:
    proto_files = sorted(PROTO_DIR.glob("*.proto"))
    if not proto_files:
        raise RuntimeError(f"No .proto files found under {PROTO_DIR}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as scratch_dir_str:
        scratch_dir = Path(scratch_dir_str)

        args = [
            "grpc_tools.protoc",
            f"-I{PROTO_ROOT}",
            f"--python_out={scratch_dir}",
            f"--pyi_out={scratch_dir}",
            *(str(p) for p in proto_files),
        ]
        exit_code = protoc.main(args)
        if exit_code != 0:
            raise RuntimeError(f"protoc failed with exit code {exit_code}")

        generated_dir = scratch_dir / "mosaico" / "v1"
        for generated_file in sorted(generated_dir.glob("*_pb2.py*")):
            # Rewrite the absolute `mosaico.v1` package imports protoc emits
            # for cross-file references into the flat `mosaicolabs.proto.v1`
            # layout this SDK actually uses.
            content = generated_file.read_text()
            content = IMPORT_RE.sub("from mosaicolabs.proto.v1 import", content)
            (OUT_DIR / generated_file.name).write_text(content)

        for proto_file in proto_files:
            print(
                f"compiled {proto_file.name} -> "
                f"src/mosaicolabs/proto/v1/{proto_file.stem}_pb2.py, "
                f"{proto_file.stem}_pb2.pyi"
            )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
