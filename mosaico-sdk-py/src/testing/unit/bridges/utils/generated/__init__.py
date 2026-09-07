import sys
from pathlib import Path

# protoc's default Python codegen emits flat, absolute cross-file imports (e.g.
# imu_pb2.py does `import common_pb2 as common__pb2`), not package-relative ones.
# That only resolves if this directory is importable as a bare top-level module
# path, so it needs to be on sys.path -- it can't be reached solely through the
# dotted `....utils.generated.imu_pb2` package chain used to import it here.
_GENERATED_DIR = str(Path(__file__).parent)
if _GENERATED_DIR not in sys.path:
    sys.path.insert(0, _GENERATED_DIR)
