"""Compare an eval run with expected.json.

Usage: python check.py <run-dir> [host] [port]

<run-dir> holds q01.out .. q07.out; each ends with a line `ANSWER:` followed by `key: value` lines (a key
repeated for a list). Integers (row counts, timestamps) must match exactly, floats to a relative 1e-6,
lists of names as sets.
"""

import json
import math
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path

from mosaicolabs import MosaicoClient


def answer(path: Path) -> dict:
    lines = path.read_text().splitlines() if path.exists() else []
    if "ANSWER:" not in lines:
        return {}
    got: dict = {}
    for line in lines[lines.index("ANSWER:") + 1 :]:
        key, sep, value = line.partition(":")
        if sep:
            got.setdefault(key.strip(), []).append(value.strip())
    return got


def matches(want, got: list[str]) -> bool:
    try:
        if isinstance(want, list):
            return set(got) == set(want)
        if len(got) != 1:
            return False
        if isinstance(want, int):
            return Decimal(got[0]) == want
        return math.isclose(float(got[0]), want, rel_tol=1e-6, abs_tol=1e-9)
    except (ValueError, InvalidOperation):
        return False


def main() -> int:
    run = Path(sys.argv[1])
    host = sys.argv[2] if len(sys.argv) > 2 else "localhost"
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 6726
    expected = json.loads((Path(__file__).parent / "expected.json").read_text())

    with MosaicoClient.connect(host, port) as client:
        # SDK 0.6.x has client.version(); 0.7 replaced it with client.info()
        info = getattr(client, "info", None) or client.version
        print(f"daemon: {info()}")
    print(f"run: {run.resolve()}")

    passed = 0
    for n in sorted(expected, key=int):
        got = answer(run / f"q{int(n):02d}.out")
        wrong = {
            k: (got.get(k), v)
            for k, v in expected[n].items()
            if not matches(v, got.get(k, []))
        }
        passed += not wrong
        detail = "".join(f"  {k}: got={g} want={w}" for k, (g, w) in wrong.items())
        print(f"q{int(n):02d} {'PASS' if not wrong else 'FAIL'}{detail}")
    print(f"{passed}/{len(expected)} PASS")
    return 0 if passed == len(expected) else 1


if __name__ == "__main__":
    sys.exit(main())
