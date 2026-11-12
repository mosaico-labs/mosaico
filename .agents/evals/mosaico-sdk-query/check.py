"""Compare an eval run with expected.json.

Usage: python check.py <run-dir> [host] [port]

<run-dir> holds q01.out .. q10.out; each ends with a line `ANSWER:` followed by one item per line
(a sequence name, `sequence topic`, or `start end` in nanoseconds). Order does not matter.
"""

import json
import sys
from pathlib import Path

from mosaicolabs import MosaicoClient


def answer_lines(path: Path) -> list[str]:
    lines = path.read_text().splitlines()
    if "ANSWER:" not in lines:
        return []
    return [
        line.strip() for line in lines[lines.index("ANSWER:") + 1 :] if line.strip()
    ]


def normalise(n: str, lines: list[str], expected: dict) -> tuple[set, set]:
    if "sequences" in expected:
        # A line may carry extra detail after the name (e.g. "race_1 ['/camera/imu']").
        return {line.split()[0] for line in lines}, set(expected["sequences"])
    if "topics" in expected:
        return {tuple(line.split()[:2]) for line in lines}, {
            tuple(t) for t in expected["topics"]
        }
    # A window line may name its sequence and topic first: take its last two integers.
    windows = set()
    for line in lines:
        ints = [int(x) for x in line.split() if x.lstrip("-").isdigit()]
        if len(ints) >= 2:
            windows.add(tuple(ints[-2:]))
    return windows, {tuple(w) for w in expected["windows_ns"]}


def main() -> int:
    run = Path(sys.argv[1])
    host = sys.argv[2] if len(sys.argv) > 2 else "localhost"
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 6726
    expected = json.loads((Path(__file__).parent / "expected.json").read_text())

    with MosaicoClient.connect(host, port) as client:
        print(f"daemon: {client.info()}")
    print(f"run: {run.resolve()}")

    passed = 0
    for n in sorted(expected, key=int):
        try:
            got, want = normalise(
                n, answer_lines(run / f"q{int(n):02d}.out"), expected[n]
            )
        except Exception as e:  # one unreadable answer fails its question, not the run
            got, want = {f"unreadable: {e}"}, set()
        ok = got == want
        passed += ok
        print(
            f"q{int(n):02d} {'PASS' if ok else 'FAIL'}"
            + ("" if ok else f"  got={sorted(got)} want={sorted(want)}")
        )
    print(f"{passed}/{len(expected)} PASS")
    return 0 if passed == len(expected) else 1


if __name__ == "__main__":
    sys.exit(main())
