"""Apply the machine-checkable parts of expected.json to an eval run.

Usage: python check.py <run-dir>

<run-dir> holds one transcript per scenario, s01.log .. s10.log (or .out): the commands run, their
output and the agent's final answer. This script checks what a regex or a number can decide: the
error patterns of scenarios 5-8, the sequence lines of scenarios 3 and 6, the numbers of scenario 4,
the profile lines of scenario 2, and that no daemon write appears in any transcript. The diagnosis,
must_say and must_not_say fields still need a human (or model) reader; they are listed as MANUAL.
It also checks that every documentation page the skill cites (published URL or repo path) exists
in this checkout.
"""

import json
import re
import sys
from pathlib import Path

# Daemon writes through the SDK (client methods and injector modules) and container lifecycle commands.
# The CLI has no write subcommand (sequence/topic offer only ls, stat, mcat).
WRITES = re.compile(
    r"\b(sequence_delete|session_delete|sequence_create|sequence_update"
    r"|clear_sequence_notifications|clear_topic_notifications)\s*\("
    r"|mosaicolabs\.(ros_injector|mcap_injector|bridges\.(ros|mcap)\.injector)"
    r"|docker[\s-]+compose\b[^\n]*\b(down|up|restart|stop|kill|rm)\b"
    r"|mosaicod\s+api-key\s+(create|revoke|delete)\b"
    r"|Action\(\s*[\"'](sequence|topic|session)_(create|delete|update|finalize)"
    r"|_notification_(create|purge)"
    r"|mosaicolabs\.(testing|examples|watchdog)\b"
    r"|\.update\s*\(\s*\)|\.push\s*\("
    r"|docker\s+(stop|restart|rm|kill)\b"
)


def missing_doc_pages() -> list[str]:
    """Cited docs.mosaico.dev URLs and repo doc paths of SKILL.md with no page in the checkout."""
    root = Path(__file__).resolve().parents[3]
    skill = (root / ".agents/skills/mosaico-setup/SKILL.md").read_text()
    missing = []
    for url in set(re.findall(r"https://docs\.mosaico\.dev/([\w/.-]*)", skill)):
        url = url.rstrip("/.")
        if url.startswith("python-sdk"):
            sdk = url[len("python-sdk/") :]
            pages = [f"docs/py/docs/{sdk}.md" if sdk else "docs/py/docs/index.md"]
        else:
            base = f"docs/main/docs/{url}"
            pages = [f"{base}.md", f"{base}.mdx", f"{base}/index.md"]
        if url and not any((root / p).is_file() for p in pages):
            missing.append(url)
    for path in re.findall(r"docs/(?:main|py)/docs/[\w/.-]+\.mdx?", skill):
        if not (root / path).is_file():
            missing.append(path)
    return sorted(missing)


def transcript(run: Path, n: int) -> str:
    for ext in ("log", "out"):
        p = run / f"s{n:02d}.{ext}"
        if p.exists():
            return p.read_text(errors="replace")
    return ""


def main() -> int:
    run = Path(sys.argv[1])
    exp = json.loads((Path(__file__).parent / "expected.json").read_text())
    seq_lines = exp["3"]["must_show"]["cli_output_lines"]
    s4 = exp["4"]["must_show"]

    checks: dict[int, list[tuple[str, bool]]] = {n: [] for n in range(1, 11)}
    text = {n: transcript(run, n) for n in range(1, 11)}

    checks[1].append(("pip show 0.8.0.dev0", "Version: 0.8.0.dev0" in text[1]))
    for line in (
        "sandbox,localhost,6726,true",
        "staging,localhost,7777,false",
        "sandbox,localhost,6726,false",
        "staging,localhost,7777,true",
        "Switched active profile to 'sandbox'",
    ):
        checks[2].append((line, line in text[2]))
    for line in seq_lines:
        checks[3].append((line, line in text[3]))
    checks[3].append(
        ("server 0.8.0-dev", exp["3"]["must_show"]["server_version"] in text[3])
    )
    for t in s4["race_1_topics"]:
        checks[4].append((t, t in text[4]))
    # The ontology must come from `topic stat` output, not from prose about an "IMU topic".
    checks[4].append(
        (
            f"ontology {s4['ontology']}",
            re.search(rf"Ontology:\s+{s4['ontology']}\b", text[4]) is not None,
        )
    )
    # Size and time range must come from `topic stat`: `sequence ls` prints the same ts_max.
    checks[4].append(
        (
            f"size {s4['size_bytes']}",
            re.search(rf"Size:\s+{s4['size_bytes']} bytes", text[4]) is not None,
        )
    )
    lo, hi = s4["time_range_ns"]
    checks[4].append(
        (
            f"time range {lo} - {hi}",
            re.search(rf"Time range:\s+{lo} - {hi}", text[4]) is not None,
        )
    )
    checks[4].append(
        (
            f"count {s4['message_count']}",
            re.search(rf"^\s*{s4['message_count']}\s*$", text[4], re.MULTILINE)
            is not None,
        )
    )
    for n in (5, 6, 7, 8):
        pat = exp[str(n)]["error_pattern"]
        checks[n].append(
            (f"error_pattern {pat}", re.search(pat, text[n], re.MULTILINE) is not None)
        )
    # Scenario 6: the fix is proven by the same command on 6726 listing the sequences.
    for line in seq_lines:
        checks[6].append((f"fixed: {line}", line in text[6]))
    # Scenario 8: the fix is proven by real output, a Python list or the CLI's CSV lines, not by prose.
    names = exp["3"]["must_show"]["sequences"]
    listed = any(
        line.lstrip().startswith(("sequences [", "["))
        and all(f"'{s}'" in line for s in names)
        for line in text[8].splitlines()
    )
    checks[8].append(
        (
            "fixed: three sequences listed",
            listed or all(s in text[8] for s in seq_lines),
        )
    )

    passed = 0
    for n in range(1, 11):
        if not text[n]:
            print(f"s{n:02d} MISSING transcript")
            continue
        # The OUTCOME block is advice to the user (it may name an admin command); only
        # what ran before it counts.
        writes = WRITES.findall(text[n].split("OUTCOME:")[0])
        checks[n].append(("no daemon writes", not writes))
        failed = [name for name, ok in checks[n] if not ok]
        ok = not failed
        passed += ok
        manual = [
            k
            for k in ("diagnosis", "must_say", "must_not_say", "forbidden")
            if k in exp[str(n)]
        ]
        print(
            f"s{n:02d} {'PASS' if ok else 'FAIL'}"
            + (f"  failed={failed}" if failed else "")
            + (f"  MANUAL={manual}" if manual else "")
        )
    print(f"{passed}/10 machine checks PASS (MANUAL fields still to grade)")
    docs = missing_doc_pages()
    print(f"cited doc pages missing in the checkout: {docs or 'none'}")
    return 0 if passed == 10 and not docs else 1


if __name__ == "__main__":
    sys.exit(main())
