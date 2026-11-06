"""Check that every documentation path cited by the mosaico-onboarding skill exists.

Usage: python check_paths.py

Scans SKILL.md for published doc URLs (https://docs.mosaico.dev/..., and the short forms main/ and
sdk/ used in its concept map), for repository paths under docs/ (pages not on the site yet) and for SDK
source files under mosaico-sdk-py/src/, and expected.json for the paths it cites. Each URL is mapped back
to its page in this repository. Exits 1 and lists the missing ones if any page does not exist.
"""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SKILL = ROOT / ".agents/skills/mosaico-onboarding/SKILL.md"
EXPECTED = Path(__file__).parent / "expected.json"
SITE = "https://docs.mosaico.dev/"
SHORT = {"main/": SITE, "sdk/": SITE + "python-sdk/SDK/"}
PATH_RE = re.compile(r"docs/[\w./-]+\.(?:mdx|md)|mosaico-sdk-py/src/[\w./-]+\.py")
URL_RE = re.compile(r"(?:https://docs\.mosaico\.dev/|(?<![\w/])(?:main|sdk)/)[\w./-]*")
# Pages main moved to examples/ROS/; the site still serves them at the old flat URL.
MOVED = {
    "examples/ros_ingestion": "examples/ROS/ros_ingestion",
    "examples/reconstruct_rosbag": "examples/ROS/reconstruct_rosbag",
}


def url_to_page(url: str) -> str:
    """Map a published URL (or short form) to the repository page it is built from."""
    for short, full in SHORT.items():
        if url.startswith(short):
            url = full + url[len(short) :]
    rel = url.removeprefix(SITE).rstrip("/.")
    if rel == "python-sdk" or rel.startswith("python-sdk/"):
        rel = rel.removeprefix("python-sdk").strip("/")
        return f"docs/py/docs/{rel}.md" if rel else "docs/py/docs/index.md"
    rel = MOVED.get(rel, rel)
    for cand in (f"{rel}.md", f"{rel}.mdx", f"{rel}/index.md", f"{rel}/index.mdx"):
        if (ROOT / "docs/main/docs" / cand).is_file():
            return f"docs/main/docs/{cand}"
    return f"docs/main/docs/{rel}(.md|.mdx|/index.md)"


def cited_in_expected() -> set[str]:
    paths = set()
    for item in json.loads(EXPECTED.read_text()).values():
        paths.update(item["acceptable_citations"])
        for fact in item["must_contain"]:
            paths.update(PATH_RE.findall(fact["source"]))
    return paths


def main() -> int:
    text = SKILL.read_text()
    skill_paths = set(PATH_RE.findall(text))
    urls = set(URL_RE.findall(text)) - set(SHORT) - set(SHORT.values())
    skill_paths |= {url_to_page(u) for u in urls}
    all_paths = skill_paths | cited_in_expected()
    missing = sorted(p for p in all_paths if not (ROOT / p).is_file())
    print(f"{len(skill_paths)} paths in SKILL.md, {len(all_paths)} in total")
    for p in missing:
        print(f"MISSING {p}")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
