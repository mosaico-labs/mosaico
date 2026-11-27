"""Check that every documentation path cited by the mosaico-docs-guide skill exists.

Usage: python check_paths.py [--live]

Scans SKILL.md for published doc URLs (https://docs.mosaico.dev/..., and the short forms main/ and
sdk/ used in its concept map), for repository paths under docs/ and for SDK source files under
mosaico-sdk-py/src/, and expected.json for the paths it cites. Each URL is mapped back to its page in
this repository; site files with no page (llms.txt, sitemap.xml) are only checked live. With --live it
also requests every published URL, following redirects, and fails on any answer other than 200.
Exits 1 and lists what is missing.
"""

import json
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[3]
SKILL = ROOT / ".agents/skills/mosaico-docs-guide/SKILL.md"
EXPECTED = Path(__file__).parent / "expected.json"
SITE = "https://docs.mosaico.dev/"
SHORT = {"main/": SITE, "sdk/": SITE + "python-sdk/SDK/"}
PATH_RE = re.compile(r"docs/[\w./-]+\.(?:mdx|md)|mosaico-sdk-py/src/[\w./-]+\.py")
URL_RE = re.compile(r"(?:https://docs\.mosaico\.dev/|(?<![\w/])(?:main|sdk)/)[\w./-]*")
SITE_FILES = (".txt", ".xml")
TIMEOUT_S = 15


def expand(url: str) -> str:
    """Turn a short form (main/..., sdk/...) into its published URL."""
    for short, full in SHORT.items():
        if url.startswith(short):
            return full + url[len(short) :]
    return url.rstrip(".")


def url_to_page(url: str) -> Optional[str]:
    """Map a published URL to the repository page it is built from (None for a site-only file)."""
    rel = url.removeprefix(SITE).rstrip("/.")
    if rel.endswith(SITE_FILES):
        return None
    if rel == "python-sdk" or rel.startswith("python-sdk/"):
        rel = rel.removeprefix("python-sdk").strip("/")
        return f"docs/py/docs/{rel}.md" if rel else "docs/py/docs/index.md"
    for cand in (f"{rel}.md", f"{rel}.mdx", f"{rel}/index.md", f"{rel}/index.mdx"):
        if (ROOT / "docs/main/docs" / cand).is_file():
            return f"docs/main/docs/{cand}"
    return f"docs/main/docs/{rel}(.md|.mdx|/index.md)"


def status(url: str) -> str:
    """HTTP status of a GET on url after redirects, or the error that prevented one."""
    req = urllib.request.Request(
        url, headers={"User-Agent": "mosaico-docs-guide-check"}
    )
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
            return str(resp.status)
    except urllib.error.HTTPError as e:
        return str(e.code)
    except (urllib.error.URLError, TimeoutError) as e:
        return f"error ({getattr(e, 'reason', e)})"


def cited_in_expected() -> set[str]:
    paths = set()
    for item in json.loads(EXPECTED.read_text()).values():
        paths.update(item["acceptable_citations"])
        for fact in item["must_contain"]:
            paths.update(PATH_RE.findall(fact["source"]))
    return paths


def main() -> int:
    live = "--live" in sys.argv[1:]
    text = SKILL.read_text()
    skill_paths = set(PATH_RE.findall(text))
    urls = {expand(u) for u in URL_RE.findall(text)} - set(SHORT.values())
    skill_paths |= {p for p in map(url_to_page, urls) if p}
    all_paths = skill_paths | cited_in_expected()
    missing = sorted(p for p in all_paths if not (ROOT / p).is_file())
    print(f"{len(skill_paths)} paths in SKILL.md, {len(all_paths)} in total")
    for p in missing:
        print(f"MISSING {p}")
    failed = []
    if live:
        for url in sorted(urls):
            code = status(url)
            if code != "200":
                failed.append(url)
                print(f"LIVE {code} {url}")
        print(f"{len(urls)} URLs requested, {len(failed)} not 200")
    return 1 if missing or failed else 0


if __name__ == "__main__":
    sys.exit(main())
