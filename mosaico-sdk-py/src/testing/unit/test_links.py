"""Checks every README.md in the repo and every Markdown/MDX file under docs/ for broken URLs.

Only http(s) links are checked (relative/anchor links are intentionally out of scope).
A link is considered broken when it definitively responds with 404/410.
"""

from __future__ import annotations

import re
import socket
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
DOCS_DIR = REPO_ROOT / "docs"

EXCLUDED_DIR_NAMES = {
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "__pycache__",
    ".docusaurus",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "site",
    "dist",
    "build",
}

LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")


@dataclass(frozen=True)
class Link:
    source: Path
    line: int
    target: str


@dataclass(frozen=True)
class BrokenLink:
    source: Path
    line: int
    target: str
    reason: str

    def __str__(self) -> str:
        return f"{self.source.relative_to(REPO_ROOT)}:{self.line}: '{self.target}' -> {self.reason}"


def _is_excluded(path: Path) -> bool:
    return any(part in EXCLUDED_DIR_NAMES for part in path.parts)


def iter_markdown_files() -> list[Path]:
    files: set[Path] = set()

    for path in REPO_ROOT.rglob("README.md"):
        if not _is_excluded(path):
            files.add(path)

    if DOCS_DIR.is_dir():
        for pattern in ("*.md", "*.mdx"):
            for path in DOCS_DIR.rglob(pattern):
                if not _is_excluded(path):
                    files.add(path)

    return sorted(files)


def extract_links(path: Path) -> list[Link]:
    links = []
    for lineno, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        for match in LINK_RE.finditer(line):
            links.append(Link(path, lineno, match.group(1).strip()))
    return links


def is_external(target: str) -> bool:
    return urlsplit(target).scheme in ("http", "https")


def _has_internet(host: str = "1.1.1.1", port: int = 443, timeout: float = 3.0) -> bool:
    try:
        socket.create_connection((host, port), timeout=timeout).close()
        return True
    except OSError:
        return False


def _check_external_url(url: str, timeout: float = 10.0) -> str | None:
    """Return an error description if `url` is definitively dead, else None."""
    for method in ("HEAD", "GET"):
        request = Request(
            url, method=method, headers={"User-Agent": "mosaico-link-checker/1.0"}
        )
        try:
            with urlopen(request, timeout=timeout) as response:
                return f"HTTP {response.status}" if response.status >= 400 else None
        except HTTPError as error:
            if error.code in (404, 410):
                return f"HTTP {error.code}"
            if method == "HEAD":
                continue  # some servers reject HEAD; retry with GET before giving up
            return None  # ambiguous (403/5xx/etc.) - not treated as a broken link
        except (URLError, TimeoutError, ConnectionError):
            return None  # network hiccup, not a broken-link signal
    return None


def find_external_link_problems() -> list[BrokenLink]:
    links_by_url: dict[str, list[Link]] = {}
    for md_file in iter_markdown_files():
        for link in extract_links(md_file):
            if is_external(link.target):
                links_by_url.setdefault(link.target, []).append(link)

    urls = list(links_by_url)
    with ThreadPoolExecutor(max_workers=8) as executor:
        errors = list(executor.map(_check_external_url, urls))

    problems = []
    for url, error in zip(urls, errors):
        if error is None:
            continue
        for link in links_by_url[url]:
            problems.append(BrokenLink(link.source, link.line, link.target, error))
    return problems


@pytest.mark.skipif(not _has_internet(), reason="no network access")
def test_external_links_are_reachable():
    problems = find_external_link_problems()
    assert not problems, "Broken external links found:\n" + "\n".join(
        str(p) for p in problems
    )
