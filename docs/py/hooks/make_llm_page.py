"""
MkDocs hook: generates docs/llms/llms-full.txt in the site output.

Uses raw markdown (on_page_markdown) rather than rendered HTML to avoid
encoding artefacts introduced by the toc permalink extension (Â¶ / ¶).
Pages are collected in nav order. The llms.md page itself is excluded.
"""

import os
import re

_SKIP = {"llms.md"}
_pages: list[tuple[str, str]] = []


def _strip_frontmatter(text: str) -> str:
    return re.sub(r"^---\n.*?\n---\n?", "", text, count=1, flags=re.DOTALL).strip()


def on_page_markdown(markdown, *, page, config, files):
    if page.file.src_path in _SKIP:
        return markdown
    title = page.title or ""
    body = _strip_frontmatter(markdown)
    _pages.append((title, body))
    return markdown


def on_post_build(config):
    out_dir = os.path.join(config["site_dir"], "llms")
    os.makedirs(out_dir, exist_ok=True)

    sections = []
    for title, body in _pages:
        if title:
            sections.append(f"# {title}\n\n{body}")
        else:
            sections.append(body)

    with open(os.path.join(out_dir, "llms-full.txt"), "w", encoding="utf-8") as fh:
        fh.write("\n\n---\n\n".join(sections) + "\n")

    _pages.clear()
