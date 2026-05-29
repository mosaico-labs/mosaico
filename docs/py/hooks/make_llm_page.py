"""
MkDocs hook: generates docs/llms/llms-full.txt in the site output.

Uses raw markdown (on_page_markdown) rather than rendered HTML to avoid
encoding artefacts introduced by the toc permalink extension (Â¶ / ¶).
API reference pages are collected via on_page_content instead because
their source is entirely mkdocstrings directives (:::); a proper
HTMLParser converts the rendered HTML to light markdown.
Pages are collected in nav order. The llms.md page itself is excluded.
"""

import os
import re
from html.parser import HTMLParser

_SKIP = {"llms.md"}
_API_REF_PREFIX = "SDK/API_reference/"
_pages: list[tuple[str, str]] = []


def _strip_frontmatter(text: str) -> str:
    return re.sub(r"^---\n.*?\n---\n?", "", text, count=1, flags=re.DOTALL).strip()


# Tags whose text content is wrapped symmetrically (same marker at open and close).
_WRAP = {"strong": "**", "b": "**", "em": "*", "i": "*"}

# Pending newlines to request at the start / end of each tag.
_NL_START = {
    "p": 2,
    "pre": 2,
    "hr": 2,
    "ul": 2,
    "ol": 2,
    "dl": 2,
    "li": 1,
    "dt": 1,
    "dd": 1,
    **{f"h{n}": 2 for n in range(1, 7)},
}
_NL_END = {
    "p": 2,
    "pre": 2,
    "ul": 2,
    "ol": 2,
    "dl": 2,
    "li": 1,
    "dt": 1,
    "dd": 2,
    **{f"h{n}": 2 for n in range(1, 7)},
}

_SKIP_TAGS = frozenset({"script", "style", "head", "nav", "svg", "footer", "details"})
_HEADING_TAGS = frozenset({f"h{n}" for n in range(1, 7)})


class _HtmlToText(HTMLParser):
    """Convert mkdocstrings HTML to clean plain text / light markdown."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._main_buf: list[str] = []
        self._cell_buf: list[str] | None = None  # non-None when inside a td/th
        self._row_cells: list[str] = []
        self._skip_depth = 0
        self._in_pre = False
        self._in_code = False
        self._list_depth = 0
        self._pending_nl = 0

    def _want_nl(self, n: int) -> None:
        if self._cell_buf is None:
            self._pending_nl = max(self._pending_nl, n)

    def _emit(self, text: str) -> None:
        if not text:
            return
        if self._cell_buf is not None:
            self._cell_buf.append(text)
        else:
            if self._pending_nl:
                self._main_buf.append("\n" * self._pending_nl)
                self._pending_nl = 0
            self._main_buf.append(text)

    def handle_starttag(self, tag: str, attrs) -> None:
        attrs_dict = dict(attrs)
        cls = attrs_dict.get("class", "")

        if self._skip_depth > 0:
            self._skip_depth += 1
            return
        if (
            tag in _SKIP_TAGS
            or "doc-symbol" in cls
            or (tag == "a" and "headerlink" in cls)
        ):
            self._skip_depth += 1
            return

        self._want_nl(_NL_START.get(tag, 0))

        if tag in _HEADING_TAGS:
            self._emit("#" * int(tag[1]) + " ")
        elif tag == "hr":
            self._emit("---")
            self._want_nl(2)
        elif tag == "pre":
            self._in_pre = True
            self._emit("```\n")
        elif tag == "code" and not self._in_pre:
            self._in_code = True
            self._emit("`")
        elif tag in _WRAP:
            self._emit(_WRAP[tag])
        elif tag in ("ul", "ol"):
            self._list_depth += 1
        elif tag == "li":
            self._emit("  " * (self._list_depth - 1) + "- ")
        elif tag == "dt":
            self._emit("**")
        elif tag == "tr":
            self._row_cells = []
            self._pending_nl = 0
        elif tag in ("td", "th"):
            self._cell_buf = []

    def handle_endtag(self, tag: str) -> None:
        if self._skip_depth > 0:
            self._skip_depth -= 1
            return

        if tag == "pre":
            self._in_pre = False
            self._emit("\n```")
        elif tag == "code" and self._in_code:
            self._in_code = False
            self._emit("`")
        elif tag in _WRAP:
            self._emit(_WRAP[tag])
        elif tag in ("ul", "ol"):
            self._list_depth = max(0, self._list_depth - 1)
        elif tag == "dt":
            self._emit("**")
        elif tag in ("td", "th"):
            if self._cell_buf is not None:
                cell = re.sub(r"\s+", " ", "".join(self._cell_buf)).strip()
                self._row_cells.append(cell)
                self._cell_buf = None
        elif tag == "tr":
            if self._row_cells:
                self._want_nl(1)
                self._emit("| " + " | ".join(self._row_cells) + " |")
                self._want_nl(1)
            self._row_cells = []

        self._want_nl(_NL_END.get(tag, 0))

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        if self._in_pre:
            if self._pending_nl:
                self._main_buf.append("\n" * self._pending_nl)
                self._pending_nl = 0
            self._main_buf.append(data)
            return
        text = re.sub(r"[\n\t ]+", " ", data)
        if text.strip():
            self._emit(text)

    def get_text(self) -> str:
        result = "".join(self._main_buf)
        return re.sub(r"\n{3,}", "\n\n", result).strip()


def _html_to_text(html: str) -> str:
    parser = _HtmlToText()
    parser.feed(html)
    return parser.get_text()


def on_page_markdown(markdown, *, page, config, files):
    if page.file.src_path in _SKIP:
        return markdown
    if page.file.src_path.startswith(_API_REF_PREFIX):
        return markdown  # collected in on_page_content after mkdocstrings renders
    title = page.title or ""
    body = _strip_frontmatter(markdown)
    _pages.append((title, body))
    return markdown


def on_page_content(html, *, page, config, files):
    if page.file.src_path.startswith(_API_REF_PREFIX):
        title = page.title or ""
        _pages.append((title, _html_to_text(html)))
    return html


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
