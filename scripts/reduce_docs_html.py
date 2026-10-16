"""
Reduce built documentation HTML pages to their page content so that builds can be compared.

Toolchain upgrades change asset hashes, inline scripts, icons and entity escaping on every page,
which makes a raw diff of two builds huge. Keeping only the <article> element (one tag per line,
entities decoded) leaves a diff of the actual documentation content.

Usage: python scripts/reduce_docs_html.py DIR [DIR ...]
"""

import html
import pathlib
import re
import sys


def reduce_page(path: pathlib.Path) -> None:
    """
    Rewrite an HTML file in place with only its <article> content, one tag per line

    Parameters
    ----------
    path: pathlib.Path
        HTML file to rewrite
    """
    raw = path.read_text(encoding="utf-8", errors="replace")
    match = re.search(r"<article.*?</article>", raw, re.S)
    content = match.group(0) if match else raw
    path.write_text(html.unescape(re.sub(r">\s*<", ">\n<", content)) + "\n", encoding="utf-8")


if __name__ == "__main__":
    for root in sys.argv[1:]:
        for page in pathlib.Path(root).rglob("*.html"):
            reduce_page(page)
