"""
Render docs/PROPOSAL.md to a print-ready PDF.

    python tools/render_proposal.py            # writes docs/KVStream-Proposal-rev2.pdf

Markdown -> HTML (markdown-it) -> Chromium print-to-PDF. Chromium is used rather
than a layout library because this document is mostly tables, and browser table
layout with real page-break control is the part that is tedious to reimplement.

Requires `pip install markdown-it-py playwright` and `playwright install chromium`.
The PDF is committed, but it is a build product: edit docs/PROPOSAL.md and re-run
this, never the PDF.

Styling targets a technical proposal that will be read on paper or in a viewer:
serif body for sustained reading, a sans face for headings and tabular data, and
a mono face for identifiers. Nothing decorative.
"""

from __future__ import annotations

import pathlib
import sys

from markdown_it import MarkdownIt
from playwright.sync_api import sync_playwright

ROOT = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".")
SOURCE = ROOT / "docs" / "PROPOSAL.md"
OUT = ROOT / "docs" / "KVStream-Proposal-rev2.pdf"

CSS = """
@page {
  size: A4;
  margin: 20mm 18mm 18mm 18mm;
  @bottom-center { content: counter(page); }
}

:root {
  --ink: #16181a;
  --soft: #3f464c;
  --muted: #6b7278;
  --rule: #d8dbd8;
  --rule-strong: #b9beba;
  --accent: #2d4a7c;
  --tint: #f2f4f1;
}

* { box-sizing: border-box; }

body {
  font-family: "Source Serif 4", "Georgia", "Times New Roman", serif;
  font-size: 10.2pt;
  line-height: 1.5;
  color: var(--ink);
  margin: 0;
  -webkit-print-color-adjust: exact;
  print-color-adjust: exact;
}

h1, h2, h3, h4 {
  font-family: "Source Sans 3", "Segoe UI", Helvetica, Arial, sans-serif;
  color: var(--ink);
  line-height: 1.22;
  text-wrap: balance;
}

/* Title block */
h1 {
  font-size: 21pt;
  font-weight: 600;
  letter-spacing: -0.01em;
  margin: 0 0 4pt;
}
h1 + p { font-size: 11pt; color: var(--soft); margin: 0 0 2pt; }

h2 {
  font-size: 13.5pt;
  font-weight: 600;
  margin: 20pt 0 6pt;
  padding-bottom: 3pt;
  border-bottom: 0.6pt solid var(--rule-strong);
  break-after: avoid;
}
h3 {
  font-size: 11.2pt;
  font-weight: 600;
  margin: 14pt 0 4pt;
  break-after: avoid;
}
h4 { font-size: 10.2pt; font-weight: 600; margin: 10pt 0 3pt; break-after: avoid; }

p { margin: 0 0 7pt; orphans: 3; widows: 3; }
strong { font-weight: 600; }
em { font-style: italic; }

ul, ol { margin: 0 0 8pt; padding-left: 16pt; }
li { margin-bottom: 3pt; }

hr { border: none; border-top: 0.6pt solid var(--rule); margin: 14pt 0; }

code {
  font-family: "IBM Plex Mono", Consolas, "Courier New", monospace;
  font-size: 0.86em;
  background: var(--tint);
  border: 0.4pt solid var(--rule);
  border-radius: 2pt;
  padding: 0.5pt 2.5pt;
}

pre {
  font-family: "IBM Plex Mono", Consolas, monospace;
  font-size: 8.4pt;
  line-height: 1.42;
  background: var(--tint);
  border: 0.4pt solid var(--rule);
  border-left: 2pt solid var(--accent);
  padding: 6pt 8pt;
  overflow-wrap: anywhere;
  white-space: pre-wrap;
  break-inside: avoid;
  margin: 0 0 8pt;
}
pre code { background: none; border: none; padding: 0; font-size: inherit; }

blockquote {
  margin: 0 0 9pt;
  padding: 7pt 10pt;
  background: var(--tint);
  border-left: 2pt solid var(--accent);
  break-inside: avoid;
}
blockquote p { margin: 0 0 5pt; }
blockquote p:last-child { margin-bottom: 0; }

table {
  border-collapse: collapse;
  width: 100%;
  font-family: "Source Sans 3", "Segoe UI", Helvetica, sans-serif;
  font-size: 8.9pt;
  line-height: 1.38;
  margin: 0 0 10pt;
  break-inside: avoid;
  font-variant-numeric: tabular-nums;
}
thead { display: table-header-group; }
th, td {
  text-align: left;
  vertical-align: top;
  padding: 4pt 7pt 4pt 0;
  border-bottom: 0.4pt solid var(--rule);
}
th {
  font-weight: 600;
  font-size: 7.8pt;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--muted);
  border-bottom: 0.7pt solid var(--rule-strong);
}
tbody tr:last-child td { border-bottom: none; }
td code, th code { font-size: 0.9em; }

a { color: var(--accent); text-decoration: none; }

/* The revision box is the first blockquote and carries the most weight. */
body > blockquote:first-of-type {
  border-left-width: 3pt;
  background: #eef2f8;
}

/* Keep a section heading with the paragraph that follows it. */
h2 + p, h3 + p, h2 + table, h3 + table, h2 + blockquote { break-before: avoid; }
"""

FONTS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?'
    "family=IBM+Plex+Mono:wght@400;500&"
    "family=Source+Sans+3:wght@400;600&"
    "family=Source+Serif+4:opsz,wght@8..60,400;8..60,600&display=swap\">"
)


def main() -> None:
    md = MarkdownIt("commonmark", {"html": True, "linkify": False}).enable("table")
    body = md.render(SOURCE.read_text(encoding="utf-8"))

    html = (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>KVStream Technical Proposal rev 2</title>{FONTS}"
        f"<style>{CSS}</style></head><body>{body}</body></html>"
    )

    staged = pathlib.Path(__file__).parent / "_proposal_render.html"
    staged.write_text(html, encoding="utf-8")

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(staged.as_uri(), wait_until="networkidle")
        page.emulate_media(media="print")
        page.pdf(
            path=str(OUT),
            format="A4",
            margin={"top": "20mm", "bottom": "18mm", "left": "18mm", "right": "18mm"},
            print_background=True,
            display_header_footer=True,
            header_template="<div></div>",
            footer_template=(
                "<div style=\"width:100%;font-size:7pt;color:#6b7278;"
                "font-family:'Segoe UI',Helvetica,sans-serif;padding:0 18mm;"
                "display:flex;justify-content:space-between;\">"
                "<span>KVStream — Technical Proposal, Revision 2</span>"
                "<span class='pageNumber'></span></div>"
            ),
        )
        browser.close()

    print(f"wrote {OUT}  ({OUT.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
