#!/usr/bin/env python3
"""Build PDF from pesticide_disaggregation_paper.md using pandoc + KaTeX + weasyprint."""

import re
import subprocess
import sys
from pathlib import Path

DOCS = Path(__file__).parent
MD_FILE = DOCS / "pesticide_disaggregation_paper.md"
HTML_FILE = DOCS / "pesticide_disaggregation_paper_rendered.html"
PDF_FILE = DOCS / "pesticide_disaggregation_paper.pdf"
CSS_FILE = DOCS / "paper.css"


def katex_render(latex: str, display: bool = False) -> str:
    """Render a LaTeX string to HTML via KaTeX CLI."""
    cmd = ["npx", "katex"]
    if display:
        cmd.append("--display-mode")
    result = subprocess.run(cmd, input=latex, capture_output=True, text=True, timeout=10)
    if result.returncode != 0:
        print(f"  KaTeX error on: {latex[:60]}... -> {result.stderr.strip()}", file=sys.stderr)
        return f'<code class="math-fallback">{latex}</code>'
    return result.stdout.strip()


def preprocess_math(md_text: str) -> str:
    """Replace $...$ and $$...$$ with KaTeX-rendered HTML."""

    # Display math first ($$...$$)
    def replace_display(m):
        rendered = katex_render(m.group(1).strip(), display=True)
        return f'\n<div class="math-display">{rendered}</div>\n'

    text = re.sub(r"\$\$(.*?)\$\$", replace_display, md_text, flags=re.DOTALL)

    # Inline math ($...$) — avoid matching $$ or currency
    def replace_inline(m):
        rendered = katex_render(m.group(1).strip(), display=False)
        return f'<span class="math-inline">{rendered}</span>'

    return re.sub(r"(?<!\$)\$(?!\$)(.+?)(?<!\$)\$(?!\$)", replace_inline, text)


def build():
    print(f"Reading {MD_FILE.name}...")
    md_text = MD_FILE.read_text()

    print("Pre-rendering LaTeX math with KaTeX...")
    md_rendered = preprocess_math(md_text)

    # Write temporary markdown with rendered math
    tmp_md = DOCS / "_paper_katex.md"
    tmp_md.write_text(md_rendered)

    # Read CSS and add KaTeX styles
    _css_text = CSS_FILE.read_text() if CSS_FILE.exists() else ""
    katex_css_url = "https://cdn.jsdelivr.net/npm/katex@0.16.45/dist/katex.min.css"

    # Build HTML with pandoc
    print("Converting to HTML with pandoc...")
    cmd = [
        "pandoc",
        str(tmp_md),
        "-o",
        str(HTML_FILE),
        "--standalone",
        f"--css={katex_css_url}",
        f"--css={CSS_FILE}",
        "--metadata",
        "title=",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Pandoc error: {result.stderr}", file=sys.stderr)

    # Convert HTML to PDF with weasyprint
    print("Converting to PDF with weasyprint...")
    cmd = ["weasyprint", str(HTML_FILE), str(PDF_FILE)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Weasyprint warnings: {result.stderr[:500]}", file=sys.stderr)

    # Cleanup temp files
    tmp_md.unlink(missing_ok=True)

    if PDF_FILE.exists():
        size_kb = PDF_FILE.stat().st_size / 1024
        print(f"PDF generated: {PDF_FILE} ({size_kb:.0f} KB)")
    else:
        print("ERROR: PDF not generated", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    build()
