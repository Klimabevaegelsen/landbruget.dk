"""Parse a single dyrenesdetektiv.dk control-record HTML page into a flat dict.

Field semantics are documented in the pipeline README. The parser is a pure
function so it can be unit-tested against captured HTML fixtures.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime

from bs4 import BeautifulSoup, Tag

SANKTION_ORDINAL: dict[str, int] = {
    "Ingen anmærkninger": 1,
    "Indskærpelse": 2,
    "Politianmeldelse": 3,
    "Bøde": 4,
}

# Bare-number fallback used when the sanktion field is e.g. "2 (2 = Indskærpelse)".
_NUM_TO_LABEL: dict[str, str] = {
    "1": "Ingen anmærkninger",
    "2": "Indskærpelse",
    "3": "Politianmeldelse",
    "4": "Bøde",
}

_LABEL_TO_KEY = {
    "Fødevarestyrelsens sagsnummer": "sagsnummer",
    "Dato for kontrol": "kontrol_dato",
    "Dyreart": "dyreart",
    "Antal dyr": "antal_dyr",
    "Årsag til kontrollen": "aarsag",
    "By": "by",
    "Besætningens CHR nummer": "chr_nummer",
    "Virksomhedens CVR nummer": "cvr_nummer",
    "Sanktion": "sanktion",
}

_DATE_RE = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")
_POSTID_RE = re.compile(r"\bpostid-(\d+)\b")
_TAG_SLUG_RE = re.compile(r"\btag-([a-z0-9æøå][a-z0-9æøå-]*)\b")
_YEAR_IN_SLUG_RE = re.compile(r"^kontrol-(\d{4})-")
# Matches the inline legend "(N = Label)" appended to the sanktion field.
_SANKTION_LEGEND_RE = re.compile(r"\(([1-4])\s*=")

# `kontrol_tag` taxonomy values that name a `dyreart` (animal type) rather than
# a kommune. Anything in the `kontrol_tag` taxonomy that isn't here and isn't a
# 4-digit year is treated as a kommune.
DYREART_NAMES: frozenset[str] = frozenset(
    {
        "Agerhøns",
        "Får",
        "Fasaner",
        "Geder",
        "Gråænder",
        "Høns af æglægningstype",
        "Høns af slagtetype",
        "Hunde",
        "Kvæg",
        "Svin",
    }
)


def _clean(text: str) -> str:
    return text.replace("\xa0", " ").strip()


def _strip_label(p: Tag, label_text: str) -> str:
    """Return the text of <p> with the leading <strong>label:</strong> removed."""
    raw = p.get_text(" ", strip=False)
    raw = raw.replace("\xa0", " ")
    # Remove the leading "Label:" (with optional trailing space inside strong).
    pattern = re.escape(label_text) + r"\s*:\s*"
    return re.sub(pattern, "", raw, count=1).strip()


def _parse_date(value: str) -> str | None:
    if not value:
        return None
    match = _DATE_RE.match(value)
    if not match:
        return None
    day, month, year = match.groups()
    try:
        return date(int(year), int(month), int(day)).isoformat()
    except ValueError:
        return None


def _parse_int(value: str) -> int | None:
    if not value:
        return None
    cleaned = value.replace(",", "").replace(".", "").replace(" ", "")
    if not cleaned.isdigit():
        return None
    return int(cleaned)


def _zero_pad(value: str, width: int) -> str | None:
    """Return value as digits zero-padded to `width`. None if blank/non-numeric or too long."""
    digits = re.sub(r"\D", "", value or "")
    if not digits or len(digits) > width:
        return None
    return digits.zfill(width)


def _parse_sanktion(raw: str) -> tuple[str | None, int | None]:
    """Split 'Indskærpelse (2 = Indskærpelse)' into ('Indskærpelse', 2).

    Handles four observed formats:
      1. 'Ingen anmærkninger (2 = Indskærpelse)' — text first, legend in parens.
      2. 'Indskærpelse (2 = Indskærpelse)'        — text matches legend.
      3. '2 (2 = Indskærpelse)'                   — bare numeric resolved via legend.
      4. 'Sanktion (2 = Indskærpelse)'            — placeholder text; ordinal
         comes from the legend, raw text preserved verbatim (also covers
         'Påbud', 'Påbud/Forbud', and other free-form labels).
    """
    if not raw:
        return None, None
    head = raw.split("(", 1)[0].strip()
    legend = _SANKTION_LEGEND_RE.search(raw)
    legend_ordinal = int(legend.group(1)) if legend else None
    if not head:
        return None, legend_ordinal
    if head in SANKTION_ORDINAL:
        return head, SANKTION_ORDINAL[head]
    if head in _NUM_TO_LABEL:
        label = _NUM_TO_LABEL[head]
        return label, SANKTION_ORDINAL[label]
    return head, legend_ordinal


def _extract_kontrol_id(soup: BeautifulSoup, html: str) -> int | None:
    body = soup.find("body")
    if body and body.get("class"):
        for cls in body["class"]:
            match = _POSTID_RE.match(cls)
            if match:
                return int(match.group(1))
    # Fallback: REST API alternate link.
    alt = soup.find("link", attrs={"type": "application/json"})
    if alt and alt.get("href"):
        tail = alt["href"].rstrip("/").rsplit("/", 1)[-1]
        if tail.isdigit():
            return int(tail)
    return None


def _extract_canonical_link(soup: BeautifulSoup) -> str | None:
    link = soup.find("link", attrs={"rel": "canonical"})
    if link and link.get("href"):
        return link["href"]
    return None


def _extract_article_tag_slugs(soup: BeautifulSoup) -> list[str]:
    """Return every `tag-<slug>` value from the article element's class list.

    Numeric `tag-<id>` classes are skipped — they refer to internal taxonomy
    plumbing (a parent term that appears on every record) and aren't useful
    for bucketing.
    """
    article = soup.find("article", class_="kontrol")
    if not article or not article.get("class"):
        return []
    slugs: list[str] = []
    for cls in article["class"]:
        match = _TAG_SLUG_RE.fullmatch(cls)
        if not match:
            continue
        slug = match.group(1)
        if slug.isdigit():
            continue
        slugs.append(slug)
    return slugs


def _bucket_tags(
    slugs: list[str],
    slug_to_name: dict[str, str],
) -> tuple[str | None, str | None, str | None]:
    """Bucket the article's tag slugs into (year, kommune, dyreart) names."""
    year: str | None = None
    kommune: str | None = None
    dyreart: str | None = None
    for slug in slugs:
        name = slug_to_name.get(slug)
        if not name or name == "NULL":
            continue
        if name.isdigit() and len(name) == 4 and year is None:
            year = name
        elif name in DYREART_NAMES and dyreart is None:
            dyreart = name
        elif kommune is None:
            kommune = name
    return year, kommune, dyreart


def _extract_kontroltekst(soup: BeautifulSoup) -> str | None:
    """Collect every <p> after `<strong>Kontroltekst:</strong>` until the `Kilde:` footer."""
    marker = soup.find(
        lambda tag: tag.name == "strong" and "Kontroltekst" in tag.get_text(strip=True)
    )
    if not marker:
        return None
    # The marker sits inside a <p>; iterate its following <p> siblings.
    container = marker.find_parent("p")
    if not container:
        return None
    parts: list[str] = []
    for sibling in container.find_next_siblings("p"):
        strong = sibling.find("strong")
        if strong and strong.get_text(strip=True).rstrip(":") in {"Kilde", "Kilder"}:
            break
        text = _clean(sibling.get_text(" ", strip=True))
        if text:
            parts.append(text)
    return "\n\n".join(parts) if parts else None


def parse_detail_html(html: str, slug_to_name: dict[str, str] | None = None) -> dict:
    """Extract a single record dict from a detail HTML page.

    `slug_to_name` maps `kontrol_tag` slugs (e.g. ``"ikast-brande"``) to their
    display name (e.g. ``"Ikast-Brande"``); used to bucket the article's tag
    slugs into year / kommune / dyreart. Pass an empty dict (or omit) to skip
    taxonomy resolution.
    """
    soup = BeautifulSoup(html, "lxml")
    slug_to_name = slug_to_name or {}

    record: dict = {
        "kontrol_id": _extract_kontrol_id(soup, html),
        "link": _extract_canonical_link(soup),
        "sagsnummer": None,
        "kontrol_dato": None,
        "dyreart": None,
        "antal_dyr": None,
        "aarsag": None,
        "by": None,
        "chr_nummer": None,
        "cvr_nummer": None,
        "sanktion": None,
        "sanktion_ordinal": None,
        "kontroltekst": None,
        "tag_year": None,
        "tag_kommune": None,
        "tag_dyreart": None,
        "parsed_at": datetime.now(UTC).isoformat(),
    }

    for strong in soup.find_all("strong"):
        label = strong.get_text(strip=True).rstrip(":").strip()
        if label not in _LABEL_TO_KEY:
            continue
        p = strong.find_parent("p")
        if p is None:
            continue
        value = _clean(_strip_label(p, label))
        key = _LABEL_TO_KEY[label]
        if key == "kontrol_dato":
            record[key] = _parse_date(value)
        elif key == "antal_dyr":
            record[key] = _parse_int(value)
        elif key == "chr_nummer":
            record[key] = _zero_pad(value, 6)
        elif key == "cvr_nummer":
            record[key] = _zero_pad(value, 8)
        elif key == "sanktion":
            text, ordinal = _parse_sanktion(value)
            record["sanktion"] = text
            record["sanktion_ordinal"] = ordinal
        elif value:
            record[key] = value

    record["kontroltekst"] = _extract_kontroltekst(soup)

    article_slugs = _extract_article_tag_slugs(soup)
    year, kommune, dyreart = _bucket_tags(article_slugs, slug_to_name)
    record["tag_year"] = year
    record["tag_kommune"] = kommune
    record["tag_dyreart"] = dyreart

    # Year fallback: derive from canonical URL slug, e.g. "kontrol-2022-...".
    if record["tag_year"] is None and record["link"]:
        url_slug = record["link"].rstrip("/").rsplit("/", 1)[-1]
        match = _YEAR_IN_SLUG_RE.match(url_slug)
        if match:
            record["tag_year"] = match.group(1)

    return record
