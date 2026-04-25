"""Bronze fetch: paginate the dyrenesdetektiv.dk WP REST index and download
every detail HTML page to disk.

Output layout (under ``output_dir``)::

    index.json          Combined REST index of all `kontrol` posts.
    kontrol_tag.json    Taxonomy snapshot for tag id → label resolution.
    details/<id>.html   One file per detail page, keyed by WP post id.
    metadata.json       Run summary (timestamp, counts, source URL).
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "Landbruget.dk/1.0 (+https://landbruget.dk)"
DEFAULT_PAGE_SIZE = 100
DEFAULT_SLEEP_SECONDS = 1.0
INDEX_FIELDS = "id,slug,link,date,modified,kontrol_tag"


def _session(user_agent: str = DEFAULT_USER_AGENT) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent, "Accept": "application/json"})
    return s


def iter_index_pages(
    base_url: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    session: requests.Session | None = None,
) -> Iterator[dict]:
    """Yield every record from the WP REST `kontrol` index, across all pages."""
    sess = session or _session()
    page = 1
    total_pages: int | None = None
    while True:
        url = f"{base_url}/wp-json/wp/v2/kontrol"
        params = {"per_page": page_size, "page": page, "_fields": INDEX_FIELDS}
        resp = sess.get(url, params=params, timeout=30)
        resp.raise_for_status()
        if total_pages is None:
            total_pages = int(resp.headers.get("X-WP-TotalPages", "1"))
            logger.info("dyrenesdetektiv index: %s pages", total_pages)
        yield from resp.json()
        if page >= total_pages:
            return
        page += 1


def fetch_taxonomy(
    base_url: str,
    session: requests.Session | None = None,
) -> list[dict]:
    """Snapshot the `kontrol_tag` taxonomy (id → label mapping)."""
    sess = session or _session()
    url = f"{base_url}/wp-json/wp/v2/kontrol_tag"
    params = {"per_page": 100, "_fields": "id,name,slug"}
    resp = sess.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


class DyrenesDetektivBronze:
    """Run a full bronze capture (index + taxonomy + every detail page)."""

    def __init__(
        self,
        base_url: str = "https://dyrenesdetektiv.dk",
        output_dir: Path | str = "data",
        sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
        user_agent: str = DEFAULT_USER_AGENT,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.output_dir = Path(output_dir)
        self.sleep_seconds = sleep_seconds
        self.user_agent = user_agent
        self.page_size = page_size
        self.session = _session(user_agent)

    def run(self, limit: int | None = None) -> dict:
        """Capture index + taxonomy, then download up to `limit` detail pages.

        Returns a manifest dict (also written to metadata.json).
        """
        start = datetime.now(UTC)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        details_dir = self.output_dir / "details"
        details_dir.mkdir(exist_ok=True)

        index_records = list(iter_index_pages(self.base_url, self.page_size, self.session))
        (self.output_dir / "index.json").write_text(
            json.dumps(index_records, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("Saved %s index records", len(index_records))

        taxonomy = fetch_taxonomy(self.base_url, self.session)
        (self.output_dir / "kontrol_tag.json").write_text(
            json.dumps(taxonomy, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        targets = index_records[:limit] if limit else index_records
        detail_count = 0
        detail_errors = 0
        total_bytes = 0
        for record in targets:
            link = record.get("link")
            wp_id = record.get("id")
            if not link or wp_id is None:
                logger.warning("Skipping record with missing link/id: %s", record)
                detail_errors += 1
                continue
            try:
                resp = self.session.get(link, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as exc:
                logger.warning("Detail fetch failed for %s: %s", link, exc)
                detail_errors += 1
                continue
            out = details_dir / f"{wp_id}.html"
            out.write_bytes(resp.content)
            detail_count += 1
            total_bytes += len(resp.content)
            if self.sleep_seconds:
                time.sleep(self.sleep_seconds)

        finished = datetime.now(UTC)
        manifest = {
            "fetch_timestamp": start.isoformat(),
            "finished_timestamp": finished.isoformat(),
            "duration_seconds": (finished - start).total_seconds(),
            "source_url": f"{self.base_url}/kontrol/",
            "base_url": self.base_url,
            "index_count": len(index_records),
            "detail_count": detail_count,
            "detail_errors": detail_errors,
            "detail_total_bytes": total_bytes,
            "limit_applied": limit,
            "user_agent": self.user_agent,
        }
        (self.output_dir / "metadata.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info(
            "Bronze run complete: %s details (%s errors), %s bytes",
            detail_count,
            detail_errors,
            total_bytes,
        )
        return manifest
