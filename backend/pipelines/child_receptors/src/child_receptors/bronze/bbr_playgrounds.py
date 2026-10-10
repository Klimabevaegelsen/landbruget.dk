"""Cursor-paged BBR technical installation fetcher."""

from __future__ import annotations

import json
import os
import re
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from urllib.parse import quote, quote_plus

import requests

from child_receptors.config import KOMMUNEKODER, SOURCE_URLS

BASE_URL = SOURCE_URLS["bbr"]
PAGE_SIZE = 1000
TIMEOUT_SECONDS = 240


def make_query(kommunekode: str, virkningstid: str, cursor: str | None = None) -> str:
    # The first page must omit `after` entirely; an empty-string cursor is a 400 from Datafordeler.
    after = f", after: {json.dumps(cursor)}" if cursor else ""
    return f"""{{
      BBR_TekniskAnlaeg(first: {PAGE_SIZE}{after}, virkningstid: {json.dumps(virkningstid)},
        where: {{ kommunekode: {{ eq: {json.dumps(kommunekode)} }}, tek020Klassifikation: {{ eq: "1905" }} }}) {{
        nodes {{ id_lokalId kommunekode status registreringFra virkningFra tek109Koordinat {{ wkt }} }}
        pageInfo {{ hasNextPage endCursor }}
      }}
    }}"""


def _redact(message: str, secret: str) -> str:
    if not secret:
        return message
    for value in (secret, quote(secret, safe=""), quote_plus(secret)):
        message = message.replace(value, "***")
    return re.sub(r"(?i)([?&]apikey=)[^&#\s]*", r"\1***", message)


def fetch_kommune(
    kommunekode: str,
    *,
    api_key: str,
    virkningstid: str,
    post=requests.post,
    retries: int = 3,
    backoff: Callable[[float], None] = time.sleep,
) -> list[dict]:
    """Fetch every page for one kommune; errors are returned to fail the national run."""
    cursor = None
    nodes: list[dict] = []
    for page_number in range(1, 100000):
        error: Exception | None = None
        for attempt in range(retries):
            try:
                response = post(
                    BASE_URL,
                    params={"apikey": api_key},
                    headers={"Content-Type": "application/json"},
                    json={"query": make_query(kommunekode, virkningstid, cursor)},
                    timeout=TIMEOUT_SECONDS,
                )
                if response.status_code >= 400:
                    # Never use raise_for_status(): its message embeds the URL incl. the API key.
                    raise RuntimeError(f"HTTP {response.status_code}: {response.text[:300]}")
                payload = response.json()
                if payload.get("errors"):
                    raise RuntimeError(f"GraphQL errors: {payload['errors']}")
                result = payload.get("data", {}).get("BBR_TekniskAnlaeg")
                if not isinstance(result, dict) or not isinstance(result.get("nodes"), list):
                    raise ValueError("GraphQL response is missing BBR_TekniskAnlaeg.nodes")
                page_info = result.get("pageInfo") or {}
                break
            except Exception as exc:
                error = RuntimeError(_redact(str(exc), api_key))
                if attempt + 1 < retries:
                    backoff(2**attempt)
        else:
            raise RuntimeError(
                f"BBR kommune {kommunekode} page {page_number} failed after {retries} retries"
            ) from error

        nodes.extend(result["nodes"])
        if not page_info.get("hasNextPage"):
            return nodes
        next_cursor = page_info.get("endCursor")
        if not next_cursor or next_cursor == cursor:
            raise ValueError(f"BBR kommune {kommunekode} returned an invalid pagination cursor")
        cursor = next_cursor
    raise RuntimeError(f"BBR kommune {kommunekode} exceeded the safety page limit")


def fetch(
    *,
    api_key: str | None = None,
    kommunekoder: tuple[str, ...] | list[str] = tuple(KOMMUNEKODER),
    now: datetime | None = None,
    post=requests.post,
    backoff: Callable[[float], None] = time.sleep,
) -> dict[str, bytes]:
    """Fetch all 99 kommune codes with six workers and emit one compact JSONL file."""
    key = api_key or os.getenv("DATAFORDELER_GRAPHQL_API_KEY")
    if not key:
        raise RuntimeError("DATAFORDELER_GRAPHQL_API_KEY is required for the BBR fetch")
    instant = now or datetime.now(UTC)
    virkningstid = instant.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    results: dict[str, list[dict]] = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(
                fetch_kommune,
                code,
                api_key=key,
                virkningstid=virkningstid,
                post=post,
                backoff=backoff,
            ): code
            for code in kommunekoder
        }
        for future in as_completed(futures):
            code = futures[future]
            results[code] = future.result()
    lines = [
        json.dumps(node, ensure_ascii=False, separators=(",", ":"))
        for code in sorted(results)
        for node in results[code]
    ]
    return {"bbr_tekniskanlaeg_1905.jsonl": ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")}
