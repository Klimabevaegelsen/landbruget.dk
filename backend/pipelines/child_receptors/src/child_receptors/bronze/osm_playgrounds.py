"""Regional Overpass playground fetcher."""

from __future__ import annotations

import json
import time
import unicodedata
from collections.abc import Callable

import requests

from child_receptors.config import OSM_REGIONS, SOURCE_URLS

ENDPOINT = SOURCE_URLS["osm"]
ENDPOINTS = (
    ENDPOINT,
    "https://overpass.private.coffee/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
)
USER_AGENT = "landbruget.dk child_receptors"
REGIONS = OSM_REGIONS
BACKOFF_SECONDS = (30, 60, 120)


def region_slug(region: str) -> str:
    normalized = unicodedata.normalize("NFKD", region).encode("ascii", "ignore").decode().lower()
    return "_".join(normalized.replace("region", "").split())


def make_query(region: str) -> str:
    escaped_region = region.replace('"', '\\"')
    return (
        f'[out:json][timeout:200];area["name"="{escaped_region}"][admin_level=4]->.a;'
        'nwr["leisure"="playground"](area.a);out center tags;'
    )


def validate_region(data: bytes, region: str) -> None:
    try:
        payload = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Overpass response for {region} is not valid JSON") from exc
    elements = payload.get("elements") if isinstance(payload, dict) else None
    if not isinstance(elements, list) or len(elements) < 300:
        count = len(elements) if isinstance(elements, list) else "not an element list"
        raise ValueError(f"Overpass {region} contains {count} elements; expected at least 300")


def fetch(
    *,
    regions: tuple[str, ...] | list[str] = REGIONS,
    post=requests.post,
    backoff: Callable[[float], None] = time.sleep,
) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for index, region in enumerate(regions):
        body: bytes | None = None
        last_error: Exception | None = None
        for endpoint in ENDPOINTS:
            for attempt in range(4):
                try:
                    response = post(
                        endpoint,
                        data={"data": make_query(region)},
                        headers={"User-Agent": USER_AGENT},
                        timeout=240,
                    )
                    response.raise_for_status()
                    body = response.content
                    validate_region(body, region)
                    break
                except Exception as exc:
                    last_error = exc
                    body = None
                    if attempt < 3:
                        backoff(BACKOFF_SECONDS[attempt])
            if body is not None:
                break
        if body is None:
            raise RuntimeError(f"Overpass fetch failed for {region} across all configured endpoints") from last_error
        files[f"osm_playgrounds_{region_slug(region)}.json"] = body
        if index + 1 < len(regions):
            backoff(15)
    return files
