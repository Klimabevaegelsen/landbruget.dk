"""GeoFA playground WFS fetcher."""

from __future__ import annotations

import json

import requests

from child_receptors.config import SOURCE_URLS

URL = SOURCE_URLS["geofa"]


def validate_geofa(data: bytes) -> None:
    try:
        payload = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("GeoFA response is not valid GeoJSON") from exc
    features = payload.get("features") if isinstance(payload, dict) else None
    if not isinstance(features, list) or len(features) < 20000:
        count = len(features) if isinstance(features, list) else "not a feature list"
        raise ValueError(f"GeoFA response contains {count} features; expected at least 20000")


def fetch(*, get=requests.get) -> dict[str, bytes]:
    response = get(URL, timeout=240)
    response.raise_for_status()
    body = response.content
    validate_geofa(body)
    return {"geofa_t5800_fac_pkt.json": body}
