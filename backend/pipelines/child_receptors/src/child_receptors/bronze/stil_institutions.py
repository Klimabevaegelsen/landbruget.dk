"""STIL Institutionsregisteret JSON fetcher."""

from __future__ import annotations

import json

import requests

from child_receptors.config import SOURCE_URLS

URL = SOURCE_URLS["stil"]


def validate_institutions(data: bytes) -> None:
    try:
        institutions = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("STIL institutions response is not valid JSON") from exc
    if not isinstance(institutions, list) or len(institutions) < 5000:
        count = len(institutions) if isinstance(institutions, list) else "not a list"
        raise ValueError(f"STIL institutions response contains {count} entries; expected at least 5000")


def fetch(*, get=requests.get) -> dict[str, bytes]:
    response = get(URL, timeout=240)
    response.raise_for_status()
    body = response.content
    validate_institutions(body)
    return {"institutions.json": body}
