"""Parity manifest helpers for user-facing Supabase replacement contracts."""

import json
from functools import lru_cache
from pathlib import Path

PARITY_MANIFEST_PATH = Path(__file__).resolve().parent.parent / "parity_manifest.yaml"


@lru_cache(maxsize=1)
def load_parity_manifest() -> dict:
    return json.loads(PARITY_MANIFEST_PATH.read_text(encoding="utf-8"))


def get_manifest_version() -> str:
    manifest = load_parity_manifest()
    return str(manifest.get("version", "unknown"))


def get_manifest_contracts() -> list[dict]:
    manifest = load_parity_manifest()
    return list(manifest.get("contracts", []))
