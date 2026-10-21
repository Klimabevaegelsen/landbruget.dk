"""Raw source fetchers and response validators."""

import csv
import io
import json


def count_rows(filename: str, data: bytes) -> int:
    """Count records in one of the source formats without changing its bytes."""
    if filename.endswith(".csv"):
        return max(0, sum(1 for _ in csv.reader(io.StringIO(data.decode("utf-8-sig")), delimiter=";")) - 1)
    if filename.endswith(".jsonl"):
        return sum(1 for line in data.splitlines() if line.strip())
    parsed = json.loads(data)
    if isinstance(parsed, list):
        return len(parsed)
    if isinstance(parsed, dict) and isinstance(parsed.get("features"), list):
        return len(parsed["features"])
    if isinstance(parsed, dict) and isinstance(parsed.get("elements"), list):
        return len(parsed["elements"])
    return 1
