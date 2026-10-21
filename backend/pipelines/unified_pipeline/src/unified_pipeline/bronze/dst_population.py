"""Fetch and parse population totals from Danmarks Statistik's FOLK1A table."""

import csv
import io

import requests

FOLK1A_DATA_URL = "https://api.statbank.dk/v1/data"
FOLK1A_REQUEST = {
    "table": "FOLK1A",
    "format": "CSV",
    "lang": "en",
    "valuePresentation": "Code",
    "variables": [
        {"code": "OMRÅDE", "values": ["*"]},
        {"code": "TID", "values": ["(1)"]},
    ],
}


def fetch_folk1a_csv() -> str:
    """Fetch the latest FOLK1A quarter as semicolon-separated CSV.

    The public StatBank API does not require a key. Three attempts are made
    for transient HTTP or connection failures.
    """
    last_error = None
    for _ in range(4):
        try:
            response = requests.post(
                FOLK1A_DATA_URL,
                json=FOLK1A_REQUEST,
                timeout=60,
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as error:
            last_error = error
    raise RuntimeError("Failed to fetch FOLK1A data after 3 retries") from last_error


def parse_folk1a_csv(text: str) -> tuple[str, int, dict[int, int]]:
    """Return the latest period, Denmark total, and municipality populations."""
    reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")), delimiter=";")
    period = None
    national_total = None
    municipality_population: dict[int, int] = {}

    for row in reader:
        area = row.get("OMRÅDE", "").strip()
        row_period = row.get("TID", "").strip()
        value = row.get("INDHOLD", "").strip().replace(" ", "")
        if not area or not row_period or not value:
            continue
        try:
            area_code = int(area)
            population = int(value)
        except ValueError as error:
            raise ValueError(f"Invalid FOLK1A row: {row!r}") from error

        if period is None:
            period = row_period
        elif row_period != period:
            raise ValueError("FOLK1A response contains more than one period")

        if area_code == 0:
            national_total = population
        elif area_code not in {81, 82, 83, 84, 85}:
            municipality_population[area_code] = population

    if period is None or national_total is None:
        raise ValueError("FOLK1A response is missing its period or Denmark total")
    return period, national_total, municipality_population
