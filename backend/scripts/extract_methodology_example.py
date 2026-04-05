#!/usr/bin/env python3
"""
Extract a real disaggregation example for the methodology page scrolly visualization.

Finds a compelling CVR+crop group with 3-5 fields, a recognizable pesticide,
high confidence score, and outputs JSON for the frontend.

Usage:
    cd backend && source venv/bin/activate
    python scripts/extract_methodology_example.py [--year 2022]
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("extract_example")

BUCKET = "landbruget-data"
DEFAULT_YEAR = 2022


def _has_r2_env() -> bool:
    return all(os.getenv(k) for k in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ACCOUNT_ID"))


def _setup_duckdb_r2(conn: duckdb.DuckDBPyConnection) -> bool:
    if not _has_r2_env():
        return False
    try:
        from common.storage.filesystem import setup_duckdb_cloud_auth

        return setup_duckdb_cloud_auth(conn)
    except Exception as e:
        log.warning(f"Native R2 auth failed: {e}")
        return False


def _get_wrangler_token() -> str:
    for config_path in [
        Path.home() / "Library" / "Preferences" / ".wrangler" / "config" / "default.toml",
        Path.home() / ".config" / ".wrangler" / "config" / "default.toml",
    ]:
        if config_path.exists():
            for line in config_path.read_text().splitlines():
                if line.startswith("oauth_token"):
                    return line.split("=", 1)[1].strip().strip('"')
    raise RuntimeError("Could not read wrangler OAuth token")


def _wrangler_list_prefix(prefix: str) -> list[str]:
    subprocess.run(["wrangler", "whoami"], capture_output=True, text=True, timeout=15)
    account_id = os.getenv("R2_ACCOUNT_ID", "a5f130bfd0d34de38f8e77f6a0f40a27")
    token = _get_wrangler_token()

    import urllib.request

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
        f"/r2/buckets/{BUCKET}/objects?prefix={prefix}&limit=1000"
    )
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})  # noqa: S310
    with urllib.request.urlopen(req) as resp:  # noqa: S310
        data = json.loads(resp.read())
    if not data.get("success"):
        raise RuntimeError(f"Cloudflare API error: {data.get('errors')}")
    result = data.get("result", [])
    objects = result.get("objects", []) if isinstance(result, dict) else result if isinstance(result, list) else []
    return [obj["key"] for obj in objects if obj.get("key", "").endswith(".parquet")]


def _wrangler_download(r2_key: str, local_path: str) -> None:
    cmd = ["wrangler", "r2", "object", "get", f"{BUCKET}/{r2_key}", "--file", local_path, "--remote"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"wrangler download failed: {result.stderr}")


def load_parquet(
    conn: duckdb.DuckDBPyConnection, r2_prefix: str, table_name: str, has_native: bool, tmpdir: str
) -> int:
    """Load parquet from R2 into DuckDB table."""
    if has_native:
        for path in [f"r2://{BUCKET}/{r2_prefix}/*.parquet", f"r2://{BUCKET}/{r2_prefix}/data.parquet"]:
            try:
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{path}')")
                return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            except Exception:  # noqa: S112
                continue

    # Wrangler fallback
    keys = _wrangler_list_prefix(r2_prefix.rstrip("/") + "/")
    parquet_keys = [k for k in keys if k.endswith(".parquet")]
    if not parquet_keys:
        raise RuntimeError(f"No parquet files found under {r2_prefix}")
    key = sorted(parquet_keys)[-1]  # latest
    log.info(f"  Downloading {key}...")
    local_path = str(Path(tmpdir) / key.replace("/", "_"))
    _wrangler_download(key, local_path)
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{local_path}')")
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def find_example(conn: duckdb.DuckDBPyConnection) -> dict:
    """Find a compelling example: CVR+crop with 3-5 fields, high confidence, recognizable product."""
    log.info("Finding compelling example...")

    # Find CVR+crop groups with 3-5 fields and perfect confidence
    result = conn.execute("""
        SELECT
            cvr_number,
            PesticideName,
            PesticideRegistrationNumber,
            DosageUnit,
            AllocationMethod,
            MatchConfidence,
            COUNT(DISTINCT field_uuid) AS n_fields,
            SUM(AllocatedArea) AS total_area,
            SUM(DosageQuantity) AS total_dose,
            LIST(field_uuid) AS field_uuids,
            LIST(AllocatedArea) AS field_areas,
            LIST(DosageQuantity) AS field_doses,
            LIST(primary_field_id) AS field_ids,
            LIST(municipality) AS municipalities
        FROM disagg
        WHERE MatchConfidence >= 0.95
          AND AllocationMethod = 'Marker_ApplicationAreaToTotalFieldArea_FieldProportional'
          AND DosageQuantity > 0
          AND field_uuid IS NOT NULL
        GROUP BY cvr_number, PesticideName, PesticideRegistrationNumber, DosageUnit, AllocationMethod, MatchConfidence, OriginalPesticideRowID
        HAVING COUNT(DISTINCT field_uuid) BETWEEN 3 AND 5
        ORDER BY MatchConfidence DESC, n_fields ASC
        LIMIT 200
    """).fetchall()

    if not result:
        raise RuntimeError("No suitable examples found")

    columns = [
        "cvr_number",
        "pesticide_name",
        "reg_nr",
        "unit",
        "method",
        "confidence",
        "n_fields",
        "total_area",
        "total_dose",
        "field_uuids",
        "field_areas",
        "field_doses",
        "field_ids",
        "municipalities",
    ]

    # Prefer well-known products
    preferred = ["roundup", "prosaro", "amistar", "boxer", "ally", "starane", "harmony"]
    best = None
    for row in result:
        d = dict(zip(columns, row, strict=False))
        name_lower = d["pesticide_name"].lower()
        if any(p in name_lower for p in preferred):
            best = d
            break

    # If no preferred product found, take the first one
    if not best:
        best = dict(zip(columns, result[0], strict=False))

    log.info(f"  Found: CVR {best['cvr_number']}, {best['pesticide_name']}, {best['n_fields']} fields")
    return best


def get_field_centroids(conn: duckdb.DuckDBPyConnection, field_uuids: list[str]) -> dict[str, tuple[float, float]]:
    """Get centroids for fields from FVM marker data."""
    uuid_list = ", ".join(f"'{u}'" for u in field_uuids)
    try:
        rows = conn.execute(f"""
            SELECT field_uuid,
                   ST_X(ST_Centroid(TRY(ST_GeomFromWKB(geometry)))) AS lng,
                   ST_Y(ST_Centroid(TRY(ST_GeomFromWKB(geometry)))) AS lat
            FROM fvm
            WHERE field_uuid IN ({uuid_list})
        """).fetchall()
    except Exception:
        # Try without WKB conversion (geometry may already be parsed)
        try:
            rows = conn.execute(f"""
                SELECT field_uuid,
                       ST_X(ST_Centroid(geometry)) AS lng,
                       ST_Y(ST_Centroid(geometry)) AS lat
                FROM fvm
                WHERE field_uuid IN ({uuid_list})
            """).fetchall()
        except Exception:
            log.warning("Could not extract centroids — geometry column format unknown. Trying bbox...")
            rows = conn.execute(f"""
                SELECT field_uuid,
                       (ST_XMin(geometry) + ST_XMax(geometry)) / 2 AS lng,
                       (ST_YMin(geometry) + ST_YMax(geometry)) / 2 AS lat
                FROM fvm
                WHERE field_uuid IN ({uuid_list})
            """).fetchall()

    return {r[0]: (round(r[1], 6), round(r[2], 6)) for r in rows if r[1] is not None}


def get_crop_name(conn: duckdb.DuckDBPyConnection, field_uuids: list[str]) -> str:
    """Get crop name from FVM marker data."""
    uuid_list = ", ".join(f"'{u}'" for u in field_uuids)
    try:
        row = conn.execute(f"""
            SELECT COALESCE(crop_name, afgroede_navn, 'Ukendt') AS name
            FROM fvm WHERE field_uuid IN ({uuid_list}) LIMIT 1
        """).fetchone()
        return row[0] if row else "Ukendt"
    except Exception:
        return "Ukendt"


def main():
    parser = argparse.ArgumentParser(description="Extract real methodology example from R2")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR, help="Pesticide year (default: 2022)")
    args = parser.parse_args()

    year = args.year
    field_year = year + 1

    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial")
    has_native = _setup_duckdb_r2(conn)
    tmpdir = tempfile.mkdtemp(prefix="extract_example_")

    # Load disaggregated data
    log.info(f"Loading gold disaggregation data for {year}_{field_year}...")
    gold_prefix = f"gold/pesticide_disaggregation_{year}_{field_year}"
    n = load_parquet(conn, gold_prefix, "disagg", has_native, tmpdir)
    log.info(f"  Loaded {n:,} rows")

    # Print columns for debugging
    cols = conn.execute("DESCRIBE disagg").fetchall()
    log.info(f"  Columns: {[c[0] for c in cols]}")

    # Find example
    example = find_example(conn)

    # Load FVM marker data for centroids
    log.info(f"Loading FVM marker data for {field_year}...")
    fvm_prefix = f"silver/fvm_marker_{field_year}"
    try:
        n_fvm = load_parquet(conn, fvm_prefix, "fvm", has_native, tmpdir)
        log.info(f"  Loaded {n_fvm:,} rows")
    except Exception as e:
        log.warning(f"  Could not load FVM data: {e}")
        n_fvm = 0

    # Get centroids
    centroids = {}
    crop_name = "Ukendt"
    if n_fvm > 0:
        centroids = get_field_centroids(conn, example["field_uuids"])
        crop_name = get_crop_name(conn, example["field_uuids"])

    # Build output
    fields = []
    for i, uuid in enumerate(example["field_uuids"]):
        centroid = centroids.get(uuid)
        fields.append(
            {
                "fieldUuid": uuid,
                "fieldId": example["field_ids"][i] if i < len(example["field_ids"]) else None,
                "areaHa": round(example["field_areas"][i], 2),
                "allocatedDose": round(example["field_doses"][i], 3),
                "centroid": list(centroid) if centroid else None,
            }
        )

    municipality = example["municipalities"][0] if example["municipalities"] else "Ukendt"

    # Area deviation
    reported_area = example["total_area"]
    total_field_area = sum(f["areaHa"] for f in fields)
    deviation_pct = round(abs(reported_area - total_field_area) / reported_area * 100, 2) if reported_area else 0

    output = {
        "year": year,
        "fieldYear": field_year,
        "cvr": example["cvr_number"],
        "cropName": crop_name,
        "pesticide": {
            "name": example["pesticide_name"],
            "registrationNumber": example["reg_nr"],
            "totalDose": round(example["total_dose"], 3),
            "unit": example["unit"],
            "reportedAreaHa": round(reported_area, 2),
        },
        "fields": sorted(fields, key=lambda f: f["areaHa"], reverse=True),
        "confidence": example["confidence"],
        "areaDeviationPct": deviation_pct,
        "allocationMethod": "Proportional arealfordeling",
        "municipality": municipality,
    }

    # Output
    output_path = Path(__file__).parent / "reports" / "methodology_example.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    log.info(f"\nOutput written to {output_path}")

    # Also print to stdout
    print("\n" + "=" * 60)  # noqa: T201
    print("METHODOLOGY EXAMPLE DATA")  # noqa: T201
    print("=" * 60)  # noqa: T201
    print(json.dumps(output, indent=2, ensure_ascii=False))  # noqa: T201

    conn.close()


if __name__ == "__main__":
    main()
