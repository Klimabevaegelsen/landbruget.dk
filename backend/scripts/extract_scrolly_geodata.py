#!/usr/bin/env python3
"""
Extract real geodata for methodology scrolly visualizations from R2.

Extracts:
1. Haderslev municipality boundary polygon (from DAGI kommuner) — simplified, WGS84
2. Municipality centroids weighted by disagg record counts — for heatmap

Usage:
    python scripts/extract_scrolly_geodata.py
"""

import json
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("extract_scrolly_geodata")

BUCKET = "landbruget-data"


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
    size = Path(local_path).stat().st_size
    log.info(f"  Downloaded {r2_key} ({size:,} bytes)")


def download_latest(prefix: str, tmpdir: str, name: str) -> str:
    """Download the latest parquet file under a prefix."""
    keys = _wrangler_list_prefix(prefix.rstrip("/") + "/")
    parquet_keys = [k for k in keys if k.endswith(".parquet")]
    if not parquet_keys:
        raise RuntimeError(f"No parquet files found under {prefix}")
    key = sorted(parquet_keys)[-1]
    local_path = str(Path(tmpdir) / f"{name}.parquet")
    _wrangler_download(key, local_path)
    return local_path


def count_coords(geojson):
    if geojson["type"] == "Polygon":
        return sum(len(ring) for ring in geojson["coordinates"])
    if geojson["type"] == "MultiPolygon":
        return sum(len(ring) for poly in geojson["coordinates"] for ring in poly)
    return 0


def main():
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial")
    tmpdir = tempfile.mkdtemp(prefix="extract_scrolly_")
    log.info(f"Working in {tmpdir}")

    # ── 1. DAGI kommuner ─────────────────────────────────────────
    log.info("Downloading DAGI kommuner...")
    dagi_path = download_latest("silver/dagi_kommuner", tmpdir, "dagi")
    conn.execute(f"CREATE TABLE dagi AS SELECT * FROM read_parquet('{dagi_path}')")
    n_dagi = conn.execute("SELECT COUNT(*) FROM dagi").fetchone()[0]
    log.info(f"  Loaded {n_dagi} municipalities")

    cols = [c[0] for c in conn.execute("DESCRIBE dagi").fetchall()]
    log.info(f"  Columns: {cols}")

    # The DAGI table has pre-computed centroid_x/centroid_y and area_m2
    # The geometry column is native GEOMETRY('OGC:CRS84') — already WGS84
    # So we can use it directly, no ST_GeomFromWKB or ST_Transform needed

    # Show sample
    sample_rows = conn.execute("SELECT code, name FROM dagi LIMIT 5").fetchall()
    log.info(f"  Sample: {sample_rows}")

    # ── 2. Extract Haderslev polygon ─────────────────────────────
    log.info("Extracting Haderslev polygon...")

    # Haderslev municipality code: try common codes
    haderslev_row = None
    for code in ["0510", "510", "0530", "530"]:
        try:
            haderslev_row = conn.execute(f"""
                SELECT ST_AsGeoJSON(
                    ST_SimplifyPreserveTopology(geometry, 0.005)
                ) AS geojson, name
                FROM dagi
                WHERE CAST(code AS VARCHAR) = '{code}'
            """).fetchone()
            if haderslev_row and haderslev_row[0]:
                log.info(f"  Found Haderslev with code={code}, name={haderslev_row[1]}")
                break
        except Exception as e:
            log.debug(f"  Code {code} failed: {e}")

    if not haderslev_row or not haderslev_row[0]:
        log.info("  Trying name-based lookup...")
        haderslev_row = conn.execute("""
            SELECT ST_AsGeoJSON(
                ST_SimplifyPreserveTopology(geometry, 0.005)
            ) AS geojson, name
            FROM dagi
            WHERE LOWER(name) LIKE '%haderslev%'
        """).fetchone()

    if not haderslev_row or not haderslev_row[0]:
        log.error("Could not find Haderslev in DAGI data!")
        all_names = conn.execute("SELECT code, name FROM dagi ORDER BY name").fetchall()
        for r in all_names:
            log.info(f"  {r[0]}: {r[1]}")
        sys.exit(1)

    haderslev_geojson = json.loads(haderslev_row[0])
    n_coords = count_coords(haderslev_geojson)
    log.info(f"  Haderslev polygon: {n_coords} coordinates at tolerance=0.005")

    # If still too many, simplify more aggressively
    if n_coords > 500:
        log.info("  Still too many coords, increasing tolerance...")
        for tol in [0.01, 0.02, 0.03, 0.05]:
            row = conn.execute(f"""
                SELECT ST_AsGeoJSON(
                    ST_SimplifyPreserveTopology(geometry, {tol})
                ) AS geojson
                FROM dagi
                WHERE LOWER(name) LIKE '%haderslev%'
            """).fetchone()
            candidate = json.loads(row[0])
            nc = count_coords(candidate)
            log.info(f"    tolerance={tol}: {nc} coordinates")
            if nc <= 500:
                haderslev_geojson = candidate
                n_coords = nc
                break
        else:
            haderslev_geojson = candidate
            n_coords = count_coords(haderslev_geojson)

    log.info(f"  Final Haderslev polygon: {n_coords} coordinates")

    # ── 3. Municipality centroids for heatmap ────────────────────
    # Use pre-computed centroid_x/centroid_y from DAGI silver layer
    log.info("Extracting municipality centroids from DAGI...")

    centroids = conn.execute("""
        SELECT name, centroid_x AS lng, centroid_y AS lat, area_m2
        FROM dagi
        WHERE name IS NOT NULL AND centroid_x IS NOT NULL
    """).fetchall()
    log.info(f"  Got {len(centroids)} municipality centroids")

    # ── 4. Disaggregation record counts per CVR ──────────────────
    log.info("Downloading pesticide disaggregation data...")
    disagg_path = download_latest("gold/pesticide_disaggregation_2022_2023", tmpdir, "disagg")
    conn.execute(f"CREATE TABLE disagg AS SELECT * FROM read_parquet('{disagg_path}')")
    n_disagg = conn.execute("SELECT COUNT(*) FROM disagg").fetchone()[0]
    log.info(f"  Loaded {n_disagg:,} disaggregation rows")

    cvr_counts = conn.execute("""
        SELECT cvr_number, COUNT(*) AS n_records
        FROM disagg
        WHERE cvr_number IS NOT NULL
        GROUP BY cvr_number
        ORDER BY n_records DESC
    """).fetchall()
    log.info(f"  {len(cvr_counts)} unique CVRs in disagg data")

    # ── 5. Build heatmap: municipality centroids weighted by area ─
    log.info("Computing heatmap weights...")
    total_area = sum(r[3] for r in centroids if r[3])

    heatmap_points = []
    for name, lng, lat, area in centroids:
        if not lng or not lat or not area:
            continue
        weight = int((area / total_area) * n_disagg) if total_area > 0 else 100
        heatmap_points.append(
            {
                "lng": round(lng, 4),
                "lat": round(lat, 4),
                "weight": max(weight, 10),
                "name": name,
            }
        )

    heatmap_points.sort(key=lambda x: x["weight"], reverse=True)
    log.info(f"  Generated {len(heatmap_points)} heatmap points")
    for hp in heatmap_points[:5]:
        log.info(f"    {hp['name']}: weight={hp['weight']}, ({hp['lng']}, {hp['lat']})")

    # ── 6. Build output ──────────────────────────────────────────
    output = {
        "haderslev_polygon": haderslev_geojson,
        "heatmap_points": heatmap_points,
        "metadata": {
            "dagi_municipalities": n_dagi,
            "disagg_records": n_disagg,
            "disagg_unique_cvrs": len(cvr_counts),
            "haderslev_coords": n_coords,
        },
    }

    output_path = Path(__file__).parent / "reports" / "scrolly_geodata.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    log.info(f"\nOutput written to {output_path}")

    log.info("\n" + "=" * 60)
    log.info("SCROLLY GEODATA EXTRACTION — COMPLETE")
    log.info("=" * 60)
    log.info(f"Haderslev polygon: {n_coords} coordinates ({haderslev_geojson['type']})")
    log.info(f"Heatmap points: {len(heatmap_points)} municipalities")
    log.info(f"Disagg records used: {n_disagg:,}")
    log.info(f"Top 5 municipalities: {[p['name'] for p in heatmap_points[:5]]}")
    log.info(f"Output: {output_path}")

    conn.close()


if __name__ == "__main__":
    main()
