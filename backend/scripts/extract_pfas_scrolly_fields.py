#!/usr/bin/env python3
"""
Extract PFAS-sprayed field geometries that intersect catchment areas.

Joins FVM marker data with pesticide disaggregation to find fields
actually sprayed with fluorinated products AND within the catchment polygons.

Usage:
    cd backend && source venv/bin/activate
    python scripts/extract_pfas_scrolly_fields.py
"""

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
log = logging.getLogger("extract_pfas_fields")

BUCKET = "landbruget-data"

# Known fluorinated product name fragments (commercial names from disaggregation)
FLUORINATED_PRODUCTS = [
    "DFF",
    "Propulse",
    "Mavrik",
    "Boxer",
    "Belkar",
    "Pictor",
    "Kalif",
    "Prosaro",
    "Proline",
    "diflufenican",
    "fluopyram",
    "tau-fluvalinat",
    "fluroxypyr",
    "prothioconazol",
    "flonicamid",
]

# Catchment polygons in EPSG:25832 (UTM) — we'll buffer these slightly
# These are the WGS84 coordinates from scrolly-geo-data.ts, but we need UTM
# So we'll use the grukos catchment data directly from R2 if possible,
# or define approximate UTM bounding boxes and use ST_Intersects with
# the catchment polygons transformed to UTM.

# WGS84 catchment polygons (from scrolly-geo-data.ts)
SKRYDSTRUP_CATCHMENT_WGS84 = {
    "type": "Polygon",
    "coordinates": [
        [
            [9.244141595163171, 55.244865947779],
            [9.241407939933111, 55.24665574805077],
            [9.242179265036919, 55.24897916482197],
            [9.250907693047356, 55.25046406265869],
            [9.259001209638114, 55.2503422220633],
            [9.261820152870985, 55.2495128020169],
            [9.26594685738193, 55.246608704083116],
            [9.26675873233253, 55.24506963528733],
            [9.274775889328861, 55.23958869633409],
            [9.27370317359429, 55.238812979328955],
            [9.271184485605842, 55.23919710868117],
            [9.270938429489554, 55.23997057213724],
            [9.258846671572243, 55.246481925638946],
            [9.252549546148915, 55.24689267006048],
            [9.247642003533327, 55.2446426452144],
            [9.244141595163171, 55.244865947779],
        ]
    ],
}

SONDER_FELDING_CATCHMENT_WGS84 = {
    "type": "Polygon",
    "coordinates": [
        [
            [8.867644709835517, 55.885830669836785],
            [8.866036569104066, 55.885641718543106],
            [8.858413670075333, 55.888329165574355],
            [8.841681827723043, 55.89579528185467],
            [8.835266492654632, 55.90027940516708],
            [8.818527935992744, 55.909640702935825],
            [8.810311488877407, 55.9132687911205],
            [8.793097982066216, 55.92563617733808],
            [8.78524257857227, 55.93340545708983],
            [8.775494921650123, 55.93804732740225],
            [8.775105982613367, 55.94013099657452],
            [8.777552280872898, 55.94180730613502],
            [8.779643577441233, 55.94210185042748],
            [8.78635477888159, 55.94046614476541],
            [8.818825216368033, 55.92653711189294],
            [8.830740551635136, 55.92030288830109],
            [8.832267975838556, 55.91817954120429],
            [8.83903188871153, 55.91524663977392],
            [8.838820040533419, 55.91028005873481],
            [8.840337177886557, 55.90798492517223],
            [8.84965868719793, 55.90457609794925],
            [8.861124387119139, 55.89657342721125],
            [8.861004874957636, 55.890463492583386],
            [8.867644709835517, 55.885830669836785],
        ]
    ],
}


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
    keys = _wrangler_list_prefix(prefix.rstrip("/") + "/")
    parquet_keys = [k for k in keys if k.endswith(".parquet")]
    if not parquet_keys:
        raise RuntimeError(f"No parquet files found under {prefix}")
    key = sorted(parquet_keys)[-1]
    local_path = str(Path(tmpdir) / f"{name}.parquet")
    _wrangler_download(key, local_path)
    return local_path


def swap_coords(geojson: dict) -> dict:
    """DuckDB ST_Transform to EPSG:4326 outputs [lat,lng] — swap to [lng,lat]."""

    def swap_ring(ring):
        return [[round(c[1], 6), round(c[0], 6)] for c in ring]

    if geojson["type"] == "Polygon":
        geojson["coordinates"] = [swap_ring(r) for r in geojson["coordinates"]]
    elif geojson["type"] == "MultiPolygon":
        geojson["coordinates"] = [[swap_ring(r) for r in poly] for poly in geojson["coordinates"]]
    return geojson


def build_product_filter_sql():
    conditions = " OR ".join(f"d.PesticideName ILIKE '%{p}%'" for p in FLUORINATED_PRODUCTS)
    return f"({conditions})"


def create_catchment_table(conn, name: str, geojson: dict):
    """Create a table with the catchment polygon transformed to EPSG:25832."""
    # DuckDB PROJ expects EPSG:4326 as [lat, lng] but GeoJSON is [lng, lat]
    # Swap coordinates before transforming
    swapped = {
        "type": geojson["type"],
        "coordinates": [[[lat, lng] for lng, lat in ring] for ring in geojson["coordinates"]],
    }
    geojson_str = json.dumps(swapped)
    conn.execute(f"""
        CREATE TABLE {name} AS
        SELECT ST_Transform(
            ST_GeomFromGeoJSON('{geojson_str}'),
            'EPSG:4326',
            'EPSG:25832'
        ) AS geometry
    """)
    # Also check with a buffer (2km) for fields that overlap the catchment edge
    conn.execute(f"""
        CREATE TABLE {name}_buffered AS
        SELECT ST_Buffer(geometry, 2000) AS geometry FROM {name}
    """)
    log.info(f"  Created catchment table {name} + 2km buffer")


def main():
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial")
    tmpdir = tempfile.mkdtemp(prefix="extract_pfas_fields_")
    log.info(f"Working in {tmpdir}")

    # ── 1. Create catchment polygons in UTM ───────────────────
    log.info("Creating catchment polygons...")
    create_catchment_table(conn, "skrydstrup_catchment", SKRYDSTRUP_CATCHMENT_WGS84)
    create_catchment_table(conn, "sonder_felding_catchment", SONDER_FELDING_CATCHMENT_WGS84)

    # ── 2. Load data ──────────────────────────────────────────
    log.info("Downloading FVM marker data (2023)...")
    fvm_path = download_latest("silver/fvm_marker_2023", tmpdir, "fvm_2023")
    conn.execute(f"CREATE TABLE fvm AS SELECT * FROM read_parquet('{fvm_path}')")
    n_fvm = conn.execute("SELECT COUNT(*) FROM fvm").fetchone()[0]
    log.info(f"  Loaded {n_fvm:,} fields")

    log.info("Downloading pesticide disaggregation (2022-2023)...")
    disagg_path = download_latest("gold/pesticide_disaggregation_2022_2023", tmpdir, "disagg")
    conn.execute(f"CREATE TABLE disagg AS SELECT * FROM read_parquet('{disagg_path}')")
    n_disagg = conn.execute("SELECT COUNT(*) FROM disagg").fetchone()[0]
    log.info(f"  Loaded {n_disagg:,} disaggregation records")

    product_filter = build_product_filter_sql()

    # ── 3. Skrydstrup: fields WITHIN catchment with PFAS ─────
    log.info("\n=== SKRYDSTRUP — fields within catchment ===")

    # First check how many fields intersect the catchment at all
    n_in_catchment = conn.execute("""
        SELECT COUNT(*)
        FROM fvm f, skrydstrup_catchment c
        WHERE ST_Intersects(f.geometry, c.geometry)
    """).fetchone()[0]
    log.info(f"  Total fields intersecting catchment: {n_in_catchment}")

    n_in_buffer = conn.execute("""
        SELECT COUNT(*)
        FROM fvm f, skrydstrup_catchment_buffered c
        WHERE ST_Intersects(f.geometry, c.geometry)
    """).fetchone()[0]
    log.info(f"  Total fields within 2km buffer: {n_in_buffer}")

    # Now find PFAS-sprayed fields within catchment (try exact first, then buffer)
    skrydstrup_results = conn.execute(f"""
        WITH pfas_fields AS (
            SELECT
                f.field_id,
                f.field_uuid,
                f.crop_name,
                f.geometry,
                ST_Area(f.geometry) / 10000 AS area_ha,
                LIST(DISTINCT d.PesticideName ORDER BY d.PesticideName) AS products,
                COUNT(DISTINCT d.PesticideName) AS n_products
            FROM fvm f
            JOIN disagg d ON f.field_uuid = d.field_uuid
            JOIN skrydstrup_catchment c ON ST_Intersects(f.geometry, c.geometry)
            WHERE f.geometry IS NOT NULL
              AND ({product_filter})
            GROUP BY f.field_id, f.field_uuid, f.crop_name, f.geometry
        )
        SELECT
            field_id, field_uuid, crop_name, area_ha, products, n_products,
            ST_AsGeoJSON(
                ST_SimplifyPreserveTopology(
                    ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326'),
                    0.00002
                )
            ) AS geojson
        FROM pfas_fields
        ORDER BY n_products DESC, area_ha DESC
    """).fetchall()

    log.info(f"  PFAS fields in catchment: {len(skrydstrup_results)}")
    for r in skrydstrup_results:
        log.info(f"    {r[0]} crop={r[2]} {r[3]:.1f}ha products={r[4]}")

    # If too few, try with buffer
    if len(skrydstrup_results) < 2:
        log.info("  Too few — trying 2km buffer...")
        skrydstrup_results = conn.execute(f"""
            WITH pfas_fields AS (
                SELECT
                    f.field_id,
                    f.field_uuid,
                    f.crop_name,
                    f.geometry,
                    ST_Area(f.geometry) / 10000 AS area_ha,
                    LIST(DISTINCT d.PesticideName ORDER BY d.PesticideName) AS products,
                    COUNT(DISTINCT d.PesticideName) AS n_products
                FROM fvm f
                JOIN disagg d ON f.field_uuid = d.field_uuid
                JOIN skrydstrup_catchment_buffered c ON ST_Intersects(f.geometry, c.geometry)
                WHERE f.geometry IS NOT NULL
                  AND ({product_filter})
                GROUP BY f.field_id, f.field_uuid, f.crop_name, f.geometry
            )
            SELECT
                field_id, field_uuid, crop_name, area_ha, products, n_products,
                ST_AsGeoJSON(
                    ST_SimplifyPreserveTopology(
                        ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326'),
                        0.00002
                    )
                ) AS geojson
            FROM pfas_fields
            ORDER BY n_products DESC, area_ha DESC
            LIMIT 5
        """).fetchall()
        log.info(f"  PFAS fields in 2km buffer: {len(skrydstrup_results)}")
        for r in skrydstrup_results:
            log.info(f"    {r[0]} crop={r[2]} {r[3]:.1f}ha products={r[4]}")

    # ── 4. Søndre-Felding: fields WITHIN catchment with PFAS ─
    log.info("\n=== SØNDRE-FELDING — fields within catchment ===")

    n_in_catchment_sf = conn.execute("""
        SELECT COUNT(*)
        FROM fvm f, sonder_felding_catchment c
        WHERE ST_Intersects(f.geometry, c.geometry)
    """).fetchone()[0]
    log.info(f"  Total fields intersecting catchment: {n_in_catchment_sf}")

    felding_results = conn.execute(f"""
        WITH pfas_fields AS (
            SELECT
                f.field_id,
                f.field_uuid,
                f.crop_name,
                f.geometry,
                ST_Area(f.geometry) / 10000 AS area_ha,
                LIST(DISTINCT d.PesticideName ORDER BY d.PesticideName) AS products,
                COUNT(DISTINCT d.PesticideName) AS n_products
            FROM fvm f
            JOIN disagg d ON f.field_uuid = d.field_uuid
            JOIN sonder_felding_catchment c ON ST_Intersects(f.geometry, c.geometry)
            WHERE f.geometry IS NOT NULL
              AND ({product_filter})
            GROUP BY f.field_id, f.field_uuid, f.crop_name, f.geometry
        )
        SELECT
            field_id, field_uuid, crop_name, area_ha, products, n_products,
            ST_AsGeoJSON(
                ST_SimplifyPreserveTopology(
                    ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326'),
                    0.00002
                )
            ) AS geojson
        FROM pfas_fields
        ORDER BY n_products DESC, area_ha DESC
    """).fetchall()

    log.info(f"  PFAS fields in catchment: {len(felding_results)}")
    for r in felding_results:
        log.info(f"    {r[0]} crop={r[2]} {r[3]:.1f}ha products={r[4]}")

    # If too few, try with buffer
    if len(felding_results) < 2:
        log.info("  Too few — trying 2km buffer...")
        felding_results = conn.execute(f"""
            WITH pfas_fields AS (
                SELECT
                    f.field_id,
                    f.field_uuid,
                    f.crop_name,
                    f.geometry,
                    ST_Area(f.geometry) / 10000 AS area_ha,
                    LIST(DISTINCT d.PesticideName ORDER BY d.PesticideName) AS products,
                    COUNT(DISTINCT d.PesticideName) AS n_products
                FROM fvm f
                JOIN disagg d ON f.field_uuid = d.field_uuid
                JOIN sonder_felding_catchment_buffered c ON ST_Intersects(f.geometry, c.geometry)
                WHERE f.geometry IS NOT NULL
                  AND ({product_filter})
                GROUP BY f.field_id, f.field_uuid, f.crop_name, f.geometry
            )
            SELECT
                field_id, field_uuid, crop_name, area_ha, products, n_products,
                ST_AsGeoJSON(
                    ST_SimplifyPreserveTopology(
                        ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326'),
                        0.00002
                    )
                ) AS geojson
            FROM pfas_fields
            ORDER BY n_products DESC, area_ha DESC
            LIMIT 6
        """).fetchall()
        log.info(f"  PFAS fields in 2km buffer: {len(felding_results)}")
        for r in felding_results:
            log.info(f"    {r[0]} crop={r[2]} {r[3]:.1f}ha products={r[4]}")

    # ── 5. Generate TypeScript ────────────────────────────────
    log.info("\n=== GENERATING TYPESCRIPT ===")

    def format_feature(row):
        geojson = json.loads(row[6])
        geojson = swap_coords(geojson)
        products = row[4] if isinstance(row[4], list) else list(row[4])
        return {
            "type": "Feature",
            "properties": {
                "kind": "field",
                "crop": row[2],
                "field_id": row[0],
                "products": products,
            },
            "geometry": geojson,
        }

    skrydstrup_features = [format_feature(r) for r in skrydstrup_results if r[6]]
    felding_features = [format_feature(r) for r in felding_results if r[6]]

    def features_to_ts(features, var_name, comment):
        lines = [f"/** {comment} */"]
        lines.append(f"export const {var_name}: GeoJSON.FeatureCollection = {{")
        lines.append("  type: 'FeatureCollection',")
        lines.append("  features: [")
        for feat in features:
            lines.append("    {")
            lines.append("      type: 'Feature',")
            props = feat["properties"]
            products_str = json.dumps(props["products"])
            lines.append(
                f"      properties: {{ kind: 'field', crop: '{props['crop']}', field_id: '{props['field_id']}', products: {products_str} }},"
            )
            geom = feat["geometry"]
            lines.append("      geometry: {")
            lines.append(f"        type: '{geom['type']}',")
            lines.append("        coordinates: [")
            for ring in geom["coordinates"]:
                coords_str = json.dumps(ring)
                lines.append(f"          {coords_str},")
            lines.append("        ],")
            lines.append("      },")
            lines.append("    },")
        lines.append("  ],")
        lines.append("};")
        return "\n".join(lines)

    ts_skrydstrup = features_to_ts(
        skrydstrup_features,
        "SKRYDSTRUP_FIELDS",
        "Fields within Skrydstrup catchment with fluorinated pesticide applications",
    )
    ts_felding = features_to_ts(
        felding_features,
        "SONDER_FELDING_FIELDS",
        "Fields within Søndre-Felding catchment with fluorinated pesticide applications",
    )

    output_path = Path(__file__).parent / "reports" / "pfas_scrolly_fields.ts"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(f"{ts_skrydstrup}\n\n{ts_felding}\n")
    log.info(f"\nTypeScript written to {output_path}")
    log.info(f"Skrydstrup: {len(skrydstrup_features)} features")
    log.info(f"Søndre-Felding: {len(felding_features)} features")

    conn.close()


if __name__ == "__main__":
    main()
