#!/usr/bin/env python3
"""
Automated data catalog generator for Landbruget.dk R2 datasets.

For each dataset in the manifest:
1. Reads parquet metadata (min/max/nulls/distinct) via DuckDB HTTP — no full download
2. Samples up to 5 distinct values per column
3. Enriches with known column mappings from pipeline source tracing
4. Asks Gemini to generate human-readable descriptions (English + Danish context)
5. Writes schema/data_catalog.json and updates manifest.json
6. Uploads both to R2
"""

import json
import logging
import os
import subprocess
import time
from pathlib import Path

import duckdb
import google.generativeai as genai

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
R2_BASE_URL = os.environ.get(
    "R2_BASE_URL",
    "https://pub-b8c2f72ba51b4fe6804e9bb92280567c.r2.dev",
)
R2_BUCKET = os.environ.get("R2_BUCKET", "landbruget-data")
GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
MANIFEST_PATH = Path(__file__).parent.parent.parent / "manifest.json"
CATALOG_PATH = Path(__file__).parent.parent.parent / "schema" / "data_catalog.json"
CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# ── Known column dictionary (from pipeline source tracing) ────────────────────
KNOWN_COLUMNS: dict[str, str] = {
    # Field identifiers
    "field_id": "Unique field identifier (Danish: Marknr). Stable ID for a field registration in a given year. NOT a permanent ID across years — use field_uuid for cross-year tracking.",
    "field_uuid": "Deterministic UUID generated from field geometry. Stable across years, allowing tracking of the same physical field over time.",
    "block_id": "Field block identifier (Danish: Markblok / MB_NR). Groups multiple fields into larger administrative blocks. Used as spatial reference unit for subsidies.",
    "primary_field_id": "Primary field reference ID, linking back to the original source field register.",
    # Company identifiers
    "cvr_number": "Danish Central Business Register number (CVR). 8-digit company/farm identifier. IMPORTANT: stored as VARCHAR (string) in most silver tables but INT32 in cvr_enrichment_companies — always use TRY_CAST(cvr_number AS INTEGER) when joining to cvr_enrichment_companies.",
    "company_uuid": "Deterministic UUID generated from CVR number. Used as primary key in gold layer company tables.",
    "company_name": "Official company name from the Danish Business Register (CVR register).",
    "company_type_description": "Legal entity type (e.g. 'Enkeltmandsvirksomhed' = sole trader, 'I/S' = partnership, 'ApS' = private limited, 'A/S' = public limited).",
    "status": "Company registration status. 'NORMAL' = active, 'OPLØST' = dissolved.",
    "founded_date": "Date the company was formally registered in the CVR register.",
    "dissolution_date": "Date the company was dissolved (null if still active).",
    "is_agricultural_company": "Boolean flag indicating whether the company is classified as an agricultural enterprise based on industry code.",
    "primary_industry_code": "Danish industry classification code (DB07). 6-digit code. Codes starting with '01' = crop farming, '02' = forestry, '03' = fishing, '10-11' = food processing.",
    "primary_industry_description": "Human-readable description of the primary industry code.",
    "pnumber_count": "Number of production units (P-numbers) associated with this CVR company.",
    # Address / geographic
    "latitude": "WGS84 latitude of company headquarters (EPSG:4326).",
    "longitude": "WGS84 longitude of company headquarters (EPSG:4326).",
    "current_municipality_name": "Name of the Danish municipality where the company is registered.",
    "current_municipality_code": "4-digit municipality code (kommunekode).",
    "current_city": "City name from company address.",
    "current_postal_code": "Danish postal code (postnummer).",
    "address_geom": "WKB binary geometry of company address point (EPSG:4326). Use ST_AsText() to convert.",
    # Field attributes
    "area_ha": "Field area in hectares. Source: IMK_areal (Imago-Marked area) — the officially registered area used for EU subsidy calculations.",
    "crop_code": "Crop classification code (Danish: Afgkode). Integer code identifying the crop type. Maps to crop_name.",
    "crop_name": "Crop name in Danish (Danish: Afgroede). E.g. 'Vinterhvede' (winter wheat), 'Vinterraps' (winter rapeseed), 'Majs' (maize), 'Græs' (grass/pasture), 'Brak' (fallow).",
    "crop_type": "Crop type name (same as crop_name in gold layer field_production tables).",
    "is_organic": "Boolean: true if the field is certified organic. Enriched from fvm_organic_areas via spatial join with fvm_marker.",
    "organic_farming": "Boolean: true if the field is organic (same as is_organic in some gold tables).",
    "organic_conversion_status": "Organic farming conversion status code: '1' = in conversion period, '2' = fully certified organic.",
    "organic_conversion_date": "Date when the field started its organic conversion process.",
    "organic_deregistration_date": "Date when the field was removed from the organic register (null if still organic).",
    "grundbetaling_eligible": "EU Basic Payment (Grundbetaling) eligibility flag. NOT an indicator of organic farming. 'Ja'=eligible, 'Nej'=not eligible.",
    "grundbetaling_area_ha": "Area (ha) reported for EU Basic Payment subsidy (Danish: GBanmeldt). Reported from 2017 onwards.",
    "journal_number": "Administrative journal number (Danish: Journalnr). Internal reference from FVM (Danish Agricultural Authority).",
    "year": "Data year. Used to filter time series tables. Always filter on specific year for performance.",
    "municipality": "Municipality name where the field is located.",
    # Environmental / land
    "bfe_number": "Cadastral parcel number (BFE-nummer). Identifies land parcels in the Danish property register (Matrikelregistret). Format varies by municipality.",
    "toerv_pct": "Peat percentage category (Danish: tørvprocent). Values: '>12' (high peat content, >12%), '6-12' (medium peat), other values = low/no peat. Critical for carbon/wetland analysis.",
    "bnbo_status": "BNBO protection zone status (Boringsnære Beskyttelsesområder = drinking water well protection zones). Status indicates whether the area requires action to protect groundwater.",
    "property_wetland_total_m2": "Total wetland area intersecting the property in m². NOTE: This column may contain zeros even for properties with wetlands — for accurate wetland areas use field_analysis_property_wetland_intersections_{year} table with ST_Area() on the geometry.",
    "property_bnbo_total_m2": "Total BNBO (drinking water protection zone) area intersecting the property in m².",
    "property_grukos_total_m2": "Total groundwater protection area (grukos) intersecting the property in m².",
    "property_intersection_area_m2": "Total intersection area between the agricultural field/property and the analysis layer in m².",
    # Yield / production
    "yield_estimate_hkg_ha": "Estimated crop yield in hectokilograms per hectare (hkg/ha). 1 hkg = 100 kg. Source: DST (Danmarks Statistik) regional yield statistics.",
    "yield_estimation_method": "Method used to estimate yield: 'dst_region_match' = matched to DST region average, 'no_yield_data' = no DST data available for this crop/year.",
    "production_estimate_hkg": "Total estimated production in hectokilograms (area_ha × yield_estimate_hkg_ha).",
    "production_unit": "Unit for production estimate (typically 'hkg' = hectokilogram).",
    # Geographic / statistical zones
    "landsdel_code": "DAGI geographic sub-region code (landsdel). E.g. 'DK011' = Byen København, 'DK021' = Østjylland. Used for DST statistical reporting.",
    "landsdel_name": "Name of the DAGI landsdel (geographic sub-region). 11 landsdele cover all of Denmark.",
    "dst_regions": "Pipe-separated DST (Danmarks Statistik) statistical region codes for this area. Used for matching to DST harvest statistics.",
    "dagi_region_name": "DAGI administrative region name. One of: Region Hovedstaden, Region Midtjylland, Region Nordjylland, Region Sjælland, Region Syddanmark.",
    "dagi_region_code": "DAGI administrative region code.",
    # Soil
    "soil_code": "JB soil classification code (KODE). Danish soil type classification system. JB1=coarse sand, JB2=fine sand, JB3=sandy loam, JB4=sandy clay, JB5=loam, JB6=clay loam, JB7=clay, JB8=heavy clay.",
    "soil_description": "Soil type description in Danish (JORD_TEKST). Full name of soil classification.",
    # Water / climate
    "projektnavn": "Name of the water restoration project (Danish: projektnavn = project name).",
    "startdato": "Project start date.",
    "slutdato": "Project end date.",
    "parameter_id": "DMI climate parameter ID. E.g. 'pot_evaporation_makkink' = potential evaporation.",
    "avg_value": "Average measurement value for the time period.",
    # GEUS groundwater
    "gridcode": "Grid cell classification code used in wetland mapping.",
    # CVR financial
    "advertisement_protection": "Boolean: true if company has opted out of marketing contact.",
    "dawa_enriched": "Boolean: true if address was successfully geocoded via DAWA (Danish Address Web API).",
    "coordinate_quality": "Quality level of geocoded coordinates from DAWA.",
    # Timestamps
    "created_at": "Record creation timestamp (when pipeline processed this record).",
    "processed_at": "Pipeline processing timestamp.",
    "processing_timestamp": "Pipeline processing timestamp.",
    "geometry": "Spatial geometry in WKB binary format (EPSG:4326 / WGS84). Use ST_AsText() to inspect, ST_Area() for area calculations.",
    "geometry_spatial": "Validated spatial geometry (EPSG:4326). Use DuckDB spatial extension functions: ST_Area(), ST_Intersects(), etc.",
    "is_valid_geometry": "Boolean: true if the geometry passed validity checks.",
}

# ── Parquet metadata extraction ───────────────────────────────────────────────


def get_parquet_stats(url: str) -> dict:
    """Read column stats from parquet metadata via DuckDB HTTP range requests."""
    try:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        # Get per-column stats from parquet footer
        rows = con.execute(f"""
            SELECT
                path_in_schema AS column_name,
                MIN(stats_min) AS stats_min,
                MAX(stats_max) AS stats_max,
                SUM(stats_null_count) AS stats_null_count,
                MAX(stats_distinct_count) AS stats_distinct_count
            FROM parquet_metadata('{url}')
            GROUP BY path_in_schema
        """).fetchall()
        # Aggregate across row groups
        stats: dict[str, dict] = {}
        for col, mn, mx, nulls, distinct in rows:
            if col not in stats:
                stats[col] = {
                    "min": mn,
                    "max": mx,
                    "null_count": int(nulls or 0),
                    "distinct_count": distinct,
                }
        con.close()
        return stats
    except Exception as e:
        log.warning(f"Could not read parquet stats from {url}: {e}")
        return {}


def get_sample_values(url: str, columns: list[str], n: int = 5) -> dict[str, list]:
    """Read a small sample of distinct non-null values per column."""
    samples: dict[str, list] = {}
    # Only sample non-geometry, non-binary columns
    sample_cols = [
        c
        for c in columns
        if not any(
            k in c.lower() for k in ("geom", "geometry", "wkt", "wkb", "_json", "uuid")
        )
    ][:20]  # cap at 20 cols to keep it fast
    if not sample_cols:
        return {}
    try:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        col_list = ", ".join(f'"{c}"' for c in sample_cols)
        rows = con.execute(f"SELECT {col_list} FROM '{url}' LIMIT 500").fetchall()
        col_data: dict[str, list] = {c: [] for c in sample_cols}
        for row in rows:
            for col, val in zip(sample_cols, row):
                if val is not None:
                    col_data[col].append(str(val))
        for col in sample_cols:
            seen = list(dict.fromkeys(col_data[col]))[:n]  # deduplicate, preserve order
            if seen:
                samples[col] = seen
        con.close()
    except Exception as e:
        log.warning(f"Could not sample values from {url}: {e}")
    return samples


# ── Gemini description generation ────────────────────────────────────────────


def build_prompt(ds: dict, stats: dict, samples: dict) -> str:
    schema = ds.get("schema", [])
    lines = []
    for col in schema:
        name = col["column_name"]
        typ = col["column_type"]
        known = KNOWN_COLUMNS.get(name, "")
        st = stats.get(name, {})
        sv = samples.get(name, [])
        parts = [f"  {name} ({typ})"]
        if known:
            parts.append(f"    known: {known}")
        if st.get("min") is not None or st.get("max") is not None:
            parts.append(f"    range: {st.get('min')} → {st.get('max')}")
        if st.get("null_count", 0) > 0:
            total = ds.get("rowCount", 1) or 1
            pct = round(100 * st["null_count"] / total, 1)
            parts.append(f"    null: {pct}%")
        if sv:
            parts.append(f"    sample values: {sv[:5]}")
        lines.append("\n".join(parts))

    return f"""You are building a data catalog for Danish agricultural data in DuckDB.

Table: {ds["name"]}
Display name: {ds["displayName"]}
Layer: {ds["layer"]}
Rows: {ds["rowCount"]:,}
Current description: {ds["description"]}

Columns:
{chr(10).join(lines)}

Task: Return a JSON object with this structure (no markdown fences):
{{
  "description": "1-2 sentence description of what this table contains and its primary use case",
  "columns": {{
    "<column_name>": {{
      "description": "concise description of what this column contains",
      "notes": "optional: join hints, data quality warnings, units, Danish terms explained"
    }}
  }}
}}

Rules:
- Be precise about Danish terms (e.g. explain 'Vinterhvede', 'Markblok', 'CVR', 'BNBO')
- For geometry columns: note they are WKB binary, use ST_AsText() / ST_Area()
- For CVR columns: note whether it's string or int32 and TRY_CAST requirement
- Keep descriptions concise (1 sentence each)
- Only include columns that need explanation — skip trivial ones like 'year', 'created_at'
"""


def generate_descriptions(ds: dict, stats: dict, samples: dict, model) -> dict:
    prompt = build_prompt(ds, stats, samples)
    try:
        result = model.generate_content(prompt)
        text = result.text.strip()
        # Strip markdown fences if present
        text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        log.warning(f"Gemini failed for {ds['name']}: {e}")
        return {"description": ds["description"], "columns": {}}


# ── Main ──────────────────────────────────────────────────────────────────────


def _is_empty_column(stats: dict, sample_values: list) -> bool:
    """Return True if a column appears to contain no real data (all null/zero/nan)."""
    if stats.get("min") is None and stats.get("max") is None:
        # No stats — check sample values
        non_empty = [
            v for v in sample_values if v not in ("nan", "0", "0.0", "None", "")
        ]
        return len(non_empty) == 0
    # Stats present but both min and max are null/nan/zero
    mn = str(stats.get("min") or "")
    mx = str(stats.get("max") or "")
    empty_vals = {"nan", "0", "0.0", "none", "", "null"}
    return mn.lower() in empty_vals and mx.lower() in empty_vals


def process_dataset(ds: dict, model) -> dict:
    url = ds["url"]
    name = ds["name"]
    schema_cols = [c["column_name"] for c in ds.get("schema", [])]

    log.info(f"Processing {name}...")

    # 1. Parquet metadata stats
    stats = get_parquet_stats(url)

    # 2. Sample values
    samples = get_sample_values(url, schema_cols)

    # 3. Gemini descriptions
    descriptions = generate_descriptions(ds, stats, samples, model)

    # 4. Build catalog entry
    entry = {
        "name": name,
        "displayName": ds["displayName"],
        "layer": ds["layer"],
        "url": url,
        "rowCount": ds["rowCount"],
        "sizeBytes": ds["sizeBytes"],
        "columns": ds.get("columns", 0),
        "description": descriptions.get("description", ds["description"]),
        "columnDescriptions": descriptions.get("columns", {}),
        "schema": ds.get("schema", []),
        "columnStats": {
            col: {
                "min": str(st.get("min", "")) if st.get("min") is not None else None,
                "max": str(st.get("max", "")) if st.get("max") is not None else None,
                "nullPct": round(
                    100 * st.get("null_count", 0) / max(ds.get("rowCount", 1), 1), 2
                ),
                "distinctCount": st.get("distinct_count"),
                "sampleValues": samples.get(col, []),
                "isEmpty": _is_empty_column(st, samples.get(col, [])),
            }
            for col, st in stats.items()
        },
    }
    return entry


def main():
    genai.configure(api_key=GOOGLE_API_KEY)
    model = genai.GenerativeModel("gemini-flash-latest")

    manifest = json.loads(MANIFEST_PATH.read_text())
    datasets = manifest["datasets"]
    log.info(f"Processing {len(datasets)} datasets...")

    # Load existing catalog if present (for resuming)
    existing: dict[str, dict] = {}
    if CATALOG_PATH.exists():
        try:
            existing_catalog = json.loads(CATALOG_PATH.read_text())
            existing = {e["name"]: e for e in existing_catalog.get("datasets", [])}
            log.info(f"Resuming — {len(existing)} already processed")
        except Exception:
            pass

    catalog_entries = []
    failed = []

    for i, ds in enumerate(datasets):
        name = ds["name"]
        if name in existing:
            log.info(f"[{i + 1}/{len(datasets)}] Skipping {name} (already done)")
            catalog_entries.append(existing[name])
            continue

        try:
            entry = process_dataset(ds, model)
            catalog_entries.append(entry)
            log.info(f"[{i + 1}/{len(datasets)}] ✓ {name}")
        except Exception as e:
            log.error(f"[{i + 1}/{len(datasets)}] ✗ {name}: {e}")
            failed.append(name)
            # Still add a basic entry so we can resume later
            catalog_entries.append(
                {
                    "name": name,
                    "description": ds["description"],
                    "layer": ds["layer"],
                    "url": ds["url"],
                    "rowCount": ds["rowCount"],
                    "sizeBytes": ds["sizeBytes"],
                    "columns": ds.get("columns", 0),
                    "schema": ds.get("schema", []),
                    "columnDescriptions": {},
                    "columnStats": {},
                }
            )

        # Save progress every 10 datasets
        if (i + 1) % 10 == 0:
            _save_catalog(catalog_entries, CATALOG_PATH)
            log.info(f"  Progress saved ({len(catalog_entries)} entries)")

        # Rate limit: ~1 request/sec
        time.sleep(1.2)

    _save_catalog(catalog_entries, CATALOG_PATH)
    log.info(
        f"\nCatalog complete: {len(catalog_entries)} entries, {len(failed)} failed"
    )
    if failed:
        log.warning(f"Failed: {failed}")

    # Update manifest descriptions from catalog
    _update_manifest(manifest, catalog_entries)

    # Upload both to R2
    _upload_to_r2(CATALOG_PATH, f"r2:{R2_BUCKET}/data_catalog.json")
    _upload_to_r2(MANIFEST_PATH, f"r2:{R2_BUCKET}/manifest.json")
    log.info("Uploaded catalog and manifest to R2.")


def _save_catalog(entries: list, path: Path):
    catalog = {
        "version": "1.0",
        "generatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "totalDatasets": len(entries),
        "datasets": entries,
    }
    path.write_text(json.dumps(catalog, indent=2, ensure_ascii=False))


def _update_manifest(manifest: dict, catalog_entries: list):
    by_name = {e["name"]: e for e in catalog_entries}
    for ds in manifest["datasets"]:
        entry = by_name.get(ds["name"])
        if entry and entry.get("description"):
            ds["description"] = entry["description"]
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    log.info("Manifest descriptions updated.")


def _upload_to_r2(local_path: Path, r2_dest: str):
    result = subprocess.run(
        ["rclone", "copyto", str(local_path), r2_dest, "--s3-no-check-bucket"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        log.error(f"Upload failed for {local_path}: {result.stderr}")
    else:
        log.info(f"Uploaded {local_path.name} → {r2_dest}")


if __name__ == "__main__":
    main()
