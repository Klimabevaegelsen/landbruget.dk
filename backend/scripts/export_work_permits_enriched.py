"""
Export work permits data enriched with CVR company details and CHR registry data.

Reads all data from cloud storage parquet files, joins them, and exports
as CSV and parquet. Includes: company info, owners, CHR production sites,
and animal production details.

Usage:
    cd backend && source venv/bin/activate
    python scripts/export_work_permits_enriched.py
"""

import logging
import os
import sys
from pathlib import Path

import duckdb

# Add backend to path so common.storage is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.storage import StorageAccess

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BUCKET = os.getenv("GCS_BUCKET", "landbruget-data")
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"


def find_latest(storage: StorageAccess, pattern: str) -> str | None:
    """Find the most recent file matching a glob pattern."""
    files = storage.list_files(pattern)
    if not files:
        return None
    files.sort()
    return files[-1]


def load_table(storage: StorageAccess, conn: duckdb.DuckDBPyConnection, table_name: str, path: str) -> bool:
    """Download a parquet file and load into DuckDB table."""
    log.info(f"Loading {table_name} from .../{'/'.join(path.split('/')[-3:])}")
    try:
        with storage._temp_download(path) as tmp:
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{tmp}')")
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        log.info(f"  -> {count:,} rows")
        return True
    except Exception as e:
        log.error(f"  Failed to load {table_name}: {e}")
        return False


def main():
    log.info("Starting work permits enriched export")

    storage = StorageAccess()
    conn = duckdb.connect()

    # 1. Find latest files for each dataset
    log.info("Finding latest data files...")

    work_permits_path = find_latest(
        storage,
        f"{BUCKET}/silver/work_permits/**/Landbrugsvisum_statistik_2025.parquet",
    )
    if not work_permits_path:
        work_permits_path = find_latest(
            storage,
            f"{BUCKET}/silver/work_permits/**/Landbrugsvisum_statistik.parquet",
        )
    if not work_permits_path:
        log.error("No work permits files found")
        return

    cvr_companies_path = find_latest(storage, f"{BUCKET}/silver/cvr_companies/*/data.parquet")
    cvr_persons_path = find_latest(storage, f"{BUCKET}/silver/cvr_persons/*/data.parquet")
    chr_properties_path = find_latest(storage, f"{BUCKET}/silver/chr/*/properties.parquet")
    chr_property_owners_path = find_latest(storage, f"{BUCKET}/silver/chr/*/property_owners.parquet")
    chr_herds_path = find_latest(storage, f"{BUCKET}/silver/chr/*/herds.parquet")

    # 2. Load all datasets
    log.info("Loading datasets...")

    if not load_table(storage, conn, "work_permits", work_permits_path):
        return

    has_companies = cvr_companies_path and load_table(storage, conn, "cvr_companies", cvr_companies_path)
    has_persons = cvr_persons_path and load_table(storage, conn, "cvr_persons", cvr_persons_path)
    has_properties = chr_properties_path and load_table(storage, conn, "chr_properties", chr_properties_path)
    has_property_owners = chr_property_owners_path and load_table(
        storage, conn, "chr_property_owners", chr_property_owners_path
    )
    has_herds = chr_herds_path and load_table(storage, conn, "chr_herds", chr_herds_path)

    # 3. Pre-aggregate CVR leadership/owners (one row per CVR with comma-separated names)
    # The cvr_persons silver parquet stores person details as JSON in person_data_json
    # Use json_array_length + generate_series to unnest names array safely
    if has_persons:
        log.info("Aggregating CVR leadership names...")
        conn.execute("""
            CREATE OR REPLACE TABLE cvr_owners_agg AS
            WITH indexed_persons AS (
                SELECT
                    cvr_number,
                    person_data_json,
                    json_array_length(person_data_json->'$.person.names') AS names_count
                FROM cvr_persons
                WHERE person_data_json->>'$.is_current' = 'true'
                    AND json_array_length(person_data_json->'$.person.names') > 0
            ),
            person_names AS (
                SELECT
                    p.cvr_number,
                    json_extract_string(p.person_data_json, '$.person.names[' || i || '].name') AS person_name,
                    json_extract_string(p.person_data_json, '$.person.names[' || i || '].is_current') AS is_current_name
                FROM indexed_persons p,
                     generate_series(0, CAST(p.names_count - 1 AS BIGINT)) AS t(i)
            )
            SELECT
                cvr_number,
                STRING_AGG(DISTINCT person_name, ', ' ORDER BY person_name) AS cvr_owners
            FROM person_names
            WHERE is_current_name = 'true'
                AND person_name IS NOT NULL
                AND person_name != ''
            GROUP BY cvr_number
        """)

    # 4. Pre-aggregate CHR data per CVR (via property_owners link)
    if has_property_owners and has_properties:
        log.info("Aggregating CHR production sites per CVR...")
        conn.execute("""
            CREATE OR REPLACE TABLE chr_sites_agg AS
            SELECT
                LPAD(CAST(po.owner_cvr AS VARCHAR), 8, '0') AS cvr_number,
                STRING_AGG(DISTINCT CAST(po.chr AS VARCHAR), ', ') AS chr_numbers,
                STRING_AGG(DISTINCT p.address || ', ' || COALESCE(p.postal_code, '') || ' ' || COALESCE(p.municipality, ''),
                    '; ') AS chr_addresses
            FROM chr_property_owners po
            LEFT JOIN chr_properties p ON po.chr = p.chr
            WHERE po.owner_cvr IS NOT NULL AND po.owner_cvr != ''
            GROUP BY LPAD(CAST(po.owner_cvr AS VARCHAR), 8, '0')
        """)

    if has_property_owners and has_herds:
        log.info("Aggregating animal production per CVR...")
        conn.execute("""
            CREATE OR REPLACE TABLE chr_animals_agg AS
            SELECT
                LPAD(CAST(po.owner_cvr AS VARCHAR), 8, '0') AS cvr_number,
                STRING_AGG(
                    DISTINCT h.species_name || CASE WHEN h.usage_type_name IS NOT NULL THEN ' (' || h.usage_type_name || ')' ELSE '' END,
                    ', '
                ) AS animal_production,
                STRING_AGG(
                    DISTINCT CASE WHEN h.is_organic THEN h.species_name || ' (organic)' ELSE NULL END,
                    ', '
                ) AS organic_production
            FROM chr_property_owners po
            JOIN chr_herds h ON po.chr = h.chr
            WHERE po.owner_cvr IS NOT NULL AND po.owner_cvr != ''
                AND h.date_ceased IS NULL
            GROUP BY LPAD(CAST(po.owner_cvr AS VARCHAR), 8, '0')
        """)

    # 5. Build the final joined query
    log.info("Building enriched export...")

    select_parts = [
        "wp.company_id AS cvr_number",
        "wp.year",
        "wp.nationality",
        "wp.first_permits_count",
    ]
    join_parts = []

    if has_companies:
        select_parts.extend(
            [
                "c.company_name",
                "c.current_full_address AS company_address",
                "c.current_postal_code AS postal_code",
                "c.current_city AS city",
                "c.current_municipality_name AS municipality",
                "c.primary_industry_code AS industry_code",
                "c.primary_industry_description AS industry_description",
                "c.is_agricultural_company",
            ]
        )
        join_parts.append("LEFT JOIN cvr_companies c ON wp.company_id = LPAD(CAST(c.cvr_number AS VARCHAR), 8, '0')")

    if has_persons:
        select_parts.append("oa.cvr_owners")
        join_parts.append("LEFT JOIN cvr_owners_agg oa ON wp.company_id = LPAD(CAST(oa.cvr_number AS VARCHAR), 8, '0')")

    if has_property_owners and has_properties:
        select_parts.extend(["sa.chr_numbers", "sa.chr_addresses"])
        join_parts.append("LEFT JOIN chr_sites_agg sa ON wp.company_id = sa.cvr_number")

    if has_property_owners and has_herds:
        select_parts.extend(["aa.animal_production", "aa.organic_production"])
        join_parts.append("LEFT JOIN chr_animals_agg aa ON wp.company_id = aa.cvr_number")

    query = f"""
        CREATE OR REPLACE TABLE enriched_export AS
        SELECT
            {", ".join(select_parts)}
        FROM work_permits wp
        {" ".join(join_parts)}
        WHERE wp.first_permits_count > 0
        ORDER BY wp.company_id, wp.year, wp.nationality
    """

    conn.execute(query)

    # 6. Log stats
    stats = conn.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT cvr_number) AS unique_companies,
            COUNT(DISTINCT nationality) AS unique_nationalities,
            MIN(year) AS min_year,
            MAX(year) AS max_year,
            SUM(first_permits_count) AS total_permits
        FROM enriched_export
    """).fetchone()

    log.info(
        f"Export stats: {stats[0]:,} rows, {stats[1]:,} companies, "
        f"{stats[2]} nationalities, years {stats[3]}-{stats[4]}, "
        f"{stats[5]:,} total permits"
    )

    if has_companies:
        match_stats = conn.execute("""
            SELECT
                COUNT(CASE WHEN company_name IS NOT NULL THEN 1 END) AS matched,
                COUNT(CASE WHEN company_name IS NULL THEN 1 END) AS unmatched
            FROM enriched_export
        """).fetchone()
        log.info(f"CVR match rate: {match_stats[0]:,} matched, {match_stats[1]:,} unmatched")

    if has_property_owners:
        chr_stats = conn.execute("""
            SELECT COUNT(CASE WHEN chr_numbers IS NOT NULL THEN 1 END) AS with_chr
            FROM enriched_export
        """).fetchone()
        log.info(f"CHR link rate: {chr_stats[0]:,} rows have CHR production sites")

    # 7. Export
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_DIR / "work_permits_enriched.csv"
    parquet_path = OUTPUT_DIR / "work_permits_enriched.parquet"

    conn.execute(f"COPY enriched_export TO '{csv_path}' (FORMAT CSV, HEADER TRUE)")
    conn.execute(f"COPY enriched_export TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    log.info(f"Exported CSV: {csv_path} ({csv_path.stat().st_size / 1024:.0f} KB)")
    log.info(f"Exported Parquet: {parquet_path} ({parquet_path.stat().st_size / 1024:.0f} KB)")

    # Show sample
    log.info("Sample rows:")
    [col[0] for col in conn.execute("DESCRIBE enriched_export").fetchall()]
    for _row in conn.execute("SELECT * FROM enriched_export LIMIT 5").fetchall():
        pass

    log.info("Done!")


if __name__ == "__main__":
    main()
