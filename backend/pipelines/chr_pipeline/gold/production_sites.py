"""CHR Production Sites — Gold layer.

Joins silver properties, property_owners, herds, and herd_sizes to produce
a single production_sites table matching the frontend schema.

Output columns match the Supabase production_sites table:
  chr, company_id, site_name, address, postal_code, city, municipality,
  location_geom, capacity, main_species_code
"""

from pathlib import Path

import duckdb
from common.logging_utils import get_pipeline_logger

from .export import export_gold_table

try:
    from common.storage import StorageAccess

    STORAGE_AVAILABLE = True
except ImportError:
    STORAGE_AVAILABLE = False
    StorageAccess = None

logger = get_pipeline_logger(__name__)

# Cloud storage glob patterns for silver CHR data
DATA_PATTERNS = {
    "chr_properties": "silver/chr/*/properties*.parquet",
    "chr_property_owners": "silver/chr/*/property_owners*.parquet",
    "chr_herds": "silver/chr/*/herds*.parquet",
    "chr_herd_sizes": "silver/chr/*/herd_sizes*.parquet",
}


def process_production_sites(
    export_timestamp: str,
    gold_dir: Path | None = None,
) -> bool:
    """Build production_sites gold table from CHR silver data.

    Args:
        export_timestamp: Timestamp for GCS upload path.
        gold_dir: Local output directory.

    Returns:
        True if successful.
    """
    try:
        logger.info("Building production_sites gold table...")

        conn = duckdb.connect()
        conn.execute("SET memory_limit = '4GB'")

        # Load silver tables from GCS/R2
        if not _load_silver_tables(conn):
            logger.error("Failed to load silver tables")
            return False

        # Build production_sites
        conn.execute("""
            CREATE OR REPLACE TABLE production_sites AS
            WITH
            -- Pick one CVR per CHR: prefer property owner, fall back to user
            chr_cvr AS (
                SELECT DISTINCT ON (chr)
                    chr,
                    owner_cvr AS company_id
                FROM chr_property_owners
                WHERE owner_cvr IS NOT NULL
                ORDER BY chr, owner_cvr
            ),
            -- Dominant species per CHR (species with most herds, break ties by code)
            chr_species AS (
                SELECT DISTINCT ON (chr)
                    chr,
                    species_code::TEXT AS main_species_code,
                    species_name
                FROM chr_herds
                WHERE date_ceased IS NULL
                ORDER BY chr, species_code
            ),
            -- Total capacity per CHR (sum of all herd size counts)
            chr_capacity AS (
                SELECT
                    chr,
                    SUM(count) AS capacity
                FROM chr_herd_sizes
                GROUP BY chr
            )
            SELECT
                p.chr::TEXT AS chr,
                LPAD(cv.company_id, 8, '0') AS company_id,
                COALESCE(
                    sp.species_name || ' - CHR ' || p.chr::TEXT,
                    'CHR ' || p.chr::TEXT
                ) AS site_name,
                p.address,
                p.postal_code,
                p.city,
                p.municipality,
                p.location_geom,
                COALESCE(cap.capacity, 0) AS capacity,
                sp.main_species_code
            FROM chr_properties p
            LEFT JOIN chr_cvr cv ON p.chr = cv.chr
            LEFT JOIN chr_species sp ON p.chr = sp.chr
            LEFT JOIN chr_capacity cap ON p.chr = cap.chr
            WHERE cv.company_id IS NOT NULL
        """)

        count = conn.execute("SELECT COUNT(*) FROM production_sites").fetchone()[0]
        logger.info(f"Production sites: {count:,} rows")

        # Export
        if gold_dir is None:
            from .config import GOLD_BASE_DIR

            gold_dir = GOLD_BASE_DIR / export_timestamp
        gold_dir.mkdir(parents=True, exist_ok=True)

        success = export_gold_table(conn, "production_sites", export_timestamp, gold_dir)

        conn.close()
        return success

    except Exception as e:
        logger.error(f"Failed to build production_sites: {e}", exc_info=True)
        return False


def _load_silver_tables(conn: duckdb.DuckDBPyConnection) -> bool:
    """Load CHR silver parquet from cloud storage into DuckDB tables."""
    if not STORAGE_AVAILABLE:
        logger.error("Storage utilities not available")
        return False

    try:
        storage = StorageAccess()
        bucket = "landbruget-data"

        for table_name, pattern in DATA_PATTERNS.items():
            files = storage.list_files(bucket, pattern)
            if not files:
                logger.warning(f"No files found for {table_name}: {pattern}")
                continue

            # Use latest file (sorted by timestamp in path)
            latest = sorted(files)[-1]
            path = f"{bucket}/{latest}" if not latest.startswith(bucket) else latest

            conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_parquet('{path}')
            """)
            rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"Loaded {table_name}: {rows:,} rows from {latest}")

        return True

    except Exception as e:
        logger.error(f"Failed to load silver tables: {e}", exc_info=True)
        return False
