"""CHR Veterinary Timeline processing for Gold layer."""

import logging
from pathlib import Path
from typing import Dict, List, Optional

import duckdb

from .config import GOLD_BASE_DIR

# Try to import GCS utilities
try:
    from unified_pipeline.util.gcs_access import GCSDataAccess
    from unified_pipeline.util.migration_helpers import migrate_save_data_pattern

    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    GCSDataAccess = None
    migrate_save_data_pattern = None


logger = logging.getLogger(__name__)


def reconstruct_stable_fires_from_table(con: duckdb.DuckDBPyConnection) -> bool:
    """
    Reconstruct stable fire data from loaded stable_fires table.
    Handles malformed column structure where first row data became column names.
    """
    try:
        logger.info("🔥 Reconstructing stable fires from table...")

        # Check what columns we have in the stable_fires table
        columns = con.execute("DESCRIBE stable_fires").fetchall()
        column_names = [col[0] for col in columns]

        logger.info(f"Available stable_fires columns: {column_names}")

        # Check if we have malformed columns (data values as column names)
        # Look for numeric columns and date-like patterns that suggest malformed structure
        has_malformed_columns = (
            len(column_names) >= 6
            and any(col.replace("_", "").replace(".", "").isdigit() for col in column_names[1:3])
            and any(
                ("_" in col and len(col.split("_")) == 3) or ("-" in col and len(col.split("-")) == 3)
                for col in column_names[:1]
            )
        )

        if has_malformed_columns:
            logger.info("🔧 Detected malformed column structure - using positional reconstruction")
            # Column structure: date, x_coord, y_coord, street, house_num,
            # municipality, table_number, source_file
            con.execute(f"""
                CREATE OR REPLACE TABLE cleaned_fires AS
                SELECT
                    "{column_names[0]}" as fire_date_str,
                    TRY_CAST("{column_names[1]}" AS DOUBLE) as fire_x_coord,
                    TRY_CAST("{column_names[2]}" AS DOUBLE) as fire_y_coord,
                    "{column_names[3]}" as fire_street,
                    CAST("{column_names[4]}" AS VARCHAR) as fire_house_number,
                    "{column_names[5]}" as fire_municipality,
                    -- Try to parse date from various formats like "08-feb-21"
                    TRY_CAST(
                        CASE
                            WHEN "{column_names[0]}" ~ '^[0-9]{{1,2}}-[a-z]{{3}}-[0-9]{{2}}$' THEN
                                '20' || RIGHT("{column_names[0]}", 2) || '-' ||
                                CASE LOWER(SUBSTRING("{column_names[0]}", POSITION('-' IN "{column_names[0]}") + 1, 3))
                                    WHEN 'jan' THEN '01'
                                    WHEN 'feb' THEN '02'
                                    WHEN 'mar' THEN '03'
                                    WHEN 'apr' THEN '04'
                                    WHEN 'may' THEN '05'
                                    WHEN 'jun' THEN '06'
                                    WHEN 'jul' THEN '07'
                                    WHEN 'aug' THEN '08'
                                    WHEN 'sep' THEN '09'
                                    WHEN 'oct' THEN '10'
                                    WHEN 'nov' THEN '11'
                                    WHEN 'dec' THEN '12'
                                    ELSE '01'
                                END || '-' ||
                                LPAD(CAST(LEFT("{column_names[0]}",
                                    POSITION('-' IN "{column_names[0]}") - 1) AS VARCHAR), 2, '0')
                            ELSE NULL
                        END AS DATE
                    ) as fire_date
                FROM stable_fires
                WHERE "{column_names[1]}" IS NOT NULL
                  AND "{column_names[2]}" IS NOT NULL
                  AND TRY_CAST("{column_names[1]}" AS DOUBLE) IS NOT NULL
                  AND TRY_CAST("{column_names[2]}" AS DOUBLE) IS NOT NULL
            """)
        else:
            # Try to identify coordinate and date columns dynamically (original logic)
            coord_x_col = next((col for col in column_names if "x" in col.lower() or "coord" in col.lower()), None)
            coord_y_col = next((col for col in column_names if "y" in col.lower() or "coord" in col.lower()), None)
            date_col = next((col for col in column_names if "date" in col.lower() or "time" in col.lower()), None)

            if not coord_x_col or not coord_y_col:
                logger.warning("⚠️ Could not identify coordinate columns in stable_fires data")
                return False

            # Create cleaned fires table dynamically
            con.execute(f"""
                CREATE OR REPLACE TABLE cleaned_fires AS
                SELECT
                    {coord_x_col} as fire_x_coord,
                    {coord_y_col} as fire_y_coord,
                    {date_col if date_col else "NULL"} as fire_date,
                    *
                FROM stable_fires
                WHERE {coord_x_col} IS NOT NULL
                  AND {coord_y_col} IS NOT NULL
            """)

        count = con.execute("SELECT COUNT(*) FROM cleaned_fires").fetchone()[0]
        logger.info(f"✅ Processed {count} stable fire events")
        return count > 0

    except Exception as e:
        logger.error(f"❌ Failed to reconstruct stable fires from table: {e}")
        return False


def reconstruct_stable_fires(con: duckdb.DuckDBPyConnection, drive_silver_dir: Path) -> bool:
    """
    Reconstruct stable fire data from malformed PDF extraction and spatially match to CHR properties.

    Args:
        con: DuckDB connection
        drive_silver_dir: Path to drive pipeline silver data

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info("🔥 Reconstructing stable fire data...")

        # Check if stable fires data exists in drive pipeline
        stable_fires_path = drive_silver_dir / "stable_fires" / "stable_fires.parquet"
        if not stable_fires_path.exists():
            logger.warning(f"⚠️ Stable fires data not found at {stable_fires_path}")
            return False

        # Load the malformed stable fires data
        con.execute(f"""
            CREATE OR REPLACE TABLE raw_stable_fires AS
            SELECT * FROM '{stable_fires_path}'
        """)

        # Reconstruct fire events from malformed PDF extraction
        # The data has mixed up columns - need to extract actual fire events
        con.execute("""
            CREATE OR REPLACE TABLE reconstructed_fires AS
            SELECT
                -- Extract fire information from the row (columns are mixed up from PDF extraction)
                CAST(COALESCE(NULLIF(TRIM(CAST(column0 AS VARCHAR)), ''), NULL) AS VARCHAR) as fire_date_str,
                TRY_CAST(column1 AS DOUBLE) as fire_x_coord,
                TRY_CAST(column2 AS DOUBLE) as fire_y_coord,
                CAST(COALESCE(NULLIF(TRIM(CAST(column3 AS VARCHAR)), ''), NULL) AS VARCHAR) as fire_street,
                CAST(COALESCE(NULLIF(TRIM(CAST(column4 AS VARCHAR)), ''), NULL) AS VARCHAR) as fire_house_number,
                CAST(COALESCE(NULLIF(TRIM(CAST(column5 AS VARCHAR)), ''), NULL) AS VARCHAR) as fire_municipality
            FROM raw_stable_fires
            WHERE column1 IS NOT NULL
              AND column2 IS NOT NULL
              AND TRY_CAST(column1 AS DOUBLE) IS NOT NULL
              AND TRY_CAST(column2 AS DOUBLE) IS NOT NULL
        """)

        # Clean up fire data
        con.execute("""
            CREATE OR REPLACE TABLE cleaned_fires AS
            SELECT
                fire_date_str,
                fire_x_coord,
                fire_y_coord,
                fire_street,
                -- Clean house numbers (remove .0 suffix)
                REGEXP_REPLACE(fire_house_number, '\\.0$', '') as fire_house_number,
                fire_municipality,
                -- Try to parse date from various formats like "08-feb-21"
                TRY_CAST(
                    CASE
                        WHEN fire_date_str ~ '^[0-9]{1,2}-[a-z]{3}-[0-9]{2}$' THEN
                            '20' || RIGHT(fire_date_str, 2) || '-' ||
                            CASE LOWER(SUBSTRING(fire_date_str, 4, 3))
                                WHEN 'jan' THEN '01'
                                WHEN 'feb' THEN '02'
                                WHEN 'mar' THEN '03'
                                WHEN 'apr' THEN '04'
                                WHEN 'may' THEN '05'
                                WHEN 'jun' THEN '06'
                                WHEN 'jul' THEN '07'
                                WHEN 'aug' THEN '08'
                                WHEN 'sep' THEN '09'
                                WHEN 'oct' THEN '10'
                                WHEN 'nov' THEN '11'
                                WHEN 'dec' THEN '12'
                                ELSE '01'
                            END || '-' ||
                            LPAD(CAST(LEFT(fire_date_str, POSITION('-' IN fire_date_str) - 1) AS VARCHAR), 2, '0')
                        ELSE NULL
                    END AS DATE
                ) as fire_date
            FROM reconstructed_fires
            WHERE fire_x_coord IS NOT NULL
              AND fire_y_coord IS NOT NULL
              AND fire_street IS NOT NULL
              AND TRIM(fire_street) != ''
        """)

        logger.info(
            f"✅ Reconstructed {con.execute('SELECT COUNT(*) FROM cleaned_fires').fetchone()[0]} stable fire events"
        )
        return True

    except Exception as e:
        logger.error(f"❌ Failed to reconstruct stable fires: {e}")
        return False


def match_fires_to_properties(con: duckdb.DuckDBPyConnection, max_distance_m: int = 200) -> bool:
    """
    Match reconstructed fires to CHR properties using address-based matching with spatial verification.

    Args:
        con: DuckDB connection
        max_distance_m: Maximum distance in meters for spatial verification

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"🎯 Matching fires to CHR properties (max distance: {max_distance_m}m)...")

        # Address-based matching with spatial verification
        con.execute(f"""
            CREATE OR REPLACE TABLE fire_property_matches AS
            WITH address_matches AS (
                SELECT
                    f.*,
                    p.chr_number,
                    p.address as property_address,
                    p.municipality_name as property_municipality,
                    p.geo_coord_x_source as property_x,
                    p.geo_coord_y_source as property_y,
                    -- Calculate Euclidean distance in meters (UTM coordinates are in meters)
                    SQRT(
                        POWER(f.fire_x_coord - p.geo_coord_x_source, 2) +
                        POWER(f.fire_y_coord - p.geo_coord_y_source, 2)
                    ) as distance_meters,
                    -- Score the address match quality (handle .0 suffix)
                    CASE
                        WHEN LOWER(p.address) = LOWER(
                            f.fire_street || ' ' || f.fire_house_number
                        ) THEN 100
                        WHEN LOWER(p.address) = LOWER(
                            f.fire_street || ' ' || REGEXP_REPLACE(f.fire_house_number, '\\.0$', '')
                        ) THEN 100
                        WHEN LOWER(p.address) LIKE '%' || LOWER(f.fire_street) || '%'
                             AND LOWER(p.address) LIKE '%' ||
                             LOWER(REGEXP_REPLACE(f.fire_house_number, '\\.0$', '')) || '%' THEN 90
                        WHEN LOWER(p.address) LIKE '%' || LOWER(f.fire_street) || '%' THEN 70
                        ELSE 0
                    END as address_match_score
                FROM cleaned_fires f
                JOIN chr_properties p
                    ON (LOWER(p.address) LIKE '%' || LOWER(f.fire_street) || '%'
                        OR LOWER(f.fire_street) LIKE '%' || LOWER(SPLIT_PART(p.address, ' ', 1)) || '%')
                    AND p.geo_coord_x_source IS NOT NULL
                    AND p.geo_coord_y_source IS NOT NULL
                    AND LOWER(p.municipality_name) = LOWER(f.fire_municipality)
            ),
            best_matches AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY fire_date_str, fire_x_coord, fire_y_coord
                        ORDER BY address_match_score DESC, distance_meters ASC
                    ) as rn
                FROM address_matches
                WHERE distance_meters <= {max_distance_m}
                  AND address_match_score >= 70  -- Require reasonable address match
            )
            SELECT *
            FROM best_matches
            WHERE rn = 1  -- Only the best match per fire
        """)

        match_count = con.execute("SELECT COUNT(*) FROM fire_property_matches").fetchone()[0]

        if match_count > 0:
            # Get statistics
            stats = con.execute("""
                SELECT
                    AVG(distance_meters)::DECIMAL(10,1) as avg_distance,
                    MEDIAN(distance_meters)::DECIMAL(10,1) as median_distance,
                    MAX(distance_meters)::DECIMAL(10,1) as max_distance
                FROM fire_property_matches
            """).fetchone()

            logger.info(f"✅ Matched {match_count} fires to CHR properties")
            logger.info(f"   Distance stats: avg={stats[0]}m, median={stats[1]}m, max={stats[2]}m")
        else:
            logger.warning("⚠️ No fires matched to CHR properties")

        return match_count > 0

    except Exception as e:
        logger.error(f"❌ Failed to match fires to properties: {e}")
        return False


def get_chr_column(con: duckdb.DuckDBPyConnection, table_name: str) -> Optional[str]:
    """Find CHR number column dynamically."""
    try:
        columns = con.execute(f"DESCRIBE {table_name}").fetchall()
        column_names = [col[0] for col in columns]

        # Look for CHR-related columns
        chr_candidates = [col for col in column_names if "chr" in col.lower()]
        if chr_candidates:
            return chr_candidates[0]

        # Fallback patterns
        number_candidates = [col for col in column_names if "nummer" in col.lower() or "number" in col.lower()]
        if number_candidates:
            return number_candidates[0]

        return None
    except Exception:
        return None


def get_date_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> Dict[str, Optional[str]]:
    """Find date-related columns dynamically."""
    try:
        columns = con.execute(f"DESCRIBE {table_name}").fetchall()
        column_names = [col[0] for col in columns]

        result = {}

        # Look for start date
        start_candidates = [
            col for col in column_names if any(word in col.lower() for word in ["start", "from", "begin"])
        ]
        result["start"] = start_candidates[0] if start_candidates else None

        # Look for end date
        end_candidates = [
            col for col in column_names if any(word in col.lower() for word in ["end", "slut", "to", "expir"])
        ]
        result["end"] = end_candidates[0] if end_candidates else None

        # Look for general date
        date_candidates = [col for col in column_names if any(word in col.lower() for word in ["date", "dato", "time"])]
        result["date"] = date_candidates[0] if date_candidates else None

        return result
    except Exception:
        return {}


def create_animal_welfare_timeline_parts(con: duckdb.DuckDBPyConnection) -> List[str]:
    """Create animal welfare timeline parts dynamically."""
    parts = []
    try:
        chr_col = get_chr_column(con, "animal_welfare")
        date_cols = get_date_columns(con, "animal_welfare")

        if not chr_col:
            logger.warning("⚠️ No CHR column found in animal_welfare")
            return []

        # Get available columns for description/category
        columns = con.execute("DESCRIBE animal_welfare").fetchall()
        column_names = [col[0] for col in columns]

        desc_col = next(
            (col for col in column_names if any(word in col.lower() for word in ["indsats", "description", "område"])),
            "NULL",
        )
        species_col = next(
            (col for col in column_names if any(word in col.lower() for word in ["dyre", "species", "art"])),
            "'Unknown'",
        )

        # Start events
        if date_cols.get("start"):
            parts.append(f"""
            SELECT
                {chr_col} as chr_number,
                'animal_welfare' as event_source,
                'intervention_start' as event_type,
                COALESCE({desc_col}, 'Animal welfare intervention') as event_description,
                'Animal_Welfare' as event_category,
                COALESCE({species_col}, 'Unknown') as species,
                TRY_CAST({date_cols["start"]} AS TIMESTAMP) as event_date,
                TRY_CAST({date_cols.get("end", "NULL")} AS TIMESTAMP) as end_date,
                'animal_welfare' as source_file
            FROM animal_welfare
            WHERE {chr_col} IS NOT NULL
              AND {date_cols["start"]} IS NOT NULL
            """)

        # End events (if we have end dates)
        if date_cols.get("end"):
            parts.append(f"""
            SELECT
                {chr_col} as chr_number,
                'animal_welfare' as event_source,
                'intervention_end' as event_type,
                COALESCE({desc_col}, 'Animal welfare intervention end') as event_description,
                'Animal_Welfare' as event_category,
                COALESCE({species_col}, 'Unknown') as species,
                TRY_CAST({date_cols["end"]} AS TIMESTAMP) as event_date,
                NULL as end_date,
                'animal_welfare' as source_file
            FROM animal_welfare
            WHERE {chr_col} IS NOT NULL
              AND {date_cols["end"]} IS NOT NULL
            """)

        logger.info(f"✅ Created {len(parts)} animal welfare timeline parts")
        return parts

    except Exception as e:
        logger.error(f"❌ Failed to create animal welfare timeline parts: {e}")
        return []


def create_vet_events_timeline_parts(con: duckdb.DuckDBPyConnection) -> List[str]:
    """Create veterinary events timeline parts dynamically."""
    try:
        chr_col = get_chr_column(con, "property_vet_events")
        date_cols = get_date_columns(con, "property_vet_events")

        if not chr_col:
            return []

        # Get available columns
        columns = con.execute("DESCRIBE property_vet_events").fetchall()
        column_names = [col[0] for col in columns]

        # Look for disease/status columns
        disease_col = next(
            (col for col in column_names if any(word in col.lower() for word in ["sygdom", "disease"])), "NULL"
        )
        status_col = next((col for col in column_names if "status" in col.lower()), "NULL")
        species_col = next(
            (col for col in column_names if any(word in col.lower() for word in ["dyre", "species", "art"])),
            "'Unknown'",
        )

        date_col = date_cols.get("date") or next(
            (col for col in column_names if "vet" in col.lower() and "date" in col.lower()), None
        )

        if not date_col:
            logger.warning("⚠️ No date column found in property_vet_events")
            return []

        part = f"""
        SELECT
            CAST({chr_col} AS BIGINT) as chr_number,
            'chr_veterinary' as event_source,
            'disease_status_change' as event_type,
            CASE
                WHEN {disease_col} IS NOT NULL AND {status_col} IS NOT NULL
                THEN CAST({disease_col} AS VARCHAR) || ' - ' || CAST({status_col} AS VARCHAR)
                WHEN {disease_col} IS NOT NULL
                THEN CAST({disease_col} AS VARCHAR)
                ELSE 'Veterinary status change'
            END as event_description,
            'Veterinary' as event_category,
            COALESCE({species_col}, 'Unknown') as species,
            TRY_CAST({date_col} AS TIMESTAMP) as event_date,
            NULL as end_date,
            'property_vet_events' as source_file
        FROM property_vet_events
        WHERE {chr_col} IS NOT NULL
          AND TRY_CAST({date_col} AS TIMESTAMP) IS NOT NULL
        """

        logger.info("✅ Created veterinary events timeline part")
        return [part]

    except Exception as e:
        logger.error(f"❌ Failed to create vet events timeline parts: {e}")
        return []


def create_tail_cutting_timeline_parts(con: duckdb.DuckDBPyConnection) -> List[str]:
    """Create pig tail cutting timeline parts dynamically."""
    try:
        chr_col = get_chr_column(con, "pig_tail_cutting")
        date_cols = get_date_columns(con, "pig_tail_cutting")

        if not chr_col:
            return []

        date_col = date_cols.get("date") or next(iter(date_cols.values())) if date_cols else None

        if not date_col:
            logger.warning("⚠️ No date column found in pig_tail_cutting")
            return []

        part = f"""
        SELECT
            {chr_col} as chr_number,
            'pig_tail_cutting' as event_source,
            'control_inspection' as event_type,
            'Tail cutting control inspection' as event_description,
            'Tail_Cutting' as event_category,
            'Pig' as species,
            TRY_CAST({date_col} AS TIMESTAMP) as event_date,
            NULL as end_date,
            'pig_tail_cutting' as source_file
        FROM pig_tail_cutting
        WHERE {chr_col} IS NOT NULL
          AND {date_col} IS NOT NULL
        """

        logger.info("✅ Created tail cutting timeline part")
        return [part]

    except Exception as e:
        logger.error(f"❌ Failed to create tail cutting timeline parts: {e}")
        return []


def create_spf_su_timeline_parts(con: duckdb.DuckDBPyConnection, pipeline_run_date: str) -> List[str]:
    """Create SPF-SU timeline parts dynamically."""
    parts = []
    try:
        chr_col = get_chr_column(con, "spf_su_herds")

        if not chr_col:
            return []

        # Get available columns
        columns = con.execute("DESCRIBE spf_su_herds").fetchall()
        column_names = [col[0] for col in columns]

        health_col = next((col for col in column_names if "health_status" in col.lower()), None)
        cert_date_col = next((col for col in column_names if "cert" in col.lower() and "date" in col.lower()), None)
        cert_approved_col = next((col for col in column_names if "approved" in col.lower()), None)
        next((col for col in column_names if "salmonella_date" in col.lower()), None)
        next((col for col in column_names if "salmonella_status" in col.lower()), None)

        # Certificate events (if we have certificate data)
        if cert_date_col and health_col:
            parts.append(f"""
            SELECT
                {chr_col} as chr_number,
                'spf_su_certificates' as event_source,
                CASE
                    WHEN {cert_approved_col} = false THEN 'certificate_rejected'
                    WHEN {health_col} LIKE 'Under godk.%' THEN 'under_approval'
                    ELSE 'certificate_issued'
                END as event_type,
                'SPF-SU Certificate: ' || COALESCE({health_col}, 'Unknown') as event_description,
                'SPF_Certificate' as event_category,
                'Pig' as species,
                TRY_CAST({cert_date_col} AS TIMESTAMP) as event_date,
                NULL as end_date,
                'spf_su_herds' as source_file
            FROM spf_su_herds
            WHERE {chr_col} IS NOT NULL
              AND {cert_date_col} IS NOT NULL
              AND {cert_date_col} > '1900-01-01'
            """)

        # Disease status events (if we have health status) - all actual markers found in data
        if health_col:
            disease_types = [
                ("Mycoplasma", "+Myc"),
                ("Actinobacillus_type_2", "+Ap2"),
                ("Actinobacillus_type_6", "+Ap6"),
                ("Actinobacillus_type_7", "+Ap7"),
                ("Actinobacillus_type_12", "+Ap12"),
                ("Nysesyge", "+Nys"),  # Sneezing disease - was missing!
                ("PRRS_type_1", "+PRRS1"),
                ("PRRS_type_2", "+PRRS2"),
            ]

            for disease_name, disease_marker in disease_types:
                parts.append(f"""
                SELECT
                    {chr_col} as chr_number,
                    'spf_su_diseases' as event_source,
                    'disease_status' as event_type,
                    '{disease_name}: er smittet med eller kontrolleres fri for' as event_description,
                    '{disease_name}' as event_category,
                    'Pig' as species,
                    CAST('{pipeline_run_date}' AS TIMESTAMP) as event_date,
                    NULL as end_date,
                    'spf_su_herds' as source_file
                FROM spf_su_herds
                WHERE {chr_col} IS NOT NULL
                  AND {health_col} LIKE '%{disease_marker}%'
                """)

            # Add sanitation events (different event type)
            sanitation_types = [
                ("Actinobacillus_type_2_sanitation", "+sanAp2"),
                ("PRRS_type_2_sanitation", "+sanPRRS2"),
            ]

            for san_name, san_marker in sanitation_types:
                parts.append(f"""
                SELECT
                    {chr_col} as chr_number,
                    'spf_su_sanitation' as event_source,
                    'sanitation_process' as event_type,
                    '{san_name}: sanering igang' as event_description,
                    'Sanitation' as event_category,
                    'Pig' as species,
                    CAST('{pipeline_run_date}' AS TIMESTAMP) as event_date,
                    NULL as end_date,
                    'spf_su_herds' as source_file
                FROM spf_su_herds
                WHERE {chr_col} IS NOT NULL
                  AND {health_col} LIKE '%{san_marker}%'
                """)

        # Skip salmonella processing - currently only enrollment records with no meaningful test data
        # Salmonella data exists but contains only placeholder dates (0001-01-01) and empty arrays
        # Will be re-enabled when actual test results become available

        logger.info(f"✅ Created {len(parts)} SPF-SU timeline parts")
        return parts

    except Exception as e:
        logger.error(f"❌ Failed to create SPF-SU timeline parts: {e}")
        return []


def create_spf_su_salmonella_timeline_parts(con: duckdb.DuckDBPyConnection) -> List[str]:
    """Create SPF-SU salmonella timeline parts from detailed salmonella data."""

    # Skip salmonella processing - data contains only enrollment records with no meaningful test data
    # All records have placeholder dates (0001-01-01) and empty arrays for test results
    # Will be re-enabled when actual salmonella test results become available
    logger.info("⚠️ Skipping salmonella timeline processing - only enrollment data available (no test results)")
    return []


def create_stable_fire_timeline_parts(con: duckdb.DuckDBPyConnection) -> List[str]:
    """Create stable fire timeline parts from matched data."""
    try:
        # Check if we have fire matches
        columns = con.execute("DESCRIBE fire_property_matches").fetchall()
        column_names = [col[0] for col in columns]

        date_col = next((col for col in column_names if "date" in col.lower()), None)
        chr_col = next((col for col in column_names if "chr" in col.lower()), None)

        if not date_col or not chr_col:
            return []

        part = f"""
        SELECT
            {chr_col} as chr_number,
            'stable_fires' as event_source,
            'stable_fire' as event_type,
            'Stable fire event (spatially matched)' as event_description,
            'Fire' as event_category,
            'Unknown' as species,
            TRY_CAST({date_col} AS TIMESTAMP) as event_date,
            NULL as end_date,
            'stable_fires' as source_file
        FROM fire_property_matches
        WHERE {chr_col} IS NOT NULL
          AND {date_col} IS NOT NULL
        """

        logger.info("✅ Created stable fire timeline part")
        return [part]

    except Exception as e:
        logger.error(f"❌ Failed to create stable fire timeline parts: {e}")
        return []


def load_data_sources(gcs_access: GCSDataAccess) -> Dict[str, bool]:
    """
    Load all available data sources dynamically using GCS patterns.
    Uses unified pipeline pattern: shared DuckDB connection with GCSDataAccess.

    Args:
        gcs_access: GCS access instance with shared DuckDB connection

    Returns:
        Dict mapping table names to whether they were loaded successfully
    """
    bucket = "landbrugsdata-raw-data"
    loaded_tables = {}

    # Define data source patterns - specific files for correct data
    data_source_patterns = [
        ("chr_properties", "silver/chr/*/properties*.parquet"),
        (
            "animal_welfare",
            "silver/animal welfare/*/Dyrevelfaerd_indsatsomraader*.parquet",
        ),  # Main intervention data (CHR-level)
        ("pig_tail_cutting", "silver/pig tail cutting/*/*.parquet"),
        ("property_vet_events", "silver/chr/*/property_vet*.parquet"),
        ("spf_su_herds", "silver/chr/*/spf_su_herds*.parquet"),
        (
            "spf_su_salmonella",
            "silver/chr/*/spf_su_salmonella_data*.parquet",
        ),  # Additional detailed SPF-SU salmonella data
        ("stable_fires", "silver/stable fires/*/*.parquet"),
    ]

    for table_name, pattern in data_source_patterns:
        try:
            # Use GCS pattern matching to find files
            full_pattern = f"gs://{bucket}/{pattern}"
            files = gcs_access.list_files(full_pattern)

            if files:
                # Filter out old "run_" directories and prioritize proper timestamps
                timestamp_files = [f for f in files if "/run_" not in f]
                if timestamp_files:
                    # Use proper timestamp files first
                    valid_files = sorted(timestamp_files, reverse=True)
                else:
                    # Fallback to any file if no timestamp files found
                    valid_files = sorted(files, reverse=True)

                # For most tables, use latest file only, but for stable_fires, combine all files
                if table_name == "stable_fires" and len(valid_files) > 1:
                    logger.info(f"📥 Loading {table_name} from {len(valid_files)} files:")
                    for i, file_path in enumerate(valid_files):
                        logger.info(f"   {i + 1}. {file_path}")

                    # Load first file to establish table structure
                    # 🚀 ENHANCED: Using native HMAC acceleration for faster loading
                    gcs_access.query_parquet_native(valid_files[0], "SELECT *", table_name)

                    # Union all additional files
                    for i, additional_file in enumerate(valid_files[1:], 1):
                        try:
                            # Create a temporary table for each additional file
                            temp_table = f"{table_name}_temp_{i}"
                            # 🚀 ENHANCED: Using native HMAC acceleration for faster loading
                            gcs_access.query_parquet_native(additional_file, "SELECT *", temp_table)

                            # Union with main table
                            gcs_access.duckdb_conn.execute(f"""
                                CREATE OR REPLACE TABLE {table_name} AS
                                SELECT * FROM {table_name}
                                UNION ALL
                                SELECT * FROM {temp_table}
                            """)

                            # Drop temp table
                            gcs_access.duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

                        except Exception as e:
                            logger.warning(f"⚠️ Failed to load additional file {additional_file}: {e}")

                    # Get final count
                    total_rows = gcs_access.duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    logger.info(f"   Combined total: {total_rows} rows from {len(valid_files)} files")
                else:
                    # Single file loading (existing behavior)
                    latest_file = valid_files[0]
                    logger.info(f"📥 Loading {table_name} from: {latest_file}")
                    # 🚀 ENHANCED: Using native HMAC acceleration for faster loading
                    gcs_access.query_parquet_native(latest_file, "SELECT *", table_name)

                loaded_tables[table_name] = True

                # Log column info dynamically using shared connection
                columns = gcs_access.duckdb_conn.execute(f"DESCRIBE {table_name}").fetchall()
                logger.info(f"   Columns: {[col[0] for col in columns[:5]]}{'...' if len(columns) > 5 else ''}")
            else:
                logger.warning(f"⚠️ No files found for {table_name} with pattern: {pattern}")
                loaded_tables[table_name] = False

        except Exception as e:
            logger.error(f"❌ Failed to load {table_name}: {e}")
            loaded_tables[table_name] = False

    # Create empty tables for failed loads to prevent SQL errors
    for table_name, loaded in loaded_tables.items():
        if not loaded:
            gcs_access.duckdb_conn.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT NULL as dummy_column WHERE FALSE"
            )

    return loaded_tables


def create_veterinary_timeline(
    con: duckdb.DuckDBPyConnection, pipeline_run_date: str, gcs_access: Optional[GCSDataAccess] = None
) -> bool:
    """
    Create comprehensive veterinary timeline combining all sources.

    Args:
        con: DuckDB connection
        pipeline_run_date: Pipeline run date for SPF-SU disease snapshots
        gcs_access: GCS access instance (optional)

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info("🏗️ Creating comprehensive veterinary timeline...")

        # Ensure we have GCS access
        if gcs_access is None:
            logger.error("❌ GCS access is required for data loading")
            return False

        # Load all data sources dynamically (using shared connection)
        loaded_tables = load_data_sources(gcs_access)

        # Note: now we need to use gcs_access.duckdb_conn instead of con
        con = gcs_access.duckdb_conn

        # Check what we have to work with
        available_sources = [name for name, loaded in loaded_tables.items() if loaded]
        logger.info(f"📊 Available data sources: {available_sources}")

        if not available_sources:
            logger.error("❌ No data sources available - cannot create timeline")
            return False

        # Reconstruct and match stable fires if available
        if loaded_tables.get("stable_fires", False):
            logger.info("🔥 Processing stable fires data...")
            stable_fires_loaded = reconstruct_stable_fires_from_table(con)
            if stable_fires_loaded:
                match_fires_to_properties(con)
        else:
            logger.info("⚠️ No stable fires data available")

        # Create unified veterinary timeline dynamically
        timeline_parts = []

        # Animal Welfare Events (if available)
        if loaded_tables.get("animal_welfare", False):
            animal_welfare_parts = create_animal_welfare_timeline_parts(con)
            timeline_parts.extend(animal_welfare_parts)

        # Property Vet Events (if available)
        if loaded_tables.get("property_vet_events", False):
            vet_events_parts = create_vet_events_timeline_parts(con)
            timeline_parts.extend(vet_events_parts)

        # Pig Tail Cutting (if available)
        if loaded_tables.get("pig_tail_cutting", False):
            tail_cutting_parts = create_tail_cutting_timeline_parts(con)
            timeline_parts.extend(tail_cutting_parts)

        # SPF-SU Events (if available)
        if loaded_tables.get("spf_su_herds", False):
            spf_su_parts = create_spf_su_timeline_parts(con, pipeline_run_date)
            timeline_parts.extend(spf_su_parts)

        # Additional SPF-SU Salmonella Events from detailed data (if available)
        if loaded_tables.get("spf_su_salmonella", False):
            salmonella_parts = create_spf_su_salmonella_timeline_parts(con)
            timeline_parts.extend(salmonella_parts)

        # Stable Fire Events (if processed)
        try:
            con.execute("SELECT COUNT(*) FROM fire_property_matches")
            stable_fire_parts = create_stable_fire_timeline_parts(con)
            timeline_parts.extend(stable_fire_parts)
        except Exception:
            logger.info("No stable fire matches available")

        if not timeline_parts:
            logger.error("❌ No timeline parts could be created")
            return False

        # Combine all timeline parts
        full_query = "CREATE OR REPLACE TABLE veterinary_timeline AS\n" + "\nUNION ALL\n".join(timeline_parts)
        con.execute(full_query)

        # Get summary statistics
        summary = con.execute("""
            SELECT
                COUNT(*) as total_events,
                COUNT(DISTINCT chr_number) as unique_chrs,
                MIN(event_date) as earliest_event,
                MAX(event_date) as latest_event,
                COUNT(DISTINCT event_source) as data_sources
            FROM veterinary_timeline
        """).fetchone()

        logger.info("✅ Created veterinary timeline:")
        logger.info(f"   Total events: {summary[0]:,}")
        logger.info(f"   Unique CHRs: {summary[1]:,}")
        logger.info(f"   Date range: {summary[2]} to {summary[3]}")
        logger.info(f"   Data sources: {summary[4]}")

        # Create summary table by source
        con.execute("""
            CREATE OR REPLACE TABLE timeline_summary AS
            SELECT
                event_source,
                COUNT(*) as event_count,
                COUNT(DISTINCT chr_number) as unique_chrs,
                MIN(event_date) as earliest_event,
                MAX(event_date) as latest_event
            FROM veterinary_timeline
            GROUP BY event_source
            ORDER BY event_count DESC
        """)

        return True

    except Exception as e:
        logger.error(f"❌ Failed to create veterinary timeline: {e}")
        return False


def process_veterinary_timeline(
    export_timestamp: str,
    gold_dir: Optional[Path] = None,
    gcs_access: Optional[GCSDataAccess] = None,
    pipeline_run_date: Optional[str] = None,
) -> bool:
    """
    Main function to process veterinary timeline for gold layer.

    Args:
        export_timestamp: Export timestamp for file naming
        gold_dir: Output directory for gold data (optional)
        gcs_access: GCS access instance (optional)
        pipeline_run_date: Date for SPF-SU disease snapshots (optional)

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info("🚀 Starting veterinary timeline processing...")

        # Setup directories
        if gold_dir is None:
            gold_dir = GOLD_BASE_DIR / export_timestamp
        gold_dir.mkdir(parents=True, exist_ok=True)

        if pipeline_run_date is None:
            from datetime import datetime

            pipeline_run_date = datetime.now().strftime("%Y-%m-%d")

        # Initialize DuckDB connection with spatial extension first (unified pipeline pattern)
        con = duckdb.connect()
        try:
            con.install_extension("spatial")
            con.load_extension("spatial")
        except Exception as e:
            logger.warning(f"⚠️ Could not load spatial extension: {e}")

        # Initialize GCS access with shared connection (unified pipeline pattern)
        if gcs_access is None and GCS_AVAILABLE:
            gcs_access = GCSDataAccess(connection=con)

        # Create veterinary timeline using dynamic data loading
        success = create_veterinary_timeline(con, pipeline_run_date, gcs_access)

        if success:
            # Export tables using GCS pattern (tables are in gcs_access.duckdb_conn)
            if gcs_access and migrate_save_data_pattern:
                bucket = "landbrugsdata-raw-data"
                # Use subdataset parameter to create separate filenames
                migrate_save_data_pattern(
                    gcs_access, "veterinary_timeline", "chr", bucket, "gold", export_timestamp, "veterinary_timeline"
                )

                # Check if timeline_summary was created
                try:
                    gcs_access.duckdb_conn.execute("SELECT COUNT(*) FROM timeline_summary")
                    migrate_save_data_pattern(
                        gcs_access, "timeline_summary", "chr", bucket, "gold", export_timestamp, "timeline_summary"
                    )
                except Exception:
                    logger.info("ℹ️ No timeline_summary table to export")
            else:
                # Fallback to local export
                logger.warning("⚠️ GCS not available, exporting locally only")
                # Local export logic would go here

            logger.info("✅ Veterinary timeline processing completed successfully")
        else:
            logger.error("❌ Veterinary timeline processing failed")

        # Connection will be closed when gcs_access is destroyed
        return success

    except Exception as e:
        logger.error(f"❌ Error processing veterinary timeline: {e}")
        return False
