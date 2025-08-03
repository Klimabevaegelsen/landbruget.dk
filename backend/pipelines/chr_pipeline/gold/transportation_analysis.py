"""CHR Transportation Analysis processing for Gold layer."""

import logging
from pathlib import Path
from typing import Dict, Optional

import duckdb

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


def setup_database(conn: duckdb.DuckDBPyConnection) -> None:
    """Set up DuckDB with necessary extensions and settings."""
    try:
        conn.install_extension("spatial")
        conn.load_extension("spatial")
    except Exception as e:
        logger.warning(f"⚠️ Could not load spatial extension: {e}")


def load_transportation_data_sources(gcs_access) -> Dict[str, bool]:
    """
    Load all transportation data sources dynamically using GCS patterns.
    Uses unified pipeline pattern: shared DuckDB connection with GCSDataAccess.
    
    Args:
        gcs_access: GCS access instance with shared DuckDB connection
        
    Returns:
        Dict mapping table names to whether they were loaded successfully
    """
    bucket = "landbrugsdata-raw-data"
    loaded_tables = {}
    
    # Define data source patterns for transportation analysis
    data_source_patterns = [
        # Core CHR data
        ("chr_properties", "silver/chr/*/properties*.parquet"),
        ("chr_herds", "silver/chr/*/herds*.parquet"),
        ("chr_dyr_movements", "silver/chr/*/chr_dyr_movement_summaries*.parquet"),
        
        # Svineflytning (domestic pig movements)
        ("svineflytning", "silver/svineflytning/*/movements*.parquet"),
        
        # International animal movements (cattle traces)
        ("intl_cattle_traces_cl", "silver/animal international movements/*/Kvaeg_udfoersel_2017_2024_cl*.parquet"),
        ("intl_cattle_traces_nt", "silver/animal international movements/*/Kvaeg_udfoersel_2017_2024_nt*.parquet"),
        ("intl_combined_traces_2024_2025", "silver/animal international movements/*/Dyr_udfoersel_2024_2025*.parquet"),
        
        # International pig movements
        ("intl_pig_cl", "silver/pig international movements/*/Grise_udfoersel_2017_2024_cl*.parquet"),
        ("intl_pig_nt", "silver/pig international movements/*/Grise_udfoersel_2017_2024_nt*.parquet"),
        ("intl_pig_2024_2025", "silver/pig international movements/*/Dyr_udfoersel_2024_2025*.parquet"),
    ]
    
    for table_name, pattern in data_source_patterns:
        try:
            # Use GCS pattern matching to find files
            full_pattern = f"gs://{bucket}/{pattern}"
            files = gcs_access.list_files(full_pattern)
            
            if files:
                # Filter out old "run_" directories and prioritize proper timestamps
                timestamp_files = [f for f in files if '/run_' not in f]
                if timestamp_files:
                    # Use proper timestamp files first
                    latest_file = sorted(timestamp_files, reverse=True)[0]
                else:
                    # Fallback to any file if no timestamp files found
                    latest_file = sorted(files, reverse=True)[0]
                logger.info(f"📥 Loading {table_name} from: {latest_file}")
                
                # Use unified pipeline pattern: query_parquet_direct with shared connection
                gcs_access.query_parquet_direct(latest_file, "SELECT *", table_name)
                loaded_tables[table_name] = True
                
                # Log row count using shared connection
                count = gcs_access.duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                logger.info(f"   ✅ {table_name}: {count:,} records")
            else:
                logger.warning(f"⚠️ No files found for {table_name} with pattern: {pattern}")
                loaded_tables[table_name] = False
                
        except Exception as e:
            logger.error(f"❌ Failed to load {table_name}: {e}")
            loaded_tables[table_name] = False
    
    # Create empty tables for failed loads to prevent SQL errors
    for table_name, loaded in loaded_tables.items():
        if not loaded:
            gcs_access.duckdb_conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT NULL as dummy_column WHERE FALSE")
    
    return loaded_tables


def create_comprehensive_certificate_matching(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create comprehensive certificate matching using all parsing methods discovered.
    
    Matches international transport with svineflytning using:
    1. Standard A,EU.DK format parsing
    2. Alternative DK/EU.DK format parsing  
    3. Certificate reconstruction for malformed certificates
    4. Deduplication to ensure 1:1 matching (fixes 127% issue)
    
    Includes comprehensive data from svineflytning with all available fields.
    """
    logger.info("🔗 Creating comprehensive certificate matching...")
    
    # First create all matches (including duplicates) - RAW table with comprehensive columns
    conn.execute('''
        CREATE OR REPLACE TABLE certificate_matched_movements_raw AS
        SELECT DISTINCT
            sv.traces_document,
            sv.sender_chr_number,
            sv.receiver_chr_number,
            sv.movement_date,
            sv.total_animals,
            sv.receiver_country_code,
            'DK' as sender_country_code,  -- All svineflytning originates from Denmark
            15 as species_code,           -- All svineflytning is pig movements (species_code = 15)
            'svineflytning' as source_dataset,
            NULL as intl_certificate,
            NULL as intl_dataset,
            'domestic' as movement_type,
            -- Additional comprehensive svineflytning fields
            sv.movement_time,
            sv.movement_sequence,
            sv.sender_city_name,
            sv.sender_postal_code,
            sv.sender_postal_district,
            sv.sender_municipality_code,
            sv.sender_municipality_name,
            sv.sender_property_created,
            sv.sender_property_updated,
            sv.sender_foreign_property,
            sv.receiver_city_name,
            sv.receiver_postal_code,
            sv.receiver_postal_district,
            sv.receiver_municipality_code,
            sv.receiver_municipality_name,
            sv.receiver_property_created,
            sv.receiver_property_updated,
            sv.receiver_foreign_property,
            sv.sow_count,
            sv.slaughter_pig_count,
            sv.containers_190l,
            sv.containers_240l,
            sv.vehicle_country_code,
            sv.vehicle_registration,
            sv.trailer_country_code,
            sv.trailer_registration,
            sv.transshipment_info,
            sv.health_certificate,
            sv.reporter_login,
            sv.report_timestamp,
            sv.processed_timestamp,
            sv.source_chunk_timestamp,
            sv.source_period_start,
            sv.source_period_end,
            sv.is_deleted,
            sv.is_invalid,
            sv.missing_animal_count
        FROM svineflytning sv
        WHERE sv.receiver_country_code = 'DK'
        
        UNION ALL
        
        -- Standard A,EU.DK format matches with _nt (with comprehensive fields)
        SELECT DISTINCT
            sv.traces_document,
            sv.sender_chr_number,
            sv.receiver_chr_number,
            sv.movement_date,
            sv.total_animals,
            sv.receiver_country_code,
            'DK' as sender_country_code,
            15 as species_code,
            'svineflytning' as source_dataset,
            nt.i_2_imsoc_reference as intl_certificate,
            'international_pig_nt' as intl_dataset,
            'international_export' as movement_type,
            -- Additional comprehensive svineflytning fields
            sv.movement_time,
            sv.movement_sequence,
            sv.sender_city_name,
            sv.sender_postal_code,
            sv.sender_postal_district,
            sv.sender_municipality_code,
            sv.sender_municipality_name,
            sv.sender_property_created,
            sv.sender_property_updated,
            sv.sender_foreign_property,
            sv.receiver_city_name,
            sv.receiver_postal_code,
            sv.receiver_postal_district,
            sv.receiver_municipality_code,
            sv.receiver_municipality_name,
            sv.receiver_property_created,
            sv.receiver_property_updated,
            sv.receiver_foreign_property,
            sv.sow_count,
            sv.slaughter_pig_count,
            sv.containers_190l,
            sv.containers_240l,
            sv.vehicle_country_code,
            sv.vehicle_registration,
            sv.trailer_country_code,
            sv.trailer_registration,
            sv.transshipment_info,
            sv.health_certificate,
            sv.reporter_login,
            sv.report_timestamp,
            sv.processed_timestamp,
            sv.source_chunk_timestamp,
            sv.source_period_start,
            sv.source_period_end,
            sv.is_deleted,
            sv.is_invalid,
            sv.missing_animal_count
        FROM svineflytning sv
        JOIN intl_pig_nt nt ON SUBSTRING(sv.traces_document, 9) = SUBSTRING(nt.i_2_imsoc_reference, 13)
        WHERE sv.receiver_country_code != 'DK' 
        AND sv.traces_document IS NOT NULL
        AND nt.i_2_imsoc_reference IS NOT NULL
        
        UNION ALL
        
        -- Standard A,EU.DK format matches with _cl (with comprehensive fields)
        SELECT DISTINCT
            sv.traces_document,
            sv.sender_chr_number,
            sv.receiver_chr_number,
            sv.movement_date,
            sv.total_animals,
            sv.receiver_country_code,
            'DK' as sender_country_code,
            15 as species_code,
            'svineflytning' as source_dataset,
            cl.i_2_certificate_reference_number as intl_certificate,
            'international_pig_cl' as intl_dataset,
            'international_export' as movement_type,
            -- Additional comprehensive svineflytning fields
            sv.movement_time,
            sv.movement_sequence,
            sv.sender_city_name,
            sv.sender_postal_code,
            sv.sender_postal_district,
            sv.sender_municipality_code,
            sv.sender_municipality_name,
            sv.sender_property_created,
            sv.sender_property_updated,
            sv.sender_foreign_property,
            sv.receiver_city_name,
            sv.receiver_postal_code,
            sv.receiver_postal_district,
            sv.receiver_municipality_code,
            sv.receiver_municipality_name,
            sv.receiver_property_created,
            sv.receiver_property_updated,
            sv.receiver_foreign_property,
            sv.sow_count,
            sv.slaughter_pig_count,
            sv.containers_190l,
            sv.containers_240l,
            sv.vehicle_country_code,
            sv.vehicle_registration,
            sv.trailer_country_code,
            sv.trailer_registration,
            sv.transshipment_info,
            sv.health_certificate,
            sv.reporter_login,
            sv.report_timestamp,
            sv.processed_timestamp,
            sv.source_chunk_timestamp,
            sv.source_period_start,
            sv.source_period_end,
            sv.is_deleted,
            sv.is_invalid,
            sv.missing_animal_count
        FROM svineflytning sv
        JOIN intl_pig_cl cl ON SUBSTRING(sv.traces_document, 9) = SUBSTRING(cl.i_2_certificate_reference_number, 10)
        WHERE sv.receiver_country_code != 'DK' 
        AND sv.traces_document IS NOT NULL
        AND cl.i_2_certificate_reference_number IS NOT NULL
        
        UNION ALL
        
        -- Standard A,EU.DK format matches with 2024-2025
        SELECT DISTINCT
            sv.traces_document,
            sv.sender_chr_number,
            sv.receiver_chr_number,
            sv.movement_date,
            sv.total_animals,
            sv.receiver_country_code,
            'DK' as sender_country_code,
            15 as species_code,
            'svineflytning' as source_dataset,
            new.i_2_imsoc_reference as intl_certificate,
            'international_pig_2024_2025' as intl_dataset,
            'international_export' as movement_type,
            -- Additional comprehensive svineflytning fields
            sv.movement_time,
            sv.movement_sequence,
            sv.sender_city_name,
            sv.sender_postal_code,
            sv.sender_postal_district,
            sv.sender_municipality_code,
            sv.sender_municipality_name,
            sv.sender_property_created,
            sv.sender_property_updated,
            sv.sender_foreign_property,
            sv.receiver_city_name,
            sv.receiver_postal_code,
            sv.receiver_postal_district,
            sv.receiver_municipality_code,
            sv.receiver_municipality_name,
            sv.receiver_property_created,
            sv.receiver_property_updated,
            sv.receiver_foreign_property,
            sv.sow_count,
            sv.slaughter_pig_count,
            sv.containers_190l,
            sv.containers_240l,
            sv.vehicle_country_code,
            sv.vehicle_registration,
            sv.trailer_country_code,
            sv.trailer_registration,
            sv.transshipment_info,
            sv.health_certificate,
            sv.reporter_login,
            sv.report_timestamp,
            sv.processed_timestamp,
            sv.source_chunk_timestamp,
            sv.source_period_start,
            sv.source_period_end,
            sv.is_deleted,
            sv.is_invalid,
            sv.missing_animal_count
        FROM svineflytning sv
        JOIN intl_pig_2024_2025 new ON SUBSTRING(sv.traces_document, 9) = SUBSTRING(new.i_2_imsoc_reference, 13)
        WHERE sv.receiver_country_code != 'DK' 
        AND sv.traces_document IS NOT NULL
        AND new.i_2_imsoc_reference IS NOT NULL
        
        UNION ALL
        
        -- DK format matches with _nt
        SELECT DISTINCT
            sv.traces_document,
            sv.sender_chr_number,
            sv.receiver_chr_number,
            sv.movement_date,
            sv.total_animals,
            sv.receiver_country_code,
            'DK' as sender_country_code,
            15 as species_code,
            'svineflytning' as source_dataset,
            nt.i_2_imsoc_reference as intl_certificate,
            'international_pig_nt' as intl_dataset,
            'international_export' as movement_type,
            -- Additional comprehensive svineflytning fields
            sv.movement_time,
            sv.movement_sequence,
            sv.sender_city_name,
            sv.sender_postal_code,
            sv.sender_postal_district,
            sv.sender_municipality_code,
            sv.sender_municipality_name,
            sv.sender_property_created,
            sv.sender_property_updated,
            sv.sender_foreign_property,
            sv.receiver_city_name,
            sv.receiver_postal_code,
            sv.receiver_postal_district,
            sv.receiver_municipality_code,
            sv.receiver_municipality_name,
            sv.receiver_property_created,
            sv.receiver_property_updated,
            sv.receiver_foreign_property,
            sv.sow_count,
            sv.slaughter_pig_count,
            sv.containers_190l,
            sv.containers_240l,
            sv.vehicle_country_code,
            sv.vehicle_registration,
            sv.trailer_country_code,
            sv.trailer_registration,
            sv.transshipment_info,
            sv.health_certificate,
            sv.reporter_login,
            sv.report_timestamp,
            sv.processed_timestamp,
            sv.source_chunk_timestamp,
            sv.source_period_start,
            sv.source_period_end,
            sv.is_deleted,
            sv.is_invalid,
            sv.missing_animal_count
        FROM svineflytning sv
        JOIN intl_pig_nt nt ON SUBSTRING(sv.traces_document, 3) = SUBSTRING(nt.i_2_imsoc_reference, 13)
        WHERE sv.receiver_country_code != 'DK' 
        AND sv.traces_document LIKE 'DK%'
        AND nt.i_2_imsoc_reference IS NOT NULL
        
        UNION ALL
        
        -- EU.DK format matches with _nt
        SELECT DISTINCT
            sv.traces_document,
            sv.sender_chr_number,
            sv.receiver_chr_number,
            sv.movement_date,
            sv.total_animals,
            sv.receiver_country_code,
            'DK' as sender_country_code,
            15 as species_code,
            'svineflytning' as source_dataset,
            nt.i_2_imsoc_reference as intl_certificate,
            'international_pig_nt' as intl_dataset,
            'international_export' as movement_type,
            -- Additional comprehensive svineflytning fields
            sv.movement_time,
            sv.movement_sequence,
            sv.sender_city_name,
            sv.sender_postal_code,
            sv.sender_postal_district,
            sv.sender_municipality_code,
            sv.sender_municipality_name,
            sv.sender_property_created,
            sv.sender_property_updated,
            sv.sender_foreign_property,
            sv.receiver_city_name,
            sv.receiver_postal_code,
            sv.receiver_postal_district,
            sv.receiver_municipality_code,
            sv.receiver_municipality_name,
            sv.receiver_property_created,
            sv.receiver_property_updated,
            sv.receiver_foreign_property,
            sv.sow_count,
            sv.slaughter_pig_count,
            sv.containers_190l,
            sv.containers_240l,
            sv.vehicle_country_code,
            sv.vehicle_registration,
            sv.trailer_country_code,
            sv.trailer_registration,
            sv.transshipment_info,
            sv.health_certificate,
            sv.reporter_login,
            sv.report_timestamp,
            sv.processed_timestamp,
            sv.source_chunk_timestamp,
            sv.source_period_start,
            sv.source_period_end,
            sv.is_deleted,
            sv.is_invalid,
            sv.missing_animal_count
        FROM svineflytning sv
        JOIN intl_pig_nt nt ON SUBSTRING(sv.traces_document, 7) = SUBSTRING(nt.i_2_imsoc_reference, 13)
        WHERE sv.receiver_country_code != 'DK' 
        AND sv.traces_document LIKE 'EU.DK.%'
        AND nt.i_2_imsoc_reference IS NOT NULL
        
        UNION ALL
        
        -- EU.DK format matches with 2024-2025
        SELECT DISTINCT
            sv.traces_document,
            sv.sender_chr_number,
            sv.receiver_chr_number,
            sv.movement_date,
            sv.total_animals,
            sv.receiver_country_code,
            'DK' as sender_country_code,
            15 as species_code,
            'svineflytning' as source_dataset,
            new.i_2_imsoc_reference as intl_certificate,
            'international_pig_2024_2025' as intl_dataset,
            'international_export' as movement_type,
            -- Additional comprehensive svineflytning fields
            sv.movement_time,
            sv.movement_sequence,
            sv.sender_city_name,
            sv.sender_postal_code,
            sv.sender_postal_district,
            sv.sender_municipality_code,
            sv.sender_municipality_name,
            sv.sender_property_created,
            sv.sender_property_updated,
            sv.sender_foreign_property,
            sv.receiver_city_name,
            sv.receiver_postal_code,
            sv.receiver_postal_district,
            sv.receiver_municipality_code,
            sv.receiver_municipality_name,
            sv.receiver_property_created,
            sv.receiver_property_updated,
            sv.receiver_foreign_property,
            sv.sow_count,
            sv.slaughter_pig_count,
            sv.containers_190l,
            sv.containers_240l,
            sv.vehicle_country_code,
            sv.vehicle_registration,
            sv.trailer_country_code,
            sv.trailer_registration,
            sv.transshipment_info,
            sv.health_certificate,
            sv.reporter_login,
            sv.report_timestamp,
            sv.processed_timestamp,
            sv.source_chunk_timestamp,
            sv.source_period_start,
            sv.source_period_end,
            sv.is_deleted,
            sv.is_invalid,
            sv.missing_animal_count
        FROM svineflytning sv
        JOIN intl_pig_2024_2025 new ON SUBSTRING(sv.traces_document, 7) = SUBSTRING(new.i_2_imsoc_reference, 13)
        WHERE sv.receiver_country_code != 'DK' 
        AND sv.traces_document LIKE 'EU.DK.%'
        AND new.i_2_imsoc_reference IS NOT NULL
        
        UNION ALL
        
        -- Numeric certificate reconstruction with _nt
        SELECT DISTINCT
            sv.traces_document,
            sv.sender_chr_number,
            sv.receiver_chr_number,
            sv.movement_date,
            sv.total_animals,
            sv.receiver_country_code,
            'DK' as sender_country_code,
            15 as species_code,
            'svineflytning' as source_dataset,
            nt.i_2_imsoc_reference as intl_certificate,
            'international_pig_nt' as intl_dataset,
            'international_export' as movement_type,
            -- Additional comprehensive svineflytning fields
            sv.movement_time,
            sv.movement_sequence,
            sv.sender_city_name,
            sv.sender_postal_code,
            sv.sender_postal_district,
            sv.sender_municipality_code,
            sv.sender_municipality_name,
            sv.sender_property_created,
            sv.sender_property_updated,
            sv.sender_foreign_property,
            sv.receiver_city_name,
            sv.receiver_postal_code,
            sv.receiver_postal_district,
            sv.receiver_municipality_code,
            sv.receiver_municipality_name,
            sv.receiver_property_created,
            sv.receiver_property_updated,
            sv.receiver_foreign_property,
            sv.sow_count,
            sv.slaughter_pig_count,
            sv.containers_190l,
            sv.containers_240l,
            sv.vehicle_country_code,
            sv.vehicle_registration,
            sv.trailer_country_code,
            sv.trailer_registration,
            sv.transshipment_info,
            sv.health_certificate,
            sv.reporter_login,
            sv.report_timestamp,
            sv.processed_timestamp,
            sv.source_chunk_timestamp,
            sv.source_period_start,
            sv.source_period_end,
            sv.is_deleted,
            sv.is_invalid,
            sv.missing_animal_count
        FROM svineflytning sv
        JOIN intl_pig_nt nt ON CONCAT(EXTRACT(YEAR FROM sv.movement_date), '.', LPAD(sv.traces_document, 7, '0')) = SUBSTRING(nt.i_2_imsoc_reference, 13)
        WHERE sv.receiver_country_code != 'DK' 
        AND sv.traces_document ~ '^[0-9]+$'
        AND nt.i_2_imsoc_reference IS NOT NULL
        
        UNION ALL
        
        -- Numeric certificate reconstruction with 2024-2025
        SELECT DISTINCT
            sv.traces_document,
            sv.sender_chr_number,
            sv.receiver_chr_number,
            sv.movement_date,
            sv.total_animals,
            sv.receiver_country_code,
            'DK' as sender_country_code,
            15 as species_code,
            'svineflytning' as source_dataset,
            new.i_2_imsoc_reference as intl_certificate,
            'international_pig_2024_2025' as intl_dataset,
            'international_export' as movement_type,
            -- Additional comprehensive svineflytning fields
            sv.movement_time,
            sv.movement_sequence,
            sv.sender_city_name,
            sv.sender_postal_code,
            sv.sender_postal_district,
            sv.sender_municipality_code,
            sv.sender_municipality_name,
            sv.sender_property_created,
            sv.sender_property_updated,
            sv.sender_foreign_property,
            sv.receiver_city_name,
            sv.receiver_postal_code,
            sv.receiver_postal_district,
            sv.receiver_municipality_code,
            sv.receiver_municipality_name,
            sv.receiver_property_created,
            sv.receiver_property_updated,
            sv.receiver_foreign_property,
            sv.sow_count,
            sv.slaughter_pig_count,
            sv.containers_190l,
            sv.containers_240l,
            sv.vehicle_country_code,
            sv.vehicle_registration,
            sv.trailer_country_code,
            sv.trailer_registration,
            sv.transshipment_info,
            sv.health_certificate,
            sv.reporter_login,
            sv.report_timestamp,
            sv.processed_timestamp,
            sv.source_chunk_timestamp,
            sv.source_period_start,
            sv.source_period_end,
            sv.is_deleted,
            sv.is_invalid,
            sv.missing_animal_count
        FROM svineflytning sv
        JOIN intl_pig_2024_2025 new ON CONCAT(EXTRACT(YEAR FROM sv.movement_date), '.', LPAD(sv.traces_document, 7, '0')) = SUBSTRING(new.i_2_imsoc_reference, 13)
        WHERE sv.receiver_country_code != 'DK' 
        AND sv.traces_document ~ '^[0-9]+$'
        AND new.i_2_imsoc_reference IS NOT NULL
        
        UNION ALL
        
        -- Partial date format matches with _nt
        SELECT DISTINCT
            sv.traces_document,
            sv.sender_chr_number,
            sv.receiver_chr_number,
            sv.movement_date,
            sv.total_animals,
            sv.receiver_country_code,
            'DK' as sender_country_code,
            15 as species_code,
            'svineflytning' as source_dataset,
            nt.i_2_imsoc_reference as intl_certificate,
            'international_pig_nt' as intl_dataset,
            'international_export' as movement_type,
            -- Additional comprehensive svineflytning fields
            sv.movement_time,
            sv.movement_sequence,
            sv.sender_city_name,
            sv.sender_postal_code,
            sv.sender_postal_district,
            sv.sender_municipality_code,
            sv.sender_municipality_name,
            sv.sender_property_created,
            sv.sender_property_updated,
            sv.sender_foreign_property,
            sv.receiver_city_name,
            sv.receiver_postal_code,
            sv.receiver_postal_district,
            sv.receiver_municipality_code,
            sv.receiver_municipality_name,
            sv.receiver_property_created,
            sv.receiver_property_updated,
            sv.receiver_foreign_property,
            sv.sow_count,
            sv.slaughter_pig_count,
            sv.containers_190l,
            sv.containers_240l,
            sv.vehicle_country_code,
            sv.vehicle_registration,
            sv.trailer_country_code,
            sv.trailer_registration,
            sv.transshipment_info,
            sv.health_certificate,
            sv.reporter_login,
            sv.report_timestamp,
            sv.processed_timestamp,
            sv.source_chunk_timestamp,
            sv.source_period_start,
            sv.source_period_end,
            sv.is_deleted,
            sv.is_invalid,
            sv.missing_animal_count
        FROM svineflytning sv
        JOIN intl_pig_nt nt ON sv.traces_document = SUBSTRING(nt.i_2_imsoc_reference, 13)
        WHERE sv.receiver_country_code != 'DK' 
        AND sv.traces_document ~ '^[0-9]{4}\\.[0-9]+$'
        AND nt.i_2_imsoc_reference IS NOT NULL
        
        UNION ALL
        
        -- Partial date format matches with 2024-2025
        SELECT DISTINCT
            sv.traces_document,
            sv.sender_chr_number,
            sv.receiver_chr_number,
            sv.movement_date,
            sv.total_animals,
            sv.receiver_country_code,
            'DK' as sender_country_code,
            15 as species_code,
            'svineflytning' as source_dataset,
            new.i_2_imsoc_reference as intl_certificate,
            'international_pig_2024_2025' as intl_dataset,
            'international_export' as movement_type,
            -- Additional comprehensive svineflytning fields
            sv.movement_time,
            sv.movement_sequence,
            sv.sender_city_name,
            sv.sender_postal_code,
            sv.sender_postal_district,
            sv.sender_municipality_code,
            sv.sender_municipality_name,
            sv.sender_property_created,
            sv.sender_property_updated,
            sv.sender_foreign_property,
            sv.receiver_city_name,
            sv.receiver_postal_code,
            sv.receiver_postal_district,
            sv.receiver_municipality_code,
            sv.receiver_municipality_name,
            sv.receiver_property_created,
            sv.receiver_property_updated,
            sv.receiver_foreign_property,
            sv.sow_count,
            sv.slaughter_pig_count,
            sv.containers_190l,
            sv.containers_240l,
            sv.vehicle_country_code,
            sv.vehicle_registration,
            sv.trailer_country_code,
            sv.trailer_registration,
            sv.transshipment_info,
            sv.health_certificate,
            sv.reporter_login,
            sv.report_timestamp,
            sv.processed_timestamp,
            sv.source_chunk_timestamp,
            sv.source_period_start,
            sv.source_period_end,
            sv.is_deleted,
            sv.is_invalid,
            sv.missing_animal_count
        FROM svineflytning sv
        JOIN intl_pig_2024_2025 new ON sv.traces_document = SUBSTRING(new.i_2_imsoc_reference, 13)
        WHERE sv.receiver_country_code != 'DK' 
        AND sv.traces_document ~ '^[0-9]{4}\\.[0-9]+$'
        AND new.i_2_imsoc_reference IS NOT NULL
    ''')
    
    # Now deduplicate to create final table - priority: 2024-2025 > NT > CL
    # Include all comprehensive columns in the final deduplicated table
    conn.execute('''
        CREATE OR REPLACE TABLE certificate_matched_movements AS
        SELECT 
            traces_document,
            sender_chr_number,
            receiver_chr_number,
            movement_date,
            total_animals,
            receiver_country_code,
            sender_country_code,
            species_code,
            source_dataset,
            intl_certificate,
            intl_dataset,
            movement_type,
            -- Additional comprehensive fields from svineflytning
            movement_time,
            movement_sequence,
            sender_city_name,
            sender_postal_code,
            sender_postal_district,
            sender_municipality_code,
            sender_municipality_name,
            sender_property_created,
            sender_property_updated,
            sender_foreign_property,
            receiver_city_name,
            receiver_postal_code,
            receiver_postal_district,
            receiver_municipality_code,
            receiver_municipality_name,
            receiver_property_created,
            receiver_property_updated,
            receiver_foreign_property,
            sow_count,
            slaughter_pig_count,
            containers_190l,
            containers_240l,
            vehicle_country_code,
            vehicle_registration,
            trailer_country_code,
            trailer_registration,
            transshipment_info,
            health_certificate,
            reporter_login,
            report_timestamp,
            processed_timestamp,
            source_chunk_timestamp,
            source_period_start,
            source_period_end,
            is_deleted,
            is_invalid,
            missing_animal_count
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY traces_document, sender_chr_number, receiver_chr_number, movement_date 
                    ORDER BY 
                        CASE 
                            WHEN intl_dataset = 'international_pig_2024_2025' THEN 1
                            WHEN intl_dataset = 'international_pig_nt' THEN 2
                            WHEN intl_dataset = 'international_pig_cl' THEN 3
                            ELSE 4
                        END
                ) as rn
            FROM certificate_matched_movements_raw
        ) ranked
        WHERE rn = 1
    ''')
    
    matched_count = conn.execute('SELECT COUNT(*) FROM certificate_matched_movements').fetchone()[0]
    intl_matched = conn.execute("SELECT COUNT(*) FROM certificate_matched_movements WHERE movement_type = 'international_export'").fetchone()[0]
    
    logger.info(f"  ✅ Created certificate_matched_movements: {matched_count:,} total movements")
    logger.info(f"  🌍 International movements matched: {intl_matched:,}")


def perform_cattle_traces_matching(conn: duckdb.DuckDBPyConnection) -> None:
    """Match cattle movements using address/date/country matching with all three traces files."""
    logger.info("🐄 Performing cattle traces matching using address/date/country...")
    
    # Create a unified cattle traces table from all three files with normalized schemas
    # Use only the most recent version of each certificate to handle duplicates/edits
    conn.execute('''
        CREATE OR REPLACE TABLE unified_cattle_traces AS
        SELECT * FROM (
            -- CL version (2017-2024) - has different schema
            SELECT 
                i_2_certificate_reference_number as certificate,
                strptime(i_15_date_and_time_of_departure, '%Y-%m-%d %H:%M:%S')::DATE as movement_date,
                i_15_date_and_time_of_departure as raw_datetime,
                i_14_place_of_loading as sender_address,
                i_14_place_of_loading_city as sender_city,
                i_14_place_of_loading_country as sender_country,
                i_13_place_of_destination as receiver_address,
                i_13_place_of_destination_city as receiver_city,
                i_13_place_of_destination_country as receiver_country,
                'intl_cattle_traces_cl' as source_file
            FROM intl_cattle_traces_cl
            WHERE i_15_date_and_time_of_departure != '-'
            
            UNION ALL
            
            -- NT version (2017-2024) - has different schema than CL, with deduplication
            SELECT 
                i_2_imsoc_reference as certificate,
                strptime(i_14_dato_og_klokkeslaet_for_afgang, '%Y-%m-%d %H:%M:%S')::DATE as movement_date,
                i_14_dato_og_klokkeslaet_for_afgang as raw_datetime,
                i_11_afsendelsessted_adresse as sender_address,
                i_11_afsendelsessted_by as sender_city,
                i_11_afsendelsessted_land as sender_country,
                i_12_bestemmelsessted_adresse as receiver_address,
                i_12_bestemmelsessted_by as receiver_city,
                i_12_bestemmelsessted_land as receiver_country,
                'intl_cattle_traces_nt' as source_file
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY i_2_imsoc_reference ORDER BY i_14_dato_og_klokkeslaet_for_afgang DESC) as rn
                FROM intl_cattle_traces_nt
                WHERE i_14_dato_og_klokkeslaet_for_afgang != '-'
            ) ranked
            WHERE rn = 1  -- Only most recent version of each certificate
            
            UNION ALL
            
            -- Combined 2024-2025 (same schema as NT) - cattle only
            SELECT 
                i_2_imsoc_reference as certificate,
                strptime(i_14_dato_og_klokkeslaet_for_afgang, '%Y-%m-%d %H:%M:%S')::DATE as movement_date,
                i_14_dato_og_klokkeslaet_for_afgang as raw_datetime,
                i_11_afsendelsessted_adresse as sender_address,
                i_11_afsendelsessted_by as sender_city,
                i_11_afsendelsessted_land as sender_country,
                i_12_bestemmelsessted_adresse as receiver_address,
                i_12_bestemmelsessted_by as receiver_city,
                i_12_bestemmelsessted_land as receiver_country,
                'intl_combined_traces_2024_2025' as source_file
            FROM intl_combined_traces_2024_2025
            WHERE i_14_dato_og_klokkeslaet_for_afgang != '-'
            AND (i_30_commodities LIKE '%cattle%' OR i_30_commodities LIKE '%bovine%' OR i_30_commodities LIKE '%Cattle%')
        ) all_traces
        -- Final deduplication across all files - use most recent record for each certificate
        QUALIFY ROW_NUMBER() OVER (PARTITION BY certificate ORDER BY raw_datetime DESC) = 1
    ''')
    
    # Match cattle traces with CHR movements using address/date/country strategy
    # IMPROVED: Only match CHR movements that are flagged as international
    conn.execute('''
        INSERT INTO certificate_matched_movements
        SELECT DISTINCT
            ct.certificate as traces_document,
            chr.reporting_herd_number as sender_chr_number,
            chr.counterparty_herd as receiver_chr_number,
            chr.movement_date,
            chr.animal_count as total_animals,
            CASE 
                WHEN ct.receiver_country != 'Danmark' AND ct.receiver_country != 'DK' THEN ct.receiver_country
                ELSE 'DK'
            END as receiver_country_code,
            CASE 
                WHEN ct.sender_country != 'Danmark' AND ct.sender_country != 'DK' THEN ct.sender_country
                ELSE 'DK'
            END as sender_country_code,
            12 as species_code,              -- Cattle = species code 12
            'chr_cattle' as source_dataset,
            ct.certificate as intl_certificate,
            ct.source_file as intl_dataset,
            'cattle_traces_matched' as movement_type,
            -- Set pig-specific fields to NULL for cattle (to maintain schema consistency)
            NULL as movement_time,
            NULL as movement_sequence,
            NULL as sender_city_name,
            NULL as sender_postal_code,
            NULL as sender_postal_district,
            NULL as sender_municipality_code,
            NULL as sender_municipality_name,
            NULL as sender_property_created,
            NULL as sender_property_updated,
            NULL as sender_foreign_property,
            NULL as receiver_city_name,
            NULL as receiver_postal_code,
            NULL as receiver_postal_district,
            NULL as receiver_municipality_code,
            NULL as receiver_municipality_name,
            NULL as receiver_property_created,
            NULL as receiver_property_updated,
            NULL as receiver_foreign_property,
            NULL as sow_count,
            NULL as slaughter_pig_count,
            NULL as containers_190l,
            NULL as containers_240l,
            NULL as vehicle_country_code,
            NULL as vehicle_registration,
            NULL as trailer_country_code,
            NULL as trailer_registration,
            NULL as transshipment_info,
            NULL as health_certificate,
            NULL as reporter_login,
            NULL as report_timestamp,
            NULL as processed_timestamp,
            NULL as source_chunk_timestamp,
            NULL as source_period_start,
            NULL as source_period_end,
            NULL as is_deleted,
            NULL as is_invalid,
            NULL as missing_animal_count
        FROM chr_dyr_movements chr
        JOIN chr_properties sender_prop ON chr.reporting_herd_number = sender_prop.chr_number
        JOIN chr_properties receiver_prop ON chr.counterparty_herd = receiver_prop.chr_number
        JOIN unified_cattle_traces ct ON chr.movement_date = ct.movement_date
        WHERE 
            -- Only process CHR movements that are flagged as international
            chr.is_international = true
            -- Match international movements (either direction)
            AND (((ct.sender_country = 'Danmark' OR ct.sender_country = 'DK') 
                  AND (ct.receiver_country != 'Danmark' AND ct.receiver_country != 'DK'))
                 OR
                 ((ct.sender_country != 'Danmark' AND ct.sender_country != 'DK')
                  AND (ct.receiver_country = 'Danmark' OR ct.receiver_country = 'DK')))
            -- Address matching: CHR sender OR receiver should match traces loading location
            AND (
                -- CHR SENDER matches traces loading (export scenario)
                (
                    -- Match by postal code area
                    LEFT(sender_prop.postal_code, 2) = LEFT(REGEXP_EXTRACT(ct.sender_address, '[0-9]{4}'), 2)
                    OR
                    -- Match by city/municipality name
                    LOWER(sender_prop.municipality_name) = LOWER(ct.sender_city)
                    OR
                    -- Match by address string similarity
                    (sender_prop.address IS NOT NULL AND ct.sender_address IS NOT NULL 
                     AND (LOWER(sender_prop.address) LIKE '%' || LOWER(SPLIT_PART(ct.sender_address, ' ', 1)) || '%'
                          OR LOWER(ct.sender_address) LIKE '%' || LOWER(SPLIT_PART(sender_prop.address, ' ', 1)) || '%'))
                )
                OR
                -- CHR RECEIVER matches traces loading (import scenario)
                (
                    -- Match by postal code area
                    LEFT(receiver_prop.postal_code, 2) = LEFT(REGEXP_EXTRACT(ct.sender_address, '[0-9]{4}'), 2)
                    OR
                    -- Match by city/municipality name
                    LOWER(receiver_prop.municipality_name) = LOWER(ct.sender_city)
                    OR
                    -- Match by address string similarity
                    (receiver_prop.address IS NOT NULL AND ct.sender_address IS NOT NULL 
                     AND (LOWER(receiver_prop.address) LIKE '%' || LOWER(SPLIT_PART(ct.sender_address, ' ', 1)) || '%'
                          OR LOWER(ct.sender_address) LIKE '%' || LOWER(SPLIT_PART(receiver_prop.address, ' ', 1)) || '%'))
                )
            )
        -- Limit to one match per CHR movement to avoid duplicates
        QUALIFY ROW_NUMBER() OVER (PARTITION BY chr.movement_summary_id ORDER BY ct.certificate) = 1
    ''')
    
    # Report matching results with improved metrics
    cattle_matches = conn.execute("SELECT COUNT(*) FROM certificate_matched_movements WHERE source_dataset = 'chr_cattle'").fetchone()[0]
    intl_chr_cattle = conn.execute('SELECT COUNT(*) FROM chr_dyr_movements WHERE is_international = true').fetchone()[0]
    total_chr_cattle = conn.execute('SELECT COUNT(*) FROM chr_dyr_movements').fetchone()[0]
    total_cattle_traces = conn.execute('SELECT COUNT(*) FROM unified_cattle_traces').fetchone()[0]
    
    logger.info(f"   ✅ Matched {cattle_matches:,} cattle movements using traces files")
    logger.info(f"   📊 International CHR cattle movements: {intl_chr_cattle:,}")
    logger.info(f"   📊 Total CHR cattle movements: {total_chr_cattle:,}")
    logger.info(f"   📊 Cattle traces available: {total_cattle_traces:,}")
    if intl_chr_cattle > 0:
        logger.info(f"   📈 International CHR → Traces matching rate: {(cattle_matches/intl_chr_cattle*100):.1f}%")


def create_destination_classifications(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create comprehensive destination and origin type classifications based on CHR business types.
    
    Uses improved classification logic that properly categorizes:
    - Rendering plants (Forarbejdningsanlæg) 
    - Collection centers and logistics facilities
    - Specialized farm types (breeding, piglet, etc.)
    - Research and quarantine facilities
    """
    logger.info("🏭 Creating destination type classifications...")
    
    conn.execute('''
        CREATE OR REPLACE TABLE unified_transportation_dataset AS
        SELECT 
            cm.*,
            
            -- Sender information from CHR herds
            sender_h.usage_type_name as sender_usage_type,
            sender_h.business_type_name as sender_business_type,
            sender_h.species_name as sender_species,
            
            -- Receiver information from CHR herds
            receiver_h.usage_type_name as receiver_usage_type,
            receiver_h.business_type_name as receiver_business_type,
            receiver_h.species_name as receiver_species,
            
            -- Sender address information
            sender_p.address as sender_address,
            sender_p.postal_code as sender_postal_code,
            sender_p.municipality_name as sender_municipality,
            
            -- Receiver address information  
            receiver_p.address as receiver_address,
            receiver_p.postal_code as receiver_postal_code,
            receiver_p.municipality_name as receiver_municipality,
            
            -- COMPREHENSIVE Destination type classification
            CASE 
                -- Slaughterhouses
                WHEN receiver_h.business_type_name LIKE '%slagteri%' THEN 'Slaughterhouse'
                
                -- Rendering plants (carcass disposal)
                WHEN receiver_h.business_type_name = 'Forarbejdningsanlæg' THEN 'Rendering Plant'
                
                -- Collection and logistics infrastructure
                WHEN receiver_h.business_type_name LIKE '%sammenbringning%' THEN 'Collection Center'
                WHEN receiver_h.business_type_name LIKE '%Afhentningsplads%' THEN 'Collection Point'
                WHEN receiver_h.business_type_name LIKE '%Køleanlæg%' THEN 'Cooling Facility'
                
                -- Production farms by type
                WHEN receiver_h.business_type_name LIKE '%produktionsbesætning%' THEN 'Production Farm'
                WHEN receiver_h.business_type_name LIKE '%avls- og opformering%' THEN 'Breeding Farm'
                WHEN receiver_h.business_type_name LIKE '%Smågriseopdræt%' THEN 'Piglet Farm'
                WHEN receiver_h.business_type_name LIKE '%Frilandssvin%' THEN 'Free-range Pig Farm'
                WHEN receiver_h.business_type_name LIKE '%Økologisk svin%' THEN 'Organic Pig Farm'
                
                -- Cattle farms by type
                WHEN receiver_h.business_type_name LIKE '%Malkekvæg%' THEN 'Dairy Farm'
                WHEN receiver_h.business_type_name LIKE '%Kødkvæg%' THEN 'Beef Farm'
                WHEN receiver_h.business_type_name LIKE '%Kviehotel%' THEN 'Heifer Hotel'
                WHEN receiver_h.business_type_name LIKE '%Slagtekalv%' THEN 'Veal Farm'
                
                -- Specialized facilities
                WHEN receiver_h.business_type_name LIKE '%karantæne%' THEN 'Quarantine Facility'
                WHEN receiver_h.business_type_name LIKE '%forsøg%' THEN 'Research Facility'
                WHEN receiver_h.business_type_name LIKE '%sædopsamling%' THEN 'AI Station'
                
                -- Trading and markets
                WHEN receiver_h.business_type_name LIKE '%handel%' OR receiver_h.business_type_name LIKE '%marked%' THEN 'Market/Trading'
                
                -- Hobby and recreational
                WHEN receiver_h.business_type_name LIKE '%Hobby%' THEN 'Hobby Farm'
                WHEN receiver_h.business_type_name LIKE '%pension%' OR receiver_h.business_type_name LIKE '%rideskole%' THEN 'Boarding/Riding School'
                WHEN receiver_h.business_type_name LIKE '%Stutteri%' THEN 'Stud Farm'
                WHEN receiver_h.business_type_name LIKE '%væddeløb%' OR receiver_h.business_type_name LIKE '%træning%' THEN 'Racing/Training'
                
                -- Other livestock operations
                WHEN receiver_h.business_type_name LIKE '%Øvrige%' THEN 'Other Livestock'
                WHEN receiver_h.business_type_name LIKE '%besætning%' THEN 'Livestock Farm'
                
                -- Environmental and seasonal
                WHEN receiver_h.business_type_name LIKE '%Sæsonafgræsning%' THEN 'Seasonal Grazing'
                WHEN receiver_h.business_type_name LIKE '%Naturpleje%' THEN 'Nature Management'
                
                -- Events and exhibitions
                WHEN receiver_h.business_type_name LIKE '%Dyrskue%' THEN 'Livestock Show'
                WHEN receiver_h.business_type_name LIKE '%Zoologisk%' THEN 'Zoo'
                
                -- International movements
                WHEN cm.movement_type = 'international_export' THEN 'International Export'
                
                -- Fallback categories
                WHEN receiver_h.business_type_name IS NOT NULL THEN 'Other Commercial'
                ELSE 'Unknown'
            END as destination_type,
            
            -- COMPREHENSIVE Origin type classification (same logic)
            CASE 
                WHEN sender_h.business_type_name LIKE '%slagteri%' THEN 'Slaughterhouse'
                WHEN sender_h.business_type_name = 'Forarbejdningsanlæg' THEN 'Rendering Plant'
                WHEN sender_h.business_type_name LIKE '%sammenbringning%' THEN 'Collection Center'
                WHEN sender_h.business_type_name LIKE '%Afhentningsplads%' THEN 'Collection Point'
                WHEN sender_h.business_type_name LIKE '%Køleanlæg%' THEN 'Cooling Facility'
                WHEN sender_h.business_type_name LIKE '%produktionsbesætning%' THEN 'Production Farm'
                WHEN sender_h.business_type_name LIKE '%avls- og opformering%' THEN 'Breeding Farm'
                WHEN sender_h.business_type_name LIKE '%Smågriseopdræt%' THEN 'Piglet Farm'
                WHEN sender_h.business_type_name LIKE '%Frilandssvin%' THEN 'Free-range Pig Farm'
                WHEN sender_h.business_type_name LIKE '%Økologisk svin%' THEN 'Organic Pig Farm'
                WHEN sender_h.business_type_name LIKE '%Malkekvæg%' THEN 'Dairy Farm'
                WHEN sender_h.business_type_name LIKE '%Kødkvæg%' THEN 'Beef Farm'
                WHEN sender_h.business_type_name LIKE '%Kviehotel%' THEN 'Heifer Hotel'
                WHEN sender_h.business_type_name LIKE '%Slagtekalv%' THEN 'Veal Farm'
                WHEN sender_h.business_type_name LIKE '%karantæne%' THEN 'Quarantine Facility'
                WHEN sender_h.business_type_name LIKE '%forsøg%' THEN 'Research Facility'
                WHEN sender_h.business_type_name LIKE '%sædopsamling%' THEN 'AI Station'
                WHEN sender_h.business_type_name LIKE '%handel%' OR sender_h.business_type_name LIKE '%marked%' THEN 'Market/Trading'
                WHEN sender_h.business_type_name LIKE '%Hobby%' THEN 'Hobby Farm'
                WHEN sender_h.business_type_name LIKE '%pension%' OR sender_h.business_type_name LIKE '%rideskole%' THEN 'Boarding/Riding School'
                WHEN sender_h.business_type_name LIKE '%Stutteri%' THEN 'Stud Farm'
                WHEN sender_h.business_type_name LIKE '%væddeløb%' OR sender_h.business_type_name LIKE '%træning%' THEN 'Racing/Training'
                WHEN sender_h.business_type_name LIKE '%Øvrige%' THEN 'Other Livestock'
                WHEN sender_h.business_type_name LIKE '%besætning%' THEN 'Livestock Farm'
                WHEN sender_h.business_type_name LIKE '%Sæsonafgræsning%' THEN 'Seasonal Grazing'
                WHEN sender_h.business_type_name LIKE '%Naturpleje%' THEN 'Nature Management'
                WHEN sender_h.business_type_name LIKE '%Dyrskue%' THEN 'Livestock Show'
                WHEN sender_h.business_type_name LIKE '%Zoologisk%' THEN 'Zoo'
                WHEN sender_h.business_type_name IS NOT NULL THEN 'Other Commercial'
                ELSE 'Unknown'
            END as origin_type,
            
            -- Processing metadata
            CURRENT_TIMESTAMP as processed_at,
            '1.0' as schema_version
            
        FROM certificate_matched_movements cm
        LEFT JOIN chr_herds sender_h ON cm.sender_chr_number = sender_h.chr_number
        LEFT JOIN chr_herds receiver_h ON cm.receiver_chr_number = receiver_h.chr_number
        LEFT JOIN chr_properties sender_p ON cm.sender_chr_number = sender_p.chr_number
        LEFT JOIN chr_properties receiver_p ON cm.receiver_chr_number = receiver_p.chr_number
    ''')
    
    total_count = conn.execute('SELECT COUNT(*) FROM unified_transportation_dataset').fetchone()[0]
    logger.info(f"  ✅ Created unified dataset: {total_count:,} movements")


def generate_summary_statistics(conn: duckdb.DuckDBPyConnection) -> None:
    """Generate comprehensive summary statistics for the unified dataset."""
    logger.info("\n📊 UNIFIED TRANSPORTATION DATASET SUMMARY")
    logger.info("=" * 60)
    
    # Overall statistics
    total_movements = conn.execute('SELECT COUNT(*) FROM unified_transportation_dataset').fetchone()[0]
    total_animals = conn.execute('SELECT SUM(total_animals) FROM unified_transportation_dataset WHERE total_animals IS NOT NULL').fetchone()[0]
    date_range = conn.execute('SELECT MIN(movement_date), MAX(movement_date) FROM unified_transportation_dataset').fetchone()
    
    logger.info("📈 Overall Statistics:")
    logger.info(f"  Total movements: {total_movements:,}")
    logger.info(f"  Total animals: {total_animals:,}")
    logger.info(f"  Date range: {date_range[0]} to {date_range[1]}")
    
    # Movement type breakdown
    logger.info("\n🔄 Movement Types:")
    movement_types = conn.execute('''
        SELECT movement_type, COUNT(*) as count, SUM(total_animals) as animals
        FROM unified_transportation_dataset 
        GROUP BY movement_type 
        ORDER BY count DESC
    ''').fetchall()
    
    for movement_type, count, animals in movement_types:
        animals_str = f"{animals:,}" if animals else "N/A"
        logger.info(f"  {movement_type}: {count:,} movements, {animals_str} animals")
    
    # Species breakdown
    logger.info("\n🐷🐄 Species Distribution:")
    species_codes = conn.execute('''
        SELECT 
            species_code,
            CASE 
                WHEN species_code = 12 THEN 'Cattle'
                WHEN species_code = 15 THEN 'Pigs' 
                ELSE 'Other'
            END as species_name,
            COUNT(*) as count, 
            SUM(total_animals) as animals
        FROM unified_transportation_dataset 
        GROUP BY species_code 
        ORDER BY count DESC
    ''').fetchall()
    
    for species_code, species_name, count, animals in species_codes:
        animals_str = f"{animals:,}" if animals else "N/A"
        logger.info(f"  {species_name} (code {species_code}): {count:,} movements, {animals_str} animals")
    
    # Destination type distribution
    logger.info("\n🏭 Top Destination Types:")
    dest_types = conn.execute('''
        SELECT destination_type, COUNT(*) as count, SUM(total_animals) as animals
        FROM unified_transportation_dataset 
        GROUP BY destination_type 
        ORDER BY count DESC 
        LIMIT 10
    ''').fetchall()
    
    for dest_type, count, animals in dest_types:
        animals_str = f"{animals:,}" if animals else "N/A"
        logger.info(f"  {dest_type}: {count:,} movements, {animals_str} animals")


def create_transportation_analysis(gcs_access) -> bool:
    """
    Create comprehensive transportation analysis combining all sources.
    
    Args:
        gcs_access: GCS access instance with shared DuckDB connection
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info("🏗️ Creating comprehensive transportation analysis...")
        
        # Setup database with spatial extension
        setup_database(gcs_access.duckdb_conn)
        
        # Load all data sources from GCS
        loaded_tables = load_transportation_data_sources(gcs_access)
        
        # Log loaded tables
        successful_loads = [table for table, success in loaded_tables.items() if success]
        failed_loads = [table for table, success in loaded_tables.items() if not success]
        
        logger.info(f"📥 Successfully loaded: {len(successful_loads)} datasets")
        logger.info(f"⚠️  Failed to load: {len(failed_loads)} datasets")
        
        if failed_loads:
            logger.warning(f"Failed datasets: {failed_loads}")
        
        # Execute pipeline steps
        create_comprehensive_certificate_matching(gcs_access.duckdb_conn)
        perform_cattle_traces_matching(gcs_access.duckdb_conn)
        create_destination_classifications(gcs_access.duckdb_conn)
        generate_summary_statistics(gcs_access.duckdb_conn)
        
        logger.info("✅ Transportation analysis created successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create transportation analysis: {e}")
        return False


def process_transportation_analysis(export_timestamp: str, 
                                  gold_dir: Optional[Path] = None,
                                  gcs_access = None) -> bool:
    """
    Main function to process transportation analysis for gold layer.
    
    Args:
        export_timestamp: Export timestamp for file naming
        gold_dir: Output directory for gold data (optional)
        gcs_access: GCS access instance (optional)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info("🚀 Starting transportation analysis processing...")
        
        # Setup directories - use local data directory for testing
        if gold_dir is None:
            from pathlib import Path
            import os
            if os.getenv("GITHUB_ACTIONS"):
                base_dir = Path(os.getenv("LOCAL_DATA_PATH", "/tmp"))
            else:
                # Use workspace data directory in a safe location
                base_dir = Path.cwd() / "data"
            gold_dir = base_dir / "gold" / "chr" / export_timestamp
        gold_dir.mkdir(parents=True, exist_ok=True)
            
        # Initialize DuckDB connection with spatial extension first (unified pipeline pattern)
        conn = duckdb.connect()
        setup_database(conn)
        
        # Initialize GCS access with shared connection (unified pipeline pattern)
        if gcs_access is None and GCS_AVAILABLE:
            gcs_access = GCSDataAccess(connection=conn)
        
        if gcs_access is None:
            logger.error("❌ GCS access is required for data loading")
            return False
        
        # Create transportation analysis using dynamic data loading
        success = create_transportation_analysis(gcs_access)
        
        if success:
            # Export tables using GCS pattern (tables are in gcs_access.duckdb_conn)
            if gcs_access and migrate_save_data_pattern:
                bucket = "landbrugsdata-raw-data"
                # Use subdataset parameter to create separate filenames
                migrate_save_data_pattern(gcs_access, "unified_transportation_dataset", "chr", bucket, "gold", export_timestamp, "transportation_analysis")
                
                logger.info("✅ Transportation analysis processing completed successfully")
            else:
                # Fallback to local export
                logger.warning("⚠️ GCS not available, exporting locally only")
            
        else:
            logger.error("❌ Transportation analysis processing failed")
            
        # Connection will be closed when gcs_access is destroyed
        return success
        
    except Exception as e:
        logger.error(f"❌ Error processing transportation analysis: {e}")
        return False


if __name__ == "__main__":
    import sys
    from datetime import datetime
    
    # Simple test with current timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    success = process_transportation_analysis(timestamp)
    sys.exit(0 if success else 1)