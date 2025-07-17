import json
import logging
import os
import shutil
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import ibis
from dotenv import load_dotenv

# Import schema documentation
# Import config
# Import table creation functions
from . import (
    animal_movements,
    antibiotic_usage,
    config,
    herds,
    properties,
    property_vet_events,
    vet_practices,
)

# Import export functions
from .export import upload_silver_data_to_gcs

# Import helpers
from .helpers import (
    _create_and_save_lookup,
    get_latest_bronze_dir,
    run_xml_parser,
)

# Try to import CVR collection utilities
try:
    from unified_pipeline.util.cvr_collection import extract_cvr_numbers_from_table, save_pipeline_cvr_numbers
    from unified_pipeline.util.gcs_access import GCSDataAccess

    CVR_COLLECTION_AVAILABLE = True
    logging.info("✅ CVR collection utilities imported successfully")
except ImportError as e:
    logging.warning(f"⚠️ CVR collection utilities not available: {e}")
    CVR_COLLECTION_AVAILABLE = False
    extract_cvr_numbers_from_table = None
    save_pipeline_cvr_numbers = None
    GCSDataAccess = None


def download_bronze_data_from_gcs(bronze_dir_override: str, local_bronze_dir: Path) -> bool:
    """
    Download bronze data from GCS to local filesystem for silver processing.

    Args:
        bronze_dir_override: The timestamp folder name (e.g., "20250713_125139")
        local_bronze_dir: Local directory to download files to

    Returns:
        True if download successful, False otherwise
    """
    if not CVR_COLLECTION_AVAILABLE or GCSDataAccess is None:
        logging.error("GCS utilities not available - cannot download bronze data")
        return False

    bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
    if not bucket_name:
        logging.error("GCS_BUCKET not set - cannot download bronze data")
        return False

    try:
        gcs_access = GCSDataAccess()

        # Create local directory
        local_bronze_dir.mkdir(parents=True, exist_ok=True)

        # List of expected bronze files
        expected_files = [
            "besaetning_list.json",
            "besaetning_details.json",
            "diko_flytninger.json",
            "chr_dyr_movement_summaries*.json",  # Added: CHR cattle movement data (streaming files)
            "ejendom_oplysninger.json",
            "ejendom_vet_events.json",
            "spf_su_herds.json",
            "stamdata_species_usage.json",
            "vetstat_antibiotics.xml",  # Optional
            "vetstat_antibiotics.json",  # Added: VetStat JSON data (also optional)
        ]

        downloaded_count = 0
        for filename in expected_files:
            try:
                gcs_path = f"gs://{bucket_name}/bronze/chr/{bronze_dir_override}/{filename}"
                local_path = local_bronze_dir / filename

                # Check if file exists in GCS
                if gcs_access.file_exists(gcs_path):
                    # Download file
                    with gcs_access.fs.open(gcs_path, "rb") as src:
                        with open(local_path, "wb") as dst:
                            import shutil

                            shutil.copyfileobj(src, dst)

                    logging.info(f"✅ Downloaded {filename} from GCS")
                    downloaded_count += 1
                else:
                    # Handle optional files
                    optional_files = [
                        "vetstat_antibiotics.xml",
                        "vetstat_antibiotics.json",
                        "chr_dyr_movement_summaries.json",
                    ]
                    if filename in optional_files:
                        logging.info(f"⚠️ Optional file {filename} not found in GCS - skipping")
                    else:
                        logging.warning(f"❌ Required file {filename} not found in GCS")

            except Exception as e:
                logging.error(f"❌ Failed to download {filename}: {e}")

        # Minimum required files: besaetning_list, besaetning_details, diko_flytninger,
        # ejendom_oplysninger, ejendom_vet_events, spf_su_herds, stamdata_species_usage (7 files)
        # Optional files: chr_dyr_movement_summaries, vetstat_antibiotics.xml, vetstat_antibiotics.json
        if downloaded_count >= 7:  # At least the required files (excluding optional files)
            logging.info(f"✅ Successfully downloaded {downloaded_count} files from GCS")
            return True
        else:
            logging.error(f"❌ Only downloaded {downloaded_count} files - insufficient for processing")
            return False

    except Exception as e:
        logging.error(f"❌ Failed to download bronze data from GCS: {e}")
        return False


# Try to import schema documentation (after sys.path setup)
SchemaDocumentationManager = None

# Configure logging
log_file_path = Path(__file__).resolve().parent / "silver_processing.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=log_file_path,
    filemode="w",
)

# Add the backend directory to sys.path for imports
from pathlib import Path

# Load environment variables
load_dotenv()

# Find the project root (directory containing 'backend' folder)
current_file = Path(__file__).resolve()
project_root = None

# Go up the directory tree to find the project root
for parent in current_file.parents:
    if (parent / "backend").is_dir():
        project_root = parent
        break

if project_root and str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Try to import schema documentation (after sys.path setup)
try:
    from backend.common.schema_documentation import SchemaDocumentationManager
except ImportError as e:
    logging.warning(f"Schema documentation not available: {e}")
    SchemaDocumentationManager = None

logging.info("--- Script execution started ---")


def _save_discovered_cvr_numbers(con: ibis.BaseBackend, raw_con, silver_dir: Path, export_timestamp: str) -> None:
    """
    Extract and save CVR numbers discovered in the CHR pipeline silver data.

    Args:
        con: DuckDB connection
        silver_dir: Silver data directory
        export_timestamp: Pipeline timestamp for identification
    """
    try:
        logging.info("📊 Starting CVR collection for CHR pipeline")

        # Define CVR tables and their CVR columns
        cvr_tables = {
            "property_owners": "owner_cvr",
            "property_users": "user_cvr",
            "herd_owners": "owner_cvr",
            "herd_users": "user_cvr",
            "antibiotic_usage": "cvr_number",
        }

        all_cvr_numbers = []

        # Extract CVR numbers from each table
        for table_name, cvr_column in cvr_tables.items():
            try:
                # Check if table exists
                tables_result = raw_con.execute("SHOW TABLES").fetchall()
                existing_tables = [table[0] for table in tables_result]

                if table_name in existing_tables:
                    cvr_numbers = extract_cvr_numbers_from_table(
                        table_name=table_name, connection=raw_con, cvr_column=cvr_column
                    )

                    if cvr_numbers:
                        all_cvr_numbers.extend(cvr_numbers)
                        logging.info(f"   • {table_name}: {len(cvr_numbers)} CVR numbers")
                    else:
                        logging.info(f"   • {table_name}: No CVR numbers found")
                else:
                    logging.warning(f"   • {table_name}: Table not found, skipping")

            except Exception as e:
                logging.warning(f"   • {table_name}: Error extracting CVR numbers - {e}")

        # Remove duplicates and sort
        unique_cvr_numbers = sorted(list(set(all_cvr_numbers)))

        if unique_cvr_numbers:
            # Initialize GCS access and save CVR numbers
            gcs_access = GCSDataAccess()

            gcs_path = save_pipeline_cvr_numbers(
                pipeline_name="chr_pipeline",
                cvr_numbers=unique_cvr_numbers,
                gcs_access=gcs_access,
                bucket="landbrugsdata-raw-data",
                timestamp=export_timestamp,
            )

            logging.info(f"✅ Saved {len(unique_cvr_numbers)} unique CVR numbers to {gcs_path}")
        else:
            logging.warning("⚠️ No CVR numbers found in CHR pipeline data")

    except Exception as e:
        logging.error(f"❌ Failed to save CVR numbers for CHR pipeline: {e}")


def process_chr_data_streaming(
    silver_dir: Path,
    bronze_timestamp: str,
    export_timestamp: Optional[str] = None,
) -> bool:
    """
    Memory-efficient CHR silver processing using streaming GCS access.

    This function processes CHR bronze data directly from GCS without loading
    everything into memory, significantly reducing memory usage and eliminating
    the risk of OOM errors on large datasets.

    Args:
        silver_dir: Path to the silver data output directory
        bronze_timestamp: The timestamp string for the bronze data (YYYYMMDD_HHMMSS)
        export_timestamp: The timestamp string for silver export (defaults to bronze_timestamp)

    Returns:
        bool: True if processing completed successfully, False otherwise
    """
    logging.info("--- Starting CHR Silver Processing (Streaming Mode) ---")

    # Create silver directory if it doesn't exist
    silver_dir.mkdir(parents=True, exist_ok=True)

    # Use bronze timestamp as export timestamp if not provided
    if export_timestamp is None:
        export_timestamp = bronze_timestamp
        logging.info(f"Using bronze timestamp as export timestamp: {export_timestamp}")

    # Get GCS bucket from environment
    bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
    if not bucket_name:
        logging.error("GCS_BUCKET environment variable not set")
        return False

    # Check if GCS utilities are available
    if not CVR_COLLECTION_AVAILABLE or GCSDataAccess is None:
        logging.error("GCS utilities not available - cannot process data")
        return False

    try:
        # Initialize GCS access with DuckDB connection
        gcs_access = GCSDataAccess()
        raw_con = gcs_access.duckdb_conn

        # Create Ibis connection wrapper for compatibility with existing silver code
        import ibis
        import ibis.backends.duckdb

        con = ibis.backends.duckdb.Backend.from_connection(raw_con)

        logging.info("✅ Initialized GCS access with Ibis-wrapped DuckDB connection")

        # Define datasets to process in order
        datasets_config = {
            "bes_list": {
                "file": "besaetning_list.json",
                "table_name": "bes_list",
                "required": True,
                "description": "Herd list data",
            },
            "bes_details": {
                "file": "besaetning_details.json",
                "table_name": "bes_details",
                "required": True,
                "description": "Herd details data",
            },
            "diko_flyt": {
                "file": "diko_flytninger.json",
                "table_name": "diko_flyt",
                "required": True,
                "description": "DIKO animal movements",
            },
            "cattle_movements": {
                "file": "chr_dyr_movement_summaries.parquet",
                "table_name": "cattle_movements",
                "required": False,
                "description": "CHR cattle movements (consolidated parquet)",
            },
            "ejendom_oplys": {
                "file": "ejendom_oplysninger.json",
                "table_name": "ejendom_oplys",
                "required": True,
                "description": "Property information",
            },
            "ejendom_vet": {
                "file": "ejendom_vet_events.json",
                "table_name": "ejendom_vet",
                "required": True,
                "description": "Property veterinary events",
            },
            "spf_su_herds": {
                "file": "spf_su_herds.json",
                "table_name": "spf_su_herds",
                "required": False,
                "description": "SPF-SU herd data",
            },
        }

        # Track successfully loaded tables
        loaded_tables = {}

        # Process each dataset individually
        for dataset_key, dataset_info in datasets_config.items():
            table_name = dataset_info["table_name"]
            logging.info(f"Processing {dataset_info['description']}: {dataset_info['file']}")

            # Handle pattern matching for streaming files (cattle_movements)
            if "*" in dataset_info["file"]:
                # This is a pattern for streaming files
                pattern = dataset_info["file"]
                gcs_pattern = f"gs://{bucket_name}/bronze/chr/{bronze_timestamp}/{pattern}"

                logging.info(f"🔍 Looking for files matching pattern: {gcs_pattern}")

                try:
                    # Use the same pattern as other successful pipelines
                    # Use gcs_access.list_files() with full GCS pattern including wildcards
                    gcs_list_pattern = f"gs://{bucket_name}/bronze/chr/{bronze_timestamp}/*"
                    all_files = gcs_access.list_files(gcs_list_pattern)

                    # Filter files matching the pattern (simple prefix matching for chr_dyr_movement_summaries_*)
                    pattern_prefix = pattern.replace("*", "").replace(".json", "")
                    matching_files = [f for f in all_files if pattern_prefix in f and f.endswith(".json")]

                    if not matching_files:
                        if dataset_info["required"]:
                            logging.error(f"❌ No files found matching required pattern: {pattern}")
                            return False
                        else:
                            logging.info(f"⚠️ No files found matching optional pattern: {pattern} - skipping")
                            continue

                    logging.info(f"📁 Found {len(matching_files)} files matching pattern: {pattern}")

                    # Process files in batches to avoid memory issues (600 files is too many at once)
                    batch_size = 50  # Process 50 files at a time
                    con.raw_sql(
                        f"CREATE TABLE {table_name} AS SELECT * FROM (VALUES (NULL)) t(dummy) WHERE FALSE"
                    )  # Create empty table

                    total_records = 0
                    for i in range(0, len(matching_files), batch_size):
                        batch_files = matching_files[i : i + batch_size]
                        batch_file_list = "', '".join(batch_files)

                        logging.info(
                            f"📦 Processing batch {i // batch_size + 1}/{(len(matching_files) + batch_size - 1) // batch_size} ({len(batch_files)} files)"
                        )

                        # Load batch into temporary table
                        con.raw_sql(f"""
                            CREATE OR REPLACE TABLE temp_batch AS 
                            SELECT * FROM read_json_auto(['{batch_file_list}'], maximum_object_size=1073741824)
                        """)

                        # Insert batch data into main table
                        con.raw_sql(f"INSERT INTO {table_name} SELECT * FROM temp_batch")

                        # Count records in this batch
                        batch_count = con.raw_sql("SELECT COUNT(*) FROM temp_batch").fetchone()[0]
                        total_records += batch_count

                        # Cleanup temp table
                        con.raw_sql("DROP TABLE temp_batch")

                    logging.info(
                        f"✅ Loaded {table_name}: {total_records:,} records from {len(matching_files)} files (processed in batches)"
                    )
                    loaded_tables[dataset_key] = table_name

                except Exception as e:
                    if dataset_info["required"]:
                        logging.error(f"❌ Failed to load required dataset {dataset_key}: {e}")
                        return False
                    else:
                        logging.warning(f"⚠️ Failed to load optional dataset {dataset_key}: {e}")
                        continue
            else:
                # Single file processing (original logic)
                gcs_path = f"gs://{bucket_name}/bronze/chr/{bronze_timestamp}/{dataset_info['file']}"

                # Check if file exists in GCS
                if not gcs_access.file_exists(gcs_path):
                    if dataset_info["required"]:
                        logging.error(f"❌ Required file not found: {gcs_path}")
                        return False
                    else:
                        logging.info(f"⚠️ Optional file not found, skipping: {gcs_path}")
                        continue

                try:
                    # Get file size for logging
                    file_size_bytes = gcs_access.get_file_size(gcs_path)
                    file_size_mb = file_size_bytes / (1024 * 1024)
                    logging.info(f"📁 File size: {file_size_mb:.1f} MB")

                    # Create table directly from GCS JSON file using streaming download
                    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as temp_file:
                        temp_path = Path(temp_file.name)

                        # Download JSON file to temp location
                        with gcs_access.fs.open(gcs_path.replace("gs://", ""), "rb") as src:
                            with open(temp_path, "wb") as dst:
                                shutil.copyfileobj(src, dst)

                        # Load into DuckDB using read_json_auto for robust JSON parsing
                        con.raw_sql(f"""
                            CREATE TABLE {table_name} AS 
                            SELECT * FROM read_json_auto('{str(temp_path)}', 
                                                       maximum_object_size=1073741824)
                        """)

                        # Cleanup temp file
                        temp_path.unlink()

                    # Get record count for verification
                    count_result = con.raw_sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    record_count = count_result[0] if count_result else 0

                    logging.info(f"✅ Loaded {table_name}: {record_count:,} records")
                    loaded_tables[dataset_key] = table_name

                    # Force garbage collection after large datasets
                    if file_size_mb > 50:  # For files > 50MB
                        import gc

                        gc.collect()
                        logging.debug(f"Forced garbage collection after loading {file_size_mb:.1f}MB file")

                except Exception as e:
                    if dataset_info["required"]:
                        logging.error(f"❌ Failed to load required dataset {dataset_key}: {e}")
                        return False
                    else:
                        logging.warning(f"⚠️ Failed to load optional dataset {dataset_key}: {e}")
                        continue

        # Handle VetStat XML data separately (if exists)
        vetstat_table_name = None
        vetstat_gcs_path = f"gs://{bucket_name}/bronze/chr/{bronze_timestamp}/vetstat_antibiotics.xml"

        if gcs_access.file_exists(vetstat_gcs_path):
            logging.info("Processing VetStat XML data...")
            try:
                # Download VetStat XML to temporary file for processing
                with tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False) as temp_xml:
                    temp_xml_path = Path(temp_xml.name)

                    # Download XML content
                    with gcs_access.fs.open(vetstat_gcs_path.replace("gs://", ""), "r") as src:
                        temp_xml.write(src.read())

                # Process XML to JSONL
                temp_jsonl_path = silver_dir / "_intermediate_vetstat.jsonl"

                if run_xml_parser(temp_xml_path, temp_jsonl_path):
                    # Load processed JSONL into DuckDB
                    con.raw_sql(f"""
                        CREATE TABLE vetstat AS 
                        SELECT * FROM read_json_auto('{str(temp_jsonl_path)}', 
                                                   maximum_object_size=1073741824)
                    """)

                    # Verify loading
                    count_result = con.raw_sql("SELECT COUNT(*) FROM vetstat").fetchone()
                    record_count = count_result[0] if count_result else 0
                    logging.info(f"✅ Loaded vetstat: {record_count:,} records")

                    vetstat_table_name = "vetstat"
                    loaded_tables["vetstat"] = vetstat_table_name

                    # Cleanup temporary JSONL
                    if temp_jsonl_path.exists():
                        temp_jsonl_path.unlink()
                else:
                    logging.warning("⚠️ VetStat XML processing failed, proceeding without antibiotic data")

                # Cleanup temporary XML
                if temp_xml_path.exists():
                    temp_xml_path.unlink()

            except Exception as e:
                logging.warning(f"⚠️ Failed to process VetStat data: {e}")
        else:
            logging.info("⚠️ VetStat XML file not found, proceeding without antibiotic data")

        # Check that essential tables were loaded
        essential_tables = ["bes_details", "ejendom_oplys"]
        missing_essential = [table for table in essential_tables if table not in loaded_tables]

        if missing_essential:
            logging.error(f"❌ Missing essential tables: {missing_essential}")
            return False

        logging.info(f"✅ Successfully loaded {len(loaded_tables)} datasets")

        # Create lookup tables
        logging.info("Creating lookup tables...")
        lookup_tables = {}

        try:
            if vetstat_table_name:
                lookup_tables["age_groups"] = _create_and_save_lookup(
                    con,
                    con.table(vetstat_table_name),
                    pk_col="Aldersgruppekode",
                    name_col="Aldersgruppe",
                    output_path=silver_dir / "age_groups.parquet",
                    table_name="age_groups",
                )
                logging.info("✅ Created age_groups lookup table")
            else:
                logging.info("⚠️ Skipping age_groups lookup (no VetStat data)")
        except Exception as e:
            logging.warning(f"⚠️ Failed to create age_groups lookup: {e}")

        # Build context for silver processing steps
        context = {
            "bes_details_table": con.table(loaded_tables["bes_details"]) if "bes_details" in loaded_tables else None,
            "diko_flyt_table": con.table(loaded_tables["diko_flyt"]) if "diko_flyt" in loaded_tables else None,
            "cattle_movements_table": con.table(loaded_tables["cattle_movements"])
            if "cattle_movements" in loaded_tables
            else None,
            "ejendom_oplys_table": con.table(loaded_tables["ejendom_oplys"])
            if "ejendom_oplys" in loaded_tables
            else None,
            "ejendom_vet_table": con.table(loaded_tables["ejendom_vet"]) if "ejendom_vet" in loaded_tables else None,
            "vetstat_table": con.table(loaded_tables["vetstat"]) if "vetstat" in loaded_tables else None,
            "spf_su_table": con.table(loaded_tables["spf_su_herds"]) if "spf_su_herds" in loaded_tables else None,
            "lookup_tables": lookup_tables,
        }

        # Process silver steps in order
        silver_steps = [
            "silver_vet_practices",
            "silver_properties",
            "silver_property_owners",
            "silver_property_users",
            "silver_herds",
            "silver_herd_owners",
            "silver_herd_users",
            "silver_herd_sizes",
            "silver_animal_movements",
            "silver_property_vet_events",
            "silver_antibiotic_usage",
            "silver_spf_su_herds",
            "silver_spf_su_health_controls",
            "silver_spf_su_salmonella_data",
        ]

        logging.info("Starting silver processing steps...")

        for step in silver_steps:
            logging.info(f"Processing silver step: {step}")
            try:
                if step == "silver_vet_practices":
                    vet_practices_table = vet_practices.create_vet_practices_table(
                        con, context.get("bes_details_table"), silver_dir
                    )
                    context["vet_practices_table"] = vet_practices_table

                elif step == "silver_properties":
                    properties_table = properties.create_properties_table(
                        con, context.get("ejendom_oplys_table"), silver_dir
                    )
                    context["properties_table"] = properties_table

                elif step == "silver_property_owners":
                    property_owners_table = properties.create_property_owners_table(
                        con, context.get("ejendom_oplys_table"), silver_dir
                    )
                    # Register table in DuckDB for CVR collection
                    if property_owners_table is not None:
                        try:
                            parquet_path = silver_dir / "property_owners.parquet"
                            if parquet_path.exists():
                                con.raw_sql(
                                    f"CREATE OR REPLACE TABLE property_owners AS SELECT * FROM read_parquet('{parquet_path}')"
                                )
                        except Exception as e:
                            logging.warning(f"Failed to register property_owners table for CVR collection: {e}")

                elif step == "silver_property_users":
                    property_users_table = properties.create_property_users_table(
                        con, context.get("ejendom_oplys_table"), silver_dir
                    )
                    # Register table in DuckDB for CVR collection
                    if property_users_table is not None:
                        try:
                            parquet_path = silver_dir / "property_users.parquet"
                            if parquet_path.exists():
                                con.raw_sql(
                                    f"CREATE OR REPLACE TABLE property_users AS SELECT * FROM read_parquet('{parquet_path}')"
                                )
                        except Exception as e:
                            logging.warning(f"Failed to register property_users table for CVR collection: {e}")

                elif step == "silver_herds":
                    herds_table = herds.create_herds_table(
                        con,
                        context.get("bes_details_table"),
                        silver_dir,
                    )
                    context["herds_table"] = herds_table

                elif step == "silver_herd_owners":
                    herd_owners_table = herds.create_herd_owners_table(
                        con, context.get("bes_details_table"), silver_dir
                    )
                    # Register table in DuckDB for CVR collection
                    if herd_owners_table is not None:
                        con.create_table("herd_owners", herd_owners_table, overwrite=True)

                elif step == "silver_herd_users":
                    herd_users_table = herds.create_herd_users_table(con, context.get("bes_details_table"), silver_dir)
                    # Register table in DuckDB for CVR collection
                    if herd_users_table is not None:
                        con.create_table("herd_users", herd_users_table, overwrite=True)

                elif step == "silver_herd_sizes":
                    herd_sizes_table = herds.create_herd_sizes_table(con, context.get("bes_details_table"), silver_dir)

                elif step == "silver_animal_movements":
                    # Process DIKO movements (always available)
                    if context.get("diko_flyt_table") is not None:
                        animal_movements_table = animal_movements.create_animal_movements_table(
                            con, context.get("diko_flyt_table"), silver_dir
                        )

                    # Process CHR_dyr cattle movements (optional - aggregated summaries format)
                    if context.get("cattle_movements_table") is not None:
                        chr_dyr_movements_table = animal_movements.create_chr_dyr_movement_summaries_table(
                            con, context.get("cattle_movements_table"), silver_dir
                        )
                    else:
                        logging.info("CHR_dyr cattle movements not available - skipping")

                elif step == "silver_property_vet_events":
                    if context.get("ejendom_vet_table") is not None:
                        property_vet_events_table = property_vet_events.create_property_vet_events_table(
                            con,
                            context.get("ejendom_vet_table"),
                            context.get("lookup_tables", {}),
                            silver_dir,
                        )

                elif step == "silver_antibiotic_usage":
                    if context.get("vetstat_table") is not None:
                        antibiotic_usage_table = antibiotic_usage.create_antibiotic_usage_table(
                            con,
                            context.get("vetstat_table"),
                            context.get("lookup_tables", {}),
                            silver_dir,
                        )
                    else:
                        logging.info("VetStat data not available - skipping antibiotic usage processing")

                elif step == "silver_spf_su_herds":
                    if context.get("spf_su_table") is not None:
                        # Import SPF-SU processing functions
                        try:
                            from . import spf_su

                            spf_su_herds_table = spf_su.create_spf_su_herds_table(
                                con, context.get("spf_su_table"), silver_dir
                            )
                        except ImportError:
                            logging.warning("SPF-SU processing module not available - skipping")
                    else:
                        logging.info("SPF-SU data not available - skipping")

                elif step == "silver_spf_su_health_controls":
                    if context.get("spf_su_table") is not None:
                        try:
                            from . import spf_su

                            spf_su_controls_table = spf_su.create_spf_su_health_controls_table(
                                con, context.get("spf_su_table"), silver_dir
                            )
                        except ImportError:
                            logging.warning("SPF-SU processing module not available - skipping")
                    else:
                        logging.info("SPF-SU data not available - skipping health controls")

                elif step == "silver_spf_su_salmonella_data":
                    if context.get("spf_su_table") is not None:
                        try:
                            from . import spf_su

                            spf_su_salmonella_table = spf_su.create_spf_su_salmonella_data_table(
                                con, context.get("spf_su_table"), silver_dir
                            )
                        except ImportError:
                            logging.warning("SPF-SU processing module not available - skipping")
                    else:
                        logging.info("SPF-SU data not available - skipping salmonella data")

                logging.info(f"✅ Completed silver step: {step}")

            except Exception as e:
                logging.error(f"❌ Failed silver step {step}: {e}", exc_info=True)
                # Continue with other steps rather than failing completely
                continue

        # Save discovered CVR numbers for pipeline integration
        try:
            _save_discovered_cvr_numbers(con, raw_con, silver_dir, export_timestamp)
        except Exception as e:
            logging.warning(f"⚠️ Failed to save CVR numbers: {e}")

        # Upload silver data to GCS
        try:
            upload_success = upload_silver_data_to_gcs(silver_dir, export_timestamp)
            if upload_success:
                logging.info("✅ Silver data uploaded to GCS successfully")
            else:
                logging.warning("⚠️ Silver data upload to GCS failed or was skipped")
        except Exception as e:
            logging.error(f"❌ Error during silver data upload to GCS: {e}")

        # Cleanup DuckDB tables to free memory
        try:
            for table_name in loaded_tables.values():
                con.raw_sql(f"DROP TABLE IF EXISTS {table_name}")

            # Drop lookup tables
            for lookup_name in lookup_tables.keys():
                con.raw_sql(f"DROP TABLE IF EXISTS {lookup_name}")

            logging.info("✅ Cleaned up DuckDB tables")
        except Exception as e:
            logging.warning(f"⚠️ Error during table cleanup: {e}")

        # Final garbage collection
        import gc

        gc.collect()

        logging.info("✅ CHR Silver Processing (Streaming Mode) completed successfully")
        return True

    except Exception as e:
        logging.error(f"❌ CHR Silver Processing (Streaming Mode) failed: {e}", exc_info=True)
        return False


# --- Main Processing Logic ---
def process_chr_data(
    silver_dir: Path = None,
    in_memory_data: Optional[Dict[str, Dict[str, List[Any]]]] = None,
    export_timestamp: Optional[str] = None,
    bronze_timestamp: Optional[str] = None,
    force_streaming: bool = False,
):
    """Main function to process CHR data from bronze to silver.

    This function now supports both streaming (memory-efficient) and legacy (in-memory) modes.
    Streaming mode is recommended for large datasets to avoid memory issues.

    Args:
        silver_dir: Path to the silver data output directory.
        in_memory_data: Dictionary containing buffered bronze data (legacy mode).
        export_timestamp: The timestamp string used for the silver export (YYYYMMDD_HHMMSS).
        bronze_timestamp: The timestamp string for bronze data (required for streaming mode).
        force_streaming: Force use of streaming mode even if in_memory_data is available.
    """
    logging.info("--- Starting CHR Silver Processing --- ")

    # Create silver directory if it doesn't exist
    silver_dir.mkdir(parents=True, exist_ok=True)

    # Ensure we have an export timestamp for GCS upload
    if export_timestamp is None:
        export_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logging.info(f"Generated export timestamp: {export_timestamp}")

    # Determine processing mode: streaming vs legacy
    use_streaming = force_streaming or (
        bronze_timestamp is not None and CVR_COLLECTION_AVAILABLE and GCSDataAccess is not None
    )

    if use_streaming and bronze_timestamp:
        logging.info("🚀 Using STREAMING mode for memory-efficient processing")
        return process_chr_data_streaming(
            silver_dir=silver_dir, bronze_timestamp=bronze_timestamp, export_timestamp=export_timestamp
        )

    # Legacy mode - original in-memory processing
    logging.info("📝 Using LEGACY mode with in-memory data")
    logging.warning("⚠️ Legacy mode may cause memory issues with large datasets. Consider using streaming mode.")

    # Determine data source mode - support both memory and file loading
    load_from_memory = in_memory_data is not None and bool(in_memory_data)

    # Try to determine bronze directory if not using memory
    bronze_dir = None
    if not load_from_memory:
        # Try to get bronze directory from environment or config
        bronze_dir_override = os.getenv("BRONZE_DATE_FOLDER_OVERRIDE")
        if bronze_dir_override:
            from . import config

            bronze_dir = config.BRONZE_BASE_DIR / bronze_dir_override

        if bronze_dir and bronze_dir.exists() and any(bronze_dir.glob("*.json")):
            logging.info(f"Silver processing source mode: bronze files from {bronze_dir}")
        else:
            logging.error("Cannot process silver data: no in_memory_data provided and no bronze files found.")
            sys.exit(1)
    else:
        logging.info("Silver processing source mode: in-memory buffer")

    # --- Define Input File Paths or Data Sources ---
    if in_memory_data:
        # Extract data from memory
        besaetning_list_data = in_memory_data.get("besaetning_list", {}).get("json", [])
        besaetning_details_data = in_memory_data.get("besaetning_details", {}).get("json", [])
        diko_flytninger_data = in_memory_data.get("diko_flytninger", {}).get("json", [])
        ejendom_oplysninger_data = in_memory_data.get("ejendom_oplysninger", {}).get("json", [])
        ejendom_vet_events_data = in_memory_data.get("ejendom_vet_events", {}).get("json", [])
        vetstat_antibiotics_data = in_memory_data.get("vetstat_antibiotics", {}).get("xml", [])

        # Write VetStat XML to temp file if needed
        vetstat_antibiotics_xml_path = None
        if vetstat_antibiotics_data:
            # Ensure the temporary XML file is cleaned up
            temp_xml_path_obj = silver_dir / "_temp_vetstat.xml"
            saved_xml_path_obj = silver_dir / f"_DEBUG_FAILED_vetstat_{export_timestamp or 'unknown'}.xml"
            try:
                with open(temp_xml_path_obj, "w") as f:
                    # Add separator compatible with VetStat XML parser's expectations
                    f.write("\n<!-- RAW_RESPONSE_SEPARATOR -->\n".join(vetstat_antibiotics_data))
                vetstat_antibiotics_xml_path = temp_xml_path_obj  # Assign path only if successfully written
                try:
                    xml_size = vetstat_antibiotics_xml_path.stat().st_size
                    logging.info(
                        f"Created temporary VetStat XML file: {vetstat_antibiotics_xml_path} (Size: {xml_size} bytes)"
                    )
                except Exception as e_stat:
                    logging.warning(f"Could not get size of temp XML file {vetstat_antibiotics_xml_path}: {e_stat}")
                    logging.info(f"Created temporary VetStat XML file: {vetstat_antibiotics_xml_path}")
            except Exception as e_write:
                logging.error(
                    f"Failed to write temporary VetStat XML file: {e_write}",
                    exc_info=True,
                )
                # Ensure path is None if write failed
                vetstat_antibiotics_xml_path = None
                # Clean up potentially partially written file
                if temp_xml_path_obj.exists():
                    try:
                        temp_xml_path_obj.unlink()
                    except OSError:
                        pass

    else:
        # Use file paths as before
        besaetning_list_path = bronze_dir / "besaetning_list.json"
        besaetning_details_path = bronze_dir / "besaetning_details.json"
        diko_flytninger_path = bronze_dir / "diko_flytninger.json"
        ejendom_oplysninger_path = bronze_dir / "ejendom_oplysninger.json"
        ejendom_vet_events_path = bronze_dir / "ejendom_vet_events.json"
        vetstat_antibiotics_xml_path = bronze_dir / "vetstat_antibiotics.xml"

    # Define intermediate path for parsed XML (placed in silver dir for easier cleanup)
    vetstat_antibiotics_jsonl_path = silver_dir / "_intermediate_vetstat.jsonl"

    # --- 1. Pre-process VetStat XML to JSONL ---
    vetstat_loaded = False
    temp_xml_created_in_silver = load_from_memory and vetstat_antibiotics_xml_path is not None
    try:
        if vetstat_antibiotics_xml_path and vetstat_antibiotics_xml_path.exists():
            try:
                if run_xml_parser(vetstat_antibiotics_xml_path, vetstat_antibiotics_jsonl_path):
                    if vetstat_antibiotics_jsonl_path.exists() and vetstat_antibiotics_jsonl_path.stat().st_size > 0:
                        vetstat_loaded = True
                        logging.info(
                            f"Successfully created intermediate VetStat JSONL: {vetstat_antibiotics_jsonl_path}"
                        )
                    else:
                        logging.warning(
                            f"XML parser succeeded but output file is empty or missing: {vetstat_antibiotics_jsonl_path}"
                        )
                else:
                    logging.warning("⚠️ VetStat XML processing failed, proceeding without antibiotic data")
                    if vetstat_antibiotics_xml_path.exists():
                        try:
                            vetstat_antibiotics_xml_path.rename(saved_xml_path_obj)
                            logging.warning(f"Saved problematic XML to {saved_xml_path_obj} for inspection.")
                            vetstat_antibiotics_xml_path = None
                        except Exception as e_save:
                            logging.error(f"Failed to save problematic XML {vetstat_antibiotics_xml_path}: {e_save}")
            except (FileNotFoundError, RuntimeError, Exception) as e:
                logging.error(
                    f"Failed to process VetStat XML: {e}. Proceeding without antibiotic data.",
                    exc_info=True,
                )
                if vetstat_antibiotics_xml_path and vetstat_antibiotics_xml_path.exists():
                    try:
                        vetstat_antibiotics_xml_path.rename(saved_xml_path_obj)
                        logging.warning(f"Saved problematic XML to {saved_xml_path_obj} for inspection.")
                        vetstat_antibiotics_xml_path = None
                    except Exception as e_save:
                        logging.error(f"Failed to save problematic XML {vetstat_antibiotics_xml_path}: {e_save}")
                # Ensure path is None if failed
                if vetstat_antibiotics_jsonl_path.exists():
                    try:
                        vetstat_antibiotics_jsonl_path.unlink()  # Clean up failed attempt
                    except OSError:
                        pass
        else:
            logging.warning(
                f"VetStat XML file not found or not provided ({'in-memory path was' if load_from_memory else 'bronze path was'} {vetstat_antibiotics_xml_path}). Skipping antibiotic data processing."
            )
            vetstat_antibiotics_jsonl_path = None
    finally:
        # Clean up the temporary XML file created from in-memory data
        if temp_xml_created_in_silver and vetstat_antibiotics_xml_path and vetstat_antibiotics_xml_path.exists():
            try:
                vetstat_antibiotics_xml_path.unlink()
                logging.info(f"Cleaned up temporary VetStat XML file: {vetstat_antibiotics_xml_path}")
            except OSError as e_del:
                logging.warning(f"Could not delete temporary VetStat XML file {vetstat_antibiotics_xml_path}: {e_del}")

    # --- 2. Initialize Ibis and DuckDB Connection ---
    logging.info("Initializing Ibis with DuckDB backend (in-memory)")
    try:
        con = ibis.duckdb.connect()  # In-memory by default
        # Install necessary DuckDB extensions if not already present
        con.con.sql("INSTALL httpfs;")
        con.con.sql("LOAD httpfs;")
        con.con.sql("INSTALL spatial;")
        con.con.sql("LOAD spatial;")
        con.con.sql("INSTALL json;")
        con.con.sql("LOAD json;")
        logging.info("DuckDB extensions httpfs, spatial, json loaded.")
    except Exception as e:
        logging.error(f"Failed to initialize DuckDB or load extensions: {e}", exc_info=True)
        sys.exit(1)

    # --- 3. Load Bronze Data into Ibis Tables ---
    logging.info("Loading bronze data into Ibis tables...")

    # Only in-memory data is supported

    raw_tables = {}

    # Define sources and their corresponding keys/paths
    sources_to_load = {
        "bes_list": {"mem_key": "besaetning_list", "file_key": "besaetning_list.json"},
        "bes_details": {
            "mem_key": "besaetning_details",
            "file_key": "besaetning_details.json",
        },
        "diko_flyt": {"mem_key": "diko_flytninger", "file_key": "diko_flytninger.json"},
        "cattle_movements": {"mem_key": "chr_dyr_movement_summaries", "file_key": "chr_dyr_movement_summaries.parquet"},
        "ejendom_oplys": {
            "mem_key": "ejendom_oplysninger",
            "file_key": "ejendom_oplysninger.json",
        },
        "ejendom_vet": {
            "mem_key": "ejendom_vet_events",
            "file_key": "ejendom_vet_events.json",
        },
        "spf_su_herds": {"mem_key": "spf_su_herds", "file_key": "spf_su_herds.json"},
        # Vetstat is handled separately due to XML -> JSONL preprocessing
    }

    for table_name, source_info in sources_to_load.items():
        logging.info(f"Loading table: {table_name}")

        input_source = None
        source_desc = "unknown"
        json_data_str = None  # To hold data for logging if needed

        # Define a helper to handle date serialization for JSON
        def date_serializer(obj):
            if isinstance(obj, date):  # Correctly handle date objects
                return obj.isoformat()
            raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

        # Reset load status for each table
        successfully_loaded = False

        # --- Attempt 1: Load from Memory using DuckDB Python API --- #
        if load_from_memory:
            logging.info(f"Attempting to load '{table_name}' from in-memory buffer...")
            data = in_memory_data.get(source_info["mem_key"], {}).get("json", [])
            if data and isinstance(data, list):
                logging.info(f"Found {len(data)} records in memory for {source_info['mem_key']}")
                # Convert list of dicts to Pandas  for robust handling - REMOVED THIS STEP
                # Instead, write to temp JSONL and use read_json
                temp_jsonl_path = None
                temp_file = None
                try:
                    # Create a temporary file to write JSONL data
                    # Use silver_dir to ensure it's within accessible/writable space if run in restricted envs
                    temp_file = tempfile.NamedTemporaryFile(
                        mode="w",
                        encoding="utf-8",
                        suffix=".jsonl",
                        delete=False,  # Keep the file until manually deleted
                        dir=silver_dir,  # Place temp file in silver dir
                        prefix=f"temp_{table_name}_",
                    )
                    temp_jsonl_path = Path(temp_file.name)
                    logging.info(
                        f"Writing in-memory data for '{table_name}' to temporary JSONL: {temp_jsonl_path.name}"
                    )

                    for record in data:
                        # Ensure complex objects are handled by json.dumps
                        try:
                            # Revised JSONL writing:
                            json_string = json.dumps(record, default=str)  # Serialize to string first
                            temp_file.write(json_string + "\n")  # Write string + newline
                        except TypeError as e_json:
                            logging.warning(
                                f"Skipping record due to JSON serialization error for table '{table_name}': {e_json}. Record sample: {str(record)[:200]}..."
                            )
                            continue  # Skip bad records

                    temp_file.flush()  # Ensure all data is written
                    temp_file.close()  # Close the file handle

                    logging.info(f"Finished writing temporary JSONL for '{table_name}'. Attempting read_json_auto...")

                    # Use DuckDB SQL directly to read JSONL and create the table
                    # read_json_auto handles schema inference and newline delimited format
                    # Pass maximum_object_size directly as a parameter to the function
                    # Set to 1GB (1073741824 bytes)
                    max_obj_size_bytes = 1073741824
                    con.con.sql(
                        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto('{str(temp_jsonl_path)}', maximum_object_size={max_obj_size_bytes});"
                    )
                    raw_tables[table_name] = con.table(table_name)  # Get Ibis table reference

                    successfully_loaded = True
                    source_desc = (
                        f"in-memory buffer via temp JSONL ({temp_jsonl_path.name}) for '{source_info['mem_key']}'"
                    )
                    logging.info(f"Successfully loaded {source_desc} into table '{table_name}' using read_json_auto.")

                except Exception as e_mem_jsonl:
                    logging.error(
                        f"Failed to load '{table_name}' from memory via temp JSONL: {e_mem_jsonl}",
                        exc_info=True,
                    )
                    # Clean up potentially created table on failure
                    try:
                        con.con.sql(f"DROP TABLE IF EXISTS {table_name};")
                    except Exception:
                        pass
                finally:
                    # Clean up the temporary JSONL file
                    if temp_jsonl_path and temp_jsonl_path.exists():
                        try:
                            temp_jsonl_path.unlink()
                            logging.info(f"Removed temporary JSONL file: {temp_jsonl_path.name}")
                        except OSError as e_del:
                            logging.warning(f"Could not delete temporary JSONL file {temp_jsonl_path.name}: {e_del}")
                    if temp_file and not temp_file.closed:
                        temp_file.close()  # Ensure closed if error occurred before explicit close

                    # CRITICAL: Force garbage collection after processing large datasets
                    if data and len(data) > 1000:
                        import gc

                        gc.collect()
                        logging.debug(
                            f"Forced garbage collection after processing {len(data)} records for {table_name}"
                        )

                    # CRITICAL: Clear the data reference to free memory immediately
                    if "data" in locals():
                        del data
            else:
                logging.warning(f"No data (or not a list) found in memory buffer for {source_info['mem_key']}.")

        # --- Attempt 2: Load from Files (when not using memory) --- #
        if not successfully_loaded and not load_from_memory and bronze_dir:
            logging.info(f"Attempting to load '{table_name}' from bronze files...")
            file_key = source_info["file_key"]

            # Handle pattern matching for streaming files
            if "*" in file_key:
                # This is a pattern for streaming files
                import glob

                pattern_path = bronze_dir / file_key
                matching_files = glob.glob(str(pattern_path))

                if matching_files:
                    try:
                        logging.info(
                            f"Loading {table_name} from {len(matching_files)} files matching pattern: {file_key}"
                        )
                        # Use DuckDB's read_json_auto with array of files
                        file_list_str = "', '".join(matching_files)
                        max_obj_size_bytes = 1073741824  # 1GB
                        con.con.sql(
                            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto(['{file_list_str}'], maximum_object_size={max_obj_size_bytes});"
                        )
                        raw_tables[table_name] = con.table(table_name)
                        successfully_loaded = True
                        logging.info(f"Successfully loaded {table_name} from {len(matching_files)} files")

                    except Exception as e_file:
                        logging.error(f"Failed to load '{table_name}' from pattern {file_key}: {e_file}")
                else:
                    logging.warning(f"No files found matching pattern for '{table_name}': {pattern_path}")
            else:
                # Single file processing (original logic)
                file_path = bronze_dir / file_key

                if file_path.exists():
                    try:
                        logging.info(f"Loading {table_name} from file: {file_path}")
                        # Detect file type and use appropriate reader
                        if file_path.suffix.lower() == ".parquet":
                            # Use read_parquet for parquet files
                            con.con.sql(
                                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{str(file_path)}');"
                            )
                        else:
                            # Use read_json_auto for JSON files
                            max_obj_size_bytes = 1073741824  # 1GB
                            con.con.sql(
                                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto('{str(file_path)}', maximum_object_size={max_obj_size_bytes});"
                            )
                        raw_tables[table_name] = con.table(table_name)
                        successfully_loaded = True
                        logging.info(f"Successfully loaded {table_name} from file {file_path}")

                    except Exception as e_file:
                        logging.error(f"Failed to load '{table_name}' from file {file_path}: {e_file}")
                else:
                    logging.warning(f"File not found for '{table_name}': {file_path}")

        if not successfully_loaded:
            # Check if this is an optional table that can be skipped
            optional_tables = ["cattle_movements", "spf_su_herds"]
            if table_name in optional_tables:
                logging.warning(
                    f"Optional table '{table_name}' not found - skipping (this is normal if the corresponding bronze step wasn't run)"
                )
            else:
                logging.error(f"Failed to load table '{table_name}' from all available sources.")
        else:
            logging.info(f"Successfully loaded table '{table_name}'")
            # Register the table in DuckDB so SQL queries can reference it by name
            try:
                # Use Ibis create_table to register the table in DuckDB
                con.create_table(table_name, raw_tables[table_name], overwrite=True)
                logging.info(f"Registered table '{table_name}' in DuckDB catalog")
            except Exception as e:
                logging.error(f"Failed to register table '{table_name}' in DuckDB: {e}")

    # Handle VetStat separately (reading from the pre-processed JSONL file in silver)
    # Construct path within the silver directory
    vetstat_antibiotics_jsonl_path = silver_dir / "_intermediate_vetstat.jsonl"
    if vetstat_antibiotics_jsonl_path.exists():  # Check if it exists in silver
        logging.info(f"Loading pre-processed VetStat data from {vetstat_antibiotics_jsonl_path.name}...")
        try:
            raw_tables["vetstat"] = con.read_json(str(vetstat_antibiotics_jsonl_path), format="newline_delimited")
            logging.info("Successfully loaded vetstat data.")
            # Register the vetstat table in DuckDB catalog
            try:
                con.create_table("vetstat", raw_tables["vetstat"], overwrite=True)
                logging.info("Registered vetstat table in DuckDB catalog")
            except Exception as e:
                logging.error(f"Failed to register vetstat table in DuckDB: {e}")
        except Exception as e:
            logging.error(f"Error loading vetstat JSONL data: {e}")
    else:
        logging.warning(
            f"Skipping VetStat table loading as pre-processed file {vetstat_antibiotics_jsonl_path} is not available."
        )

    # Check essential tables
    essential_tables = ["bes_details", "ejendom_oplys"]
    missing_essential = [table for table in essential_tables if table not in raw_tables]
    if missing_essential:
        logging.error(f"Missing essential tables: {missing_essential}")
    else:
        logging.info("All essential tables loaded successfully")

    # --- Check if essential tables were loaded ---
    if "bes_details" not in raw_tables:
        logging.error("Essential table 'bes_details' could not be loaded. Aborting processing.")
        sys.exit(1)
    if "ejendom_oplys" not in raw_tables:
        logging.error("Essential table 'ejendom_oplys' could not be loaded. Aborting processing.")
        sys.exit(1)

    # --- 4. Create & Populate Lookup Tables ---
    logging.info("Creating and saving lookup tables...")
    lookup_tables = {}

    # Create age_groups lookup (simpler source)
    try:
        if "vetstat" in raw_tables:
            lookup_tables["age_groups"] = _create_and_save_lookup(
                con,
                raw_tables["vetstat"],
                pk_col="Aldersgruppekode",
                name_col="Aldersgruppe",
                output_path=silver_dir / "age_groups.parquet",
                table_name="age_groups",
            )
        else:
            logging.warning("Could not create age_groups lookup: 'vetstat' table missing.")
    except Exception as e:
        logging.error(f"Failed age_groups lookup creation: {e}")

    # --- 5. Process Silver Steps in Order ---
    context = {
        "bes_details_table": raw_tables.get("bes_details"),
        "diko_flyt_table": raw_tables.get("diko_flyt"),
        "cattle_movements_table": raw_tables.get("cattle_movements"),  # May be None if animal_movements step wasn't run
        "ejendom_oplys_table": raw_tables.get("ejendom_oplys"),
        "ejendom_vet_table": raw_tables.get("ejendom_vet"),
        "vetstat_table": raw_tables.get("vetstat"),
        "spf_su_table": raw_tables.get("spf_su_herds"),
        "lookup_tables": lookup_tables,
    }

    # Define silver steps in order
    silver_steps = [
        "silver_vet_practices",
        "silver_properties",
        "silver_property_owners",
        "silver_property_users",
        "silver_herds",
        "silver_herd_owners",
        "silver_herd_users",
        "silver_herd_sizes",
        "silver_animal_movements",
        "silver_property_vet_events",
        "silver_antibiotic_usage",
        "silver_spf_su_herds",
        "silver_spf_su_health_controls",
        "silver_spf_su_salmonella_data",
    ]

    # Process each silver step
    for step in silver_steps:
        logging.info(f"Processing silver step: {step}")
        try:
            if step == "silver_vet_practices":
                vet_practices_table = vet_practices.create_vet_practices_table(
                    con, context.get("bes_details_table"), silver_dir
                )
                context["vet_practices_table"] = vet_practices_table

            elif step == "silver_properties":
                properties_table = properties.create_properties_table(
                    con, context.get("ejendom_oplys_table"), silver_dir
                )
                context["properties_table"] = properties_table

            elif step == "silver_property_owners":
                property_owners_table = properties.create_property_owners_table(
                    con, context.get("ejendom_oplys_table"), silver_dir
                )
                # Register table in DuckDB for CVR collection (load from saved parquet to avoid reference issues)
                if property_owners_table is not None:
                    try:
                        parquet_path = silver_dir / "property_owners.parquet"
                        if parquet_path.exists():
                            con.con.execute(
                                f"CREATE OR REPLACE TABLE property_owners AS SELECT * FROM read_parquet('{parquet_path}')"
                            )
                    except Exception as e:
                        logging.warning(f"Failed to register property_owners table for CVR collection: {e}")

            elif step == "silver_property_users":
                property_users_table = properties.create_property_users_table(
                    con, context.get("ejendom_oplys_table"), silver_dir
                )
                # Register table in DuckDB for CVR collection (load from saved parquet to avoid reference issues)
                if property_users_table is not None:
                    try:
                        parquet_path = silver_dir / "property_users.parquet"
                        if parquet_path.exists():
                            con.con.execute(
                                f"CREATE OR REPLACE TABLE property_users AS SELECT * FROM read_parquet('{parquet_path}')"
                            )
                    except Exception as e:
                        logging.warning(f"Failed to register property_users table for CVR collection: {e}")

            elif step == "silver_herds":
                herds_table = herds.create_herds_table(
                    con,
                    context.get("bes_details_table"),
                    silver_dir,
                )
                context["herds_table"] = herds_table

            elif step == "silver_herd_owners":
                herd_owners_table = herds.create_herd_owners_table(con, context.get("bes_details_table"), silver_dir)
                # Register table in DuckDB for CVR collection
                if herd_owners_table is not None:
                    con.create_table("herd_owners", herd_owners_table, overwrite=True)

            elif step == "silver_herd_users":
                herd_users_table = herds.create_herd_users_table(con, context.get("bes_details_table"), silver_dir)
                # Register table in DuckDB for CVR collection
                if herd_users_table is not None:
                    con.create_table("herd_users", herd_users_table, overwrite=True)

            elif step == "silver_herd_sizes":
                herd_sizes_table = herds.create_herd_sizes_table(con, context.get("bes_details_table"), silver_dir)

            elif step == "silver_animal_movements":
                # Process DIKO movements (always available)
                animal_movements_table = animal_movements.create_animal_movements_table(
                    con, context.get("diko_flyt_table"), silver_dir
                )

                # Process CHR_dyr cattle movements (optional - aggregated summaries format)
                if context.get("cattle_movements_table") is not None:
                    chr_dyr_movements_table = animal_movements.create_chr_dyr_movement_summaries_table(
                        con, context.get("cattle_movements_table"), silver_dir
                    )
                else:
                    logging.info(
                        "CHR_dyr cattle movements not available - skipping (animal_movements bronze step not run)"
                    )

            elif step == "silver_property_vet_events":
                property_vet_events_table = property_vet_events.create_property_vet_events_table(
                    con,
                    context.get("ejendom_vet_table"),  # Reverted to original context get
                    context.get("lookup_tables", {}),
                    silver_dir,
                )

            elif step == "silver_antibiotic_usage":
                antibiotic_usage_table = antibiotic_usage.create_antibiotic_usage_table(
                    con,
                    context.get("vetstat_table"),
                    context.get("lookup_tables", {}),
                    silver_dir,
                )
                # Register table in DuckDB for CVR collection
                if antibiotic_usage_table is not None:
                    con.create_table("antibiotic_usage", antibiotic_usage_table, overwrite=True)

            elif step == "silver_spf_su_herds":
                if context.get("spf_su_table") is not None:
                    from . import spf_su

                    spf_su_herds_table = spf_su.create_spf_su_herds_table(con, context.get("spf_su_table"), silver_dir)
                else:
                    logging.warning("Cannot create SPF-SU herds table: spf_su_raw is None")

            elif step == "silver_spf_su_health_controls":
                if context.get("spf_su_table") is not None:
                    from . import spf_su

                    spf_su_health_controls_table = spf_su.create_spf_su_health_controls_table(
                        con, context.get("spf_su_table"), silver_dir
                    )
                else:
                    logging.warning("Cannot create SPF-SU health controls table: spf_su_raw is None")

            elif step == "silver_spf_su_salmonella_data":
                if context.get("spf_su_table") is not None:
                    from . import spf_su

                    spf_su_salmonella_data_table = spf_su.create_spf_su_salmonella_data_table(
                        con, context.get("spf_su_table"), silver_dir
                    )
                else:
                    logging.warning("Cannot create SPF-SU salmonella data table: spf_su_raw is None")

        except Exception as e:
            logging.error(f"Error in silver step {step}: {e}", exc_info=True)
            # Continue with next step instead of failing completely
            continue

    # --- 12. CVR Collection (Right after silver processing, before cleanup) ---
    if CVR_COLLECTION_AVAILABLE:
        _save_discovered_cvr_numbers(con, con.con, silver_dir, export_timestamp)
    else:
        logging.warning("CVR collection disabled due to import error")

        # --- 13. Generate Schema Documentation ---
        if SchemaDocumentationManager is not None:
            logging.info("Generating schema documentation for CHR silver tables...")
            try:
                # Get the pipeline start time from the silver_dir timestamp
                dir_name = silver_dir.name
                if len(dir_name) == 15 and dir_name[8] == "_":  # Format: YYYYMMDD_HHMMSS
                    pipeline_start_time = datetime.strptime(dir_name, "%Y%m%d_%H%M%S")
                else:
                    pipeline_start_time = datetime.now()

                # Initialize schema documentation manager
                schema_manager = SchemaDocumentationManager(
                    connection=con.con,  # Use the DuckDB connection
                    pipeline_name="chr_pipeline",
                    pipeline_start_time=pipeline_start_time,
                    logger=logging.getLogger(__name__),
                )

                # Get list of tables that were actually created
                tables_query = "SHOW TABLES"
                tables_result = con.con.execute(tables_query).fetchall()
                silver_tables = [
                    table[0]
                    for table in tables_result
                    if table[0]
                    not in ["bes_details", "diko_flyt", "ejendom_oplys", "ejendom_vet", "vetstat", "cattle_movements"]
                ]

                if silver_tables:
                    # Generate documentation for all silver tables
                    schema_files = schema_manager.generate_all_documentation(silver_tables, stage="silver")
                    logging.info(
                        f"Generated schema documentation for {len(silver_tables)} tables: {', '.join(silver_tables)}"
                    )

                    # Commit to GitHub - but handle the permission error gracefully
                    try:
                        schema_manager.commit_to_github()
                        logging.info("Schema documentation committed to GitHub")
                    except Exception as git_error:
                        logging.warning(f"Failed to commit to GitHub: {git_error}")
                        logging.info("Schema documentation generated locally but not committed to GitHub")
                else:
                    logging.warning("No silver tables found for schema documentation")

            except Exception as e:
                logging.error(f"Failed to generate schema documentation: {e}", exc_info=True)
                # Don't fail the pipeline if schema documentation fails
        else:
            logging.warning("Schema documentation disabled due to import error")

    # --- 14. Upload Silver Data to GCS ---
    try:
        upload_success = upload_silver_data_to_gcs(silver_dir, export_timestamp)
        if upload_success:
            logging.info("✅ Silver data uploaded to GCS successfully")
        else:
            logging.warning("⚠️ Silver data upload to GCS failed or was skipped")
    except Exception as e:
        logging.error(f"❌ Error during silver data upload to GCS: {e}")

    # --- 15. Cleanup Intermediate Files ---
    if vetstat_antibiotics_jsonl_path and vetstat_antibiotics_jsonl_path.exists():
        try:
            vetstat_antibiotics_jsonl_path.unlink()
            logging.info(f"Removed intermediate file: {vetstat_antibiotics_jsonl_path}")
        except OSError as e:
            logging.warning(f"Could not remove intermediate file {vetstat_antibiotics_jsonl_path}: {e}")

    # CRITICAL: Comprehensive cleanup of all temporary files and memory
    try:
        # Clean up any remaining temporary files in the silver directory
        temp_pattern_files = [
            silver_dir.glob("temp_*"),
            silver_dir.glob("_temp_*"),
            silver_dir.glob("*.tmp"),
            silver_dir.glob("*.jsonl"),
        ]

        for pattern in temp_pattern_files:
            for temp_file in pattern:
                try:
                    if temp_file.exists():
                        temp_file.unlink()
                        logging.debug(f"Cleaned up temporary file: {temp_file.name}")
                except Exception as e:
                    logging.warning(f"Could not remove temporary file {temp_file}: {e}")

        # Close DuckDB connection to free resources
        if "con" in locals() and con:
            try:
                con.con.close()
                logging.info("Closed DuckDB connection")
            except Exception as e:
                logging.warning(f"Error closing DuckDB connection: {e}")

        # Clear large variables from memory
        if "raw_tables" in locals():
            del raw_tables
        if "context" in locals():
            del context
        if "lookup_tables" in locals():
            del lookup_tables
        if "in_memory_data" in locals() and in_memory_data:
            in_memory_data.clear()
            del in_memory_data

        # Force final garbage collection
        import gc

        gc.collect()
        logging.info("Completed comprehensive cleanup of temporary files and memory")

    except Exception as e:
        logging.warning(f"Error during comprehensive cleanup: {e}")

    logging.info(f"Silver data processing finished. Output located in: {silver_dir}")


if __name__ == "__main__":
    # --- Determine Input and Output Directories ---
    try:
        logging.info("Determining input bronze directory...")
        # Use config constants
        if config.BRONZE_DATE_FOLDER_OVERRIDE:
            input_bronze_dir = config.BRONZE_BASE_DIR / config.BRONZE_DATE_FOLDER_OVERRIDE
            if not input_bronze_dir.is_dir():
                raise FileNotFoundError(f"Specified bronze directory does not exist: {input_bronze_dir}")
            logging.info(f"Using specified bronze data directory: {input_bronze_dir.name}")
        else:
            # Need to pass the base dir explicitly now
            input_bronze_dir = get_latest_bronze_dir(config.BRONZE_BASE_DIR)  # This call logs info
        logging.info(f"Determined input bronze directory: {input_bronze_dir}")
    except FileNotFoundError as e:
        logging.error(f"Error determining bronze data directory: {e}")
        sys.exit(1)

    # Create timestamped output directory using pipeline start time
    pipeline_start_time = datetime.now()
    processing_timestamp = pipeline_start_time.strftime("%Y%m%d_%H%M%S")
    # Use config constant
    output_silver_dir = config.SILVER_BASE_DIR / processing_timestamp
    logging.info(f"Determined output silver directory: {output_silver_dir}")

    # --- Execute Processing ---
    try:
        logging.info("Starting process_chr_data function...")
        process_chr_data(
            bronze_dir=input_bronze_dir, silver_dir=output_silver_dir
        )  # process_chr_data needs to be defined above
        logging.info("Finished process_chr_data function.")
    except Exception as e:
        logging.critical(f"An unhandled error occurred during data processing: {e}", exc_info=True)
        sys.exit(1)

    logging.info("--- Script execution finished ---")
