import contextlib
import json
import logging
import os
import shutil
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
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
from .export import upload_silver_data_to_storage

# Import helpers
from .helpers import (
    _create_and_save_lookup,
    get_latest_bronze_dir,
    run_xml_parser,
)

# Try to import CVR collection utilities
try:
    from common.storage import StorageAccess
    from unified_pipeline.util.cvr_collection import (
        extract_cvr_numbers_from_table,
        save_pipeline_cvr_numbers,
    )

    CVR_COLLECTION_AVAILABLE = True
    logging.info("CVR collection utilities imported successfully")
except ImportError as e:
    logging.warning(f"CVR collection utilities not available: {e}")
    CVR_COLLECTION_AVAILABLE = False
    extract_cvr_numbers_from_table = None
    save_pipeline_cvr_numbers = None
    StorageAccess = None


def download_bronze_data_from_storage(bronze_dir_override: str, local_bronze_dir: Path) -> bool:
    """
    Download bronze data from GCS to local filesystem for silver processing.

    Args:
        bronze_dir_override: The timestamp folder name (e.g., "20250713_125139")
        local_bronze_dir: Local directory to download files to

    Returns:
        True if download successful, False otherwise
    """
    if not CVR_COLLECTION_AVAILABLE or StorageAccess is None:
        logging.error("GCS utilities not available - cannot download bronze data")
        return False

    bucket_name = (
        os.getenv("STORAGE_BUCKET")
        or os.getenv("R2_BUCKET")
        or os.getenv("GCS_BUCKET", "landbruget-data")
    )
    if not bucket_name:
        logging.error("GCS_BUCKET not set - cannot download bronze data")
        return False

    try:
        storage_access = StorageAccess()

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
                storage_path = f"{bucket_name}/bronze/chr/{bronze_dir_override}/{filename}"
                local_path = local_bronze_dir / filename

                # Check if file exists in storage
                if storage_access.file_exists(storage_path):
                    fs_path = storage_path
                    with (
                        storage_access.fs.open(fs_path, "rb") as src,
                        open(local_path, "wb") as dst,
                    ):
                        shutil.copyfileobj(src, dst)

                    logging.info(f"Downloaded {filename} from GCS")
                    downloaded_count += 1
                else:
                    # Handle optional files
                    optional_files = [
                        "vetstat_antibiotics.xml",
                        "vetstat_antibiotics.json",
                        "chr_dyr_movement_summaries.json",
                    ]
                    if filename in optional_files:
                        logging.info(f"Optional file {filename} not found in GCS - skipping")
                    else:
                        logging.warning(f"Required file {filename} not found in GCS")

            except Exception as e:
                logging.error(f"Failed to download {filename}: {e}")

        # Minimum required files: besaetning_list, besaetning_details, diko_flytninger,
        # ejendom_oplysninger, ejendom_vet_events, spf_su_herds, stamdata_species_usage (7 files)
        # Optional files: chr_dyr_movement_summaries, vetstat_antibiotics.xml, vetstat_antibiotics.json
        if downloaded_count >= 7:  # At least the required files (excluding optional files)
            logging.info(f"Successfully downloaded {downloaded_count} files from GCS")
            return True
        logging.error(f"Only downloaded {downloaded_count} files - insufficient for processing")
        return False

    except Exception as e:
        logging.error(f"Failed to download bronze data from GCS: {e}")
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
# Note: pathlib.Path already imported at top of file

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


def _save_discovered_cvr_numbers(
    con: duckdb.DuckDBPyConnection, silver_dir: Path, export_timestamp: str
) -> None:
    """
    Extract and save CVR numbers discovered in the CHR pipeline silver data.

    Args:
        con: DuckDB connection
        silver_dir: Silver data directory
        export_timestamp: Pipeline timestamp for identification
    """
    try:
        logging.info("Starting CVR collection for CHR pipeline")

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
                tables_result = con.execute("SHOW TABLES").fetchall()
                existing_tables = [table[0] for table in tables_result]

                if table_name in existing_tables:
                    cvr_numbers = extract_cvr_numbers_from_table(
                        table_name=table_name, connection=con, cvr_column=cvr_column
                    )

                    if cvr_numbers:
                        all_cvr_numbers.extend(cvr_numbers)
                        logging.info(f"   - {table_name}: {len(cvr_numbers)} CVR numbers")
                    else:
                        logging.info(f"   - {table_name}: No CVR numbers found")
                else:
                    logging.warning(f"   - {table_name}: Table not found, skipping")

            except Exception as e:
                logging.warning(f"   - {table_name}: Error extracting CVR numbers - {e}")

        # Remove duplicates and sort
        unique_cvr_numbers = sorted(set(all_cvr_numbers))

        if unique_cvr_numbers:
            # Initialize GCS access and save CVR numbers
            storage_access = StorageAccess()

            storage_path = save_pipeline_cvr_numbers(
                pipeline_name="chr_pipeline",
                cvr_numbers=unique_cvr_numbers,
                storage_access=storage_access,
                bucket="landbruget-data",
                timestamp=export_timestamp,
            )

            logging.info(f"Saved {len(unique_cvr_numbers)} unique CVR numbers to {storage_path}")
        else:
            logging.warning("No CVR numbers found in CHR pipeline data")

    except Exception as e:
        logging.error(f"Failed to save CVR numbers for CHR pipeline: {e}")


def process_chr_data_streaming(
    silver_dir: Path,
    bronze_timestamp: str,
    export_timestamp: str | None = None,
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

    # Create silver directory if it doesn't exist and if provided
    if silver_dir is not None:
        silver_dir.mkdir(parents=True, exist_ok=True)

    # Use bronze timestamp as export timestamp if not provided
    if export_timestamp is None:
        export_timestamp = bronze_timestamp
        logging.info(f"Using bronze timestamp as export timestamp: {export_timestamp}")

    # Get GCS bucket from environment
    bucket_name = (
        os.getenv("STORAGE_BUCKET")
        or os.getenv("R2_BUCKET")
        or os.getenv("GCS_BUCKET", "landbruget-data")
    )
    if not bucket_name:
        logging.error("GCS_BUCKET environment variable not set")
        return False

    # Check if GCS utilities are available
    if not CVR_COLLECTION_AVAILABLE or StorageAccess is None:
        logging.error("GCS utilities not available - cannot process data")
        return False

    try:
        # Initialize GCS access with DuckDB connection
        storage_access = StorageAccess()
        con = storage_access.duckdb_conn

        logging.info("Initialized GCS access with DuckDB connection")

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
                "file": "*chr_dyr_movement_summaries.parquet",
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

                # MINIMAL FIX: For CHR movement data, search across month-suffixed directories
                if dataset_key == "cattle_movements":
                    logging.info(
                        "Looking for CHR movement files across month-suffixed bronze directories..."
                    )

                    try:
                        # Use storage_access.fs (s3fs via common layer) for directory listing
                        fs = storage_access.fs

                        # Find all month-suffixed directories for this bronze timestamp
                        all_dirs = fs.ls(f"{bucket_name}/bronze/chr/")
                        bronze_dirs = [d for d in all_dirs if bronze_timestamp in d]

                        logging.info(
                            f"Found {len(bronze_dirs)} bronze directories matching timestamp {bronze_timestamp}"
                        )

                        matching_files = []
                        pattern_prefix = pattern.replace("*", "").replace(".parquet", "")

                        for bronze_dir in bronze_dirs:
                            try:
                                # List files in this directory
                                dir_files = fs.ls(bronze_dir)

                                # Find matching files
                                dir_matches = [
                                    f
                                    for f in dir_files
                                    if pattern_prefix in f and f.endswith(".parquet")
                                ]
                                matching_files.extend(dir_matches)
                                logging.info(
                                    f"Found {len(dir_matches)} CHR movement files in {bronze_dir}"
                                )

                            except Exception as dir_e:
                                logging.warning(f"Could not list files in {bronze_dir}: {dir_e}")
                                continue

                        logging.info(f"Total CHR movement files found: {len(matching_files)}")

                    except Exception as e:
                        logging.error(f"Error discovering month-suffixed bronze directories: {e}")
                        matching_files = []
                else:
                    # Original logic for other datasets
                    gcs_pattern = f"{bucket_name}/bronze/chr/{bronze_timestamp}/{pattern}"
                    logging.info(f"Looking for files matching pattern: {gcs_pattern}")

                    try:
                        gcs_list_pattern = f"{bucket_name}/bronze/chr/{bronze_timestamp}/*"
                        all_files = storage_access.list_files(gcs_list_pattern)

                        # Filter files matching the pattern (handle both .json and .parquet files)
                        if pattern.endswith(".parquet"):
                            pattern_prefix = pattern.replace("*", "").replace(".parquet", "")
                            matching_files = [
                                f
                                for f in all_files
                                if pattern_prefix in f and f.endswith(".parquet")
                            ]
                        else:
                            pattern_prefix = pattern.replace("*", "").replace(".json", "")
                            matching_files = [
                                f for f in all_files if pattern_prefix in f and f.endswith(".json")
                            ]

                    except Exception as e:
                        logging.error(f"Error listing files for pattern {pattern}: {e}")
                        matching_files = []

                if not matching_files:
                    if dataset_info["required"]:
                        logging.error(f"No files found matching required pattern: {pattern}")
                        return False
                    logging.info(f"No files found matching optional pattern: {pattern} - skipping")
                    continue

                logging.info(f"Found {len(matching_files)} files matching pattern: {pattern}")

                try:
                    # Process files in batches to avoid memory issues (600 files is too many at once)
                    batch_size = 50  # Process 50 files at a time

                    # Create main table from first batch to get correct schema
                    table_created = False
                    total_records = 0

                    for i in range(0, len(matching_files), batch_size):
                        batch_files = matching_files[i : i + batch_size]
                        batch_file_list = "', '".join(batch_files)

                        logging.info(
                            f"Processing batch {i // batch_size + 1}/"
                            f"{(len(matching_files) + batch_size - 1) // batch_size} "
                            f"({len(batch_files)} files)"
                        )

                        # Load batch into temporary table - use appropriate reader based on file type
                        if pattern.endswith(".parquet"):
                            con.execute(f"""
                                CREATE OR REPLACE TABLE temp_batch AS
                                SELECT * FROM read_parquet(['{batch_file_list}'])
                            """)
                        else:
                            con.execute(f"""
                                CREATE OR REPLACE TABLE temp_batch AS
                                SELECT * FROM read_json_auto(['{batch_file_list}'], maximum_object_size=1073741824)
                            """)

                        # Create main table with correct schema from first batch
                        if not table_created:
                            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_batch")
                            table_created = True
                        else:
                            # Insert batch data into main table
                            con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_batch")

                        # Count records in this batch
                        batch_count = con.execute("SELECT COUNT(*) FROM temp_batch").fetchone()[0]
                        total_records += batch_count

                        # Cleanup temp table
                        con.execute("DROP TABLE temp_batch")

                    logging.info(
                        f"Loaded {table_name}: {total_records:,} records from "
                        f"{len(matching_files)} files (processed in batches)"
                    )
                    loaded_tables[dataset_key] = table_name

                except Exception as e:
                    if dataset_info["required"]:
                        logging.error(f"Failed to load required dataset {dataset_key}: {e}")
                        return False
                    logging.warning(f"Failed to load optional dataset {dataset_key}: {e}")
                    continue
            else:
                # Single file processing - check both base directory and CHR-group-suffixed directories
                storage_path = f"{bucket_name}/bronze/chr/{bronze_timestamp}/{dataset_info['file']}"

                # Check if file exists in base GCS directory
                file_found = storage_access.file_exists(storage_path)

                # If not found in base directory, search CHR-group-suffixed directories (for ejendom etc.)
                if not file_found:
                    try:
                        # Use storage_access.fs (s3fs via common layer) for directory listing
                        fs = storage_access.fs

                        # Find all suffixed directories for this bronze timestamp
                        all_dirs = fs.ls(f"{bucket_name}/bronze/chr/")
                        suffixed_dirs = [
                            d
                            for d in all_dirs
                            if bronze_timestamp in d
                            and d != f"{bucket_name}/bronze/chr/{bronze_timestamp}"
                        ]

                        if suffixed_dirs:
                            logging.info(
                                f"Checking {len(suffixed_dirs)} CHR-group-suffixed directories for {dataset_info['file']}"
                            )

                            # Check each suffixed directory
                            for suffixed_dir in suffixed_dirs:
                                alt_storage_path = f"{suffixed_dir}/{dataset_info['file']}"
                                if storage_access.file_exists(alt_storage_path):
                                    storage_path = (
                                        alt_storage_path  # Use the file from suffixed directory
                                    )
                                    file_found = True
                                    logging.info(
                                        f"Found {dataset_info['file']} in CHR-group directory: {suffixed_dir}"
                                    )
                                    break
                    except Exception as e:
                        logging.warning(f"Error searching CHR-group directories: {e}")

                if not file_found:
                    if dataset_info["required"]:
                        logging.error(f"Required file not found: {storage_path}")
                        return False
                    logging.info(f"Optional file not found, skipping: {storage_path}")
                    continue

                try:
                    # Get file size for logging
                    file_size_bytes = storage_access.get_file_size(storage_path)
                    file_size_mb = file_size_bytes / (1024 * 1024)
                    logging.info(f"File size: {file_size_mb:.1f} MB")

                    # Detect file type and use appropriate DuckDB function
                    file_extension = dataset_info["file"].split(".")[-1].lower()

                    if file_extension == "parquet":
                        # Download parquet file to temp location (DuckDB can't read directly from GCS)
                        logging.info(f"Loading parquet file via temp download: {storage_path}")
                        with tempfile.NamedTemporaryFile(
                            suffix=".parquet", delete=False
                        ) as temp_file:
                            temp_path = Path(temp_file.name)

                            # Download parquet file to temp location
                            with (
                                storage_access.fs.open(storage_path, "rb") as src,
                                open(temp_path, "wb") as dst,
                            ):
                                shutil.copyfileobj(src, dst)

                            # Load into DuckDB using read_parquet
                            con.execute(f"""
                                CREATE TABLE {table_name} AS
                                SELECT * FROM read_parquet('{temp_path!s}')
                            """)

                            # Cleanup temp file
                            temp_path.unlink()
                    else:
                        # Download JSON/other files to temp location for processing
                        file_suffix = f".{file_extension}" if file_extension else ".json"
                        with tempfile.NamedTemporaryFile(
                            suffix=file_suffix, delete=False
                        ) as temp_file:
                            temp_path = Path(temp_file.name)

                            # Download file to temp location
                            with (
                                storage_access.fs.open(storage_path, "rb") as src,
                                open(temp_path, "wb") as dst,
                            ):
                                shutil.copyfileobj(src, dst)

                            # Load into DuckDB using read_json_auto for robust JSON parsing
                            con.execute(f"""
                                CREATE TABLE {table_name} AS
                                SELECT * FROM read_json_auto('{temp_path!s}',
                                                           maximum_object_size=1073741824)
                            """)

                            # Cleanup temp file
                            temp_path.unlink()

                    # Get record count for verification
                    count_result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    record_count = count_result[0] if count_result else 0

                    logging.info(f"Loaded {table_name}: {record_count:,} records")
                    loaded_tables[dataset_key] = table_name

                    # Force garbage collection after large datasets
                    if file_size_mb > 50:  # For files > 50MB
                        import gc

                        gc.collect()
                        logging.debug(
                            f"Forced garbage collection after loading {file_size_mb:.1f}MB file"
                        )

                except Exception as e:
                    if dataset_info["required"]:
                        logging.error(f"Failed to load required dataset {dataset_key}: {e}")
                        return False
                    logging.warning(f"Failed to load optional dataset {dataset_key}: {e}")
                    continue

        # Handle VetStat JSON data separately (if exists)
        vetstat_table_name = None

        # Look for VetStat JSON files from all CHR groups and months (consolidated from XML in bronze layer)
        vetstat_json_files = []

        # First try the traditional single file approach (for backward compatibility)
        traditional_path = f"{bucket_name}/bronze/chr/{bronze_timestamp}/vetstat_antibiotics.json"
        if storage_access.file_exists(traditional_path):
            vetstat_json_files.append(traditional_path)
            logging.info(f"Found traditional VetStat file: {traditional_path}")
        else:
            # Look for CHR group and month-specific files
            logging.info(
                "Traditional VetStat file not found, looking for CHR group-specific files..."
            )

            try:
                # Use storage_access.fs (s3fs via common layer) for directory listing
                fs = storage_access.fs

                # Find all directories that match the bronze timestamp with suffixes
                all_dirs = fs.ls(f"{bucket_name}/bronze/chr/")
                bronze_dirs = [d for d in all_dirs if bronze_timestamp in d]

                logging.info(
                    f"Found {len(bronze_dirs)} bronze directories for timestamp {bronze_timestamp}"
                )

                # Look for vetstat_antibiotics.json in each directory
                for bronze_dir in bronze_dirs:
                    try:
                        vetstat_file_path = f"{bronze_dir}/vetstat_antibiotics.json"
                        if storage_access.file_exists(vetstat_file_path):
                            vetstat_json_files.append(vetstat_file_path)
                            logging.info(f"Found VetStat file: {vetstat_file_path}")
                    except Exception as e:
                        logging.warning(f"Error checking for VetStat file in {bronze_dir}: {e}")
                        continue

            except Exception as e:
                logging.error(f"Error discovering CHR group VetStat files: {e}")

        logging.info(f"Total VetStat JSON files found: {len(vetstat_json_files)}")

        if vetstat_json_files:
            logging.info(f"Processing {len(vetstat_json_files)} VetStat JSON files")
            try:
                # Create temporary files for all VetStat JSON data
                all_vetstat_data = []

                for json_file in vetstat_json_files:
                    try:
                        # Download and parse each JSON file
                        json_data = storage_access.download_json(json_file)
                        logging.info(f"Downloaded VetStat JSON: {len(json_data)} top-level items")

                        # Handle nested structure: list of lists of dictionaries
                        if isinstance(json_data, list):
                            for item in json_data:
                                if isinstance(item, list):
                                    # Flatten nested lists
                                    all_vetstat_data.extend(item)
                                else:
                                    # Direct dictionary
                                    all_vetstat_data.append(item)
                        else:
                            all_vetstat_data.append(json_data)
                    except Exception as e:
                        logging.warning(f"Failed to process VetStat JSON file {json_file}: {e}")
                        continue

                logging.info(f"Total VetStat records after flattening: {len(all_vetstat_data)}")

                if all_vetstat_data:
                    # Save consolidated JSON to temporary file
                    temp_jsonl_path = silver_dir / "_intermediate_vetstat.jsonl"
                    with open(temp_jsonl_path, "w") as f:
                        for record in all_vetstat_data:
                            f.write(json.dumps(record) + "\n")

                    logging.info(f"Saved {len(all_vetstat_data)} records to temporary JSONL file")

                    # Load consolidated JSONL into DuckDB
                    try:
                        con.execute(f"""
                            CREATE TABLE vetstat AS
                            SELECT * FROM read_json_auto('{temp_jsonl_path!s}',
                                                       maximum_object_size=1073741824)
                        """)
                        logging.info("Successfully created VetStat table in DuckDB")
                    except Exception as e:
                        logging.error(f"Failed to create VetStat table in DuckDB: {e}")
                        raise

                    # Verify loading
                    count_result = con.execute("SELECT COUNT(*) FROM vetstat").fetchone()
                    record_count = count_result[0] if count_result else 0
                    logging.info(
                        f"Loaded vetstat: {record_count:,} records from {len(vetstat_json_files)} files"
                    )

                    if record_count == 0:
                        logging.warning(
                            "VetStat table was created but contains 0 records - this may indicate a parsing issue"
                        )

                    vetstat_table_name = "vetstat"
                    loaded_tables["vetstat"] = vetstat_table_name

                    # Cleanup temporary JSONL
                    if temp_jsonl_path.exists():
                        temp_jsonl_path.unlink()
                else:
                    logging.warning(
                        "No VetStat data found in JSON files, proceeding without antibiotic data"
                    )

            except Exception as e:
                logging.warning(f"Failed to process VetStat JSON data: {e}")
        else:
            logging.info("VetStat JSON files not found, proceeding without antibiotic data")

        # Check that essential tables were loaded
        essential_tables = ["bes_details", "ejendom_oplys"]
        missing_essential = [table for table in essential_tables if table not in loaded_tables]

        if missing_essential:
            logging.error(f"Missing essential tables: {missing_essential}")
            return False

        logging.info(f"Successfully loaded {len(loaded_tables)} datasets")

        # Create lookup tables
        logging.info("Creating lookup tables...")
        lookup_tables = {}

        try:
            if vetstat_table_name:
                lookup_tables["age_groups"] = _create_and_save_lookup(
                    con,
                    vetstat_table_name,
                    pk_col="Aldersgruppekode",
                    name_col="Aldersgruppe",
                    output_path=silver_dir / "age_groups.parquet",
                    table_name="age_groups",
                )
                logging.info("Created age_groups lookup table")
            else:
                logging.info("Skipping age_groups lookup (no VetStat data)")
        except Exception as e:
            logging.warning(f"Failed to create age_groups lookup: {e}")

        # Build context for silver processing steps - use table name strings
        context = {
            "bes_details_table": loaded_tables.get("bes_details"),
            "diko_flyt_table": loaded_tables.get("diko_flyt"),
            "cattle_movements_table": loaded_tables.get("cattle_movements"),
            "ejendom_oplys_table": loaded_tables.get("ejendom_oplys"),
            "ejendom_vet_table": loaded_tables.get("ejendom_vet"),
            "vetstat_table": loaded_tables.get("vetstat"),
            "spf_su_table": loaded_tables.get("spf_su_herds"),
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
                                con.execute(
                                    f"CREATE OR REPLACE TABLE property_owners AS "
                                    f"SELECT * FROM read_parquet('{parquet_path}')"
                                )
                        except Exception as e:
                            logging.warning(
                                f"Failed to register property_owners table for CVR collection: {e}"
                            )

                elif step == "silver_property_users":
                    property_users_table = properties.create_property_users_table(
                        con, context.get("ejendom_oplys_table"), silver_dir
                    )
                    # Register table in DuckDB for CVR collection
                    if property_users_table is not None:
                        try:
                            parquet_path = silver_dir / "property_users.parquet"
                            if parquet_path.exists():
                                con.execute(
                                    f"CREATE OR REPLACE TABLE property_users AS "
                                    f"SELECT * FROM read_parquet('{parquet_path}')"
                                )
                        except Exception as e:
                            logging.warning(
                                f"Failed to register property_users table for CVR collection: {e}"
                            )

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
                        try:
                            parquet_path = silver_dir / "herd_owners.parquet"
                            if parquet_path.exists():
                                con.execute(
                                    f"CREATE OR REPLACE TABLE herd_owners AS "
                                    f"SELECT * FROM read_parquet('{parquet_path}')"
                                )
                        except Exception as e:
                            logging.warning(
                                f"Failed to register herd_owners table for CVR collection: {e}"
                            )

                elif step == "silver_herd_users":
                    herd_users_table = herds.create_herd_users_table(
                        con, context.get("bes_details_table"), silver_dir
                    )
                    # Register table in DuckDB for CVR collection
                    if herd_users_table is not None:
                        try:
                            parquet_path = silver_dir / "herd_users.parquet"
                            if parquet_path.exists():
                                con.execute(
                                    f"CREATE OR REPLACE TABLE herd_users AS "
                                    f"SELECT * FROM read_parquet('{parquet_path}')"
                                )
                        except Exception as e:
                            logging.warning(
                                f"Failed to register herd_users table for CVR collection: {e}"
                            )

                elif step == "silver_herd_sizes":
                    herds.create_herd_sizes_table(con, context.get("bes_details_table"), silver_dir)

                elif step == "silver_animal_movements":
                    # Process DIKO movements (always available)
                    if context.get("diko_flyt_table") is not None:
                        animal_movements.create_animal_movements_table(
                            con, context.get("diko_flyt_table"), silver_dir
                        )

                    # Process CHR_dyr cattle movements (optional - aggregated summaries format)
                    if context.get("cattle_movements_table") is not None:
                        animal_movements.create_chr_dyr_movement_summaries_table(
                            con, context.get("cattle_movements_table"), silver_dir
                        )
                    else:
                        logging.info("CHR_dyr cattle movements not available - skipping")

                elif step == "silver_property_vet_events":
                    if context.get("ejendom_vet_table") is not None:
                        property_vet_events.create_property_vet_events_table(
                            con,
                            context.get("ejendom_vet_table"),
                            context.get("lookup_tables", {}),
                            silver_dir,
                        )

                elif step == "silver_antibiotic_usage":
                    if context.get("vetstat_table") is not None:
                        antibiotic_usage.create_antibiotic_usage_table(
                            con,
                            context.get("vetstat_table"),
                            context.get("lookup_tables", {}),
                            silver_dir,
                        )
                    else:
                        logging.info(
                            "VetStat data not available - skipping antibiotic usage processing"
                        )

                elif step == "silver_spf_su_herds":
                    if context.get("spf_su_table") is not None:
                        # Import SPF-SU processing functions
                        try:
                            from . import spf_su

                            spf_su.create_spf_su_herds_table(
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

                            spf_su.create_spf_su_health_controls_table(
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

                            spf_su.create_spf_su_salmonella_data_table(
                                con, context.get("spf_su_table"), silver_dir
                            )
                        except ImportError:
                            logging.warning("SPF-SU processing module not available - skipping")
                    else:
                        logging.info("SPF-SU data not available - skipping salmonella data")

                logging.info(f"Completed silver step: {step}")

            except Exception as e:
                logging.error(f"Failed silver step {step}: {e}", exc_info=True)
                # Continue with other steps rather than failing completely
                continue

        # Save discovered CVR numbers for pipeline integration
        try:
            _save_discovered_cvr_numbers(con, silver_dir, export_timestamp)
        except Exception as e:
            logging.warning(f"Failed to save CVR numbers: {e}")

        # Upload silver data to GCS
        try:
            upload_success = upload_silver_data_to_storage(silver_dir, export_timestamp)
            if upload_success:
                logging.info("Silver data uploaded to GCS successfully")
            else:
                logging.warning("Silver data upload to GCS failed or was skipped")
        except Exception as e:
            logging.error(f"Error during silver data upload to GCS: {e}")

        # Cleanup DuckDB tables to free memory
        try:
            for table_name in loaded_tables.values():
                con.execute(f"DROP TABLE IF EXISTS {table_name}")

            # Drop lookup tables
            for lookup_name in lookup_tables:
                con.execute(f"DROP TABLE IF EXISTS {lookup_name}")

            logging.info("Cleaned up DuckDB tables")
        except Exception as e:
            logging.warning(f"Error during table cleanup: {e}")

        # Final garbage collection
        import gc

        gc.collect()

        logging.info("CHR Silver Processing (Streaming Mode) completed successfully")
        return True

    except Exception as e:
        logging.error(f"CHR Silver Processing (Streaming Mode) failed: {e}", exc_info=True)
        return False


# --- Main Processing Logic ---
def process_chr_data(
    silver_dir: Path | None = None,
    in_memory_data: dict[str, dict[str, list[Any]]] | None = None,
    export_timestamp: str | None = None,
    bronze_timestamp: str | None = None,
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
        bronze_timestamp is not None and CVR_COLLECTION_AVAILABLE and StorageAccess is not None
    )

    if use_streaming and bronze_timestamp:
        logging.info("Using STREAMING mode for memory-efficient processing")
        return process_chr_data_streaming(
            silver_dir=silver_dir,
            bronze_timestamp=bronze_timestamp,
            export_timestamp=export_timestamp,
        )

    # Legacy mode - original in-memory processing
    logging.info("Using LEGACY mode with in-memory data")
    logging.warning(
        "Legacy mode may cause memory issues with large datasets. Consider using streaming mode."
    )

    # Determine data source mode - support both memory and file loading
    load_from_memory = in_memory_data is not None and bool(in_memory_data)

    # Try to determine bronze directory if not using memory
    bronze_dir = None
    if not load_from_memory:
        # Try to get bronze directory from environment or config
        bronze_dir_override = os.getenv("BRONZE_DATE_FOLDER_OVERRIDE")
        if bronze_dir_override:
            bronze_dir = config.BRONZE_BASE_DIR / bronze_dir_override

        if bronze_dir and bronze_dir.exists() and any(bronze_dir.glob("*.json")):
            logging.info(f"Silver processing source mode: bronze files from {bronze_dir}")
        else:
            logging.error(
                "Cannot process silver data: no in_memory_data provided and no bronze files found."
            )
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
        saved_xml_path_obj = None
        if vetstat_antibiotics_data:
            # Ensure the temporary XML file is cleaned up
            temp_xml_path_obj = silver_dir / "_temp_vetstat.xml"
            saved_xml_path_obj = (
                silver_dir / f"_DEBUG_FAILED_vetstat_{export_timestamp or 'unknown'}.xml"
            )
            try:
                with open(temp_xml_path_obj, "w") as f:
                    # Add separator compatible with VetStat XML parser's expectations
                    f.write("\n<!-- RAW_RESPONSE_SEPARATOR -->\n".join(vetstat_antibiotics_data))
                vetstat_antibiotics_xml_path = (
                    temp_xml_path_obj  # Assign path only if successfully written
                )
                try:
                    xml_size = vetstat_antibiotics_xml_path.stat().st_size
                    logging.info(
                        f"Created temporary VetStat XML file: {vetstat_antibiotics_xml_path} (Size: {xml_size} bytes)"
                    )
                except Exception as e_stat:
                    logging.warning(
                        f"Could not get size of temp XML file {vetstat_antibiotics_xml_path}: {e_stat}"
                    )
                    logging.info(
                        f"Created temporary VetStat XML file: {vetstat_antibiotics_xml_path}"
                    )
            except Exception as e_write:
                logging.error(
                    f"Failed to write temporary VetStat XML file: {e_write}",
                    exc_info=True,
                )
                # Ensure path is None if write failed
                vetstat_antibiotics_xml_path = None
                # Clean up potentially partially written file
                if temp_xml_path_obj.exists():
                    with contextlib.suppress(OSError):
                        temp_xml_path_obj.unlink()

    else:
        # Use file paths as before
        besaetning_list_path = bronze_dir / "besaetning_list.json"
        besaetning_details_path = bronze_dir / "besaetning_details.json"
        diko_flytninger_path = bronze_dir / "diko_flytninger.json"
        ejendom_oplysninger_path = bronze_dir / "ejendom_oplysninger.json"
        ejendom_vet_events_path = bronze_dir / "ejendom_vet_events.json"
        vetstat_antibiotics_xml_path = bronze_dir / "vetstat_antibiotics.xml"
        saved_xml_path_obj = None

    # Define intermediate path for parsed XML (placed in silver dir for easier cleanup)
    vetstat_antibiotics_jsonl_path = silver_dir / "_intermediate_vetstat.jsonl"

    # --- 1. Pre-process VetStat XML to JSONL ---
    vetstat_loaded = False
    temp_xml_created_in_silver = load_from_memory and vetstat_antibiotics_xml_path is not None
    try:
        if vetstat_antibiotics_xml_path and vetstat_antibiotics_xml_path.exists():
            try:
                if run_xml_parser(vetstat_antibiotics_xml_path, vetstat_antibiotics_jsonl_path):
                    if (
                        vetstat_antibiotics_jsonl_path.exists()
                        and vetstat_antibiotics_jsonl_path.stat().st_size > 0
                    ):
                        vetstat_loaded = True
                        logging.info(
                            f"Successfully created intermediate VetStat JSONL: {vetstat_antibiotics_jsonl_path}"
                        )
                    else:
                        logging.warning(
                            f"XML parser succeeded but output file is empty or missing: "
                            f"{vetstat_antibiotics_jsonl_path}"
                        )
                else:
                    logging.warning(
                        "VetStat XML processing failed, proceeding without antibiotic data"
                    )
                    if vetstat_antibiotics_xml_path.exists() and saved_xml_path_obj:
                        try:
                            vetstat_antibiotics_xml_path.rename(saved_xml_path_obj)
                            logging.warning(
                                f"Saved problematic XML to {saved_xml_path_obj} for inspection."
                            )
                            vetstat_antibiotics_xml_path = None
                        except Exception as e_save:
                            logging.error(
                                f"Failed to save problematic XML {vetstat_antibiotics_xml_path}: {e_save}"
                            )
            except (FileNotFoundError, RuntimeError, Exception) as e:
                logging.error(
                    f"Failed to process VetStat XML: {e}. Proceeding without antibiotic data.",
                    exc_info=True,
                )
                if (
                    vetstat_antibiotics_xml_path
                    and vetstat_antibiotics_xml_path.exists()
                    and saved_xml_path_obj
                ):
                    try:
                        vetstat_antibiotics_xml_path.rename(saved_xml_path_obj)
                        logging.warning(
                            f"Saved problematic XML to {saved_xml_path_obj} for inspection."
                        )
                        vetstat_antibiotics_xml_path = None
                    except Exception as e_save:
                        logging.error(
                            f"Failed to save problematic XML {vetstat_antibiotics_xml_path}: {e_save}"
                        )
                # Ensure path is None if failed
                if vetstat_antibiotics_jsonl_path.exists():
                    with contextlib.suppress(OSError):
                        vetstat_antibiotics_jsonl_path.unlink()  # Clean up failed attempt
        else:
            logging.warning(
                f"VetStat XML file not found or not provided "
                f"({'in-memory path was' if load_from_memory else 'bronze path was'} "
                f"{vetstat_antibiotics_xml_path}). Skipping antibiotic data processing."
            )
            vetstat_antibiotics_jsonl_path = None
    finally:
        # Clean up the temporary XML file created from in-memory data
        if (
            temp_xml_created_in_silver
            and vetstat_antibiotics_xml_path
            and vetstat_antibiotics_xml_path.exists()
        ):
            try:
                vetstat_antibiotics_xml_path.unlink()
                logging.info(
                    f"Cleaned up temporary VetStat XML file: {vetstat_antibiotics_xml_path}"
                )
            except OSError as e_del:
                logging.warning(
                    f"Could not delete temporary VetStat XML file {vetstat_antibiotics_xml_path}: {e_del}"
                )

    # --- 2. Initialize DuckDB Connection ---
    logging.info("Initializing DuckDB backend (in-memory)")
    try:
        con = duckdb.connect()  # In-memory by default
        # Install necessary DuckDB extensions if not already present
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        con.execute("INSTALL json;")
        con.execute("LOAD json;")
        logging.info("DuckDB extensions httpfs, json loaded.")
    except Exception as e:
        logging.error(f"Failed to initialize DuckDB or load extensions: {e}", exc_info=True)
        sys.exit(1)

    # --- 3. Load Bronze Data into DuckDB Tables ---
    logging.info("Loading bronze data into DuckDB tables...")

    raw_tables = {}

    # Define sources and their corresponding keys/paths
    sources_to_load = {
        "bes_list": {"mem_key": "besaetning_list", "file_key": "besaetning_list.json"},
        "bes_details": {
            "mem_key": "besaetning_details",
            "file_key": "besaetning_details.json",
        },
        "diko_flyt": {"mem_key": "diko_flytninger", "file_key": "diko_flytninger.json"},
        "cattle_movements": {
            "mem_key": "chr_dyr_movement_summaries",
            "file_key": "chr_dyr_movement_summaries.parquet",
        },
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
                temp_jsonl_path = None
                temp_file = None
                try:
                    # Create a temporary file to write JSONL data
                    # Using delete=False with manual cleanup is intentional for this use case
                    temp_file = tempfile.NamedTemporaryFile(  # noqa: SIM115
                        mode="w",
                        encoding="utf-8",
                        suffix=".jsonl",
                        delete=False,
                        dir=silver_dir,
                        prefix=f"temp_{table_name}_",
                    )
                    temp_jsonl_path = Path(temp_file.name)
                    logging.info(
                        f"Writing in-memory data for '{table_name}' to temporary JSONL: {temp_jsonl_path.name}"
                    )

                    for record in data:
                        try:
                            json_string = json.dumps(record, default=str)
                            temp_file.write(json_string + "\n")
                        except TypeError as e_json:
                            logging.warning(
                                f"Skipping record due to JSON serialization error for table '{table_name}': {e_json}. "
                                f"Record sample: {str(record)[:200]}..."
                            )
                            continue

                    temp_file.flush()
                    temp_file.close()

                    logging.info(
                        f"Finished writing temporary JSONL for '{table_name}'. Attempting read_json_auto..."
                    )

                    max_obj_size_bytes = 1073741824
                    con.execute(
                        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM "
                        f"read_json_auto('{temp_jsonl_path!s}', "
                        f"maximum_object_size={max_obj_size_bytes});"
                    )
                    raw_tables[table_name] = table_name

                    successfully_loaded = True
                    source_desc = f"in-memory buffer via temp JSONL ({temp_jsonl_path.name}) for '{source_info['mem_key']}'"
                    logging.info(
                        f"Successfully loaded {source_desc} into table '{table_name}' using read_json_auto."
                    )

                except Exception as e_mem_jsonl:
                    logging.error(
                        f"Failed to load '{table_name}' from memory via temp JSONL: {e_mem_jsonl}",
                        exc_info=True,
                    )
                    with contextlib.suppress(Exception):
                        con.execute(f"DROP TABLE IF EXISTS {table_name};")
                finally:
                    if temp_jsonl_path and temp_jsonl_path.exists():
                        try:
                            temp_jsonl_path.unlink()
                            logging.info(f"Removed temporary JSONL file: {temp_jsonl_path.name}")
                        except OSError as e_del:
                            logging.warning(
                                f"Could not delete temporary JSONL file {temp_jsonl_path.name}: {e_del}"
                            )
                    if temp_file and not temp_file.closed:
                        temp_file.close()

                    if data and len(data) > 1000:
                        import gc

                        gc.collect()
                        logging.debug(
                            f"Forced garbage collection after processing {len(data)} records for {table_name}"
                        )

                    if "data" in locals():
                        del data
            else:
                logging.warning(
                    f"No data (or not a list) found in memory buffer for {source_info['mem_key']}."
                )

        # --- Attempt 2: Load from Files (when not using memory) --- #
        if not successfully_loaded and not load_from_memory and bronze_dir:
            logging.info(f"Attempting to load '{table_name}' from bronze files...")
            file_key = source_info["file_key"]

            if "*" in file_key:
                import glob

                pattern_path = bronze_dir / file_key
                matching_files = glob.glob(str(pattern_path))

                if matching_files:
                    try:
                        logging.info(
                            f"Loading {table_name} from {len(matching_files)} files matching pattern: {file_key}"
                        )
                        file_list_str = "', '".join(matching_files)

                        if file_key.endswith(".parquet"):
                            con.execute(
                                f"CREATE OR REPLACE TABLE {table_name} AS "
                                f"SELECT * FROM read_parquet(['{file_list_str}']);"
                            )
                        else:
                            max_obj_size_bytes = 1073741824
                            con.execute(
                                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM "
                                f"read_json_auto(['{file_list_str}'], "
                                f"maximum_object_size={max_obj_size_bytes});"
                            )
                        raw_tables[table_name] = table_name
                        successfully_loaded = True
                        logging.info(
                            f"Successfully loaded {table_name} from {len(matching_files)} files"
                        )

                    except Exception as e_file:
                        logging.error(
                            f"Failed to load '{table_name}' from pattern {file_key}: {e_file}"
                        )
                else:
                    logging.warning(
                        f"No files found matching pattern for '{table_name}': {pattern_path}"
                    )
            else:
                file_path = bronze_dir / file_key

                if file_path.exists():
                    try:
                        logging.info(f"Loading {table_name} from file: {file_path}")
                        if file_path.suffix.lower() == ".parquet":
                            con.execute(
                                f"CREATE OR REPLACE TABLE {table_name} AS "
                                f"SELECT * FROM read_parquet('{file_path!s}');"
                            )
                        else:
                            max_obj_size_bytes = 1073741824
                            con.execute(
                                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM "
                                f"read_json_auto('{file_path!s}', "
                                f"maximum_object_size={max_obj_size_bytes});"
                            )
                        raw_tables[table_name] = table_name
                        successfully_loaded = True
                        logging.info(f"Successfully loaded {table_name} from file {file_path}")

                    except Exception as e_file:
                        logging.error(
                            f"Failed to load '{table_name}' from file {file_path}: {e_file}"
                        )
                else:
                    logging.warning(f"File not found for '{table_name}': {file_path}")

        if not successfully_loaded:
            optional_tables = ["cattle_movements", "spf_su_herds"]
            if table_name in optional_tables:
                logging.warning(
                    f"Optional table '{table_name}' not found - skipping "
                    f"(this is normal if the corresponding bronze step wasn't run)"
                )
            else:
                logging.error(f"Failed to load table '{table_name}' from all available sources.")
        else:
            logging.info(f"Successfully loaded table '{table_name}'")

    # Handle VetStat separately
    vetstat_antibiotics_jsonl_path = silver_dir / "_intermediate_vetstat.jsonl"
    if vetstat_antibiotics_jsonl_path.exists():
        logging.info(
            f"Loading pre-processed VetStat data from {vetstat_antibiotics_jsonl_path.name}..."
        )
        try:
            con.execute(
                f"CREATE OR REPLACE TABLE vetstat AS SELECT * FROM "
                f"read_json_auto('{vetstat_antibiotics_jsonl_path!s}', format='newline_delimited')"
            )
            raw_tables["vetstat"] = "vetstat"
            logging.info("Successfully loaded vetstat data.")
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

    if "bes_details" not in raw_tables:
        logging.error("Essential table 'bes_details' could not be loaded. Aborting processing.")
        sys.exit(1)
    if "ejendom_oplys" not in raw_tables:
        logging.error("Essential table 'ejendom_oplys' could not be loaded. Aborting processing.")
        sys.exit(1)

    # --- 4. Create & Populate Lookup Tables ---
    logging.info("Creating and saving lookup tables...")
    lookup_tables = {}

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
        "cattle_movements_table": raw_tables.get("cattle_movements"),
        "ejendom_oplys_table": raw_tables.get("ejendom_oplys"),
        "ejendom_vet_table": raw_tables.get("ejendom_vet"),
        "vetstat_table": raw_tables.get("vetstat"),
        "spf_su_table": raw_tables.get("spf_su_herds"),
        "lookup_tables": lookup_tables,
    }

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
                if property_owners_table is not None:
                    try:
                        parquet_path = silver_dir / "property_owners.parquet"
                        if parquet_path.exists():
                            con.execute(
                                f"CREATE OR REPLACE TABLE property_owners AS "
                                f"SELECT * FROM read_parquet('{parquet_path}')"
                            )
                    except Exception as e:
                        logging.warning(
                            f"Failed to register property_owners table for CVR collection: {e}"
                        )

            elif step == "silver_property_users":
                property_users_table = properties.create_property_users_table(
                    con, context.get("ejendom_oplys_table"), silver_dir
                )
                if property_users_table is not None:
                    try:
                        parquet_path = silver_dir / "property_users.parquet"
                        if parquet_path.exists():
                            con.execute(
                                f"CREATE OR REPLACE TABLE property_users AS "
                                f"SELECT * FROM read_parquet('{parquet_path}')"
                            )
                    except Exception as e:
                        logging.warning(
                            f"Failed to register property_users table for CVR collection: {e}"
                        )

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
                if herd_owners_table is not None:
                    try:
                        parquet_path = silver_dir / "herd_owners.parquet"
                        if parquet_path.exists():
                            con.execute(
                                f"CREATE OR REPLACE TABLE herd_owners AS "
                                f"SELECT * FROM read_parquet('{parquet_path}')"
                            )
                    except Exception as e:
                        logging.warning(
                            f"Failed to register herd_owners table for CVR collection: {e}"
                        )

            elif step == "silver_herd_users":
                herd_users_table = herds.create_herd_users_table(
                    con, context.get("bes_details_table"), silver_dir
                )
                if herd_users_table is not None:
                    try:
                        parquet_path = silver_dir / "herd_users.parquet"
                        if parquet_path.exists():
                            con.execute(
                                f"CREATE OR REPLACE TABLE herd_users AS "
                                f"SELECT * FROM read_parquet('{parquet_path}')"
                            )
                    except Exception as e:
                        logging.warning(
                            f"Failed to register herd_users table for CVR collection: {e}"
                        )

            elif step == "silver_herd_sizes":
                herds.create_herd_sizes_table(con, context.get("bes_details_table"), silver_dir)

            elif step == "silver_animal_movements":
                animal_movements.create_animal_movements_table(
                    con, context.get("diko_flyt_table"), silver_dir
                )

                if context.get("cattle_movements_table") is not None:
                    animal_movements.create_chr_dyr_movement_summaries_table(
                        con, context.get("cattle_movements_table"), silver_dir
                    )
                else:
                    logging.info(
                        "CHR_dyr cattle movements not available - skipping (animal_movements bronze step not run)"
                    )

            elif step == "silver_property_vet_events":
                property_vet_events.create_property_vet_events_table(
                    con,
                    context.get("ejendom_vet_table"),
                    context.get("lookup_tables", {}),
                    silver_dir,
                )

            elif step == "silver_antibiotic_usage":
                antibiotic_usage.create_antibiotic_usage_table(
                    con,
                    context.get("vetstat_table"),
                    context.get("lookup_tables", {}),
                    silver_dir,
                )
                if context.get("vetstat_table") is not None:
                    try:
                        parquet_path = silver_dir / "antibiotic_usage.parquet"
                        if parquet_path.exists():
                            con.execute(
                                f"CREATE OR REPLACE TABLE antibiotic_usage AS "
                                f"SELECT * FROM read_parquet('{parquet_path}')"
                            )
                    except Exception as e:
                        logging.warning(
                            f"Failed to register antibiotic_usage table for CVR collection: {e}"
                        )

            elif step == "silver_spf_su_herds":
                if context.get("spf_su_table") is not None:
                    from . import spf_su

                    spf_su.create_spf_su_herds_table(con, context.get("spf_su_table"), silver_dir)
                else:
                    logging.warning("Cannot create SPF-SU herds table: spf_su_raw is None")

            elif step == "silver_spf_su_health_controls":
                if context.get("spf_su_table") is not None:
                    from . import spf_su

                    spf_su.create_spf_su_health_controls_table(
                        con, context.get("spf_su_table"), silver_dir
                    )
                else:
                    logging.warning(
                        "Cannot create SPF-SU health controls table: spf_su_raw is None"
                    )

            elif step == "silver_spf_su_salmonella_data":
                if context.get("spf_su_table") is not None:
                    from . import spf_su

                    spf_su.create_spf_su_salmonella_data_table(
                        con, context.get("spf_su_table"), silver_dir
                    )
                else:
                    logging.warning(
                        "Cannot create SPF-SU salmonella data table: spf_su_raw is None"
                    )

        except Exception as e:
            logging.error(f"Error in silver step {step}: {e}", exc_info=True)
            continue

    # --- 12. CVR Collection ---
    if CVR_COLLECTION_AVAILABLE:
        _save_discovered_cvr_numbers(con, silver_dir, export_timestamp)
    else:
        logging.warning("CVR collection disabled due to import error")

    # --- 13. Generate Schema Documentation ---
    if SchemaDocumentationManager is not None:
        logging.info("Generating schema documentation for CHR silver tables...")
        try:
            dir_name = silver_dir.name
            if len(dir_name) == 15 and dir_name[8] == "_":
                pipeline_start_time = datetime.strptime(dir_name, "%Y%m%d_%H%M%S")
            else:
                pipeline_start_time = datetime.now()

            schema_manager = SchemaDocumentationManager(
                connection=con,
                pipeline_name="chr_pipeline",
                pipeline_start_time=pipeline_start_time,
                logger=logging.getLogger(__name__),
            )

            tables_query = "SHOW TABLES"
            tables_result = con.execute(tables_query).fetchall()
            silver_tables = [
                table[0]
                for table in tables_result
                if table[0]
                not in [
                    "bes_details",
                    "diko_flyt",
                    "ejendom_oplys",
                    "ejendom_vet",
                    "vetstat",
                    "cattle_movements",
                ]
            ]

            if silver_tables:
                schema_files = schema_manager.generate_all_documentation(
                    silver_tables, stage="silver"
                )
                logging.info(
                    f"Generated schema documentation for {len(silver_tables)} tables: {', '.join(silver_tables)}"
                )

                try:
                    schema_manager.commit_to_github()
                    logging.info("Schema documentation committed to GitHub")
                except Exception as git_error:
                    logging.warning(f"Failed to commit to GitHub: {git_error}")
                    logging.info(
                        "Schema documentation generated locally but not committed to GitHub"
                    )
            else:
                logging.warning("No silver tables found for schema documentation")

        except Exception as e:
            logging.error(f"Failed to generate schema documentation: {e}", exc_info=True)
    else:
        logging.warning("Schema documentation disabled due to import error")

    # --- 14. Upload Silver Data to GCS ---
    try:
        upload_success = upload_silver_data_to_storage(silver_dir, export_timestamp)
        if upload_success:
            logging.info("Silver data uploaded to GCS successfully")
        else:
            logging.warning("Silver data upload to GCS failed or was skipped")
    except Exception as e:
        logging.error(f"Error during silver data upload to GCS: {e}")

    # --- 15. Cleanup Intermediate Files ---
    if vetstat_antibiotics_jsonl_path and vetstat_antibiotics_jsonl_path.exists():
        try:
            vetstat_antibiotics_jsonl_path.unlink()
            logging.info(f"Removed intermediate file: {vetstat_antibiotics_jsonl_path}")
        except OSError as e:
            logging.warning(
                f"Could not remove intermediate file {vetstat_antibiotics_jsonl_path}: {e}"
            )

    try:
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

        if "con" in locals() and con:
            try:
                con.close()
                logging.info("Closed DuckDB connection")
            except Exception as e:
                logging.warning(f"Error closing DuckDB connection: {e}")

        if "raw_tables" in locals():
            del raw_tables
        if "context" in locals():
            del context
        if "lookup_tables" in locals():
            del lookup_tables
        if "in_memory_data" in locals() and in_memory_data:
            in_memory_data.clear()
            del in_memory_data

        import gc

        gc.collect()
        logging.info("Completed comprehensive cleanup of temporary files and memory")

    except Exception as e:
        logging.warning(f"Error during comprehensive cleanup: {e}")

    logging.info(f"Silver data processing finished. Output located in: {silver_dir}")
    return None


if __name__ == "__main__":
    try:
        logging.info("Determining input bronze directory...")
        if config.BRONZE_DATE_FOLDER_OVERRIDE:
            input_bronze_dir = config.BRONZE_BASE_DIR / config.BRONZE_DATE_FOLDER_OVERRIDE
            if not input_bronze_dir.is_dir():
                raise FileNotFoundError(
                    f"Specified bronze directory does not exist: {input_bronze_dir}"
                )
            logging.info(f"Using specified bronze data directory: {input_bronze_dir.name}")
        else:
            input_bronze_dir = get_latest_bronze_dir(config.BRONZE_BASE_DIR)
        logging.info(f"Determined input bronze directory: {input_bronze_dir}")
    except FileNotFoundError as e:
        logging.error(f"Error determining bronze data directory: {e}")
        sys.exit(1)

    pipeline_start_time = datetime.now()
    processing_timestamp = pipeline_start_time.strftime("%Y%m%d_%H%M%S")
    output_silver_dir = config.SILVER_BASE_DIR / processing_timestamp
    logging.info(f"Determined output silver directory: {output_silver_dir}")

    try:
        logging.info("Starting process_chr_data function...")
        process_chr_data(bronze_dir=input_bronze_dir, silver_dir=output_silver_dir)
        logging.info("Finished process_chr_data function.")
    except Exception as e:
        logging.critical(f"An unhandled error occurred during data processing: {e}", exc_info=True)
        sys.exit(1)

    logging.info("--- Script execution finished ---")
