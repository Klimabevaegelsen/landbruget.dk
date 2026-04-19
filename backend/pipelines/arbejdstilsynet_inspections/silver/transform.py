"""
Silver layer transformation for Arbejdstilsynet (Work Inspections) data.

This module transforms raw CSV inspection data from the bronze layer into cleaned,
structured Parquet format using vanilla DuckDB for processing.
"""

import logging
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import duckdb
from common.logging_utils import get_pipeline_logger

# Add CVR collection import
try:
    # Import the CVR collection utility from the unified pipeline
    # Find the unified pipeline directory
    current_file = Path(__file__).resolve()
    project_root = None

    for parent in current_file.parents:
        unified_pipeline_path = parent / "backend" / "pipelines" / "unified_pipeline" / "src"
        if unified_pipeline_path.exists():
            project_root = unified_pipeline_path
            break

    if project_root and str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from unified_pipeline.common.uuid_utils import LandbrugsdataUUID
    from unified_pipeline.util.cvr_api_client import CVRAPIClient
    from unified_pipeline.util.cvr_collection import save_pipeline_cvr_numbers

    CVR_COLLECTION_AVAILABLE = True
except ImportError as e:
    # Graceful fallback if CVR collection is not available
    save_pipeline_cvr_numbers = None
    CVRAPIClient = None
    LandbrugsdataUUID = None
    CVR_COLLECTION_AVAILABLE = False
    logging.warning(f"CVR collection not available: {e}")


def _get_optimized_storage_access() -> type | None:
    """
    Get optimized cloud storage access with robust import handling.

    Returns StorageAccess if available, otherwise None for fallback.
    """
    try:
        # Primary import path - should work when common.storage is properly installed
        from common.storage import StorageAccess

        logging.info("Successfully imported optimized StorageAccess")
        return StorageAccess
    except ImportError as e:
        logging.warning(f"Could not import optimized StorageAccess: {e}")
        logging.warning(
            "Falling back to basic storage - ensure common.storage is installed for optimal performance"
        )
        return None


# Get optimized cloud storage access class or None if not available
OptimizedStorageAccess = _get_optimized_storage_access()


class CloudStorage:
    """Google Cloud Storage backend for arbejdstilsynet_inspections files - OPTIMIZED VERSION."""

    def __init__(self, bucket_name, prefix="silver/arbejdstilsynet_inspections"):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.is_available = self._check_storage_available()

        # Initialize optimized cloud storage access
        self.storage = None
        self.use_optimized = False

        if self.is_available and OptimizedStorageAccess:
            try:
                self.storage = OptimizedStorageAccess()
                self.use_optimized = True
                logging.info(
                    "Arbejdstilsynet Silver CloudStorage: Initialized optimized cloud storage access"
                )
            except Exception as e:
                logging.warning(f"Failed to initialize optimized cloud storage access: {e}")
                self.storage = None
                self.use_optimized = False

    def _check_storage_available(self) -> bool:
        """Check if cloud storage is available (R2/S3 via common.storage)."""
        try:
            from common.storage.filesystem import get_r2_filesystem  # noqa: F401

            return True
        except (OSError, ImportError):
            logging.warning("Cloud storage not available. Using local storage only.")
            return False

    def upload_file(self, local_path, storage_path=None) -> bool:
        """Upload a file to storage bucket using optimized streaming."""
        if not self.is_available:
            logging.warning("Cloud storage not available, skipping upload")
            return False

        if storage_path is None:
            # Use the timestamp from the local path
            timestamp = os.path.basename(os.path.dirname(local_path))
            filename = os.path.basename(local_path)
            storage_path = f"{self.prefix}/{timestamp}/{filename}"

        try:
            # Use streaming upload if available
            if self.use_optimized and self.storage:
                full_storage_path = f"{self.bucket_name}/{storage_path}"
                fs_path = f"{self.bucket_name}/{storage_path}"

                # Stream file directly without loading into memory
                with (
                    open(local_path, "rb") as file_obj,
                    self.storage.fs.open(fs_path, "wb") as cloud_file,
                ):
                    shutil.copyfileobj(file_obj, cloud_file)

                logging.info(f"Uploaded {local_path} to {full_storage_path} (optimized streaming)")
                return True
            # Fallback to s3fs upload if optimized access failed
            from common.storage.filesystem import get_r2_filesystem

            fs = get_r2_filesystem()
            dest_path = f"{self.bucket_name}/{storage_path}"
            fs.put(local_path, dest_path)
            logging.info(f"Uploaded {local_path} to {dest_path} (fallback)")
            return True

        except Exception as e:
            logging.error(f"Failed to upload to cloud storage: {e}")
            return False


class SilverPipeline:
    """
    A class to handle the silver layer transformations for arbejdstilsynet_inspections data.
    Transforms raw CSV data into cleaned and structured parquet format using vanilla DuckDB.
    """

    def __init__(self, start_date=None, end_date=None, storage_bucket=None, log_level="INFO"):
        """Initialize the Silver Pipeline with paths, constants, and logging setup."""
        # Setup logging
        logging.basicConfig(
            level=getattr(logging, log_level),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        self.logger = get_pipeline_logger("SilverPipeline")

        # Store parameters
        self.start_date = start_date
        self.end_date = end_date
        self.storage_bucket = storage_bucket

        # Constants and paths - use pipeline start time consistently
        self.pipeline_start_time = datetime.now()
        self.timestamp = self.pipeline_start_time.strftime("%Y%m%d_%H%M%S")
        self.now = self.timestamp  # Use same timestamp for consistency
        self.pipeline_root = os.path.dirname(os.path.abspath(__file__))
        self.data_root = os.path.join(self.pipeline_root, "..", "bronze", "data")
        self.silver_dir = os.path.join(self.pipeline_root, "..", "silver", "data")
        self.output_dir = os.path.join(self.silver_dir, self.timestamp)
        self.output_parquet = os.path.join(self.output_dir, "workplace_inspections.parquet")
        self.temp_dir = None

        # Column renaming and conventions
        self.column_rename = {
            "Dato": "date",
            "Antal": "case_count",
            "Afgørelse": "decision",
            "Arbejdsmiljøproblem (emne)": "work_env_issue",
            "Påklaget": "appealed",
            "Efterkommet": "complied",
            "Produktionsenhed": "company_name",
            "P-nummer": "company_id",
            "Branche": "industry",
            "Produktionenhedens adresse": "company_address",
        }

        # DuckDB connection
        self.con = None
        self.df = None
        self.input_csv = None

    def setup_output_directories(self) -> bool:
        """Create output directories if they don't exist."""
        try:
            os.makedirs(self.silver_dir, exist_ok=True)
            os.makedirs(self.output_dir, exist_ok=True)
            # Create a temporary directory for processing
            self.temp_dir = tempfile.mkdtemp(prefix="silver_transform_")
            self.logger.info(f"Created temporary directory: {self.temp_dir}")
            return True
        except Exception as e:
            self.logger.error(f"Error creating output directories: {e!s}")
            return False

    def find_latest_bronze_data(self) -> bool:
        """Find the latest bronze directory and CSV file."""
        try:
            self.logger.info(f"Looking for bronze data in: {self.data_root}")
            bronze_dirs = [
                d
                for d in os.listdir(self.data_root)
                if os.path.isdir(os.path.join(self.data_root, d))
            ]

            if not bronze_dirs:
                self.logger.error(f"No bronze data directories found in {self.data_root}")
                return False

            latest_bronze_dir = max(bronze_dirs)
            self.input_csv = os.path.join(self.data_root, latest_bronze_dir, "data_merged.csv")

            if not os.path.exists(self.input_csv):
                self.logger.error(f"Input CSV file not found: {self.input_csv}")
                return False

            self.logger.info(f"Found latest bronze data: {self.input_csv}")
            return True
        except Exception as e:
            self.logger.error(f"Error finding latest bronze data: {e!s}")
            return False

    def connect_database(self) -> bool:
        """Connect to DuckDB."""
        try:
            self.con = duckdb.connect(":memory:")
            self.logger.info("Connected to DuckDB (in-memory)")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to DuckDB: {e!s}")
            return False

    def load_data(self) -> bool:
        """Load CSV data using DuckDB."""
        try:
            # Read CSV file into a table
            self.con.execute(f"""
                CREATE TABLE raw_data AS
                SELECT * FROM read_csv('{self.input_csv}', auto_detect=true)
            """)

            # Get column count
            result = self.con.execute("SELECT * FROM raw_data LIMIT 1").description
            col_count = len(result) if result else 0

            self.logger.info(f"Loaded data with {col_count} columns")
            return True
        except Exception as e:
            self.logger.error(f"Error loading data from CSV: {e!s}")
            return False

    def rename_columns(self) -> bool:
        """Rename columns according to conventions using SQL."""
        try:
            # Get current column names
            result = self.con.execute("SELECT * FROM raw_data LIMIT 1").description
            current_columns = [desc[0] for desc in result]

            # Build column select list with renames
            column_selects = []
            for col in current_columns:
                if col in self.column_rename:
                    new_name = self.column_rename[col]
                    column_selects.append(f'"{col}" AS {new_name}')
                else:
                    # Keep column as-is but quote it for safety
                    column_selects.append(f'"{col}"')

            # Create new table with renamed columns
            self.con.execute(f"""
                CREATE OR REPLACE TABLE renamed_data AS
                SELECT {", ".join(column_selects)}
                FROM raw_data
            """)

            # Get new column names for logging
            result = self.con.execute("SELECT * FROM renamed_data LIMIT 1").description
            new_columns = [desc[0] for desc in result]
            self.logger.info(f"Columns after rename: {new_columns}")
            return True
        except Exception as e:
            self.logger.error(f"Error renaming columns: {e!s}")
            return False

    def validate_column_names(self) -> bool:
        """Validate column names against conventions."""
        try:
            result = self.con.execute("SELECT * FROM renamed_data LIMIT 1").description
            columns = [desc[0] for desc in result]

            for name in columns:
                if len(name.split("_")) > 5:
                    self.logger.error(f"Column name '{name}' exceeds 5-word limit.")
                    return False
                if not name.islower():
                    self.logger.error(f"Column name '{name}' must be lowercase.")
                    return False

            self.logger.info("All column names validated successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error validating column names: {e!s}")
            return False

    def deduplicate(self):
        """Remove duplicate rows using DuckDB SQL."""
        try:
            original_rows = self.con.execute("SELECT COUNT(*) FROM renamed_data").fetchone()[0]

            self.con.execute("""
                CREATE OR REPLACE TABLE deduped_data AS
                SELECT DISTINCT * FROM renamed_data
            """)

            new_rows = self.con.execute("SELECT COUNT(*) FROM deduped_data").fetchone()[0]

            if original_rows > new_rows:
                self.logger.info(f"Removed {original_rows - new_rows} duplicate rows")
            else:
                self.logger.info("No duplicate rows found")

            return True
        except Exception as e:
            self.logger.error(f"Error removing duplicates: {e!s}")
            return False

    def normalize_enums(self):
        """Normalize enums and special characters using DuckDB SQL."""
        try:
            # Get current column names
            result = self.con.execute("SELECT * FROM deduped_data LIMIT 1").description
            columns = [desc[0] for desc in result]

            # Build column transformations
            enum_fields = ["decision", "work_env_issue", "appealed", "complied", "industry"]
            column_selects = []

            for col in columns:
                if col in enum_fields:
                    # Normalize: lowercase, replace Danish chars, trim
                    column_selects.append(f"""
                        LOWER(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        TRIM(CAST("{col}" AS VARCHAR)),
                                        'ae', 'ae'
                                    ),
                                    'oe', 'oe'
                                ),
                                'aa', 'aa'
                            )
                        ) AS {col}
                    """)
                else:
                    column_selects.append(f'"{col}"')

            self.con.execute(f"""
                CREATE OR REPLACE TABLE normalized_data AS
                SELECT {", ".join(column_selects)}
                FROM deduped_data
            """)

            self.logger.info("Normalized enum fields")
            return True
        except Exception as e:
            self.logger.error(f"Error normalizing enums: {e!s}")
            return False

    def handle_null_values(self):
        """Convert empty strings to null values using DuckDB SQL."""
        try:
            # Get current column names and types
            result = self.con.execute("SELECT * FROM normalized_data LIMIT 1").description
            columns = [desc[0] for desc in result]

            # Get column types
            col_info = self.con.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'normalized_data'
            """).fetchall()

            type_map = {row[0]: row[1] for row in col_info}

            # Build column transformations
            column_selects = []
            for col in columns:
                col_type = type_map.get(col, "").lower()
                if "varchar" in col_type or "text" in col_type or col_type == "unknown":
                    # Convert empty strings to NULL using NULLIF
                    column_selects.append(f"NULLIF(TRIM(CAST(\"{col}\" AS VARCHAR)), '') AS {col}")
                else:
                    column_selects.append(f'"{col}"')

            self.con.execute(f"""
                CREATE OR REPLACE TABLE nulled_data AS
                SELECT {", ".join(column_selects)}
                FROM normalized_data
            """)

            self.logger.info("Handled null values in string columns")
            return True
        except Exception as e:
            self.logger.error(f"Error handling null values: {e!s}")
            return False

    def cast_types(self):
        """Cast columns to appropriate types using DuckDB SQL."""
        try:
            # Get current column names
            result = self.con.execute("SELECT * FROM nulled_data LIMIT 1").description
            columns = [desc[0] for desc in result]

            # Build column transformations
            column_selects = []
            for col in columns:
                if col == "date":
                    column_selects.append(f'TRY_CAST("{col}" AS DATE) AS {col}')
                elif col == "case_count" or col == "company_id":
                    column_selects.append(f'TRY_CAST("{col}" AS BIGINT) AS {col}')
                elif col == "appealed":
                    # Convert 'paaklaget' to 1, else 0
                    column_selects.append(f"""
                        CASE WHEN LOWER(CAST("{col}" AS VARCHAR)) = 'paaklaget' THEN 1 ELSE 0 END AS {col}
                    """)
                elif col == "complied":
                    # Convert 'efterkommet' to 1, else 0
                    column_selects.append(f"""
                        CASE WHEN LOWER(CAST("{col}" AS VARCHAR)) = 'efterkommet' THEN 1 ELSE 0 END AS {col}
                    """)
                else:
                    column_selects.append(f'"{col}"')

            self.con.execute(f"""
                CREATE OR REPLACE TABLE typed_data AS
                SELECT {", ".join(column_selects)}
                FROM nulled_data
            """)

            self.logger.info("Cast columns to appropriate types")
            return True
        except Exception as e:
            self.logger.error(f"Error casting types: {e!s}")
            return False

    def filter_by_date(self):
        """Filter data by date range if start_date or end_date are provided."""
        try:
            if self.start_date is None and self.end_date is None:
                self.logger.info("No date range provided, skipping date filtering")
                # Just copy the table
                self.con.execute("""
                    CREATE OR REPLACE TABLE filtered_data AS
                    SELECT * FROM typed_data
                """)
                return True

            # Check if date column exists
            result = self.con.execute("SELECT * FROM typed_data LIMIT 1").description
            columns = [desc[0] for desc in result]

            if "date" not in columns:
                self.logger.warning("Date column not found, cannot filter by date range")
                return False

            # Build filter conditions
            conditions = []
            start_date = None
            end_date = None

            if self.start_date is not None:
                if isinstance(self.start_date, str):
                    start_date = datetime.strptime(self.start_date, "%Y-%m-%d").date()
                else:
                    start_date = self.start_date
                conditions.append(f"date >= '{start_date}'")

            if self.end_date is not None:
                if isinstance(self.end_date, str):
                    end_date = datetime.strptime(self.end_date, "%Y-%m-%d").date()
                else:
                    end_date = self.end_date
                conditions.append(f"date <= '{end_date}'")

            original_count = self.con.execute("SELECT COUNT(*) FROM typed_data").fetchone()[0]

            where_clause = " AND ".join(conditions) if conditions else "1=1"
            self.con.execute(f"""
                CREATE OR REPLACE TABLE filtered_data AS
                SELECT * FROM typed_data
                WHERE {where_clause}
            """)

            filtered_count = self.con.execute("SELECT COUNT(*) FROM filtered_data").fetchone()[0]

            # Log the date range used for filtering
            date_range_msg = ""
            if start_date:
                date_range_msg += f"from {start_date}"
            if end_date:
                date_range_msg += f" to {end_date}"

            self.logger.info(f"Filtered data by date range: {date_range_msg.strip()}")
            self.logger.info(
                f"Rows before filtering: {original_count}, rows after: {filtered_count}"
            )

            return True

        except Exception as e:
            self.logger.error(f"Error filtering by date: {e!s}")
            return False

    def check_for_pii(self):
        """Check for potential PII data (like CPR numbers) and replace if found."""
        try:
            # Execute DuckDB query to get a pandas DataFrame
            self.df = self.con.execute("SELECT * FROM filtered_data").fetchdf()

            pii_found = False
            for col in self.df.columns:
                # Check string columns for potential PII (10-digit numbers like CPR)
                if (
                    self.df[col].dtype == object
                    and self.df[col].astype(str).str.contains(r"\b\d{10}\b").any()
                ):
                    self.logger.warning(f"Potential PII detected in column: {col}")
                    pii_found = True
                    # Replace with deterministic UUID if found
                    if LandbrugsdataUUID:
                        self.df[col] = (
                            self.df[col]
                            .astype(str)
                            .apply(
                                lambda v: (
                                    LandbrugsdataUUID.generate_deterministic_uuid(
                                        "cvr-anonymized", str(v)
                                    )
                                    if re.match(r"\b\d{10}\b", str(v))
                                    else v
                                )
                            )
                        )
                    else:
                        # Fallback to random UUID if LandbrugsdataUUID not available
                        import uuid

                        self.df[col] = (
                            self.df[col]
                            .astype(str)
                            .apply(
                                lambda v: (
                                    str(uuid.uuid4()) if re.match(r"\b\d{10}\b", str(v)) else v
                                )
                            )
                        )

            if not pii_found:
                self.logger.info("No potential PII detected")

            return True
        except Exception as e:
            self.logger.error(f"Error checking for PII: {e!s}")
            return False

    def extract_and_save_cvr_numbers(self):
        """Extract P-numbers and map them to CVR numbers using the CVR register."""
        if not CVR_COLLECTION_AVAILABLE or save_pipeline_cvr_numbers is None:
            self.logger.info("CVR collection utility not available - skipping CVR extraction")
            return True

        try:
            self.logger.info("Extracting P-numbers for CVR mapping...")

            # Extract unique P-numbers from the data
            if "company_id" not in self.df.columns:
                self.logger.warning(
                    "No company_id (P-number) column found - skipping CVR extraction"
                )
                return True

            # Get unique P-numbers, filtering out null/invalid values
            p_numbers = self.df["company_id"].dropna().astype(str).unique()
            p_numbers = [p for p in p_numbers if p.isdigit() and len(p) >= 8]

            if not p_numbers:
                self.logger.warning("No valid P-numbers found - skipping CVR extraction")
                return True

            self.logger.info(f"Found {len(p_numbers)} unique P-numbers to map to CVR numbers")

            # Initialize CVR API client
            cvr_username = os.getenv("CVR_USERNAME", "Martin_Collignon_CVR_I_SKYEN")
            cvr_password = os.getenv("CVR_PASSWORD", "3a37d029-9588-4c00-8a09-3d2901452d45")

            cvr_client = CVRAPIClient(username=cvr_username, password=cvr_password)

            # Map P-numbers to CVR numbers using bulk CVR API
            self.logger.info(f"Using bulk P-number to CVR mapping for {len(p_numbers)} P-numbers")

            # Use bulk P-number fetching from CVR enrichment system
            pnumber_results = cvr_client.fetch_multiple_pnumbers(
                pnumbers=p_numbers,
                fetch_all_fields=False,  # Only fetch basic fields for performance
                enrich_with_geometry=False,  # Skip geocoding for performance
                batch_size=50,  # Process in batches of 50
            )

            # Extract CVR numbers from P-number results
            cvr_numbers = set()
            successful_mappings = 0

            for pnumber, pnumber_data in pnumber_results.get("results", {}).items():
                try:
                    if pnumber_data and "company_relations" in pnumber_data:
                        # Extract parent CVR number from company relations
                        for relation in pnumber_data["company_relations"]:
                            if relation.get("is_current", False) and relation.get("cvr_number"):
                                cvr_number = str(relation["cvr_number"])
                                if len(cvr_number) == 8 and cvr_number.isdigit():
                                    cvr_numbers.add(cvr_number)
                                    successful_mappings += 1
                                    self.logger.debug(
                                        f"Mapped P-number {pnumber} to CVR {cvr_number}"
                                    )
                                    break  # Use first valid current relation
                except Exception as e:
                    self.logger.debug(f"Could not extract CVR from P-number {pnumber}: {e}")
                    continue

            # Log bulk processing results
            total_processed = len(pnumber_results.get("results", {}))
            efficiency_info = pnumber_results.get("summary", {})

            self.logger.info(
                f"Bulk P-number processing completed: {successful_mappings}/{total_processed} successful mappings"
            )
            self.logger.info(
                f"Efficiency: {efficiency_info.get('api_calls', 0)} API calls for {len(p_numbers)} P-numbers "
                f"({efficiency_info.get('efficiency_gain', '1.0x')} efficiency gain)"
            )
            self.logger.info(
                f"Final result: {len(cvr_numbers)} unique CVR numbers from {len(p_numbers)} P-numbers "
                f"({len(cvr_numbers) / len(p_numbers) * 100:.1f}% success rate)"
            )

            if cvr_numbers:
                # Create P-number to CVR mapping dictionary
                pnumber_to_cvr = {}
                for pnumber, pnumber_data in pnumber_results.get("results", {}).items():
                    try:
                        if pnumber_data and "company_relations" in pnumber_data:
                            for relation in pnumber_data["company_relations"]:
                                if relation.get("is_current", False) and relation.get("cvr_number"):
                                    pnumber_to_cvr[int(pnumber)] = relation["cvr_number"]
                                    break
                    except Exception as e:
                        self.logger.debug(f"Could not map P-number {pnumber}: {e}")
                        continue

                # Add CVR numbers to the dataframe
                self.df["cvr_number"] = self.df["company_id"].map(pnumber_to_cvr)

                # Log mapping statistics
                mapped_count = self.df["cvr_number"].notna().sum()
                total_count = len(self.df)
                success_rate = mapped_count / total_count * 100
                self.logger.info(
                    f"CVR mapping results: {mapped_count}/{total_count} records "
                    f"({success_rate:.1f}%) have CVR numbers"
                )

                # Save CVR numbers using the collection utility
                cvr_storage_path = save_pipeline_cvr_numbers(
                    pipeline_name="arbejdstilsynet_inspections",
                    cvr_numbers=list(cvr_numbers),
                    storage_access=None,  # Will use default cloud storage access
                    bucket=self.storage_bucket or "landbruget-data",
                    timestamp=self.timestamp,
                )

                self.logger.info(f"CVR numbers saved to: {cvr_storage_path}")
                self.logger.info(
                    f"Arbejdstilsynet CVR collection completed: {len(cvr_numbers)} unique CVR numbers"
                )
            else:
                self.logger.warning("No CVR numbers could be mapped from P-numbers")
                # Add empty CVR column
                self.df["cvr_number"] = None

            return True

        except Exception as e:
            self.logger.error(f"Error extracting CVR numbers: {e}")
            # Don't fail the pipeline if CVR extraction fails
            return True

    def save_output(self):
        """Save the transformed data to parquet with enhanced cloud storage export."""
        try:
            # Try native cloud storage export first if OptimizedStorageAccess is available
            cloud_export_success = False
            if self.storage_bucket and OptimizedStorageAccess:
                try:
                    storage_access = OptimizedStorageAccess()
                    storage_path = f"{self.storage_bucket}/silver/arbejdstilsynet_inspections/{self.timestamp}/workplace_inspections.parquet"

                    # Convert pandas DataFrame back to DuckDB table for native export
                    temp_conn = duckdb.connect(":memory:")
                    temp_conn.register("workplace_inspections_temp", self.df)

                    # Use native cloud storage export with server-side compression
                    storage_access.export_to_storage_native(
                        connection=temp_conn,
                        table_name="workplace_inspections_temp",
                        storage_path=storage_path,
                        compression="zstd",
                    )

                    self.logger.info(f"Native cloud storage export successful: {storage_path}")
                    cloud_export_success = True
                except Exception as e:
                    self.logger.warning(
                        f"Native cloud storage export failed, using local export + upload: {e}"
                    )

            # Save to temp location first (always create local copy)
            temp_output = os.path.join(self.temp_dir, "workplace_inspections.parquet")
            self.df.to_parquet(temp_output, index=False)

            # Move to final location
            os.makedirs(os.path.dirname(self.output_parquet), exist_ok=True)
            shutil.move(temp_output, self.output_parquet)

            self.logger.info(f"Silver layer saved locally to: {self.output_parquet}")

            # Upload to Google Cloud Storage if bucket name is provided (fallback method)
            if self.storage_bucket and not cloud_export_success:
                self.upload_to_storage()

            return True
        except Exception as e:
            self.logger.error(f"Error saving output: {e!s}")
            return False

    def upload_to_storage(self):
        """Upload processed data to Google Cloud Storage."""
        try:
            if not self.storage_bucket:
                self.logger.info("No storage bucket specified, skipping upload")
                return True

            self.logger.info(f"Uploading to storage bucket: {self.storage_bucket}")
            cloud_storage = CloudStorage(bucket_name=self.storage_bucket)
            success = cloud_storage.upload_file(self.output_parquet)

            if success:
                self.logger.info("Successfully uploaded to cloud storage")
            else:
                self.logger.warning("Failed to upload to cloud storage")

            return success
        except Exception as e:
            self.logger.error(f"Error uploading to cloud storage: {e!s}")
            return False

    def generate_schema_documentation(self):
        """Generate schema documentation for the processed data."""
        try:
            self.logger.info("Generating schema documentation for arbejdstilsynet inspections")

            # Import schema documentation (with path adjustment)
            import sys
            from pathlib import Path

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

            from backend.common.schema_documentation import SchemaDocumentationManager

            # Use the pipeline start time we already have
            pipeline_start_time = self.pipeline_start_time

            # Load the parquet file into DuckDB for schema documentation
            doc_conn = duckdb.connect()

            table_name = "arbejdstilsynet_processed"
            doc_conn.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{self.output_parquet}')"
            )

            # Initialize schema documentation manager
            schema_manager = SchemaDocumentationManager(
                connection=doc_conn,
                pipeline_name="arbejdstilsynet_inspections",
                pipeline_start_time=pipeline_start_time,
                logger=self.logger,
            )

            # Generate documentation for the table
            schema_manager.generate_all_documentation([table_name], stage="silver")
            self.logger.info("Generated schema documentation for arbejdstilsynet inspections")

            # Commit to GitHub
            schema_manager.commit_to_github()
            self.logger.info("Arbejdstilsynet schema documentation committed to GitHub")

            return True

        except Exception as e:
            self.logger.error(f"Failed to generate schema documentation: {e}", exc_info=True)
            # Don't fail the pipeline if schema documentation fails
            return True

    def cleanup(self):
        """Clean up temporary resources."""
        try:
            if self.temp_dir and os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
                self.logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
            if self.con:
                self.con.close()
                self.logger.info("Closed DuckDB connection")
            return True
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e!s}")
            return False

    def run(self):
        """Run the entire silver pipeline process."""
        try:
            steps = [
                self.setup_output_directories,
                self.find_latest_bronze_data,
                self.connect_database,
                self.load_data,
                self.rename_columns,
                self.validate_column_names,
                self.deduplicate,
                self.normalize_enums,
                self.handle_null_values,
                self.cast_types,
                self.filter_by_date,
                self.check_for_pii,
                self.extract_and_save_cvr_numbers,  # Move before save_output to include CVR numbers
                self.save_output,
                self.generate_schema_documentation,
            ]

            for step in steps:
                step_name = step.__name__
                self.logger.info(f"Starting step: {step_name}")
                if not step():
                    self.logger.error(f"Pipeline failed at step: {step_name}")
                    return False

            self.logger.info("Silver pipeline completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Unexpected error in silver pipeline: {e!s}")
            return False
        finally:
            self.cleanup()


def main(start_date=None, end_date=None, storage_bucket=None, log_level="INFO"):
    """Main function to run the silver pipeline."""
    pipeline = SilverPipeline(start_date, end_date, storage_bucket, log_level)
    success = pipeline.run()

    if not success:
        # Raise an exception if the pipeline was not successful
        raise RuntimeError("Silver pipeline failed to execute.")
    # Return 0 for success if called as a module, though direct __main__ call will exit
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        if exit_code != 0:  # Should not happen if RuntimeError is raised
            exit(exit_code)  # Defensive, main() now raises
        exit(0)  # Explicit success exit
    except RuntimeError as e:
        # Logger might not be configured if __main__ is run and SilverPipeline init fails before logger setup
        # So, print to stderr as well
        logging.error(f"Silver Pipeline ERROR: {e}")
        get_pipeline_logger(__name__).error(f"Silver Pipeline execution failed: {e}", exc_info=True)
        exit(1)
    except Exception as e:
        logging.error(f"Silver Pipeline UNEXPECTED ERROR: {e}")
        get_pipeline_logger(__name__).error(
            f"Unexpected error in silver pipeline __main__: {e}", exc_info=True
        )
        exit(1)
