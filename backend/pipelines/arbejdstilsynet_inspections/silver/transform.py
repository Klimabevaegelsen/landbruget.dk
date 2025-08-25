import logging
import os
import re
import shutil
import sys
import tempfile
import uuid
from datetime import datetime

import ibis
from google.cloud import storage

# Add CVR collection import
try:
    # Import the CVR collection utility from the unified pipeline
    from pathlib import Path

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

    from unified_pipeline.util.cvr_api_client import CVRAPIClient
    from unified_pipeline.util.cvr_collection import save_pipeline_cvr_numbers

    CVR_COLLECTION_AVAILABLE = True
except ImportError as e:
    # Graceful fallback if CVR collection is not available
    save_pipeline_cvr_numbers = None
    CVRAPIClient = None
    CVR_COLLECTION_AVAILABLE = False
    logging.warning(f"CVR collection not available: {e}")


def _get_optimized_gcs_access() -> type | None:
    """
    Get optimized GCS access with robust import handling.

    Returns GCSDataAccess if available, otherwise None for fallback.
    """
    try:
        # Primary import path - should work when unified_pipeline is properly installed
        from unified_pipeline.util.gcs_access import GCSDataAccess

        logging.info("✅ Successfully imported optimized GCSDataAccess")
        return GCSDataAccess
    except ImportError as e:
        logging.warning(f"⚠️ Could not import optimized GCSDataAccess: {e}")
        logging.warning(
            "⚠️ Falling back to basic storage - ensure unified_pipeline is installed for optimal performance"
        )
        return None


# Get optimized GCS access class or None if not available
OptimizedGCSDataAccess = _get_optimized_gcs_access()


class GCSStorage:
    """Google Cloud Storage backend for arbejdstilsynet_inspections files - OPTIMIZED VERSION."""

    def __init__(self, bucket_name, prefix="silver/arbejdstilsynet_inspections"):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.is_available = self._check_gcs_available()

        # ✅ OPTIMIZED: Initialize optimized GCS access
        self.gcs_access = None
        self.use_optimized = False

        if self.is_available and OptimizedGCSDataAccess:
            try:
                self.gcs_access = OptimizedGCSDataAccess()
                self.use_optimized = True
                logging.info("✅ Arbejdstilsynet Silver GCSStorage: Initialized optimized GCS access")
            except Exception as e:
                logging.warning(f"Failed to initialize optimized GCS access: {e}")
                self.gcs_access = None
                self.use_optimized = False

    def _check_gcs_available(self) -> bool:
        """Check if GCS is available (Google Cloud Storage library is installed)."""
        try:
            _ = storage
            return True
        except (ImportError, NameError):
            logging.warning("Google Cloud Storage library not available. Using local storage only.")
            return False

    def upload_file(self, local_path, gcs_path=None) -> bool:
        """Upload a file to GCS bucket using optimized streaming."""
        if not self.is_available:
            logging.warning("GCS not available, skipping upload")
            return False

        if gcs_path is None:
            # Use the timestamp from the local path
            timestamp = os.path.basename(os.path.dirname(local_path))
            filename = os.path.basename(local_path)
            gcs_path = f"{self.prefix}/{timestamp}/{filename}"

        try:
            # ✅ OPTIMIZED: Use streaming upload if available
            if self.use_optimized and self.gcs_access:
                full_gcs_path = f"gs://{self.bucket_name}/{gcs_path}"

                # Stream file directly without loading into memory
                with open(local_path, "rb") as file_obj:
                    with self.gcs_access.fs.open(full_gcs_path, "wb") as gcs_file:
                        import shutil

                        shutil.copyfileobj(file_obj, gcs_file)

                logging.info(f"✅ Uploaded {local_path} to {full_gcs_path} (optimized streaming)")
                return True
            else:
                # Fallback to old method if optimized access failed
                client = storage.Client()
                bucket = client.bucket(self.bucket_name)
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(local_path)
                logging.info(f"Uploaded {local_path} to gs://{self.bucket_name}/{gcs_path} (fallback)")
                return True

        except Exception as e:
            logging.error(f"Failed to upload to GCS: {e}")
            return False


class SilverPipeline:
    """
    A class to handle the silver layer transformations for arbejdstilsynet_inspections data.
    Transforms raw CSV data into cleaned and structured parquet format.
    """

    def __init__(self, start_date=None, end_date=None, gcs_bucket=None, log_level="INFO"):
        """Initialize the Silver Pipeline with paths, constants, and logging setup."""
        # Setup logging
        logging.basicConfig(
            level=getattr(logging, log_level),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger("SilverPipeline")

        # Store parameters
        self.start_date = start_date
        self.end_date = end_date
        self.gcs_bucket = gcs_bucket

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

        # DuckDB connection via Ibis
        self.con = None
        self.raw = None
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
            self.logger.error(f"Error creating output directories: {str(e)}")
            return False

    def find_latest_bronze_data(self) -> bool:
        """Find the latest bronze directory and CSV file."""
        try:
            self.logger.info(f"Looking for bronze data in: {self.data_root}")
            bronze_dirs = [d for d in os.listdir(self.data_root) if os.path.isdir(os.path.join(self.data_root, d))]

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
            self.logger.error(f"Error finding latest bronze data: {str(e)}")
            return False

    def connect_database(self) -> bool:
        """Connect to DuckDB via Ibis."""
        try:
            self.con = ibis.duckdb.connect("data.ddb")
            self.logger.info("Connected to DuckDB via Ibis")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to DuckDB: {str(e)}")
            return False

    def load_data(self) -> bool:
        """Load CSV data using Ibis."""
        try:
            self.raw = self.con.read_csv(self.input_csv)
            self.logger.info(f"Loaded data with {len(self.raw.columns)} columns")
            return True
        except Exception as e:
            self.logger.error(f"Error loading data from CSV: {str(e)}")
            return False

    def rename_columns(self) -> bool:
        """Rename columns according to conventions."""
        try:
            # Stepwise renaming to avoid IbisTypeError
            for old, new in self.column_rename.items():
                if old in self.raw.columns and new not in self.raw.columns:
                    # Use Ibis rename with new_name=old_name as kwargs
                    self.raw = self.raw.rename(**{new: old})

            self.logger.info(f"Columns after rename: {self.raw.columns}")
            return True
        except Exception as e:
            self.logger.error(f"Error renaming columns: {str(e)}")
            return False

    def validate_column_names(self) -> bool:
        """Validate column names against conventions."""
        try:
            for name in self.raw.columns:
                if len(name.split("_")) > 5:
                    self.logger.error(f"Column name '{name}' exceeds 5-word limit.")
                    return False
                if not name.islower():
                    self.logger.error(f"Column name '{name}' must be lowercase.")
                    return False

            self.logger.info("All column names validated successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error validating column names: {str(e)}")
            return False

    def deduplicate(self):
        """Remove duplicate rows."""
        try:
            original_rows = self.raw.count().execute()
            self.raw = self.raw.distinct()
            new_rows = self.raw.count().execute()

            if original_rows > new_rows:
                self.logger.info(f"Removed {original_rows - new_rows} duplicate rows")
            else:
                self.logger.info("No duplicate rows found")

            return True
        except Exception as e:
            self.logger.error(f"Error removing duplicates: {str(e)}")
            return False

    def normalize_enum_ibis(self, col):
        """Normalize a column's values by replacing Danish characters and standardizing case."""
        return col.cast("string").lower().replace("æ", "ae").replace("ø", "oe").replace("å", "aa").strip()

    def normalize_enums(self):
        """Normalize enums and special characters."""
        try:
            for field in [
                "decision",
                "work_env_issue",
                "appealed",
                "complied",
                "industry",
            ]:
                if field in self.raw.columns:
                    self.raw = self.raw.mutate(**{field: self.normalize_enum_ibis(self.raw[field])})

            self.logger.info("Normalized enum fields")
            return True
        except Exception as e:
            self.logger.error(f"Error normalizing enums: {str(e)}")
            return False

    def handle_null_values(self):
        """Convert empty strings to null values."""
        try:
            for field in self.raw.columns:
                if str(self.raw[field].type()) == "string":
                    self.raw = self.raw.mutate(
                        **{
                            field: ibis.cases(
                                (
                                    self.raw[field].isnull() | (self.raw[field].cast("string") == ""),
                                    None,
                                ),
                                else_=self.raw[field],
                            )
                        }
                    )

            self.logger.info("Handled null values in string columns")
            return True
        except Exception as e:
            self.logger.error(f"Error handling null values: {str(e)}")
            return False

    def cast_types(self):
        """Cast columns to appropriate types."""
        try:
            mutate_dict = {}

            if "date" in self.raw.columns:
                mutate_dict["date"] = self.raw.date.cast("date")

            if "case_count" in self.raw.columns:
                mutate_dict["case_count"] = self.raw.case_count.cast("int64")

            if "company_id" in self.raw.columns:
                mutate_dict["company_id"] = self.raw.company_id.cast("int64")

            if "appealed" in self.raw.columns:
                mutate_dict["appealed"] = (self.raw.appealed == "paaklaget").ifelse(1, 0)

            if "complied" in self.raw.columns:
                mutate_dict["complied"] = (self.raw.complied == "efterkommet").ifelse(1, 0)

            if mutate_dict:
                self.raw = self.raw.mutate(**mutate_dict)
                self.logger.info("Cast columns to appropriate types")

            return True
        except Exception as e:
            self.logger.error(f"Error casting types: {str(e)}")
            return False

    def filter_by_date(self):
        """Filter data by date range if start_date or end_date are provided."""
        try:
            if self.start_date is None and self.end_date is None:
                self.logger.info("No date range provided, skipping date filtering")
                return True

            if "date" not in self.raw.columns:
                self.logger.warning("Date column not found, cannot filter by date range")
                return False

            # Build the filter expression based on available date parameters
            filter_expr = None
            start_date = None
            end_date = None

            # Handle start_date if provided
            if self.start_date is not None:
                if isinstance(self.start_date, str):
                    start_date = datetime.strptime(self.start_date, "%Y-%m-%d").date()
                else:
                    start_date = self.start_date
                filter_expr = self.raw.date >= start_date

            # Handle end_date if provided
            if self.end_date is not None:
                if isinstance(self.end_date, str):
                    end_date = datetime.strptime(self.end_date, "%Y-%m-%d").date()
                else:
                    end_date = self.end_date

                # Add to existing expression or create new one
                if filter_expr is not None:
                    filter_expr = filter_expr & (self.raw.date <= end_date)
                else:
                    filter_expr = self.raw.date <= end_date

            # Apply the filter if we have any date constraints
            if filter_expr is not None:
                original_count = self.raw.count().execute()
                self.raw = self.raw.filter(filter_expr)
                filtered_count = self.raw.count().execute()

                # Log the date range used for filtering
                date_range_msg = ""
                if start_date:
                    date_range_msg += f"from {start_date}"
                if end_date:
                    date_range_msg += f" to {end_date}"

                self.logger.info(f"Filtered data by date range: {date_range_msg.strip()}")
                self.logger.info(f"Rows before filtering: {original_count}, rows after: {filtered_count}")

            return True

        except Exception as e:
            self.logger.error(f"Error filtering by date: {str(e)}")
            return False

    def check_for_pii(self):
        """Check for potential PII data (like CPR numbers) and replace if found."""
        try:
            # Execute Ibis expression to get a pandas DataFrame
            self.df = self.raw.execute()

            pii_found = False
            for col in self.df.columns:
                if self.df[col].dtype == object:  # Check string columns
                    if self.df[col].astype(str).str.contains(r"\b\d{10}\b").any():
                        self.logger.warning(f"⚠️ Potential PII detected in column: {col}")
                        pii_found = True
                        # Replace with UUIDv4 if found
                        self.df[col] = (
                            self.df[col]
                            .astype(str)
                            .apply(lambda v: str(uuid.uuid4()) if re.match(r"\b\d{10}\b", str(v)) else v)
                        )

            if not pii_found:
                self.logger.info("No potential PII detected")

            return True
        except Exception as e:
            self.logger.error(f"Error checking for PII: {str(e)}")
            return False

    def extract_and_save_cvr_numbers(self):
        """Extract P-numbers and map them to CVR numbers using the CVR register."""
        if not CVR_COLLECTION_AVAILABLE or save_pipeline_cvr_numbers is None:
            self.logger.info("ℹ️ CVR collection utility not available - skipping CVR extraction")
            return True

        try:
            self.logger.info("🔍 Extracting P-numbers for CVR mapping...")

            # Extract unique P-numbers from the data
            if "company_id" not in self.df.columns:
                self.logger.warning("⚠️ No company_id (P-number) column found - skipping CVR extraction")
                return True

            # Get unique P-numbers, filtering out null/invalid values
            p_numbers = self.df["company_id"].dropna().astype(str).unique()
            p_numbers = [p for p in p_numbers if p.isdigit() and len(p) >= 8]

            if not p_numbers:
                self.logger.warning("⚠️ No valid P-numbers found - skipping CVR extraction")
                return True

            self.logger.info(f"📋 Found {len(p_numbers)} unique P-numbers to map to CVR numbers")

            # Initialize CVR API client
            cvr_username = os.getenv("CVR_USERNAME", "Martin_Collignon_CVR_I_SKYEN")
            cvr_password = os.getenv("CVR_PASSWORD", "3a37d029-9588-4c00-8a09-3d2901452d45")

            cvr_client = CVRAPIClient(username=cvr_username, password=cvr_password)

            # Map P-numbers to CVR numbers using bulk CVR API
            self.logger.info(f"🚀 Using bulk P-number to CVR mapping for {len(p_numbers)} P-numbers")

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
                                    self.logger.debug(f"✅ Mapped P-number {pnumber} to CVR {cvr_number}")
                                    break  # Use first valid current relation
                except Exception as e:
                    self.logger.debug(f"⚠️ Could not extract CVR from P-number {pnumber}: {e}")
                    continue

            # Log bulk processing results
            total_processed = len(pnumber_results.get("results", {}))
            efficiency_info = pnumber_results.get("summary", {})

            self.logger.info(
                f"🎯 Bulk P-number processing completed: {successful_mappings}/{total_processed} successful mappings"
            )
            self.logger.info(
                f"📊 Efficiency: {efficiency_info.get('api_calls', 0)} API calls for {len(p_numbers)} P-numbers "
                f"({efficiency_info.get('efficiency_gain', '1.0x')} efficiency gain)"
            )
            self.logger.info(
                f"✅ Final result: {len(cvr_numbers)} unique CVR numbers from {len(p_numbers)} P-numbers "
                f"({len(cvr_numbers)/len(p_numbers)*100:.1f}% success rate)"
            )

            if cvr_numbers:
                # Save CVR numbers using the collection utility
                cvr_gcs_path = save_pipeline_cvr_numbers(
                    pipeline_name="arbejdstilsynet_inspections",
                    cvr_numbers=list(cvr_numbers),
                    gcs_access=None,  # Will use default GCS access
                    bucket=self.gcs_bucket or "landbrugsdata-raw-data",
                    timestamp=self.timestamp,
                )

                self.logger.info(f"📋 CVR numbers saved to: {cvr_gcs_path}")
                self.logger.info(f"✅ Arbejdstilsynet CVR collection completed: {len(cvr_numbers)} CVR numbers")
            else:
                self.logger.warning("⚠️ No CVR numbers could be mapped from P-numbers")

            return True

        except Exception as e:
            self.logger.error(f"❌ Error extracting CVR numbers: {e}")
            # Don't fail the pipeline if CVR extraction fails
            return True

    def save_output(self):
        """Save the transformed data to parquet with enhanced GCS export."""
        try:
            # 🚀 ENHANCED: Try native GCS export first if OptimizedGCSDataAccess is available
            gcs_export_success = False
            if self.gcs_bucket and OptimizedGCSDataAccess:
                try:
                    gcs_access = OptimizedGCSDataAccess()
                    gcs_path = f"gs://{self.gcs_bucket}/silver/arbejdstilsynet_inspections/{self.timestamp}/workplace_inspections.parquet"

                    # Convert pandas DataFrame back to DuckDB table for native export
                    import duckdb

                    temp_conn = duckdb.connect(":memory:")
                    temp_conn.register("workplace_inspections_temp", self.df)

                    # Use native GCS export with server-side compression
                    gcs_access.export_to_gcs_native(
                        connection=temp_conn,
                        table_name="workplace_inspections_temp",
                        gcs_path=gcs_path,
                        compression="zstd",
                    )

                    self.logger.info(f"✅ Native GCS export successful: {gcs_path}")
                    gcs_export_success = True
                except Exception as e:
                    self.logger.warning(f"Native GCS export failed, using local export + upload: {e}")

            # Save to temp location first (always create local copy)
            temp_output = os.path.join(self.temp_dir, "workplace_inspections.parquet")
            self.df.to_parquet(temp_output, index=False)

            # Move to final location
            os.makedirs(os.path.dirname(self.output_parquet), exist_ok=True)
            shutil.move(temp_output, self.output_parquet)

            self.logger.info(f"✅ Silver layer saved locally to: {self.output_parquet}")

            # Upload to Google Cloud Storage if bucket name is provided (fallback method)
            if self.gcs_bucket and not gcs_export_success:
                self.upload_to_gcs()

            return True
        except Exception as e:
            self.logger.error(f"Error saving output: {str(e)}")
            return False

    def upload_to_gcs(self):
        """Upload processed data to Google Cloud Storage."""
        try:
            if not self.gcs_bucket:
                self.logger.info("No GCS bucket specified, skipping upload")
                return True

            self.logger.info(f"Uploading to GCS bucket: {self.gcs_bucket}")
            gcs = GCSStorage(bucket_name=self.gcs_bucket)
            success = gcs.upload_file(self.output_parquet)

            if success:
                self.logger.info("✅ Successfully uploaded to GCS")
            else:
                self.logger.warning("⚠️ Failed to upload to GCS")

            return success
        except Exception as e:
            self.logger.error(f"Error uploading to GCS: {str(e)}")
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
            import duckdb

            doc_conn = duckdb.connect()

            table_name = "arbejdstilsynet_processed"
            doc_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{self.output_parquet}')")

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
            return True
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
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
                self.save_output,
                self.extract_and_save_cvr_numbers,
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
            self.logger.error(f"Unexpected error in silver pipeline: {str(e)}")
            return False
        finally:
            self.cleanup()


def main(start_date=None, end_date=None, gcs_bucket=None, log_level="INFO"):
    """Main function to run the silver pipeline."""
    pipeline = SilverPipeline(start_date, end_date, gcs_bucket, log_level)
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
        logging.getLogger(__name__).error(f"Silver Pipeline execution failed: {e}", exc_info=True)
        exit(1)
    except Exception as e:
        logging.error(f"Silver Pipeline UNEXPECTED ERROR: {e}")
        logging.getLogger(__name__).error(f"Unexpected error in silver pipeline __main__: {e}", exc_info=True)
        exit(1)
