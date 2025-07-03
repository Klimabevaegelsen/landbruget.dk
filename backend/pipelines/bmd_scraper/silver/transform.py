"""
Transform raw BMD Excel data into cleaned, structured Parquet format.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import duckdb

# Configure module logger
logger = logging.getLogger("bmd_pipeline.silver.transform")


def _get_optimized_gcs_access():
    """
    Get optimized GCS access with robust import handling.

    Returns GCSDataAccess if available, otherwise None for fallback.
    """
    try:
        # Primary import path - should work when unified_pipeline is properly installed
        from unified_pipeline.util.gcs_access import GCSDataAccess

        logger.info("✅ Successfully imported optimized GCSDataAccess")
        return GCSDataAccess
    except ImportError as e:
        logger.warning(f"⚠️ Could not import optimized GCSDataAccess: {e}")
        logger.warning("⚠️ Falling back to basic storage - ensure unified_pipeline is installed for optimal performance")
        return None


# Get optimized GCS access class or None if not available
OptimizedGCSDataAccess = _get_optimized_gcs_access()


class OptimizedGCSStorage:
    """Optimized GCS storage for BMD silver layer with fallback support."""

    def __init__(self, bucket_name: str, prefix: str = "silver/bmd"):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.use_optimized = False

        # Try to use optimized storage
        if OptimizedGCSDataAccess:
            try:
                self.gcs_access = OptimizedGCSDataAccess()
                self.use_optimized = True
                logger.info(f"✅ BMD Silver: Using optimized GCS access for bucket: {bucket_name}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to initialize optimized GCS access: {e}")
                self._init_fallback(bucket_name)
        else:
            self._init_fallback(bucket_name)

    def _init_fallback(self, bucket_name: str):
        """Initialize fallback GCS storage."""
        try:
            from google.cloud import storage

            self.gcs_client = storage.Client()
            self.gcs_bucket = self.gcs_client.bucket(bucket_name)
            logger.info(f"✅ BMD Silver: Using fallback GCS for bucket: {bucket_name}")
        except ImportError:
            raise ImportError("google-cloud-storage is required but not available")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize GCS storage: {e}")

    def upload_file(self, local_path: Path, gcs_path: str = None) -> bool:
        """Upload file to GCS with optimized or fallback method."""
        try:
            if gcs_path is None:
                timestamp = local_path.parent.name
                filename = local_path.name
                gcs_path = f"{self.prefix}/{timestamp}/{filename}"

            if self.use_optimized:
                # Use optimized upload
                full_gcs_path = f"gs://{self.bucket_name}/{gcs_path}"
                with open(local_path, "rb") as file_obj:
                    with self.gcs_access.fs.open(full_gcs_path, "wb") as gcs_file:
                        import shutil

                        shutil.copyfileobj(file_obj, gcs_file)
                logger.info(f"✅ Uploaded {local_path} to {full_gcs_path} (optimized)")
            else:
                # Use fallback upload
                blob = self.gcs_bucket.blob(gcs_path)
                blob.upload_from_filename(str(local_path))
                logger.info(f"✅ Uploaded {local_path} to gs://{self.bucket_name}/{gcs_path} (fallback)")

            return True

        except Exception as e:
            logger.error(f"Failed to upload to GCS: {e}")
            return False


class BMDTransformer:
    """
    Transforms raw BMD Excel data into a clean, structured format using DuckDB.

    This class handles:
    - Reading raw Excel data with DuckDB
    - Cleaning and normalizing fields with SQL
    - Type casting with SQL
    - Data validation
    - PFAS detection based on active ingredients
    - Saving to Parquet format
    """

    def __init__(self, input_file: Path, output_dir: Path):
        """
        Initialize the transformer.

        Args:
            input_file: Path to raw Excel file from Bronze stage
            output_dir: Directory to save processed Parquet file
        """
        self.input_file = input_file
        self.output_dir = output_dir
        self.timestamp = input_file.parent.name
        # Get pipeline start time from timestamp directory name
        try:
            self.pipeline_start_time = datetime.strptime(self.timestamp, "%Y%m%d_%H%M%S")
        except ValueError:
            # Fallback if timestamp format is unexpected
            self.pipeline_start_time = datetime.now()
        self.metadata = {}
        self.conn = None

        # Try to load metadata from bronze stage
        metadata_path = input_file.parent / "metadata.json"
        if metadata_path.exists():
            with open(metadata_path, "r", encoding="utf-8") as f:
                self.metadata = json.load(f)
                logger.info(f"Loaded bronze metadata from {metadata_path}")

        # Create mapping for status fields normalization
        self.status_mapping = {
            # Danish status values to standardized values
            "Godkendt": "approved",
            "Udgået": "expired",
            "Tilladt": "permitted",
            "Ikke godkendt": "not_approved",
            # Add more mappings as needed
        }

        # PFAS active ingredients list from https://www.dn.dk/media/115884/forbrug-af-pfas-pesticider-23-24.pdf
        self.pfas_active_ingredients = {
            "fluazinam",
            "fluopyram",
            "diflufenican",
            "mefentrifluconazol",
            "tau-fluvalinat",
            "lambda-cyhalothrin",
            "pyroxsulam",
            "oxathiapiprolin",
            "flonicamid",
            "fludioxonil",
            "triflusulfuron-methyl",
            "gamma-cyhalothrin",
            "picolinafen",
        }

        # Create the DuckDB connection to be used throughout the transformation
        self.conn = duckdb.connect(database=":memory:")

    def __del__(self):
        """Clean up DuckDB connection on object destruction."""
        if self.conn is not None:
            try:
                self.conn.close()
            except:
                pass

    def read_excel(self) -> str:
        """
        Read the raw Excel file into a DuckDB table.

        Returns:
            Name of the DuckDB table with the raw data
        """
        logger.info(f"Reading Excel file from {self.input_file}")
        try:
            # Read Excel file directly into DuckDB table
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE raw_data AS
                SELECT * FROM read_xlsx('{self.input_file}', range = 'A4:AR', stop_at_empty = true, header=true);
            """)

            # Get basic information about the table
            result = self.conn.execute("SELECT COUNT(*) as row_count FROM raw_data").fetchone()
            row_count = result[0]

            result = self.conn.execute("SELECT * FROM raw_data LIMIT 1").description
            columns = [desc[0] for desc in result]

            logger.info(f"Read {row_count} rows and {len(columns)} columns")

            # Store column information in metadata
            self.silver_metadata = {
                "transform_timestamp": self.pipeline_start_time.isoformat(),
                "source_file": str(self.input_file),
                "bronze_timestamp": self.timestamp,
                "row_count": row_count,
                "column_count": len(columns),
                "columns": columns,
            }

            return "raw_data"

        except Exception as e:
            logger.exception(f"Error reading Excel file: {e}")
            raise

    def clean_column_names(self, table_name: str) -> str:
        """
        Standardize column names to lowercase with underscores.

        Args:
            table_name: Name of the DuckDB table with raw data

        Returns:
            Name of the DuckDB table with standardized column names
        """
        logger.info("Cleaning column names")

        try:
            # Get the current columns
            result = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 1").description
            columns = [desc[0] for desc in result]

            # Create a mapping for column names
            column_mapping = {}
            for col in columns:
                # Convert to lowercase, replace spaces with underscores
                clean_col = col.lower().strip().replace(" ", "_")
                # Remove special characters
                clean_col = "".join(c if c.isalnum() or c == "_" else "_" for c in clean_col)
                # Remove consecutive underscores
                while "__" in clean_col:
                    clean_col = clean_col.replace("__", "_")
                # Remove leading/trailing underscores
                clean_col = clean_col.strip("_")

                column_mapping[col] = clean_col

            # Create SQL for column renaming
            column_selects = []
            for original, clean in column_mapping.items():
                column_selects.append(f'"{original}" AS {clean}')

            # Create a new table with cleaned column names
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE cleaned_columns AS
                SELECT {", ".join(column_selects)}
                FROM {table_name};
            """)

            # Log the column mapping
            logger.info(f"Column mapping: {column_mapping} Column mapping lens: {len(column_mapping)}")

            return "cleaned_columns"

        except Exception as e:
            logger.exception(f"Error cleaning column names: {e}")
            raise

    def handle_semicolon_lists(self, table_name: str) -> str:
        """
        Convert semicolon-separated values in columns to arrays for better querying.

        Args:
            table_name: Name of the DuckDB table to process

        Returns:
            Name of the DuckDB table with arrays for semicolon-separated values
        """
        logger.info("Converting semicolon-separated lists to arrays")

        try:
            # Get column names
            result = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 1").description
            columns = [desc[0] for desc in result]

            # Identify potential list columns
            list_columns = []
            potential_list_columns = [
                "bruger_pesticid",
                "bruger_biocid",
                "mindre_anvendelse_nr",
                "mindre_anvendelse_godkendelsesindehaver",
            ]

            # Check if cleaned column names match any potential list columns
            for col in columns:
                clean_col = col.lower().replace(" ", "_").replace("-", "_")
                if clean_col in potential_list_columns or ";" in str(
                    self.conn.execute(f"SELECT {col} FROM {table_name} LIMIT 1").fetchone()[0]
                ):
                    # Sample a few rows to confirm semicolon presence
                    sample = self.conn.execute(f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                        WHERE {col} LIKE '%;%'
                        LIMIT 10
                    """).fetchone()[0]

                    if sample > 0:
                        list_columns.append(col)
                        logger.info(f"Detected semicolon-separated list in column: {col}")

            if not list_columns:
                # No list columns found, return the original table
                logger.info("No semicolon-separated lists found to convert")
                return table_name

            # Create SQL for converting semicolon lists to arrays
            array_conversions = []
            for col in columns:
                if col in list_columns:
                    # Split the column by semicolon and convert to an array
                    array_conversions.append(f"""
                        CASE
                            WHEN {col} IS NULL OR {col} = '' THEN NULL
                            ELSE string_split({col}, ';')
                        END AS {col}
                    """)
                else:
                    array_conversions.append(col)

            # Create a table with array conversions
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE array_converted AS
                SELECT {", ".join(array_conversions)}
                FROM {table_name};
            """)

            # Store metadata about array conversions
            self.silver_metadata["array_conversions"] = {
                "columns": list_columns,
                "description": "Semicolon-separated values converted to arrays",
            }

            return "array_converted"

        except Exception as e:
            logger.exception(f"Error converting semicolon lists to arrays: {e}")
            raise

    def clean_data(self, table_name: str) -> str:
        """
        Clean and normalize the data.

        Args:
            table_name: Name of the DuckDB table to clean

        Returns:
            Name of the DuckDB table with cleaned data
        """
        logger.info("Cleaning and normalizing data")

        try:
            # Get column names
            result = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 1").description
            columns = [desc[0] for desc in result]

            # Identify text columns for trimming
            text_columns = []
            for col in columns:
                col_type = self.conn.execute(f"SELECT typeof({col}) FROM {table_name} LIMIT 1").fetchone()[0]
                if col_type.lower() in ["varchar", "string", "text"]:
                    text_columns.append(col)

            # Create SQL for trimming text columns
            trims = []
            for col in columns:
                if col in text_columns:
                    trims.append(f"TRIM({col}) AS {col}")
                else:
                    trims.append(col)
            logger.info(f"Trims {len(trims)}")
            # Create a table with trimmed text
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE trimmed_data AS
                SELECT {", ".join(trims)}
                FROM {table_name};
            """)

            normalized_table = "trimmed_data"

            # Handle semicolon-separated lists in a separate function
            list_processed_table = self.handle_semicolon_lists(normalized_table)

            # Drop duplicates
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE cleaned_data AS
                SELECT DISTINCT *
                FROM {list_processed_table};
            """)

            # Check for dropped duplicates
            orig_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            new_count = self.conn.execute("SELECT COUNT(*) FROM cleaned_data").fetchone()[0]
            if orig_count > new_count:
                logger.warning(f"Removed {orig_count - new_count} duplicate rows")

            return "cleaned_data"

        except Exception as e:
            logger.exception(f"Error cleaning data: {e}")
            raise

    def parse_dates(self, table_name: str) -> str:
        """
        Parse and standardize date columns.

        Args:
            table_name: Name of the DuckDB table

        Returns:
            Name of the DuckDB table with date columns parsed
        """
        logger.info("Parsing date columns")

        try:
            # Get column names
            result = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 1").description
            columns = [desc[0] for desc in result]

            # Identify potential date columns
            date_columns = []
            for col in columns:
                if any(
                    date_term in col.lower()
                    for date_term in ["date", "dato", "godkendelsesdato", "udløbsdato", "expiry"]
                ):
                    date_columns.append(col)

            # If we have date columns, parse them
            if date_columns:
                # Create SQL for date parsing
                date_casts = []
                for col in columns:
                    if col in date_columns:
                        # Try to cast to DATE
                        date_casts.append(f"TRY_CAST({col} AS DATE) AS {col}")
                    else:
                        date_casts.append(col)

                # Create a table with parsed dates
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE date_parsed AS
                    SELECT {", ".join(date_casts)}
                    FROM {table_name};
                """)

                # Log the date columns found
                logger.info(f"Parsed date columns: {date_columns}")

                return "date_parsed"
            else:
                logger.info("No date columns found to parse")
                return table_name

        except Exception as e:
            logger.exception(f"Error parsing dates: {e}")
            raise

    def add_pfas_indicator(self, table_name: str) -> str:
        """
        Add a PFAS indicator column based on active ingredients.

        Args:
            table_name: Name of the DuckDB table to process

        Returns:
            Name of the DuckDB table with PFAS indicator column added
        """
        logger.info("Adding PFAS indicator column based on active ingredients")

        try:
            # Get column names to find the active ingredients column
            result = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 1").description
            columns = [desc[0] for desc in result]

            # Find the active ingredients column (should be aktivstofnavn_e after cleaning)
            active_ingredient_col = None
            for col in columns:
                if any(term in col.lower() for term in ["aktivstofnavn", "active_ingredient", "ingredient_name"]):
                    active_ingredient_col = col
                    break

            # If not found with specific terms, check for column containing both "navn" and "aktivstof"
            if not active_ingredient_col:
                for col in columns:
                    if "navn" in col.lower() and "aktivstof" in col.lower():
                        active_ingredient_col = col
                        break

            if not active_ingredient_col:
                logger.warning("No active ingredient column found, skipping PFAS detection")
                return table_name

            logger.info(f"Using column '{active_ingredient_col}' for PFAS detection")

            # Create PFAS detection SQL using LIKE patterns for each PFAS ingredient
            pfas_conditions = []
            for pfas_ingredient in self.pfas_active_ingredients:
                # Use LOWER and LIKE for case-insensitive partial matching
                pfas_conditions.append(f"LOWER({active_ingredient_col}) LIKE '%{pfas_ingredient.lower()}%'")

            pfas_check_sql = " OR ".join(pfas_conditions)

            # Create new table with PFAS indicator
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE pfas_enhanced AS
                SELECT *,
                    CASE 
                        WHEN {active_ingredient_col} IS NULL OR {active_ingredient_col} = '' THEN NULL
                        WHEN {pfas_check_sql} THEN true
                        ELSE false
                    END AS contains_pfas
                FROM {table_name};
            """)

            # Log PFAS detection statistics
            pfas_count = self.conn.execute("""
                SELECT COUNT(*) 
                FROM pfas_enhanced 
                WHERE contains_pfas = true
            """).fetchone()[0]

            total_count = self.conn.execute("SELECT COUNT(*) FROM pfas_enhanced").fetchone()[0]

            logger.info(
                f"PFAS detection complete: {pfas_count} out of {total_count} products contain PFAS ({pfas_count / total_count:.1%})"
            )

            # Store PFAS detection metadata
            self.silver_metadata["pfas_detection"] = {
                "pfas_ingredients_checked": list(self.pfas_active_ingredients),
                "active_ingredient_column": active_ingredient_col,
                "pfas_products_count": pfas_count,
                "total_products_count": total_count,
                "pfas_percentage": round(pfas_count / total_count * 100, 2) if total_count > 0 else 0,
            }

            return "pfas_enhanced"

        except Exception as e:
            logger.exception(f"Error adding PFAS indicator: {e}")
            raise

    def validate_data(self, table_name: str) -> Tuple[str, Dict[str, List[str]]]:
        """
        Validate the cleaned data and report issues.

        Args:
            table_name: Name of the DuckDB table

        Returns:
            Tuple of (table_name, validation issues)
        """
        logger.info("Validating data")

        validation_issues = {}

        try:
            # Get column names
            result = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 1").description
            columns = [desc[0] for desc in result]

            # Get row count
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

            # Check for missing values
            missing_values = []
            for col in columns:
                null_count = self.conn.execute(f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE {col} IS NULL
                """).fetchone()[0]

                if null_count > 0:
                    missing_values.append(f"{col}: {null_count} missing values ({null_count / row_count:.1%})")
                    logger.warning(f"Column {col} has {null_count} missing values ({null_count / row_count:.1%})")

            if missing_values:
                validation_issues["missing_values"] = missing_values

            # TODO: check outlier later

            # Store validation results in metadata
            self.silver_metadata["validation"] = {"has_issues": bool(validation_issues), "issues": validation_issues}

            return table_name, validation_issues

        except Exception as e:
            logger.exception(f"Error validating data: {e}")
            raise

    def save_parquet(self, table_name: str) -> Path:
        """
        Save the processed data as a Parquet file.

        Args:
            table_name: Name of the DuckDB table

        Returns:
            Path to the saved Parquet file
        """
        # Create the output filename with descriptive name (no timestamp - that's in the folder)
        output_filename = "pesticide_products.parquet"
        output_path = self.output_dir / output_filename

        logger.info(f"Saving Parquet file to {output_path}")

        try:
            # Export directly from DuckDB to Parquet
            self.conn.execute(f"""
                COPY (SELECT * FROM {table_name})
                TO '{output_path}' (FORMAT 'parquet')
            """)

            self.conn.execute(f"""
                COPY (SELECT * FROM {table_name})
                TO '{self.output_dir / "pesticide_products.xlsx"}' (FORMAT 'xlsx')
            """)
            # Save metadata file
            metadata_path = self.output_dir / "metadata.json"
            self.silver_metadata["output_file"] = str(output_path)
            self.silver_metadata["output_size_bytes"] = os.path.getsize(output_path)

            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(self.silver_metadata, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved metadata to {metadata_path}")

            return output_path

        except Exception as e:
            logger.exception(f"Error saving parquet: {e}")
            raise

    def transform(self) -> Path:
        """
        Execute the full transformation process.

        Returns:
            Path to the processed Parquet file
        """
        logger.info(f"Starting transformation of {self.input_file}")

        try:
            # 1. Read the Excel file into DuckDB
            table_name = self.read_excel()

            # 2. Clean column names
            table_name = self.clean_column_names(table_name)

            # 3. Clean and normalize data
            table_name = self.clean_data(table_name)

            # 4. Parse dates
            table_name = self.parse_dates(table_name)

            # 5. Add PFAS indicator
            table_name = self.add_pfas_indicator(table_name)

            # 6. Validate data
            table_name, validation_issues = self.validate_data(table_name)

            # 7. Save to Parquet
            output_path = self.save_parquet(table_name)

            logger.info(f"Transformation completed successfully: {output_path}")
            return output_path

        finally:
            # Close the DuckDB connection
            if self.conn is not None:
                self.conn.close()
                self.conn = None


def upload_to_gcs(local_path: Path, bucket_name: str) -> bool:
    """
    Upload silver data to Google Cloud Storage.

    Args:
        local_path: Path to the local file
        bucket_name: GCS bucket name

    Returns:
        True if upload successful, False otherwise
    """
    try:
        storage = OptimizedGCSStorage(bucket_name=bucket_name, prefix="silver/bmd")

        # Upload the Parquet file
        success = storage.upload_file(local_path)

        # Upload the metadata file
        metadata_path = local_path.parent / "metadata.json"
        if metadata_path.exists():
            storage.upload_file(metadata_path)

        return success
    except Exception as e:
        logger.exception(f"Error uploading to GCS: {e}")
        return False
