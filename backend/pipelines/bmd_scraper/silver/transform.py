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
from bronze.export import GCSStorage

# Configure module logger
logger = logging.getLogger("bmd_pipeline.silver.transform")


class BMDTransformer:
    """
    Transforms raw BMD Excel data into a clean, structured format using DuckDB.

    This class handles:
    - Reading raw Excel data with DuckDB
    - Cleaning and normalizing fields with SQL
    - Type casting with SQL
    - Data validation
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
                "transform_timestamp": datetime.now().isoformat(),
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
                SELECT {', '.join(column_selects)}
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
                SELECT {', '.join(array_conversions)}
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
                SELECT {', '.join(trims)}
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
                    SELECT {', '.join(date_casts)}
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
                    missing_values.append(f"{col}: {null_count} missing values ({null_count/row_count:.1%})")
                    logger.warning(f"Column {col} has {null_count} missing values ({null_count/row_count:.1%})")

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
        # Create the output filename with timestamp
        output_filename = f"bmd_data_{self.timestamp}.parquet"
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
                TO '{self.output_dir/f"bmd_data_{self.timestamp}.xlsx"}' (FORMAT 'xlsx')
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

            # 5. Validate data
            table_name, validation_issues = self.validate_data(table_name)

            # 6. Save to Parquet
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
        storage = GCSStorage(bucket_name=bucket_name, prefix="silver/bmd")

        # Upload the Parquet file
        success = storage.upload_file(str(local_path))

        # Upload the metadata file
        metadata_path = local_path.parent / "metadata.json"
        if metadata_path.exists():
            storage.upload_file(str(metadata_path))

        return success
    except Exception as e:
        logger.exception(f"Error uploading to GCS: {e}")
        return False
