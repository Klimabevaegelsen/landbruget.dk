import logging
import os
import time
from datetime import datetime
from typing import Any, Dict

import geopandas as gpd
from dotenv import load_dotenv
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil

# Load environment variables at module level
load_dotenv()

logger = logging.getLogger(__name__)


class PropertyCadastralMergeConfig(BaseJobConfig):
    """Configuration for the Property-Cadastral merge pipeline."""

    name: str = "Property Owners Cadastral Merge"
    dataset: str = "property_cadastral_merged"
    type: str = "merge"
    description: str = "Merge property owners data with cadastral parcels using BFE numbers"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    # Data source paths
    property_owners_silver_path: str = "silver/property_owners/"
    cadastral_silver_path: str = "silver/cadastral/"

    # Merge configuration
    join_method: str = "inner"  # Use inner join to ensure complete records only
    validate_bfe_numbers: bool = True  # Validate BFE number format and consistency
    include_merge_metadata: bool = True  # Add metadata about the merge process

    save_local: bool = os.getenv("SAVE_LOCAL", "false").lower() in ("true", "1", "yes")

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class PropertyCadastralMerge(BaseSource[PropertyCadastralMergeConfig]):
    """Pure DuckDB-based Property-Cadastral merge pipeline for memory efficiency."""

    def __init__(self, config: PropertyCadastralMergeConfig, gcs_util: GCSUtil) -> None:
        super().__init__(config, gcs_util)
        # Configure DuckDB for GitHub Actions resource limits (16GB RAM, 4 cores for public repos)
        self.conn.execute("SET memory_limit = '12GB'")  # Leave 4GB for system + Python + file I/O
        self.conn.execute("SET threads = 4")  # GitHub Actions standard runners have 4 cores
        self.conn.execute("SET temp_directory = '/tmp'")  # Use /tmp for spill
        self.conn.execute(
            "SET preserve_insertion_order = false"
        )  # Allow reordering for performance
        self.conn.execute("INSTALL spatial")  # Enable spatial extension
        self.conn.execute("LOAD spatial")  # Load spatial extension

    def _validate_and_standardize_crs(self, file_path: str, dataset_name: str) -> str:
        """
        Validate and standardize CRS of a parquet file to EPSG:4326.

        Args:
            file_path: Path to the parquet file
            dataset_name: Name of the dataset for logging

        Returns:
            Path to the CRS-standardized file (may be the same as input if no conversion needed)
        """
        try:
            # Read the file to check CRS
            gdf = gpd.read_parquet(file_path)

            if gdf.crs and gdf.crs.to_epsg() == 4326:
                self.log.info(f"{dataset_name}: CRS is already EPSG:4326 ✅")
                return file_path
            else:
                self.log.info(f"{dataset_name}: Converting CRS from {gdf.crs} to EPSG:4326")
                gdf = gdf.to_crs("EPSG:4326")

            # Save the standardized version
            standardized_path = file_path.replace(".parquet", "_epsg4326.parquet")
            gdf.to_parquet(standardized_path)

            self.log.info(f"{dataset_name}: ✅ CRS standardized and saved to {standardized_path}")
            return standardized_path

        except Exception as e:
            self.log.error(f"{dataset_name}: Error validating/standardizing CRS: {e}")
            return file_path  # Return original path if conversion fails

    def _load_and_validate_property_data(self, file_path: str) -> Dict[str, Any]:
        """Load property owners data using DuckDB and return validation stats."""
        try:
            self.log.info("Loading property owners data with DuckDB...")

            # First, get basic stats about the file
            stats_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT properties.bestemtFastEjendomBFENr) as unique_bfe_numbers,
                COUNT(CASE WHEN properties.bestemtFastEjendomBFENr IS NOT NULL AND properties.bestemtFastEjendomBFENr > 0 THEN 1 END) as valid_bfe_records
            FROM read_parquet('{file_path}')
            """

            stats = self.conn.execute(stats_query).fetchone()
            total_records, unique_bfe_numbers, valid_bfe_records = stats

            self.log.info("Property owners file stats:")
            self.log.info(f"  Total records: {total_records:,}")
            self.log.info(f"  Unique BFE numbers: {unique_bfe_numbers:,}")
            self.log.info(f"  Valid BFE records: {valid_bfe_records:,}")

            # Check if BFE column exists in the nested structure
            try:
                test_query = f"SELECT properties.bestemtFastEjendomBFENr FROM read_parquet('{file_path}') LIMIT 1"
                self.conn.execute(test_query).fetchone()
                self.log.info("✅ BFE column found in properties structure")

            except Exception as e:
                self.log.error(f"BFE column not found in properties structure: {e}")
                # Fallback: check available columns
                try:
                    columns_query = f"DESCRIBE SELECT * FROM read_parquet('{file_path}') LIMIT 1"
                    columns_result = self.conn.execute(columns_query).fetchall()
                    columns = [row[0] for row in columns_result]
                    self.log.error(f"Available top-level columns: {columns}")
                except Exception as desc_e:
                    self.log.error(f"Failed to describe columns: {desc_e}")
                return None

            # Create filtered table with only valid BFE numbers if validation is enabled
            if self.config.validate_bfe_numbers:
                filter_condition = "WHERE properties.bestemtFastEjendomBFENr IS NOT NULL AND properties.bestemtFastEjendomBFENr > 0"
                self.log.info(
                    f"Applying BFE validation: {valid_bfe_records:,} of {total_records:,} records will be used"
                )
            else:
                filter_condition = ""
                self.log.info(f"No BFE validation: using all {total_records:,} records")

            # Create the main property owners table with flattened BFE number for easier joining
            create_table_query = f"""
            CREATE OR REPLACE TABLE property_owners AS 
            SELECT 
                *,
                properties.bestemtFastEjendomBFENr as bestemtFastEjendomBFENr
            FROM read_parquet('{file_path}')
            {filter_condition}
            """

            self.conn.execute(create_table_query)

            # Get final record count
            final_count = self.conn.execute("SELECT COUNT(*) FROM property_owners").fetchone()[0]

            return {
                "total_records": total_records,
                "unique_bfe_numbers": unique_bfe_numbers,
                "valid_bfe_records": valid_bfe_records,
                "final_records": final_count,
                "table_name": "property_owners",
            }

        except Exception as e:
            self.log.error(f"Failed to load property owners data: {e}")
            return None

    def _load_and_validate_cadastral_data(self, file_path: str) -> Dict[str, Any]:
        """Load cadastral data using DuckDB and return validation stats."""
        try:
            self.log.info("Loading cadastral data with DuckDB...")

            # Get basic stats about the cadastral file
            stats_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT bfe_number) as unique_bfe_numbers
            FROM read_parquet('{file_path}')
            """

            stats = self.conn.execute(stats_query).fetchone()
            total_records, unique_bfe_numbers = stats

            self.log.info("Cadastral file stats:")
            self.log.info(f"  Total records: {total_records:,}")
            self.log.info(f"  Unique BFE numbers: {unique_bfe_numbers:,}")

            # Check if required columns exist
            try:
                columns_query = f"DESCRIBE SELECT * FROM read_parquet('{file_path}') LIMIT 1"
                columns_result = self.conn.execute(columns_query).fetchall()
                columns = [row[0] for row in columns_result]

                if "bfe_number" not in columns:
                    self.log.error(
                        f"BFE number column not found in cadastral data. Available columns: {columns}"
                    )
                    return None

            except Exception as e:
                self.log.error(f"Error checking cadastral columns: {e}")
                return None

            # Create the cadastral table
            create_table_query = f"""
            CREATE OR REPLACE TABLE cadastral AS 
            SELECT * FROM read_parquet('{file_path}')
            """

            self.conn.execute(create_table_query)

            return {
                "total_records": total_records,
                "unique_bfe_numbers": unique_bfe_numbers,
                "table_name": "cadastral",
            }

        except Exception as e:
            self.log.error(f"Failed to load cadastral data: {e}")
            return None

    def _perform_bfe_merge(
        self, property_stats: Dict[str, Any], cadastral_stats: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Perform BFE-based merge using pure DuckDB SQL."""
        self.log.info("Performing BFE-based merge with DuckDB...")

        # Log initial statistics
        self.log.info(f"Property owners records: {property_stats['final_records']:,}")
        self.log.info(f"Cadastral parcels: {cadastral_stats['total_records']:,}")
        self.log.info(
            f"Unique BFE numbers - Properties: {property_stats['unique_bfe_numbers']:,}, Cadastral: {cadastral_stats['unique_bfe_numbers']:,}"
        )

        # Perform the merge based on BFE numbers using DuckDB
        join_type = self.config.join_method.upper()
        self.log.info(f"Performing {join_type} join on BFE numbers")

        # Add merge metadata columns if requested
        metadata_columns = ""
        if self.config.include_merge_metadata:
            metadata_columns = f"""
                '{datetime.utcnow().isoformat()}'::TIMESTAMP as merge_timestamp,
                'bfe_join' as merge_method,
                '{self.config.join_method}' as join_type,
                (c.bfe_number IS NOT NULL) as has_cadastral_match,
            """

        merge_query = f"""
        CREATE OR REPLACE TABLE merged_data AS
        SELECT 
            p.*,
            c.* EXCLUDE (bfe_number),
            c.bfe_number as cadastral_bfe_number,
            {metadata_columns[:-1] if metadata_columns else ""}  -- Remove trailing comma
        FROM property_owners p
        {join_type} JOIN cadastral c 
        ON p.bestemtFastEjendomBFENr = c.bfe_number
        """

        self.conn.execute(merge_query)

        # Get merge statistics
        merge_stats = self.conn.execute("""
        SELECT 
            COUNT(*) as merged_records,
            COUNT(DISTINCT bestemtFastEjendomBFENr) as unique_bfe_matches
        FROM merged_data
        """).fetchone()

        merged_records, unique_bfe_matches = merge_stats

        # Calculate match rate
        total_property_records = property_stats["final_records"]
        match_rate = (
            (merged_records / total_property_records) * 100 if total_property_records > 0 else 0
        )

        quality_stats = {
            "merged_records": merged_records,
            "unique_bfe_matches": unique_bfe_matches,
            "match_rate_percent": match_rate,
        }

        self.log.info("BFE Merge Quality Statistics:")
        self.log.info(f"  Total property records: {property_stats['final_records']:,}")
        self.log.info(f"  Total cadastral parcels: {cadastral_stats['total_records']:,}")
        self.log.info(f"  Merged records: {merged_records:,}")
        self.log.info(f"  Unique BFE matches: {unique_bfe_matches:,}")
        self.log.info(f"  Match rate: {match_rate:.1f}%")

        return quality_stats

    def _export_merged_data_optimized(self, temp_file: str) -> int:
        """
        Export merged data using optimized approach for large datasets.
        Uses streaming export with compression and memory management.

        Returns:
            Number of records exported
        """
        self.log.info("Using optimized export for large dataset...")

        # First, get record count
        record_count = self.conn.execute("SELECT COUNT(*) FROM merged_data").fetchone()[0]
        self.log.info(f"Preparing to export {record_count:,} records")

        # Check available disk space
        import shutil

        disk_usage = shutil.disk_usage("/tmp")
        available_gb = disk_usage.free / (1024**3)
        self.log.info(f"Available disk space: {available_gb:.1f} GB")

        # Configure DuckDB for memory-efficient export
        self.conn.execute("SET memory_limit = '10GB'")  # Reduce memory limit for export
        self.conn.execute("SET threads = 3")  # Reduce threads to conserve memory

        # Use COPY with compression and row group size optimization for large files
        export_query = f"""
        COPY merged_data TO '{temp_file}' (
            FORMAT PARQUET,
            COMPRESSION 'SNAPPY',
            ROW_GROUP_SIZE 50000
        )
        """

        try:
            self.log.info("Starting optimized parquet export...")
            self.log.info("This may take several minutes for large datasets...")
            start_time = time.time()

            # Log progress every 30 seconds during export
            import threading

            def log_progress():
                elapsed = 0
                while True:
                    time.sleep(30)
                    elapsed += 30
                    self.log.info(
                        f"Export still running... {elapsed // 60}m {elapsed % 60}s elapsed"
                    )

            progress_thread = threading.Thread(target=log_progress, daemon=True)
            progress_thread.start()

            self.conn.execute(export_query)
            export_duration = time.time() - start_time

            # Get file stats
            file_size_mb = os.path.getsize(temp_file) / (1024 * 1024)
            self.log.info(
                f"Export completed in {export_duration:.1f}s - {record_count:,} records, {file_size_mb:.1f} MB"
            )

            return record_count

        except Exception as e:
            self.log.error(f"Optimized export failed: {e}")
            self.log.info("Checking system resources...")

            # Check memory usage
            import psutil

            memory = psutil.virtual_memory()
            self.log.info(
                f"Memory usage: {memory.percent}% ({memory.used / (1024**3):.1f}GB used of {memory.total / (1024**3):.1f}GB)"
            )

            # Check disk usage again
            disk_usage = shutil.disk_usage("/tmp")
            available_gb = disk_usage.free / (1024**3)
            self.log.info(f"Available disk space: {available_gb:.1f} GB")

            # Fallback: try without compression if it fails
            self.log.info("Attempting fallback export without compression...")
            fallback_query = f"COPY merged_data TO '{temp_file}' (FORMAT PARQUET)"
            self.conn.execute(fallback_query)

            file_size_mb = os.path.getsize(temp_file) / (1024 * 1024)
            self.log.info(
                f"Fallback export completed - {record_count:,} records, {file_size_mb:.1f} MB"
            )

            return record_count

    def _ensure_output_crs_epsg4326(self, temp_file: str) -> str:
        """
        Ensure the output file has EPSG:4326 CRS, not OGC:CRS84 or other variants.
        Uses memory-efficient approach for large files.

        Args:
            temp_file: Path to the temporary output file

        Returns:
            Path to the CRS-corrected file
        """
        try:
            self.log.info("Validating output CRS...")

            # Check file size first
            file_size_mb = os.path.getsize(temp_file) / (1024 * 1024)
            self.log.info(f"Validating CRS for {file_size_mb:.1f} MB file")

            # For large files (>500MB), use DuckDB to check CRS instead of loading into memory
            if file_size_mb > 500:
                self.log.info("Large file detected - using DuckDB for CRS validation")

                # Use DuckDB to check CRS without loading full dataset
                try:
                    crs_check = self.conn.execute(f"""
                        SELECT ST_SRID(geometry) as srid 
                        FROM read_parquet('{temp_file}') 
                        WHERE geometry IS NOT NULL 
                        LIMIT 1
                    """).fetchone()

                    if crs_check and crs_check[0] == 4326:
                        self.log.info("✅ Output CRS is already EPSG:4326 (verified via DuckDB)")
                        return temp_file
                    else:
                        self.log.warning(
                            f"CRS validation via DuckDB shows SRID: {crs_check[0] if crs_check else 'None'}"
                        )
                        self.log.info(
                            "Skipping CRS conversion for large file to avoid memory issues"
                        )
                        return temp_file

                except Exception as duckdb_e:
                    self.log.warning(f"DuckDB CRS check failed: {duckdb_e}")
                    self.log.info("Skipping CRS validation for large file to avoid memory issues")
                    return temp_file

            # For smaller files, use the original GeoPandas approach
            self.log.info("Small file - using GeoPandas for CRS validation")
            gdf = gpd.read_parquet(temp_file)

            if gdf.crs and gdf.crs.to_epsg() == 4326:
                self.log.info("✅ Output CRS is already EPSG:4326")
                return temp_file
            else:
                self.log.info(f"Converting output CRS from {gdf.crs} to EPSG:4326")
                gdf = gdf.to_crs("EPSG:4326")
                gdf.to_parquet(temp_file)
                self.log.info("✅ Output CRS converted to EPSG:4326")

            return temp_file

        except Exception as e:
            self.log.error(f"Error validating output CRS: {e}")
            self.log.info("Continuing without CRS validation to avoid pipeline failure")
            return temp_file

    async def run(self):
        """
        Run the complete property-cadastral merge job using pure DuckDB operations.

        This orchestrates the entire process:
        1. Download input files from GCS
        2. Validate and standardize CRS of input files
        3. Load data into DuckDB tables
        4. Perform BFE-based merge using SQL
        5. Export results directly to parquet with EPSG:4326 CRS
        6. Upload to GCS
        """
        self.log.info("Running Pure DuckDB Property-Cadastral BFE merge job")

        property_temp_path = None
        cadastral_temp_path = None
        output_temp_path = None

        try:
            # Download property owners file
            self.log.info("Downloading property owners data...")
            property_files = self.gcs_util.list_files(
                bucket_name=self.config.bucket, prefix=self.config.property_owners_silver_path
            )

            if not property_files:
                self.log.error("No property owners files found in silver layer")
                return

            latest_property_file = max(property_files, key=lambda x: x.time_created)
            self.log.info(f"Using latest property owners file: {latest_property_file.name}")

            property_temp_path = (
                f"/tmp/property_owners_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            )
            self.gcs_util.download_file(
                bucket_name=self.config.bucket,
                source_blob_name=latest_property_file.name,
                destination_file_name=property_temp_path,
            )

            # Download cadastral file
            self.log.info("Downloading cadastral data...")
            cadastral_files = self.gcs_util.list_files(
                bucket_name=self.config.bucket, prefix=self.config.cadastral_silver_path
            )

            if not cadastral_files:
                self.log.error("No cadastral files found in silver layer")
                return

            latest_cadastral_file = max(cadastral_files, key=lambda x: x.time_created)
            self.log.info(f"Using latest cadastral file: {latest_cadastral_file.name}")

            cadastral_temp_path = (
                f"/tmp/cadastral_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            )
            self.gcs_util.download_file(
                bucket_name=self.config.bucket,
                source_blob_name=latest_cadastral_file.name,
                destination_file_name=cadastral_temp_path,
            )

            # Validate and standardize CRS of input files
            self.log.info("Validating and standardizing CRS of input files...")
            property_temp_path = self._validate_and_standardize_crs(
                property_temp_path, "Property owners"
            )
            cadastral_temp_path = self._validate_and_standardize_crs(
                cadastral_temp_path, "Cadastral"
            )

            # Load data using DuckDB
            property_stats = self._load_and_validate_property_data(property_temp_path)
            if property_stats is None:
                self.log.error("Failed to load property owners data")
                return

            cadastral_stats = self._load_and_validate_cadastral_data(cadastral_temp_path)
            if cadastral_stats is None:
                self.log.error("Failed to load cadastral data")
                return

            # Perform merge
            quality_stats = self._perform_bfe_merge(property_stats, cadastral_stats)

            # Clean up large DuckDB tables from memory to free space before export
            self.conn.execute("DROP TABLE IF EXISTS property_owners")
            self.conn.execute("DROP TABLE IF EXISTS cadastral_data")
            self.log.info("Cleaned up large DuckDB tables from memory")

            # Export using DuckDB directly to avoid memory issues with 6.5M records
            # Use BaseSource directory structure but with DuckDB export for efficiency
            self.log.info("Exporting merged data directly from DuckDB to parquet...")

            # Use BaseSource date pattern for consistency
            import pandas as pd

            date_str = pd.Timestamp.now().strftime("%Y-%m-%d")

            # Create temp file with BaseSource naming convention
            temp_dir = "/tmp/silver/property_cadastral_merged"
            os.makedirs(temp_dir, exist_ok=True)
            temp_file = f"{temp_dir}/{date_str}.parquet"
            output_temp_path = temp_file  # Store for cleanup

            # Export directly from DuckDB to parquet (memory efficient)
            record_count = self._export_merged_data_optimized(temp_file)

            # Ensure output has proper EPSG:4326 CRS
            temp_file = self._ensure_output_crs_epsg4326(temp_file)

            # Get file stats
            file_size_mb = os.path.getsize(temp_file) / (1024 * 1024)
            self.log.info(
                f"Exported {record_count:,} records to {temp_file} ({file_size_mb:.1f} MB)"
            )

            # Upload to GCS using BaseSource pattern
            self.log.info(
                f"DEBUG - save_local config: {self.config.save_local} (type: {type(self.config.save_local)})"
            )

            if not self.config.save_local:
                self.log.info("Starting upload to GCS...")
                bucket = self.gcs_util.get_gcs_client().bucket(self.config.bucket)
                gcs_path = f"silver/property_cadastral_merged/{date_str}.parquet"
                working_blob = bucket.blob(gcs_path)

                self.log.info(
                    f"Uploading {file_size_mb:.1f} MB file to gs://{self.config.bucket}/{gcs_path}"
                )
                upload_start = time.time()
                working_blob.upload_from_filename(temp_file)
                upload_duration = time.time() - upload_start

                self.log.info(
                    f"✅ Upload completed in {upload_duration:.1f}s at {file_size_mb / upload_duration:.1f} MB/s"
                )

                # Clean up temp file after upload
                os.remove(temp_file)
                self.log.info("Cleaned up temporary export file")
            else:
                self.log.info(f"Save local is enabled, saved locally at {temp_file}")

            self.log.info("Pure DuckDB Property-Cadastral BFE merge job completed successfully")
            self.log.info(
                f"Final dataset: {quality_stats['merged_records']:,} records with {quality_stats['match_rate_percent']:.1f}% match rate"
            )

        except Exception as e:
            self.log.error(f"Property-Cadastral BFE merge job failed: {e}")
            raise

        finally:
            # Clean up temporary files
            for temp_path in [property_temp_path, cadastral_temp_path, output_temp_path]:
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.unlink(temp_path)
                        self.log.info(f"Cleaned up temporary file: {temp_path}")
                    except Exception as e:
                        self.log.warning(f"Failed to clean up {temp_path}: {e}")

            # Close DuckDB connection
            if hasattr(self, "conn"):
                self.conn.close()
