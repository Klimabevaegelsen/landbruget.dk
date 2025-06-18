import logging
import os
from datetime import datetime
from typing import Any, Dict

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

    save_local: bool = os.getenv("SAVE_LOCAL", False)

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class PropertyCadastralMerge(BaseSource[PropertyCadastralMergeConfig]):
    """Pure DuckDB-based Property-Cadastral merge pipeline for memory efficiency."""

    def __init__(self, config: PropertyCadastralMergeConfig, gcs_util: GCSUtil) -> None:
        super().__init__(config, gcs_util)
        # Configure DuckDB for GitHub Actions resource limits
        self.conn.execute("SET memory_limit = '4GB'")
        self.conn.execute("SET threads = 2")  # GitHub Actions has 2 cores
        self.conn.execute("INSTALL spatial")  # Enable spatial extension
        self.conn.execute("LOAD spatial")  # Load spatial extension

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

        self.log.info(f"BFE join completed. Result: {merged_records:,} records")

        # Calculate match rates
        if self.config.join_method == "inner":
            match_rate = (
                (merged_records / property_stats["final_records"]) * 100
                if property_stats["final_records"] > 0
                else 0
            )
        elif self.config.join_method == "left":
            matched_count = self.conn.execute(
                "SELECT COUNT(*) FROM merged_data WHERE cadastral_bfe_number IS NOT NULL"
            ).fetchone()[0]
            match_rate = (
                (matched_count / property_stats["final_records"]) * 100
                if property_stats["final_records"] > 0
                else 0
            )
        else:
            match_rate = 0

        quality_stats = {
            "total_properties": property_stats["final_records"],
            "total_cadastral_parcels": cadastral_stats["total_records"],
            "merged_records": merged_records,
            "unique_bfe_matches": unique_bfe_matches,
            "match_rate_percent": match_rate,
            "join_method": self.config.join_method,
        }

        # Log quality statistics
        self.log.info("BFE Merge Quality Statistics:")
        self.log.info(f"  Total property records: {property_stats['final_records']:,}")
        self.log.info(f"  Total cadastral parcels: {cadastral_stats['total_records']:,}")
        self.log.info(f"  Merged records: {merged_records:,}")
        self.log.info(f"  Unique BFE matches: {unique_bfe_matches:,}")
        self.log.info(f"  Match rate: {match_rate:.1f}%")

        return quality_stats

    def _export_to_parquet(self, output_path: str) -> None:
        """Export merged data directly to parquet using DuckDB."""
        self.log.info(f"Exporting merged data to parquet: {output_path}")

        try:
            # Export from DuckDB to parquet
            export_query = f"""
            COPY merged_data TO '{output_path}' (FORMAT PARQUET)
            """

            self.conn.execute(export_query)

            # Get file size for logging
            file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
            record_count = self.conn.execute("SELECT COUNT(*) FROM merged_data").fetchone()[0]

            self.log.info(
                f"Exported {record_count:,} records to {output_path} ({file_size_mb:.1f} MB)"
            )

        except Exception as e:
            self.log.error(f"Failed to export merged data: {e}")
            raise

    async def run(self):
        """
        Run the complete property-cadastral merge job using pure DuckDB operations.

        This orchestrates the entire process:
        1. Download input files from GCS
        2. Load data into DuckDB tables
        3. Perform BFE-based merge using SQL
        4. Export results directly to parquet
        5. Upload to GCS
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

            # Export to parquet
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_temp_path = f"/tmp/{self.config.dataset}_{timestamp}.parquet"
            self._export_to_parquet(output_temp_path)

            # Save the result using the proper BaseSource method
            # Load as GeoDataFrame since _save_data expects that
            import geopandas as gpd

            # Load the exported data as GeoDataFrame for proper saving
            gdf = gpd.read_parquet(output_temp_path)

            # Handle geometry column if it exists
            geometry_cols = [
                col
                for col in gdf.columns
                if col.lower().endswith("geometry") or col.lower() == "geom"
            ]
            if geometry_cols:
                # Use the first geometry column found
                geom_col = geometry_cols[0]
                gdf = gdf.set_geometry(geom_col)
                # Set CRS if not already set (common Danish coordinate system)
                if gdf.crs is None:
                    gdf = gdf.set_crs(
                        "EPSG:25832", allow_override=True
                    )  # ETRS89 / UTM zone 32N (common for Denmark)
            else:
                # If no geometry column, convert to regular DataFrame for _save_data
                # Note: BaseSource._save_data expects GeoDataFrame, so we create one without geometry
                gdf = gpd.GeoDataFrame(gdf)

            # Use BaseSource._save_data method which handles the GCS upload properly
            self._save_data(gdf, self.config.dataset, self.config.bucket)

            # Clean up temp file
            if os.path.exists(output_temp_path):
                os.remove(output_temp_path)

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
