import os
import tempfile
from typing import Any, Dict, Optional

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.gcs_util import GCSUtil


class PropertyCadastralMergeGoldConfig(BaseJobConfig):
    """Configuration for Property-Cadastral merge gold layer."""

    name: str = "Property Cadastral Merge Gold"
    dataset: str = "property_cadastral_merged"
    type: str = "gold"
    description: str = "Merge property owners with cadastral data for business analytics"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    # Input silver datasets
    property_owners_dataset: str = "property_owners"
    cadastral_dataset: str = "cadastral"

    # Merge configuration
    join_method: str = "inner"
    validate_bfe_numbers: bool = True
    include_merge_metadata: bool = True

    # Quality thresholds
    min_match_rate: float = 0.8  # Minimum acceptable match rate

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class PropertyCadastralMergeGold(BaseSource[PropertyCadastralMergeGoldConfig], GoldJobInterface):
    """
    Gold layer processor for property-cadastral merge.

    Combines property owners and cadastral silver data to create
    business-ready datasets for analytics and downstream consumption.
    """

    def __init__(self, config: PropertyCadastralMergeGoldConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)

        # Configure DuckDB for large dataset processing with optimal settings for 16GB RAM
        self.conn.execute("SET memory_limit = '12GB'")  # Use 75% of available 16GB RAM
        self.conn.execute("SET threads = 4")  # Use all available CPU cores
        self.conn.execute("SET temp_directory = '/tmp'")  # Use disk for temp storage
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")

    def _load_silver_data_streaming(
        self, silver_data: Optional[Dict[str, Any]]
    ) -> tuple[Optional[str], Optional[str]]:
        """Load property owners and cadastral data paths for streaming processing."""

        property_path = None
        cadastral_path = None

        if silver_data:
            # Use in-memory data - but this is not recommended for large datasets
            self.log.warning(
                "In-memory data passing not recommended for large datasets - using streaming"
            )
            return None, None
        else:
            # Get file paths for streaming processing
            self.log.info("Setting up streaming data processing from GCS storage")

            # Get property owners data path
            self.log.info(
                f"Looking for property owners data in silver/{self.config.property_owners_dataset}/"
            )
            try:
                files = self.gcs_util.list_files(
                    bucket_name=self.config.bucket,
                    prefix=f"silver/{self.config.property_owners_dataset}/",
                )
                if files:
                    latest_file = max(files, key=lambda x: x.time_created)
                    property_path = f"gs://{self.config.bucket}/{latest_file.name}"
                    self.log.info(f"Found property owners data: {latest_file.name}")
                else:
                    self.log.warning("No property owners data found in silver layer")
            except Exception as e:
                self.log.error(f"Error finding property owners data: {e}")

            # Get cadastral data path
            self.log.info(f"Looking for cadastral data in silver/{self.config.cadastral_dataset}/")
            try:
                files = self.gcs_util.list_files(
                    bucket_name=self.config.bucket,
                    prefix=f"silver/{self.config.cadastral_dataset}/",
                )
                if files:
                    latest_file = max(files, key=lambda x: x.time_created)
                    cadastral_path = f"gs://{self.config.bucket}/{latest_file.name}"
                    self.log.info(f"Found cadastral data: {latest_file.name}")
                else:
                    self.log.warning("No cadastral data found in silver layer")
            except Exception as e:
                self.log.error(f"Error finding cadastral data: {e}")

        return property_path, cadastral_path

    def _stream_merge_to_gcs(self, property_path: str, cadastral_path: str) -> Dict[str, Any]:
        """Perform streaming BFE-based merge and save directly to GCS without loading into memory."""

        # Download files locally first for DuckDB processing
        self.log.info("Downloading property owners data locally...")
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_prop:
            property_local_path = tmp_prop.name

        bucket_name = property_path.split("/")[2]
        blob_name = "/".join(property_path.split("/")[3:])
        self.gcs_util.download_file(bucket_name, blob_name, property_local_path)

        # Download cadastral file
        self.log.info("Downloading cadastral data locally...")
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_cad:
            cadastral_local_path = tmp_cad.name

        bucket_name = cadastral_path.split("/")[2]
        blob_name = "/".join(cadastral_path.split("/")[3:])
        self.gcs_util.download_file(bucket_name, blob_name, cadastral_local_path)

        # Create temporary output file
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_out:
            output_local_path = tmp_out.name

        try:
            self.log.info("Creating property_owners table from local parquet file...")
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE property_owners AS 
                SELECT * FROM read_parquet('{property_local_path}')
            """)

            prop_count = self.conn.execute("SELECT COUNT(*) FROM property_owners").fetchone()[0]
            self.log.info(f"Property owners table created with {prop_count:,} records")

            self.log.info("Creating cadastral table from local parquet file...")
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE cadastral AS 
                SELECT * FROM read_parquet('{cadastral_local_path}')
            """)

            cadastral_count = self.conn.execute("SELECT COUNT(*) FROM cadastral").fetchone()[0]
            self.log.info(f"Cadastral table created with {cadastral_count:,} records")

            # Check available columns
            prop_columns = [
                row[0] for row in self.conn.execute("DESCRIBE property_owners").fetchall()
            ]
            cadastral_columns = [
                row[0] for row in self.conn.execute("DESCRIBE cadastral").fetchall()
            ]

            self.log.info(f"Property columns available: {prop_columns[:10]}...")
            self.log.info(f"Cadastral columns available: {cadastral_columns[:10]}...")

            # DuckDB automatically parses the nested JSON structure
            # We can access BFE number directly from the properties struct
            try:
                # Test if we can access the BFE number from the nested structure
                test_result = self.conn.execute("""
                    SELECT properties.bestemtFastEjendomBFENr as bfe_number 
                    FROM property_owners 
                    LIMIT 1
                """).fetchone()

                if test_result and test_result[0] is not None:
                    self.log.info(
                        f"Successfully accessed BFE number from nested structure: {test_result[0]}"
                    )
                    bfe_column = "properties.bestemtFastEjendomBFENr"
                else:
                    raise ValueError("BFE number is null in test query")

            except Exception as e:
                self.log.error(f"Failed to access BFE from nested structure: {e}")
                raise ValueError(f"Cannot access BFE number from property data structure: {e}")

            # Perform the merge and save directly to parquet using DuckDB COPY
            self.log.info("Performing BFE-based merge and streaming to local parquet...")
            merge_and_copy_query = f"""
            COPY (
                SELECT 
                    p.{bfe_column} as bfe_number,
                    p.properties.ejendePerson as person_data,
                    p.properties.ejendeVirksomhed as company_data,
                    p.properties.ejerforholdskode as ownership_code,
                    p.properties.faktiskEjerandel_naevner as ownership_denominator,
                    p.properties.faktiskEjerandel_taeller as ownership_numerator,
                    c.business_event,
                    c.business_process,
                    c.authority,
                    c.is_worker_housing,
                    c.is_common_lot,
                    c.has_owner_apartments,
                    c.registration_from as cadastral_registration_from,
                    c.effect_from as cadastral_effect_from
                FROM property_owners p
                INNER JOIN cadastral c ON p.{bfe_column} = c.bfe_number
                WHERE p.{bfe_column} IS NOT NULL
            ) TO '{output_local_path}' (FORMAT PARQUET)
            """

            self.conn.execute(merge_and_copy_query)

            # Get merge statistics without loading data into memory
            merged_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT 1
                    FROM property_owners p
                    INNER JOIN cadastral c ON p.{bfe_column} = c.bfe_number
                    WHERE p.{bfe_column} IS NOT NULL
                )
            """).fetchone()[0]

            self.log.info(f"Merge completed with {merged_count:,} records")

            if merged_count == 0:
                raise ValueError("Merge resulted in 0 records - check BFE number alignment")

            # Calculate match rate
            match_rate = (merged_count / prop_count) if prop_count > 0 else 0
            quality_stats = {
                "total_properties": prop_count,
                "merged_records": merged_count,
                "match_rate": match_rate,
                "match_rate_percent": match_rate * 100,
            }

            self.log.info(f"Merge quality: {quality_stats['match_rate_percent']:.1f}% match rate")

            # Upload the parquet file directly to GCS
            timestamp = self.date_pattern
            gcs_path = f"gold/{self.config.dataset}/{timestamp}/{self.config.dataset}.parquet"

            self.log.info(f"Uploading merged data to GCS: gs://{self.config.bucket}/{gcs_path}")
            self.gcs_util.upload_file(
                bucket_name=self.config.bucket,
                source_file_path=output_local_path,
                destination_blob_name=gcs_path,
            )

            self.log.info(
                f"Successfully uploaded merged dataset to GCS with {merged_count:,} records"
            )

            return quality_stats

        finally:
            # Clean up temporary files
            try:
                if os.path.exists(property_local_path):
                    os.unlink(property_local_path)
                if os.path.exists(cadastral_local_path):
                    os.unlink(cadastral_local_path)
                if os.path.exists(output_local_path):
                    os.unlink(output_local_path)
            except Exception:
                pass

    def _validate_merge_quality(self, quality_stats: Dict[str, Any]) -> Dict[str, Any]:
        """Validate merge quality using pre-calculated statistics."""

        match_rate = quality_stats["match_rate"]

        if match_rate < self.config.min_match_rate:
            self.log.warning(
                f"Match rate {match_rate:.1%} below threshold {self.config.min_match_rate:.1%}"
            )

        return quality_stats

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """
        Run the property-cadastral merge gold processing.

        Args:
            silver_data: Optional dictionary of silver datasets for in-memory processing
        """
        self.log.info("Starting Property-Cadastral Merge Gold processing")

        try:
            # Load silver data from GCS storage or in-memory
            property_path, cadastral_path = self._load_silver_data_streaming(silver_data)

            # Check if we have the required data
            if cadastral_path is None:
                self.log.error("No cadastral data available - cannot proceed with merge")
                return

            if property_path is None:
                self.log.error("No property owners data available - cannot proceed with merge")
                return

            self.log.info(f"Using property data: {property_path}")
            self.log.info(f"Using cadastral data: {cadastral_path}")

            # Perform streaming merge directly to GCS
            quality_stats = self._stream_merge_to_gcs(property_path, cadastral_path)

            # Validate quality
            self._validate_merge_quality(quality_stats)

            self.log.info(
                f"Property-Cadastral merge completed: {quality_stats['merged_records']:,} records "
                f"with {quality_stats['match_rate_percent']:.1f}% match rate"
            )

        except Exception as e:
            self.log.error(f"Property-Cadastral merge failed: {e}")
            raise

        finally:
            if hasattr(self, "conn"):
                self.conn.close()
