"""
Export module for pesticide disaggregation pipeline.

This module handles the standardized export of disaggregated pesticide data
to GCS following the pipeline standardization patterns.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from common.schema_documentation import SchemaDocumentationManager
from common.storage_interface import GCSStorage, LocalStorage, StorageInterface

logger = logging.getLogger(__name__)


class PesticideExporter:
    """Handles export of disaggregated pesticide data following standardized patterns."""

    def __init__(self, db_manager, config, pipeline_start_time: Optional[datetime] = None):
        """Initialize the exporter with database manager and configuration."""
        self.db = db_manager
        self.config = config
        self.pipeline_start_time = pipeline_start_time or datetime.now()
        self.timestamp = self.pipeline_start_time.strftime("%Y%m%d_%H%M%S")

        # Initialize schema documentation manager
        self.schema_manager = SchemaDocumentationManager(
            connection=self.db.conn,
            pipeline_name="pesticide_disaggregation_pipeline",
            pipeline_start_time=self.pipeline_start_time,
            logger=logger,
        )

    def get_storage_interface(self) -> StorageInterface:
        """Get the appropriate storage interface based on environment."""
        environment = os.getenv("ENVIRONMENT", "dev")
        gcs_bucket = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

        if environment == "prod" and gcs_bucket:
            logger.info(f"Using GCS storage with bucket: {gcs_bucket}")
            return GCSStorage(gcs_bucket)
        else:
            logger.info("Using local storage for development")
            return LocalStorage()

    def export_disaggregated_data(self) -> None:
        """Export disaggregated pesticide data with only essential columns."""
        logger.info("Exporting disaggregated pesticide data...")

        # SQL query to select only the essential columns
        export_query = """
        SELECT 
            DisaggregatedID,
            OriginalPesticideRowID,
            CompanyRegistrationNumber,
            PesticideName,
            PesticideRegistrationNumber,
            DosageQuantity,
            DosageUnit,
            MatchedFieldID,
            MatchedBlockID,
            AllocatedArea,
            AllocationMethod,
            MatchConfidence,
            DisaggregationDate
        FROM disaggregated_pesticide_applications
        ORDER BY DisaggregationDate, CompanyRegistrationNumber, MatchedFieldID
        """

        storage = self.get_storage_interface()

        # Generate filename following standardization patterns
        filename = "pesticide_disaggregated.parquet"

        if isinstance(storage, GCSStorage):
            # Export to GCS following the standard path pattern
            gcs_path = f"silver/pesticide_disaggregation_pipeline/{self.timestamp}/{filename}"

            # Export to temporary local file first
            temp_path = Path(f"/tmp/{filename}")
            self.db.execute_query(f"COPY ({export_query}) TO '{str(temp_path)}' (FORMAT PARQUET)")

            # Upload to GCS
            import pandas as pd

            df = pd.read_parquet(temp_path)
            storage.save_parquet(df, gcs_path)

            # Clean up temp file
            temp_path.unlink()

            logger.info(f"Exported disaggregated data to GCS: gs://{storage.bucket}/{gcs_path}")

        else:
            # Export locally for development
            output_dir = self.config.RESOLVED_OUTPUT_DIR or Path("outputs")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / filename

            self.db.execute_query(f"COPY ({export_query}) TO '{str(output_path)}' (FORMAT PARQUET)")
            logger.info(f"Exported disaggregated data locally: {output_path}")

    def generate_schema_documentation(self) -> str:
        """Generate schema documentation for the disaggregated table."""
        logger.info("Generating schema documentation...")

        # Generate documentation for the main output table
        doc_path = self.schema_manager.generate_table_documentation(
            table_name="disaggregated_pesticide_applications", stage="silver"
        )

        logger.info(f"Schema documentation generated: {doc_path}")
        return doc_path

    def commit_schema_to_github(self) -> bool:
        """Commit schema documentation to GitHub."""
        logger.info("Committing schema documentation to GitHub...")

        commit_message = f"Update pesticide disaggregation schema documentation - {self.timestamp}"
        success = self.schema_manager.commit_to_github(commit_message)

        if success:
            logger.info("Schema documentation committed to GitHub successfully")
        else:
            logger.warning("Failed to commit schema documentation to GitHub")

        return success

    def get_export_summary(self) -> dict:
        """Get summary statistics of the exported data."""
        try:
            summary_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT CompanyRegistrationNumber) as unique_companies,
                COUNT(DISTINCT MatchedFieldID) as unique_fields,
                COUNT(DISTINCT MatchedBlockID) as unique_blocks,
                COUNT(DISTINCT PesticideName) as unique_pesticides,
                SUM(AllocatedArea) as total_allocated_area,
                MIN(DisaggregationDate) as earliest_processing,
                MAX(DisaggregationDate) as latest_processing
            FROM disaggregated_pesticide_applications
            """

            result = self.db.execute_query(summary_query)[0]

            return {
                "total_records": result[0],
                "unique_companies": result[1],
                "unique_fields": result[2],
                "unique_blocks": result[3],
                "unique_pesticides": result[4],
                "total_allocated_area_ha": float(result[5]) if result[5] else 0.0,
                "processing_period": {"earliest": result[6], "latest": result[7]},
                "export_timestamp": self.timestamp,
                "schema_documentation_generated": True,
            }

        except Exception as e:
            logger.error(f"Error generating export summary: {e}")
            return {"error": str(e)}
