"""Base class for Stage 0 pre-filtering operations."""

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class PreFilteringStageBase(FieldAnalysisStageBase):
    """Base class for Stage 0 pre-filtering operations."""

    def __init__(self, config: FieldAnalysisStageConfig, stage_name: str):
        super().__init__(config, f"Stage 0: {stage_name}")

    def _load_fields_for_filtering(self):
        """Load agricultural fields as BUILD side for spatial indexing."""
        self.log.info("Loading agricultural fields for pre-filtering (BUILD side)...")
        self._load_silver_dataset(CONFIG.get_agricultural_fields_dataset(), "agricultural_fields")

        # Optimize fields table for spatial indexing
        self.conn.execute("""
            CREATE OR REPLACE TABLE fields_for_filtering AS
            SELECT 
                field_id,
                block_id,
                cvr_number,
                year,
                geometry
            FROM agricultural_fields
        """)

        fields_count = self.conn.execute("SELECT COUNT(*) FROM fields_for_filtering").fetchone()[0]
        self.log.info(f"✅ Loaded {fields_count:,} fields for pre-filtering")

        # Configure DuckDB for optimal spatial indexing
        self.conn.execute("SET preserve_insertion_order=false")
        self.conn.execute("SET threads=4")  # Use full CPU for Stage 0
        self.conn.execute("SET max_temp_directory_size='12GB'")

    def _get_stage0_output_path(self, dataset_name: str) -> str:
        """Get GCS path for Stage 0 pre-filtered output following standard pipeline pattern."""
        from datetime import datetime

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"gs://{CONFIG.bucket}/gold/{dataset_name}/{timestamp}/data.parquet"

    def _save_prefiltered_dataset(self, table_name: str, output_dataset_name: str):
        """
        Save pre-filtered dataset to GCS using optimized export.

        Args:
            table_name: DuckDB table name to export
            output_dataset_name: Name for the output dataset in GCS
        """
        import os
        import tempfile
        from datetime import datetime

        try:
            # Create timestamp and GCS path following the standard pattern
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{output_dataset_name}.parquet"
            gcs_path = f"gold/{output_dataset_name}/{timestamp}/{filename}"

            # Create temporary file for export
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
                temp_path = tmp_file.name

            # Export table to temporary file using DuckDB COPY
            self.conn.execute(f"""
                COPY {table_name} TO '{temp_path}' 
                (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
            """)

            # Upload to GCS using gcs_access
            full_gcs_path = f"gs://{CONFIG.bucket}/{gcs_path}"

            # Use gcs_access fs to upload
            with open(temp_path, "rb") as src:
                with self.gcs_access.fs.open(full_gcs_path, "wb") as dst:
                    import shutil

                    shutil.copyfileobj(src, dst)

            # Clean up temporary file
            if os.path.exists(temp_path):
                os.unlink(temp_path)

            # Log export statistics
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log.info(f"✅ Saved {count:,} rows to {full_gcs_path}")

        except Exception as e:
            self.log.error(f"❌ Failed to save pre-filtered dataset {output_dataset_name}: {e}")
            # Clean up temp file on error
            if "temp_path" in locals() and os.path.exists(temp_path):
                os.unlink(temp_path)
            raise
