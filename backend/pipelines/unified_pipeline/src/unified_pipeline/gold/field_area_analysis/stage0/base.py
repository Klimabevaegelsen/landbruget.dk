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
        self._load_silver_dataset(CONFIG.agricultural_fields_dataset, "agricultural_fields")

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
