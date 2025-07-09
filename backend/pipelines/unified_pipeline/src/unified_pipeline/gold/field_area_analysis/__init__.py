"""
Multi-Stage Field Area Analysis Pipeline

Optimized for GitHub Actions with massive parallelization and proper DuckDB spatial joins.
"""

from typing import Any, Dict, Optional

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.log_util import Logger

from .base import FieldAnalysisStageConfig
from .cli import run_all_stages

__version__ = "1.0.0"


class FieldAreaAnalysisGoldConfig(BaseJobConfig):
    """Configuration for field area analysis gold processor."""

    name: str = "Field Area Analysis Gold"
    dataset: str = "field_area_analysis"
    type: str = "gold"
    description: str = "Comprehensive field area analysis with environmental factors"
    frequency: str = "yearly"
    bucket: str = "landbrugsdata-raw-data"


class FieldAreaAnalysisGold(BaseSource[FieldAreaAnalysisGoldConfig], GoldJobInterface):
    """
    Field Area Analysis Gold Pipeline

    This pipeline performs comprehensive field area analysis including:
    - BNBO (Biodiversity and Nature) status coverage
    - Wetland coverage analysis
    - Water project intersections
    - Soil type analysis
    - Property cadastral relationships
    """

    def __init__(self, config: FieldAreaAnalysisGoldConfig):
        super().__init__(config)
        self.log = Logger.get_logger()

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Execute the complete field area analysis pipeline."""
        self.log.info("🚀 Starting Field Area Analysis Gold Pipeline")

        try:
            # Create the internal config for the multi-stage pipeline
            internal_config = FieldAnalysisStageConfig()

            # Run all stages of the field area analysis pipeline
            results = await run_all_stages(internal_config)

            self.log.info("✅ Field Area Analysis Gold Pipeline completed successfully")
            self.log.info(f"Pipeline completed in {results['pipeline_total_time']:.1f} seconds")

        except Exception as e:
            self.log.error(f"❌ Field Area Analysis Gold Pipeline failed: {e}")
            raise
