"""
Carbon Emissions Gold Layer Configuration

Configuration for farm-level carbon emission calculations.
"""

import os

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig


class CarbonEmissionsGoldConfig(BaseJobConfig):
    """Configuration for Carbon Emissions gold layer."""

    name: str = "Carbon Emissions Gold"
    dataset: str = "carbon_emissions"
    type: str = "gold"
    description: str = (
        "Farm-level carbon emission estimates from livestock, fields, and fertilizer data"
    )
    frequency: str = "weekly"
    bucket: str = (
        os.getenv("STORAGE_BUCKET")
        or os.getenv("R2_BUCKET")
        or os.getenv("GCS_BUCKET", "landbruget-data")
    )

    # Processing parameters
    target_year: int | None = None
    batch_size: int = 500
    batch_number: int | None = None
    total_batches: int | None = None
    test_limit: int | None = None

    model_config = ConfigDict(extra="forbid")

    def apply_cli_filters(self, cli_config) -> None:
        """Apply CLI config overrides."""
        if hasattr(cli_config, "target_year") and cli_config.target_year:
            object.__setattr__(self, "target_year", cli_config.target_year)
        if hasattr(cli_config, "batch_number") and cli_config.batch_number is not None:
            object.__setattr__(self, "batch_number", cli_config.batch_number)
        if hasattr(cli_config, "total_batches") and cli_config.total_batches:
            object.__setattr__(self, "total_batches", cli_config.total_batches)
        if hasattr(cli_config, "test_limit") and cli_config.test_limit:
            object.__setattr__(self, "test_limit", cli_config.test_limit)
