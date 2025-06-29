import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel


# Configuration
@dataclass
class Config:
    """Configuration settings for the analysis."""

    DATA_DIR: Path = Path(".")
    OUTPUT_DIR: Path = Path("outputs")  # Relative path name for the output directory
    RESOLVED_OUTPUT_DIR: Optional[Path] = None
    AREA_TOLERANCE_PCT: float = 2.0
    MAX_FIELDS_FOR_SUBSET_SUM: int = 20  # DEPRECATED: No longer used (subset sum matching removed)

    # GCS Configuration
    GCS_BUCKET: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Target pesticide year (can be overridden)
    PESTICIDE_YEAR: int = int(os.getenv("PESTICIDE_YEAR", "2021"))

    def get_gcs_silver_sources(self, pesticide_year: int = None) -> Dict[str, str]:
        """
        Get GCS silver data sources for a given pesticide year.
        Implements the Y+1 temporal pattern discovered in our analysis.
        Uses the actual GCS data structure discovered during testing.

        Args:
            pesticide_year: The year of pesticide data to process

        Returns:
            Dictionary mapping dataset names to GCS silver paths
        """
        if pesticide_year is None:
            pesticide_year = self.PESTICIDE_YEAR

        # Apply Y+1 temporal pattern: pesticide year X uses field boundaries from year X+1
        field_year = pesticide_year + 1

        return {
            "marker": f"silver/fvm_marker_{field_year}",
            "pesticide": "silver/Pesticides",  # Pesticide data is in Pesticides directory
            # REMOVED: "jordbrugsanalyser" - redundant validation dataset (99.98% match with marker)
            # REMOVED: "gkea": "silver/Fertiliser" - GKEA data removed from pipeline
        }


class DatasetConfig(BaseModel):
    """Configuration for a single dataset."""

    name: str
    file_path: Path
    required_columns: List[str]
    area_column: str
    cvr_column: str
