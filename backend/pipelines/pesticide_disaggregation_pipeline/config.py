from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel


# Configuration
@dataclass
class Config:
    """Configuration settings for the analysis."""

    DATA_DIR: Path = Path("/Users/martincollignon/landbrugsdata/landbruget.dk")
    OUTPUT_DIR: Path = Path("outputs")  # Relative path name for the output directory
    RESOLVED_OUTPUT_DIR: Optional[Path] = None
    AREA_TOLERANCE_PCT: float = 2.0
    MAX_FIELDS_FOR_SUBSET_SUM: int = (
        20  # Maximum number of fields to consider for subset sum
    )
    DATASETS: Dict[str, str] = field(
        default_factory=lambda: {
            "marker": "marker_marker_2022.parquet",
            "jordbrugsanalyser": "jordbrugsanalyser_marker22.parquet",
            "oekologiske_arealer": "oekologiske_arealer_2022.parquet",
            "pesticide": "pesticiddata_2021_2022.parquet",
            "gkea": "GKEA2022_Markplan_med_Gødningsoplysninger.parquet",
        }
    )


class DatasetConfig(BaseModel):
    """Configuration for a single dataset."""

    name: str
    file_path: Path
    required_columns: List[str]
    area_column: str
    cvr_column: str
