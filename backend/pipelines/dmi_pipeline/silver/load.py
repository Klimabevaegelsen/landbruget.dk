"""
DMI Climate Data Loading Layer
Handles storage of processed climate data in Parquet format.
Manages file system operations and data persistence.
"""

import logging
import pandas as pd
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataLoader:
    """Manages storage of processed climate data in Parquet format"""
    def __init__(self):
        pass

    def save_data(self, df: pd.DataFrame, output_dir: Path, filename: str) -> None:
        """Persists processed data to disk in Parquet format"""
        if df.empty:
            logger.warning("Empty DataFrame, skipping save")
            return

        # Create output directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save as parquet
        output_path = output_dir / f"{filename}.parquet"
        df.to_parquet(output_path)
        logger.info(f"Saved data to {output_path}")