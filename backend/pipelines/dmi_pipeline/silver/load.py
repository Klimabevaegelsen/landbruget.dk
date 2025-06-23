"""
DMI Climate Data Loading Layer
Handles storage of processed climate data in Parquet format.
Manages file system operations and data persistence.
"""

import logging
from pathlib import Path
from typing import Optional

import duckdb

# Configure logging
logger = logging.getLogger(__name__)


class DataLoader:
    """Manages storage of processed climate data in Parquet format"""

    def __init__(self):
        pass

    def save_data(self, result: Optional[duckdb.DuckDBPyRelation], output_dir: Path, filename: str) -> bool:
        """Persists processed data to disk in Parquet format"""
        if result is None:
            logger.warning("Empty result, skipping save")
            return False

        try:
            # Create output directory if it doesn't exist
            output_dir.mkdir(parents=True, exist_ok=True)

            # Convert result to DataFrame
            df = result.df()

            # Create a temporary view for the result
            con = duckdb.connect(":memory:")
            con.execute("INSTALL spatial;")
            con.execute("LOAD spatial;")
            con.register("result_view", df)

            # Save as parquet using DuckDB's native parquet export
            output_path = output_dir / f"{filename}.parquet"
            con.execute(f"COPY result_view TO '{output_path}' (FORMAT PARQUET)")
            logger.info(f"Successfully saved data to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Error saving data to {output_dir}: {str(e)}")
            return False

    def load_data(self, input_path: Path) -> Optional[duckdb.DuckDBPyRelation]:
        """Load data from a Parquet file"""
        try:
            if not input_path.exists():
                logger.warning(f"Input file does not exist: {input_path}")
                return None

            # Create DuckDB connection and enable spatial extension
            con = duckdb.connect(":memory:")
            con.execute("INSTALL spatial;")
            con.execute("LOAD spatial;")

            # Load data from parquet file
            result = con.execute(f"SELECT * FROM read_parquet('{input_path}')")
            logger.info(f"Successfully loaded data from {input_path}")
            return result

        except Exception as e:
            logger.error(f"Error loading data from {input_path}: {str(e)}")
            return None

    def get_latest_data(self, data_dir: Path, pattern: str = "*.parquet") -> Optional[Path]:
        """Get the path to the latest data file matching the pattern"""
        try:
            if not data_dir.exists():
                logger.warning(f"Data directory does not exist: {data_dir}")
                return None

            # Find all matching files
            files = list(data_dir.glob(pattern))
            if not files:
                logger.warning(f"No files found matching pattern {pattern} in {data_dir}")
                return None

            # Get the latest file based on modification time
            latest_file = max(files, key=lambda x: x.stat().st_mtime)
            logger.info(f"Found latest data file: {latest_file}")
            return latest_file

        except Exception as e:
            logger.error(f"Error finding latest data file: {str(e)}")
            return None
