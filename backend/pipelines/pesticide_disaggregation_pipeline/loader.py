import logging
from typing import Dict

# Assuming Config and DatabaseManager will be imported from other modules
# from .config import Config
# from .database import DatabaseManager

logger = logging.getLogger(__name__)


class DatasetLoader:
    """Handles loading and validation of datasets."""

    def __init__(self, db_manager, config):  # Simplified types for now
        """Initialize with database manager and configuration."""
        self.db = db_manager
        self.config = config

    def load_datasets(self) -> None:
        """Load all datasets into DuckDB tables."""
        for table_name, file_name in self.config.DATASETS.items():
            # Construct the full path to the Parquet file
            # DATA_DIR is Path object from Config, file_name is str
            file_path = self.config.DATA_DIR / file_name
            if not file_path.exists():
                logger.error(f"Dataset file not found: {file_path}")
                raise FileNotFoundError(f"Dataset file not found: {file_path}")
            self.db.create_table(table_name, str(file_path))  # Pass file_path as string

    def validate_dataset(self, table_name: str) -> Dict:
        """Validate dataset structure and content."""
        try:
            schema = self.db.execute_query(f"DESCRIBE {table_name}")
            count = self.db.execute_query(
                f"SELECT COUNT(*) as count FROM {table_name}"
            )[0][0]
            null_counts = {}
            for col_info in schema:
                col_name = col_info[0]
                # Ensure column name is properly quoted if it contains special characters or spaces
                # However, DESCRIBE should return simple names. If issues arise, this is a place to check.
                null_count = self.db.execute_query(
                    f'SELECT COUNT(*) FROM {table_name} WHERE "{col_name}" IS NULL'
                )[0][0]
                null_counts[col_name] = null_count

            return {
                "schema": schema,
                "total_records": count,
                "null_counts": null_counts,
            }
        except Exception as e:
            logger.error(f"Error validating dataset {table_name}: {str(e)}")
            raise
