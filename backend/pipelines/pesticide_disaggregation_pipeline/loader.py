import logging
import tempfile
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)


class DatasetLoader:
    """Handles loading and validation of datasets from GCS silver layer."""

    def __init__(self, db_manager, config):
        """Initialize with database manager and configuration."""
        self.db = db_manager
        self.config = config

        # Initialize GCS filesystem
        try:
            import gcsfs

            self.gcs = gcsfs.GCSFileSystem()
            logger.info(f"✅ GCS initialized, using bucket: {config.GCS_BUCKET}")
        except ImportError:
            raise ImportError("gcsfs is required for GCS integration. Install with: pip install gcsfs")

    def find_latest_data_path(self, base_path: str, pattern: str = "data.parquet") -> str:
        """Find the most recent data file in a GCS directory structure"""
        try:
            # List all directories/files matching the pattern
            paths = self.gcs.glob(f"{base_path}/*/{pattern}")
            if not paths:
                # Try direct pattern match
                paths = self.gcs.glob(f"{base_path}/{pattern}")

            if not paths:
                raise FileNotFoundError(f"No files found matching pattern {base_path}/*/{pattern}")

            # Sort by path (timestamp directories sort chronologically)
            latest_path = sorted(paths)[-1]
            return f"gs://{latest_path}"

        except Exception as e:
            logger.error(f"Error finding latest data for {base_path}: {e}")
            raise

    def find_specific_file(self, base_path: str, filename: str) -> str:
        """Find a specific file in a GCS directory structure (for pesticide and GKEA data)"""
        try:
            # Look for the file in timestamped subdirectories
            paths = self.gcs.glob(f"{base_path}/*/{filename}")
            if not paths:
                # Try direct pattern match
                paths = self.gcs.glob(f"{base_path}/{filename}")

            # For GKEA files, try alternative naming patterns if not found
            if not paths and "GKEA" in filename:
                # Try with _Aktindsigt suffix for some years
                alt_filename = filename.replace(".parquet", "_Aktindsigt.parquet")
                paths = self.gcs.glob(f"{base_path}/*/{alt_filename}")
                if not paths:
                    paths = self.gcs.glob(f"{base_path}/{alt_filename}")
                if paths:
                    logger.info(f"📋 Found GKEA file with alternative naming: {alt_filename}")

            if not paths:
                raise FileNotFoundError(f"No files found matching {base_path}/*/{filename} or alternative patterns")

            # Sort by path (timestamp directories sort chronologically) and take the latest
            latest_path = sorted(paths)[-1]
            return f"gs://{latest_path}"

        except Exception as e:
            logger.error(f"Error finding specific file {filename} in {base_path}: {e}")
            raise

    def download_gcs_file(self, gcs_path: str, local_filename: str) -> str:
        """Download a GCS file to local storage and return the local path"""
        # Create temp directory for this run
        temp_dir = Path(tempfile.gettempdir()) / "pesticide_disaggregation_cache"
        temp_dir.mkdir(exist_ok=True)

        local_path = temp_dir / local_filename

        # Skip download if file already exists locally
        if local_path.exists():
            logger.info(f"📁 Using cached file: {local_path}")
            return str(local_path)

        logger.info(f"⬇️  Downloading {gcs_path} to {local_path}")

        # Remove gs:// prefix for gcsfs
        gcs_file_path = gcs_path.replace("gs://", "")

        try:
            # Download using gcsfs
            self.gcs.get(gcs_file_path, str(local_path))
            logger.info(f"✅ Downloaded {local_path.name} ({local_path.stat().st_size / 1024 / 1024:.1f} MB)")
            return str(local_path)
        except Exception as e:
            logger.error(f"❌ Failed to download {gcs_path}: {e}")
            raise

    def get_dataset_paths(self, pesticide_year: int = None) -> Dict[str, str]:
        """Get paths to all required datasets from GCS silver layer"""

        if pesticide_year is None:
            pesticide_year = self.config.PESTICIDE_YEAR

        logger.info(f"🔍 Loading datasets for pesticide year {pesticide_year}")
        logger.info("🌐 Discovering latest silver data from GCS...")

        gcs_sources = self.config.get_gcs_silver_sources(pesticide_year)
        local_paths = {}

        # Define specific file patterns for each dataset type
        field_year = pesticide_year + 1
        file_patterns = {
            "marker": "data.parquet",
            "jordbrugsanalyser": "data.parquet",
            "pesticide": f"pesticiddata_{pesticide_year}_{pesticide_year + 1}.parquet",
            "gkea": f"GKEA{field_year}_Markplan_med_Gødningsoplysninger.parquet",
        }

        for dataset_name, gcs_base_path in gcs_sources.items():
            try:
                full_gcs_path = f"{self.config.GCS_BUCKET}/{gcs_base_path}"
                pattern = file_patterns.get(dataset_name, "data.parquet")

                # Use appropriate method based on dataset type
                if dataset_name in ["pesticide", "gkea"]:
                    gcs_file_path = self.find_specific_file(full_gcs_path, pattern)
                else:
                    gcs_file_path = self.find_latest_data_path(full_gcs_path, pattern)

                # Download to local cache
                local_filename = f"{dataset_name}_{pesticide_year}.parquet"
                local_path = self.download_gcs_file(gcs_file_path, local_filename)
                local_paths[dataset_name] = local_path

                logger.info(f"✅ {dataset_name}: {gcs_file_path} -> {local_filename}")

            except Exception as e:
                logger.error(f"❌ Failed to load {dataset_name} from GCS: {e}")
                raise FileNotFoundError(f"Required dataset {dataset_name} not found in GCS silver layer: {e}")

        return local_paths

    def load_datasets(self, pesticide_year: int = None) -> None:
        """Load all datasets into DuckDB tables."""
        dataset_paths = self.get_dataset_paths(pesticide_year)

        for table_name, file_path in dataset_paths.items():
            logger.info(f"📊 Loading {table_name} from {file_path}")
            self.db.create_table(table_name, file_path)

    def validate_dataset(self, table_name: str) -> Dict:
        """Validate dataset structure and content."""
        try:
            schema = self.db.execute_query(f"DESCRIBE {table_name}")
            count = self.db.execute_query(f"SELECT COUNT(*) as count FROM {table_name}")[0][0]
            null_counts = {}
            for col_info in schema:
                col_name = col_info[0]
                # Ensure column name is properly quoted if it contains special characters or spaces
                null_count = self.db.execute_query(f'SELECT COUNT(*) FROM {table_name} WHERE "{col_name}" IS NULL')[0][
                    0
                ]
                null_counts[col_name] = null_count

            return {
                "schema": schema,
                "total_records": count,
                "null_counts": null_counts,
            }
        except Exception as e:
            logger.error(f"Error validating dataset {table_name}: {str(e)}")
            raise
