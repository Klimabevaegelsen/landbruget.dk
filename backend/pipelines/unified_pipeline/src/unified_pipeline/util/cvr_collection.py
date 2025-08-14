"""
CVR Collection Utility

This module provides standardized functions for:
1. Pipelines to save CVR numbers they discover during processing
2. CVR enrichment pipeline to collect all discovered CVR numbers

The approach uses a standard file structure in GCS:
- Each pipeline saves CVR numbers to: cvr_collections/{pipeline_name}/{timestamp}/cvr_numbers.json
- CVR enrichment pipeline reads from all cvr_collections/*/ folders
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from unified_pipeline.util.gcs_access import GCSDataAccess
from unified_pipeline.util.log_util import Logger


class CVRCollectionManager:
    """
    Manager for CVR number collection across pipelines.

    This class provides methods for:
    - Saving CVR numbers from individual pipelines
    - Collecting all CVR numbers for enrichment
    - Managing the standard CVR collection file structure
    """

    def __init__(self, gcs_access: GCSDataAccess, bucket: str = "landbrugsdata-raw-data"):
        """
        Initialize CVR collection manager.

        Args:
            gcs_access: GCS data access instance
            bucket: GCS bucket for CVR collections
        """
        self.gcs_access = gcs_access
        self.bucket = bucket
        self.log = Logger.get_logger()

    def save_pipeline_cvr_numbers(
        self, pipeline_name: str, cvr_numbers: List[str], timestamp: Optional[str] = None
    ) -> str:
        """
        Save CVR numbers discovered by a pipeline to the standard location.

        Args:
            pipeline_name: Name of the pipeline (e.g., 'pesticide_disaggregation')
            cvr_numbers: List of CVR numbers discovered
            timestamp: Optional timestamp, defaults to current time

        Returns:
            GCS path where CVR numbers were saved
        """
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Remove duplicates and sort
        unique_cvr_numbers = sorted(list(set(cvr_numbers)))

        # Create CVR collection data
        cvr_data = {
            "pipeline_name": pipeline_name,
            "timestamp": timestamp,
            "cvr_count": len(unique_cvr_numbers),
            "cvr_numbers": unique_cvr_numbers,
            "generated_at": datetime.now().isoformat(),
        }

        # Define GCS path
        gcs_path = f"cvr_collections/{pipeline_name}/{timestamp}/cvr_numbers.json"

        # Save to GCS
        self.gcs_access.upload_json(cvr_data, f"gs://{self.bucket}/{gcs_path}")

        self.log.info(
            f"Saved {len(unique_cvr_numbers)} CVR numbers from {pipeline_name} to {gcs_path}"
        )

        return gcs_path

    def collect_all_cvr_numbers(self) -> Dict[str, Any]:
        """
        Collect all CVR numbers from all pipelines.

        Returns:
            Dictionary with collected CVR data including:
            - all_cvr_numbers: Set of all unique CVR numbers
            - pipeline_sources: Dict mapping CVR numbers to source pipelines
            - collection_summary: Summary of collection process
        """
        self.log.info("Starting CVR number collection from all pipelines")

        all_cvr_numbers = set()
        pipeline_sources = {}  # CVR -> list of pipelines that found it
        collection_summary = {
            "pipelines_processed": 0,
            "total_files_found": 0,
            "total_cvr_numbers_collected": 0,
            "collection_timestamp": datetime.now().isoformat(),
        }

        try:
            # List all pipeline folders in cvr_collections/
            pipeline_folders = self._list_pipeline_folders()

            for pipeline_folder in pipeline_folders:
                pipeline_name = pipeline_folder.rstrip("/").split("/")[-1]
                self.log.info(f"Processing CVR collections from pipeline: {pipeline_name}")

                # Get latest CVR collection for this pipeline
                latest_cvr_data = self._get_latest_cvr_collection(pipeline_name)

                if latest_cvr_data:
                    collection_summary["pipelines_processed"] += 1
                    collection_summary["total_files_found"] += 1

                    cvr_numbers = latest_cvr_data.get("cvr_numbers", [])
                    collection_summary["total_cvr_numbers_collected"] += len(cvr_numbers)

                    # Add to master set and track sources
                    for cvr in cvr_numbers:
                        all_cvr_numbers.add(cvr)
                        if cvr not in pipeline_sources:
                            pipeline_sources[cvr] = []
                        pipeline_sources[cvr].append(pipeline_name)

                    self.log.info(f"Collected {len(cvr_numbers)} CVR numbers from {pipeline_name}")
                else:
                    self.log.warning(f"No CVR collection found for pipeline: {pipeline_name}")

        except Exception as e:
            self.log.error(f"Error collecting CVR numbers: {e}")
            raise

        collection_summary["unique_cvr_numbers"] = len(all_cvr_numbers)

        # Calculate deduplication statistics
        total_collected = collection_summary["total_cvr_numbers_collected"]
        unique_collected = len(all_cvr_numbers)
        duplicates_removed = total_collected - unique_collected

        collection_summary["duplicates_removed"] = duplicates_removed
        collection_summary["deduplication_rate"] = (
            (duplicates_removed / total_collected * 100) if total_collected > 0 else 0
        )

        self.log.info("📊 CVR collection complete:")
        self.log.info(f"   • Total CVR numbers collected: {total_collected:,}")
        self.log.info(f"   • Unique CVR numbers: {unique_collected:,}")
        self.log.info(f"   • Duplicates removed: {duplicates_removed:,}")
        self.log.info(f"   • Deduplication rate: {collection_summary['deduplication_rate']:.1f}%")
        self.log.info(f"   • Source pipelines: {collection_summary['pipelines_processed']}")

        return {
            "all_cvr_numbers": all_cvr_numbers,
            "pipeline_sources": pipeline_sources,
            "collection_summary": collection_summary,
        }

    def _get_latest_cvr_collection(self, pipeline_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest CVR collection for a specific pipeline.

        Args:
            pipeline_name: Name of the pipeline

        Returns:
            Latest CVR collection data or None if not found
        """
        try:
            # List timestamp folders for this pipeline
            timestamp_folders = self._list_timestamp_folders(pipeline_name)

            if not timestamp_folders:
                return None

            # Get the latest timestamp folder, handling different timestamp formats
            timestamps = [folder.rstrip("/").split("/")[-1] for folder in timestamp_folders]
            latest_timestamp = self._get_latest_timestamp(timestamps)

            # Load the CVR numbers file
            cvr_file_path = f"cvr_collections/{pipeline_name}/{latest_timestamp}/cvr_numbers.json"

            return self.gcs_access.download_json(f"gs://{self.bucket}/{cvr_file_path}")

        except Exception as e:
            self.log.warning(f"Could not load CVR collection for {pipeline_name}: {e}")
            return None

    def _list_pipeline_folders(self) -> List[str]:
        """List all pipeline folders in cvr_collections/."""
        try:
            # List all files in cvr_collections/ and extract unique pipeline names
            pattern = f"gs://{self.bucket}/cvr_collections/*/*/*"
            files = self.gcs_access.list_files(pattern)

            # Extract unique pipeline names from file paths
            pipeline_names = set()
            for file_path in files:
                # Extract pipeline name from path like: gs://bucket/cvr_collections/pipeline_name/timestamp/file.json
                parts = file_path.replace(f"gs://{self.bucket}/", "").split("/")
                if len(parts) >= 3 and parts[0] == "cvr_collections":
                    pipeline_names.add(parts[1])

            # Return as folder paths
            return [f"cvr_collections/{name}/" for name in sorted(pipeline_names)]

        except Exception as e:
            self.log.warning(f"Could not list pipeline folders: {e}")
            return []

    def _list_timestamp_folders(self, pipeline_name: str) -> List[str]:
        """List all timestamp folders for a specific pipeline."""
        try:
            # List all files for this pipeline and extract unique timestamps
            pattern = f"gs://{self.bucket}/cvr_collections/{pipeline_name}/*/*"
            files = self.gcs_access.list_files(pattern)

            # Extract unique timestamps from file paths
            timestamps = set()
            for file_path in files:
                # Extract timestamp from path like: gs://bucket/cvr_collections/pipeline_name/timestamp/file.json
                parts = file_path.replace(f"gs://{self.bucket}/", "").split("/")
                if len(parts) >= 4 and parts[0] == "cvr_collections" and parts[1] == pipeline_name:
                    timestamps.add(parts[2])

            # Return as folder paths
            return [f"cvr_collections/{pipeline_name}/{ts}/" for ts in sorted(timestamps)]

        except Exception as e:
            self.log.warning(f"Could not list timestamp folders for {pipeline_name}: {e}")
            return []

    def _get_latest_timestamp(self, timestamps: List[str]) -> str:
        """
        Get the latest timestamp from a list, handling different timestamp formats.

        Args:
            timestamps: List of timestamp strings

        Returns:
            Latest timestamp string
        """
        if not timestamps:
            raise ValueError("No timestamps provided")

        # Separate different timestamp formats and filter out non-timestamp folders
        standard_timestamps = []  # Format: YYYYMMDD_HHMMSS
        run_timestamps = []  # Format: run_XX_XXXXXXXXXX

        for ts in timestamps:
            if ts.startswith("run_"):
                run_timestamps.append(ts)
            elif self._is_standard_timestamp(ts):
                standard_timestamps.append(ts)
            # Skip non-timestamp folders like 'test', 'temp', etc.

        # Prefer standard timestamps (they're more recent format)
        if standard_timestamps:
            return sorted(standard_timestamps)[-1]
        elif run_timestamps:
            # For run timestamps, sort by the numeric part after the last underscore
            def extract_run_number(run_ts):
                try:
                    return int(run_ts.split("_")[-1])
                except (ValueError, IndexError):
                    return 0

            return sorted(run_timestamps, key=extract_run_number)[-1]
        else:
            # If no valid timestamps found, raise an error
            raise ValueError(f"No valid timestamp folders found in: {timestamps}")

    def _is_standard_timestamp(self, timestamp: str) -> bool:
        """
        Check if a string matches the standard timestamp format YYYYMMDD_HHMMSS.

        Args:
            timestamp: String to check

        Returns:
            True if it matches the standard format
        """
        if len(timestamp) != 15 or timestamp[8] != "_":
            return False

        try:
            # Check if date part (YYYYMMDD) and time part (HHMMSS) are numeric
            date_part = timestamp[:8]
            time_part = timestamp[9:]
            int(date_part)  # Should be 8 digits
            int(time_part)  # Should be 6 digits
            return len(date_part) == 8 and len(time_part) == 6
        except ValueError:
            return False


def extract_cvr_numbers_from_table(
    table_name: str, connection, cvr_column: str = "cvr_number"
) -> List[str]:
    """
    Extract unique CVR numbers from a DuckDB table.

    Args:
        table_name: Name of the table to extract from
        connection: DuckDB connection
        cvr_column: Name of the CVR column

    Returns:
        List of unique CVR numbers
    """
    try:
        query = f"""
        SELECT DISTINCT {cvr_column}
        FROM {table_name}
        WHERE {cvr_column} IS NOT NULL
        AND {cvr_column} != ''
        AND LENGTH(TRIM({cvr_column})) = 8
        AND {cvr_column} ~ '^[1-9][0-9]{{7}}$'  -- 8 digits, not starting with 0
        ORDER BY {cvr_column}
        """

        result = connection.execute(query).fetchall()
        return [row[0] for row in result]

    except Exception as e:
        Logger.get_logger().error(f"Error extracting CVR numbers from {table_name}: {e}")
        return []


def save_pipeline_cvr_numbers(
    pipeline_name: str,
    cvr_numbers: List[str],
    gcs_access: Optional[GCSDataAccess] = None,
    bucket: str = "landbrugsdata-raw-data",
    timestamp: Optional[str] = None,
) -> str:
    """
    Convenience function to save CVR numbers from a pipeline.

    Args:
        pipeline_name: Name of the pipeline
        cvr_numbers: List of CVR numbers to save
        gcs_access: GCS data access instance (creates default if None)
        bucket: GCS bucket name
        timestamp: Optional timestamp

    Returns:
        GCS path where data was saved
    """
    # Create default GCS access if none provided
    if gcs_access is None:
        from unified_pipeline.util.gcs_access import GCSDataAccess

        gcs_access = GCSDataAccess()

    manager = CVRCollectionManager(gcs_access, bucket)
    return manager.save_pipeline_cvr_numbers(pipeline_name, cvr_numbers, timestamp)
