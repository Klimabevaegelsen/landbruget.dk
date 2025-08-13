"""
Shared configuration for CVR enrichment pipeline steps.

This module defines common configuration classes and utilities used across
all CVR enrichment pipeline steps to ensure consistency and maintainability.
"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class CVREnrichmentStep(str, Enum):
    """Enumeration of CVR enrichment pipeline steps."""

    COLLECTION = "collection"
    COMPANY_FETCHING = "company_fetching"
    PNUMBER_FETCHING = "pnumber_fetching"
    FINANCIAL_DOCUMENTS = "financial_documents"
    ADDRESS_GEOCODING = "address_geocoding"
    DATA_CONSOLIDATION = "data_consolidation"


class CVREnrichmentSharedConfig(BaseModel):
    """
    Shared configuration for all CVR enrichment pipeline steps.

    This configuration is used across all pipeline steps to ensure
    consistent behavior and data handling.
    """

    # Data storage configuration
    bucket: str = "landbrugsdata-raw-data"
    dataset: str = "cvr_enrichment"

    # CVR API configuration
    cvr_api_rate_limit: float = Field(
        default=0.1, description="Minimum interval between CVR API requests (seconds)"
    )

    # Batch processing configuration
    default_batch_size: int = Field(
        default=50, description="Default batch size for processing companies"
    )

    max_concurrent_requests: int = Field(
        default=5, description="Maximum number of concurrent API requests"
    )

    api_batch_size: int = Field(
        default=100,
        description="Number of CVR numbers or P-numbers to fetch per API call using "
                    "'terms' query (optimized for maximum efficiency)",
    )

    # Address geocoding configuration
    enable_address_geocoding: bool = Field(
        default=True, description="Whether to enable address geocoding via DAWA API"
    )

    geocoding_current_addresses_only: bool = Field(
        default=True, description="Whether to geocode only current addresses (not historical)"
    )

    # Financial documents configuration
    fetch_financial_documents: bool = Field(
        default=True, description="Whether to fetch financial documents for each company"
    )

    max_financial_documents: int = Field(
        default=10, description="Maximum number of financial documents to fetch per company"
    )

    parse_financial_xml: bool = Field(
        default=True, description="Whether to download and parse XML financial documents"
    )

    # P-number configuration
    fetch_pnumber_data: bool = Field(
        default=True, description="Whether to fetch P-number data for companies"
    )

    pnumber_current_addresses_only: bool = Field(
        default=True,
        description="Whether to fetch only current P-number addresses (not historical)",
    )

    # Testing configuration
    test_limit: Optional[int] = Field(
        default=None,
        description="Limit number of CVR numbers to process for testing (None = no limit)",
    )

    # Output configuration
    save_raw_responses: bool = Field(
        default=True, description="Whether to save raw API responses for debugging"
    )

    # Step execution configuration
    current_step: Optional[CVREnrichmentStep] = Field(
        default=None, description="Current pipeline step being executed"
    )

    batch_number: Optional[int] = Field(
        default=None, description="Current batch number for parallel processing"
    )

    total_batches: Optional[int] = Field(
        default=None, description="Total number of batches in the current step"
    )

    # Independent execution configuration
    enable_independent_execution: bool = Field(
        default=True,
        description="Whether to enable independent step execution by fetching "
                    "latest files from GCS",
    )

    max_days_back_for_inputs: int = Field(
        default=30,
        description="Maximum number of days to look back when fetching latest input files from GCS",
    )

    fallback_to_pipeline_dependencies: bool = Field(
        default=True,
        description="Whether to fall back to pipeline dependencies if latest files are not found",
    )

    model_config = {"frozen": True}


def get_step_output_path(
    step: CVREnrichmentStep,
    date_pattern: str,
    batch_number: Optional[int] = None,
    bucket: str = "landbrugsdata-raw-data",
) -> str:
    """
    Get the GCS output path for a specific pipeline step.

    Args:
        step: Pipeline step
        date_pattern: Date pattern for the pipeline run
        batch_number: Optional batch number for parallel processing
        bucket: GCS bucket name

    Returns:
        GCS path for the step output
    """
    base_path = f"gs://{bucket}/gold/cvr_enrichment/{date_pattern}"

    if batch_number is not None:
        return f"{base_path}/{step.value}_batch_{batch_number:03d}.parquet"
    else:
        return f"{base_path}/{step.value}.parquet"


def get_step_input_paths(
    step: CVREnrichmentStep,
    date_pattern: str,
    total_batches: Optional[int] = None,
    bucket: str = "landbrugsdata-raw-data",
    enable_independent_execution: bool = True,
    max_days_back: int = 30,
) -> list[str]:
    """
    Get the GCS input paths for a specific pipeline step.

    This function intelligently determines whether to use pipeline dependencies
    or independent execution based on data availability:

    1. If independent_execution is disabled, always use pipeline dependencies
    2. If independent_execution is enabled:
       - First check if pipeline dependencies exist (artifacts or GCS files)
       - If they exist, use them (pipeline mode)
       - If they don't exist, fetch latest files from GCS (independent mode)

    This ensures that steps running as part of a pipeline workflow use artifacts,
    while steps running independently fetch the latest available data.

    Args:
        step: Pipeline step
        date_pattern: Date pattern for the pipeline run
        total_batches: Total number of batches (for batch-dependent steps)
        bucket: GCS bucket name
        enable_independent_execution: Whether to enable smart independent execution
        max_days_back: Maximum days to look back for latest files (independent mode)

    Returns:
        List of GCS paths for the step inputs
    """
    # If independent execution is disabled, use traditional pipeline dependencies
    if not enable_independent_execution:
        return _get_traditional_input_paths(step, date_pattern, bucket)

    # Check if we're running as part of a pipeline (artifacts available) or truly independently
    # First try to find pipeline dependencies from the current run
    pipeline_paths = _get_traditional_input_paths(step, date_pattern, bucket)

    # Check if pipeline dependencies exist (indicating we're part of a pipeline run)
    pipeline_dependencies_exist = _check_pipeline_dependencies_exist(pipeline_paths)

    if pipeline_dependencies_exist:
        # We're running as part of a pipeline - use pipeline dependencies (artifacts)
        return pipeline_paths

    # We're running independently - fetch latest available files from GCS using existing utility
    return _get_latest_input_paths_from_gcs(step, bucket, max_days_back)


def _get_latest_input_paths_from_gcs(
    step: CVREnrichmentStep, bucket: str, max_days_back: int
) -> list[str]:
    """
    Get the latest available input files for a step from GCS using existing utility.

    Args:
        step: Pipeline step
        bucket: GCS bucket name
        max_days_back: Maximum days to look back

    Returns:
        List of GCS paths for the latest available inputs
    """
    from unified_pipeline.util.gcs_access import GCSDataAccess

    try:
        gcs_access = GCSDataAccess()
        base_pattern = f"gs://{bucket}/gold/cvr_enrichment"

        # Step-specific input logic with latest file fetching using existing utility
        if step == CVREnrichmentStep.COLLECTION:
            # Collection step has no inputs (reads from all pipeline CVR collections)
            return []

        elif step == CVREnrichmentStep.COMPANY_FETCHING:
            # Company fetching depends on collection step
            pattern = f"gs://{bucket}/gold/cvr_enrichment_collection/*/data.parquet"
            latest_file = _find_latest_file_with_pattern(gcs_access, pattern, max_days_back)
            return [latest_file] if latest_file else []

        elif step == CVREnrichmentStep.PNUMBER_FETCHING:
            # P-number fetching depends on company fetching
            pattern = f"{base_pattern}/*/company_fetching.parquet"
            latest_file = _find_latest_file_with_pattern(gcs_access, pattern, max_days_back)
            return [latest_file] if latest_file else []

        elif step == CVREnrichmentStep.FINANCIAL_DOCUMENTS:
            # Financial documents depend on company fetching
            pattern = f"{base_pattern}/*/company_fetching.parquet"
            latest_file = _find_latest_file_with_pattern(gcs_access, pattern, max_days_back)
            return [latest_file] if latest_file else []

        elif step == CVREnrichmentStep.ADDRESS_GEOCODING:
            # Address geocoding depends on both company and P-number data
            company_pattern = f"{base_pattern}/*/company_fetching.parquet"
            pnumber_pattern = f"{base_pattern}/*/pnumber_fetching.parquet"

            latest_company = _find_latest_file_with_pattern(
                gcs_access, company_pattern, max_days_back
            )
            latest_pnumber = _find_latest_file_with_pattern(
                gcs_access, pnumber_pattern, max_days_back
            )

            inputs = []
            if latest_company:
                inputs.append(latest_company)
            if latest_pnumber:
                inputs.append(latest_pnumber)
            return inputs

        elif step == CVREnrichmentStep.DATA_CONSOLIDATION:
            # Data consolidation depends on all previous steps
            patterns = [
                f"{base_pattern}/*/company_fetching.parquet",
                f"{base_pattern}/*/pnumber_fetching.parquet",
                f"{base_pattern}/*/financial_documents.parquet",
                f"{base_pattern}/*/address_geocoding.parquet",
            ]

            inputs = []
            for pattern in patterns:
                latest_file = _find_latest_file_with_pattern(gcs_access, pattern, max_days_back)
                if latest_file:
                    inputs.append(latest_file)
            return inputs

        else:
            return []

    except Exception as e:
        from unified_pipeline.util.log_util import Logger

        logger = Logger.get_logger()
        logger.warning(f"Error finding latest files from GCS: {e}")
        return []


def _find_latest_company_data_file(gcs_access, pattern: str, max_days_back: int) -> Optional[str]:
    """
    Find the latest file matching a pattern that contains company data
    (has company_data_json column).

    Args:
        gcs_access: GCSDataAccess instance
        pattern: GCS file pattern to search
        max_days_back: Maximum days to look back

    Returns:
        Path to latest company data file or None
    """
    from datetime import datetime, timedelta

    try:
        # Get files with timestamps using existing utility
        files_with_timestamps = gcs_access.list_files_with_timestamps(pattern)

        if not files_with_timestamps:
            return None

        # Filter files within the time window and check for company data
        cutoff_date = datetime.now() - timedelta(days=max_days_back)
        company_files = []

        for filepath, timestamp in files_with_timestamps:
            try:
                # Handle timezone-aware vs timezone-naive comparison
                if timestamp.tzinfo is not None:
                    # timestamp is timezone-aware, make cutoff_date timezone-aware too
                    from datetime import timezone

                    if cutoff_date.tzinfo is None:
                        cutoff_date = cutoff_date.replace(tzinfo=timezone.utc)
                else:
                    # timestamp is timezone-naive, ensure cutoff_date is timezone-naive
                    if cutoff_date.tzinfo is not None:
                        cutoff_date = cutoff_date.replace(tzinfo=None)

                if timestamp >= cutoff_date:
                    # Check if this file contains company data
                    if _has_company_data(gcs_access, filepath):
                        company_files.append((filepath, timestamp))
            except Exception as e:
                from unified_pipeline.util.log_util import Logger

                logger = Logger.get_logger()
                logger.warning(f"Error checking file {filepath}: {e}")
                continue

        if not company_files:
            return None

        # Sort by timestamp and return the latest
        company_files.sort(key=lambda x: x[1], reverse=True)
        return company_files[0][0]

    except Exception as e:
        from unified_pipeline.util.log_util import Logger

        logger = Logger.get_logger()
        logger.warning(f"Error finding latest company data file with pattern {pattern}: {e}")
        return None


def _has_company_data(gcs_access, filepath: str) -> bool:
    """
    Check if a file contains company data by looking for company_data_json column.

    Args:
        gcs_access: GCSDataAccess instance
        filepath: Path to the file

    Returns:
        True if file contains company data, False otherwise
    """
    try:
        import duckdb
        import gcsfs

        # Create a temporary connection for checking file structure
        temp_conn = duckdb.connect()
        temp_conn.install_extension("spatial")
        temp_conn.load_extension("spatial")
        fs = gcsfs.GCSFileSystem()
        temp_conn.register_filesystem(fs)

        columns_result = temp_conn.execute(f"""
            DESCRIBE (SELECT * FROM read_parquet('{filepath}') LIMIT 1)
        """).fetchall()

        column_names = [col[0] for col in columns_result]
        temp_conn.close()
        return "company_data_json" in column_names

    except Exception:
        return False


def _find_latest_file_with_pattern(gcs_access, pattern: str, max_days_back: int) -> Optional[str]:
    """
    Find the latest file matching a pattern using the existing GCS utility.

    Args:
        gcs_access: GCSDataAccess instance
        pattern: GCS file pattern to search
        max_days_back: Maximum days to look back

    Returns:
        Path to latest file or None
    """
    from datetime import datetime, timedelta

    try:
        # Get files with timestamps using existing utility
        files_with_timestamps = gcs_access.list_files_with_timestamps(pattern)

        if not files_with_timestamps:
            return None

        # Filter files within the time window
        cutoff_date = datetime.now() - timedelta(days=max_days_back)
        recent_files = []
        for filepath, timestamp in files_with_timestamps:
            try:
                # Handle timezone-aware vs timezone-naive comparison
                if timestamp.tzinfo is not None:
                    # timestamp is timezone-aware, make cutoff_date timezone-aware too
                    from datetime import timezone

                    if cutoff_date.tzinfo is None:
                        cutoff_date = cutoff_date.replace(tzinfo=timezone.utc)
                else:
                    # timestamp is timezone-naive, ensure cutoff_date is timezone-naive
                    if cutoff_date.tzinfo is not None:
                        cutoff_date = cutoff_date.replace(tzinfo=None)

                if timestamp >= cutoff_date:
                    recent_files.append((filepath, timestamp))
            except Exception as e:
                from unified_pipeline.util.log_util import Logger

                logger = Logger.get_logger()
                logger.warning(f"Error comparing timestamps for {filepath}: {e}")
                # Include the file anyway to avoid missing data
                recent_files.append((filepath, timestamp))

        if not recent_files:
            return None

        # Sort by timestamp (most recent first) and return the latest
        recent_files.sort(key=lambda x: x[1], reverse=True)
        return recent_files[0][0]  # Return the file path

    except Exception as e:
        from unified_pipeline.util.log_util import Logger

        logger = Logger.get_logger()
        logger.warning(f"Error searching for pattern {pattern}: {e}")
        return None


def _check_pipeline_dependencies_exist(pipeline_paths: list[str]) -> bool:
    """
    Check if pipeline dependencies exist, indicating we're part of a pipeline run.

    This function checks for both local artifacts (GitHub Actions) and GCS files.

    Args:
        pipeline_paths: List of expected pipeline dependency paths

    Returns:
        True if pipeline dependencies exist, False otherwise
    """
    import os

    if not pipeline_paths:
        return False  # No dependencies expected

    # Check for local artifacts first (GitHub Actions workflow)
    local_artifact_patterns = {
        "collection.parquet": "/tmp/cvr_collection_data.parquet",
        "company_fetching.parquet": "/tmp/cvr_company_data.parquet",
        "pnumber_fetching.parquet": "/tmp/cvr_pnumber_data.parquet",
        "financial_documents.parquet": "/tmp/cvr_financial_data.parquet",
        "address_geocoding.parquet": "/tmp/cvr_address_data.parquet",
    }

    # If running in GitHub Actions, check for local artifacts
    if os.getenv("GITHUB_ACTIONS") == "true":
        for pipeline_path in pipeline_paths:
            # Extract the file type from the pipeline path
            for file_type, local_path in local_artifact_patterns.items():
                if file_type in pipeline_path and os.path.exists(local_path):
                    return True  # Found at least one local artifact
        return False  # No local artifacts found

    # Check GCS files existence (for non-GitHub Actions environments)
    try:
        from google.cloud import storage

        # Parse bucket and path from GCS URLs
        for pipeline_path in pipeline_paths[:1]:  # Check just the first one for efficiency
            if pipeline_path.startswith("gs://"):
                # Extract bucket and blob path
                parts = pipeline_path[5:].split("/", 1)
                if len(parts) == 2:
                    bucket_name, blob_path = parts

                    client = storage.Client()
                    bucket = client.bucket(bucket_name)
                    blob = bucket.blob(blob_path)

                    if blob.exists():
                        return True  # Found at least one pipeline file

        return False  # No pipeline files found

    except Exception:
        # If we can't check GCS, assume independent execution
        return False


def _get_traditional_input_paths(
    step: CVREnrichmentStep, date_pattern: str, bucket: str = "landbrugsdata-raw-data"
) -> list[str]:
    """
    Get traditional pipeline dependency input paths (original behavior).

    Args:
        step: Pipeline step
        date_pattern: Date pattern for the pipeline run
        bucket: GCS bucket name

    Returns:
        List of GCS paths for the step inputs
    """
    base_path = f"gs://{bucket}/gold/cvr_enrichment/{date_pattern}"

    # Step-specific input logic
    if step == CVREnrichmentStep.COLLECTION:
        # Collection step has no inputs (reads from all pipeline CVR collections)
        return []

    elif step == CVREnrichmentStep.COMPANY_FETCHING:
        # Company fetching depends on collection step
        return [f"{base_path}/collection.parquet"]

    elif step == CVREnrichmentStep.PNUMBER_FETCHING:
        # P-number fetching depends on company fetching (no batching)
        return [f"{base_path}/company_fetching.parquet"]

    elif step == CVREnrichmentStep.FINANCIAL_DOCUMENTS:
        # Financial documents depend on company fetching (no batching)
        return [f"{base_path}/company_fetching.parquet"]

    elif step == CVREnrichmentStep.ADDRESS_GEOCODING:
        # Address geocoding depends on both company and P-number data (no batching)
        return [f"{base_path}/company_fetching.parquet", f"{base_path}/pnumber_fetching.parquet"]

    elif step == CVREnrichmentStep.DATA_CONSOLIDATION:
        # Data consolidation depends on all previous steps (no batching)
        return [
            f"{base_path}/company_fetching.parquet",
            f"{base_path}/pnumber_fetching.parquet",
            f"{base_path}/financial_documents.parquet",
            f"{base_path}/address_geocoding.parquet",
        ]

    else:
        return []
