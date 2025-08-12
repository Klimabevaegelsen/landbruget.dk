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
        default=0.1, 
        description="Minimum interval between CVR API requests (seconds)"
    )
    
    # Batch processing configuration
    default_batch_size: int = Field(
        default=50, 
        description="Default batch size for processing companies"
    )
    
    max_concurrent_requests: int = Field(
        default=5, 
        description="Maximum number of concurrent API requests"
    )
    
    api_batch_size: int = Field(
        default=100,
        description="Number of CVR numbers or P-numbers to fetch per API call using 'terms' query (optimized for maximum efficiency)"
    )
    
    # Address geocoding configuration
    enable_address_geocoding: bool = Field(
        default=True, 
        description="Whether to enable address geocoding via DAWA API"
    )
    
    geocoding_current_addresses_only: bool = Field(
        default=True, 
        description="Whether to geocode only current addresses (not historical)"
    )
    
    # Financial documents configuration
    fetch_financial_documents: bool = Field(
        default=True, 
        description="Whether to fetch financial documents for each company"
    )
    
    max_financial_documents: int = Field(
        default=10, 
        description="Maximum number of financial documents to fetch per company"
    )
    
    parse_financial_xml: bool = Field(
        default=True, 
        description="Whether to download and parse XML financial documents"
    )
    
    # P-number configuration
    fetch_pnumber_data: bool = Field(
        default=True, 
        description="Whether to fetch P-number data for companies"
    )
    
    pnumber_current_addresses_only: bool = Field(
        default=True,
        description="Whether to fetch only current P-number addresses (not historical)"
    )
    
    # Testing configuration
    test_limit: Optional[int] = Field(
        default=None, 
        description="Limit number of CVR numbers to process for testing (None = no limit)"
    )
    
    # Output configuration
    save_raw_responses: bool = Field(
        default=True, 
        description="Whether to save raw API responses for debugging"
    )
    
    # Step execution configuration
    current_step: Optional[CVREnrichmentStep] = Field(
        default=None,
        description="Current pipeline step being executed"
    )
    
    batch_number: Optional[int] = Field(
        default=None,
        description="Current batch number for parallel processing"
    )
    
    total_batches: Optional[int] = Field(
        default=None,
        description="Total number of batches in the current step"
    )
    
    # Independent execution configuration
    enable_independent_execution: bool = Field(
        default=True,
        description="Whether to enable independent step execution by fetching latest files from GCS"
    )
    
    max_days_back_for_inputs: int = Field(
        default=30,
        description="Maximum number of days to look back when fetching latest input files from GCS"
    )
    
    fallback_to_pipeline_dependencies: bool = Field(
        default=True,
        description="Whether to fall back to pipeline dependencies if latest files are not found"
    )
    
    model_config = {"frozen": True}


def get_step_output_path(
    step: CVREnrichmentStep, 
    date_pattern: str, 
    batch_number: Optional[int] = None,
    bucket: str = "landbrugsdata-raw-data"
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
    max_days_back: int = 30
) -> list[str]:
    """
    Get the GCS input paths for a specific pipeline step.
    
    Args:
        step: Pipeline step
        date_pattern: Date pattern for the pipeline run
        total_batches: Total number of batches (for batch-dependent steps)
        bucket: GCS bucket name
        enable_independent_execution: Whether to fetch latest files independently
        max_days_back: Maximum days to look back for latest files
        
    Returns:
        List of GCS paths for the step inputs
    """
    # If independent execution is disabled, use traditional pipeline dependencies
    if not enable_independent_execution:
        return _get_traditional_input_paths(step, date_pattern, bucket)
    
    # Independent execution: fetch latest available files from GCS
    from unified_pipeline.util.gcs_latest_fetcher import create_gcs_fetcher
    
    fetcher = create_gcs_fetcher(bucket)
    
    # Step-specific input logic with latest file fetching
    if step == CVREnrichmentStep.COLLECTION:
        # Collection step has no inputs (reads from all pipeline CVR collections)
        return []
    
    elif step == CVREnrichmentStep.COMPANY_FETCHING:
        # Company fetching depends on collection step
        latest_collection = fetcher.find_latest_cvr_collection_data(max_days_back)
        return [latest_collection] if latest_collection else []
    
    elif step == CVREnrichmentStep.PNUMBER_FETCHING:
        # P-number fetching depends on company fetching
        latest_company = fetcher.find_latest_company_data(max_days_back)
        return [latest_company] if latest_company else []
    
    elif step == CVREnrichmentStep.FINANCIAL_DOCUMENTS:
        # Financial documents depend on company fetching
        latest_company = fetcher.find_latest_company_data(max_days_back)
        return [latest_company] if latest_company else []
    
    elif step == CVREnrichmentStep.ADDRESS_GEOCODING:
        # Address geocoding depends on both company and P-number data
        latest_company = fetcher.find_latest_company_data(max_days_back)
        latest_pnumber = fetcher.find_latest_pnumber_data(max_days_back)
        
        inputs = []
        if latest_company:
            inputs.append(latest_company)
        if latest_pnumber:
            inputs.append(latest_pnumber)
        return inputs
    
    elif step == CVREnrichmentStep.DATA_CONSOLIDATION:
        # Data consolidation depends on all previous steps
        company_data, pnumber_data, financial_data, address_data = fetcher.find_latest_consolidation_inputs(max_days_back)
        
        inputs = []
        if company_data:
            inputs.append(company_data)
        if pnumber_data:
            inputs.append(pnumber_data)
        if financial_data:
            inputs.append(financial_data)
        if address_data:
            inputs.append(address_data)
        return inputs
    
    else:
        return []


def _get_traditional_input_paths(
    step: CVREnrichmentStep, 
    date_pattern: str, 
    bucket: str = "landbrugsdata-raw-data"
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
        return [
            f"{base_path}/company_fetching.parquet",
            f"{base_path}/pnumber_fetching.parquet"
        ]
    
    elif step == CVREnrichmentStep.DATA_CONSOLIDATION:
        # Data consolidation depends on all previous steps (no batching)
        return [
            f"{base_path}/company_fetching.parquet",
            f"{base_path}/pnumber_fetching.parquet", 
            f"{base_path}/financial_documents.parquet",
            f"{base_path}/address_geocoding.parquet"
        ]
    
    else:
        return []
