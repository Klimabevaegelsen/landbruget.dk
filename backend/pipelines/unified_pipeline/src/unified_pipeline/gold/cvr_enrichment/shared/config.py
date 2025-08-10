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
    bucket: str = "landbrugsdata-raw-data"
) -> list[str]:
    """
    Get the GCS input paths for a specific pipeline step.
    
    Args:
        step: Pipeline step
        date_pattern: Date pattern for the pipeline run
        total_batches: Total number of batches (for batch-dependent steps)
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
        # P-number fetching depends on company fetching batches
        if total_batches:
            return [
                f"{base_path}/company_fetching_batch_{i:03d}.parquet" 
                for i in range(1, total_batches + 1)
            ]
        else:
            return [f"{base_path}/company_fetching.parquet"]
    
    elif step == CVREnrichmentStep.FINANCIAL_DOCUMENTS:
        # Financial documents depend on company fetching batches
        if total_batches:
            return [
                f"{base_path}/company_fetching_batch_{i:03d}.parquet" 
                for i in range(1, total_batches + 1)
            ]
        else:
            return [f"{base_path}/company_fetching.parquet"]
    
    elif step == CVREnrichmentStep.ADDRESS_GEOCODING:
        # Address geocoding depends on both company and P-number data
        inputs = []
        if total_batches:
            inputs.extend([
                f"{base_path}/company_fetching_batch_{i:03d}.parquet"
                for i in range(1, total_batches + 1)
            ])
            inputs.extend([
                f"{base_path}/pnumber_fetching_batch_{i:03d}.parquet"
                for i in range(1, total_batches + 1)
            ])
        else:
            inputs = [
                f"{base_path}/company_fetching.parquet",
                f"{base_path}/pnumber_fetching.parquet"
            ]
        return inputs
    
    elif step == CVREnrichmentStep.DATA_CONSOLIDATION:
        # Data consolidation depends on all previous steps
        inputs = []
        if total_batches:
            for step_name in ["company_fetching", "pnumber_fetching", "financial_documents", "address_geocoding"]:
                inputs.extend([
                    f"{base_path}/{step_name}_batch_{i:03d}.parquet" 
                    for i in range(1, total_batches + 1)
                ])
        else:
            inputs = [
                f"{base_path}/company_fetching.parquet",
                f"{base_path}/pnumber_fetching.parquet", 
                f"{base_path}/financial_documents.parquet",
                f"{base_path}/address_geocoding.parquet"
            ]
        return inputs
    
    else:
        return []
