"""
CVR Enrichment Pipeline - Split into Parallelizable Steps

This package contains the split CVR enrichment pipeline that processes
CVR data in multiple parallelizable steps for improved performance,
maintainability, and debuggability.

Pipeline Steps:
1. CVR Collection - Collect and deduplicate CVR numbers from all pipelines
2. Company Fetching - Fetch comprehensive company data from CVR register
3. P-Number Fetching - Fetch P-number (production unit) data for additional addresses
4. Financial Documents - Fetch and parse financial documents and XML data
5. Address Geocoding - Enrich all addresses with geometry via DAWA API and apply primary address
   selection

Each step can be run independently and supports batch processing for
parallel execution via GitHub Actions job matrices.
"""

# Note: Legacy CVREnrichmentGold classes are imported directly in app.py to avoid circular imports

# New modular CVR enrichment steps
from .address_geocoding import AddressGeocoding, AddressGeocodingConfig
from .company_fetching import CompanyFetching, CompanyFetchingConfig
from .cvr_collection import CVRCollection, CVRCollectionConfig
from .financial_documents import FinancialDocuments, FinancialDocumentsConfig
from .pnumber_fetching import PNumberFetching, PNumberFetchingConfig

__all__ = [
    # New modular steps
    "CVRCollection",
    "CVRCollectionConfig",
    "CompanyFetching",
    "CompanyFetchingConfig",
    "PNumberFetching",
    "PNumberFetchingConfig",
    "FinancialDocuments",
    "FinancialDocumentsConfig",
    "AddressGeocoding",
    "AddressGeocodingConfig",
]
