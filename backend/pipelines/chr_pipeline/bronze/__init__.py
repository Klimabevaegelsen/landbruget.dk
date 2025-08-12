"""CHR Pipeline Bronze Layer - Animal Movement Data Loading.

This package contains the bronze layer functionality for loading animal movement data
from the CHR_dyr system. The functionality has been separated into focused modules:

- auth.py: SOAP client authentication and creation
- persistence.py: Problematic herds tracking and persistence
- volume_management.py: High-volume herd detection and chunking
- utils.py: Shared utilities and helper functions
- data_processing.py: Data aggregation and processing
- animal_movements.py: Main movement loading logic
- load_chr_dyr.py: Main interface and consolidated processing

Main entry points:
- load_animal_movements: Load movements for a single herd
- load_cattle_movement_summaries: Load movements with chunking for high-volume herds
- create_chr_dyr_client: Create authenticated SOAP client
"""

# Main interfaces
from .animal_movements import load_animal_movements, load_cattle_movement_summaries
from .auth import (
    create_besaetning_client,
    create_chr_dyr_client,
    create_diko_client,
    create_ejendom_client,
    create_robust_soap_client,
    create_stamdata_client,
    get_fvm_credentials,
    get_legacy_fvm_credentials,
)
from .data_processing import aggregate_cattle_movements
from .load_chr_dyr import (
    finalize_consolidated_processing,
    initialize_consolidated_processing,
    load_animal_movements_task,
)
from .persistence import add_problematic_herd, get_problematic_herds, is_problematic_herd
from .utils import create_base_request, parse_date
from .volume_management import (
    add_high_volume_herd,
    detect_herd_volume,
    get_optimal_date_range,
    is_high_volume_herd,
)
from .herd_discovery import (
    discover_herd_volumes_for_year,
    load_previous_discovery_results,
    classify_herd_volume,
)

__all__ = [
    # Main loading functions
    "load_animal_movements",
    "load_cattle_movement_summaries",
    "load_animal_movements_task",
    # Authentication
    "create_chr_dyr_client",
    "create_stamdata_client",
    "create_diko_client", 
    "create_ejendom_client",
    "create_besaetning_client",
    "create_robust_soap_client",
    "get_fvm_credentials",
    "get_legacy_fvm_credentials",
    # Data processing
    "aggregate_cattle_movements",
    # Persistence
    "add_problematic_herd",
    "is_problematic_herd",
    "get_problematic_herds",
    # Volume management
    "add_high_volume_herd",
    "detect_herd_volume",
    "get_optimal_date_range",
    "is_high_volume_herd",
    # Herd discovery
    "discover_herd_volumes_for_year",
    "load_previous_discovery_results", 
    "classify_herd_volume",
    # Utilities
    "create_base_request",
    "parse_date",
    # Consolidated processing
    "initialize_consolidated_processing",
    "finalize_consolidated_processing",
]
