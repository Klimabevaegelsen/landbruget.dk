"""
Integration module for enhanced address selection in CVR API client.

This module provides a drop-in replacement for the current primary address
selection logic in the CVR API client.
"""

from typing import List, Dict, Any, Optional
from .enhanced_address_selection import (
    EnhancedAddressSelector,
    AddressSelectionStrategy,
    AddressSelectionConfig
)


def select_primary_address_enhanced(
    addresses: List[Dict[str, Any]], 
    strategy: str = "hybrid",
    prefer_current_only: bool = True,
    require_geocoding: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Enhanced primary address selection for CVR companies.
    
    This function can replace the current primary address selection logic
    in the CVR API client.
    
    Args:
        addresses: List of address dictionaries with CVR address structure
        strategy: Selection strategy ("first_geocoded", "business_priority", 
                 "best_quality", "most_complete", "hybrid")
        prefer_current_only: Whether to prefer current addresses
        require_geocoding: Whether to require DAWA geocoding
        
    Returns:
        Selected primary address dictionary or None if no suitable address found
    """
    if not addresses:
        return None
    
    try:
        # Map string strategy to enum
        strategy_enum = AddressSelectionStrategy(strategy)
    except ValueError:
        # Fallback to hybrid if invalid strategy provided
        strategy_enum = AddressSelectionStrategy.HYBRID
    
    # Create configuration
    config = AddressSelectionConfig(
        strategy=strategy_enum,
        prefer_current_only=prefer_current_only,
        require_geocoding=require_geocoding
    )
    
    # Select primary address
    selector = EnhancedAddressSelector(config)
    return selector.select_primary_address(addresses)


def get_primary_address_geometry_enhanced(
    addresses: List[Dict[str, Any]], 
    strategy: str = "hybrid"
) -> Optional[Dict[str, Any]]:
    """
    Get primary address geometry using enhanced selection logic.
    
    This function mimics the structure of the current primary_address_geometry
    creation in the CVR API client.
    
    Args:
        addresses: List of address dictionaries
        strategy: Selection strategy to use
        
    Returns:
        Primary address geometry dictionary or None
    """
    primary_address = select_primary_address_enhanced(addresses, strategy)
    
    if not primary_address:
        return None
    
    # Create geometry structure matching current CVR API client format
    return {
        "latitude": primary_address.get("latitude"),
        "longitude": primary_address.get("longitude"), 
        "coordinate_system": primary_address.get("coordinate_system", "WGS84"),
        "srid": primary_address.get("srid", 4326),
        "geometry_wkt": primary_address.get("geometry_wkt"),
        "geometry_geojson": primary_address.get("geometry_geojson"),
        "coordinate_quality": primary_address.get("coordinate_quality"),
        "coordinate_source": primary_address.get("coordinate_source"),
    }


def analyze_address_selection_for_company(
    company_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Analyze address selection options for a company.
    
    Args:
        company_data: Company data dictionary with addresses
        
    Returns:
        Analysis results showing what different strategies would select
    """
    addresses = company_data.get("addresses", [])
    
    if not addresses:
        return {
            "company_name": company_data.get("company_name"),
            "cvr_number": company_data.get("cvr_number"),
            "total_addresses": 0,
            "analysis": "No addresses found"
        }
    
    selector = EnhancedAddressSelector()
    analysis = selector.analyze_selection_options(addresses)
    
    # Add company context
    analysis["company_name"] = company_data.get("company_name")
    analysis["cvr_number"] = company_data.get("cvr_number")
    analysis["current_primary"] = company_data.get("primary_address_geometry")
    
    return analysis


# Configuration for different use cases
SELECTION_CONFIGS = {
    "conservative": {
        "strategy": "first_geocoded",
        "description": "Current behavior - minimal change risk"
    },
    "business_focused": {
        "strategy": "business_priority", 
        "description": "Prioritize business-relevant address types"
    },
    "quality_focused": {
        "strategy": "best_quality",
        "description": "Prioritize coordinate quality"
    },
    "balanced": {
        "strategy": "hybrid",
        "description": "Balance all factors for best overall selection"
    }
}


def get_recommended_strategy_for_company(addresses: List[Dict[str, Any]]) -> str:
    """
    Recommend the best selection strategy based on company's address profile.
    
    Args:
        addresses: List of company addresses
        
    Returns:
        Recommended strategy name
    """
    if not addresses:
        return "first_geocoded"
    
    current_addresses = [addr for addr in addresses if addr.get('is_current')]
    geocoded_addresses = [addr for addr in addresses if addr.get('dawa_enriched')]
    
    # If only one current+geocoded address, any strategy will work
    current_and_geocoded = [
        addr for addr in addresses 
        if addr.get('is_current') and addr.get('dawa_enriched')
    ]
    
    if len(current_and_geocoded) <= 1:
        return "first_geocoded"  # No benefit from enhanced logic
    
    # If many addresses with mixed types and qualities, use hybrid
    if len(addresses) > 10 and len(current_addresses) > 2:
        return "hybrid"
    
    # If addresses have varying quality, prioritize quality
    qualities = set(addr.get('coordinate_quality') for addr in geocoded_addresses)
    if len(qualities) > 1 and 'A' in qualities:
        return "best_quality"
    
    # If mixed address types, use business priority
    address_types = set(addr.get('address_type') for addr in addresses)
    if len(address_types) > 1:
        return "business_priority"
    
    # Default to balanced approach
    return "hybrid"


# Example integration patch for CVR API client
def create_primary_address_geometry_patch():
    """
    Create a patch that can be applied to the CVR API client to use enhanced selection.
    
    Returns:
        Dictionary with before/after code snippets for the patch
    """
    return {
        "file": "unified_pipeline/util/cvr_api_client.py",
        "function": "enrich_company_with_geometry / enrich_pnumber_with_geometry",
        "current_code": """
# Add primary address geometry to top level for easy access
current_geocoded = [
    addr
    for addr in enriched_addresses
    if addr.get("is_current") and addr.get("dawa_enriched")
]

if current_geocoded:
    primary_address = current_geocoded[0]  # Use first current geocoded address
    company_data["primary_address_geometry"] = {
        "latitude": primary_address.get("latitude"),
        "longitude": primary_address.get("longitude"),
        # ... rest of geometry structure
    }
        """,
        "enhanced_code": """
# Import enhanced address selection
from .address_selection_integration import get_primary_address_geometry_enhanced

# Add primary address geometry using enhanced selection
primary_geometry = get_primary_address_geometry_enhanced(
    enriched_addresses, 
    strategy="hybrid"  # or make this configurable
)

if primary_geometry:
    company_data["primary_address_geometry"] = primary_geometry
        """,
        "benefits": [
            "Intelligent address selection for companies with many addresses",
            "Configurable selection strategies",
            "Better business logic for address prioritization",
            "Maintains backward compatibility",
            "Improved coordinate quality selection"
        ]
    }
    
    
def create_configuration_examples():
    """
    Create configuration examples for different deployment scenarios.
    
    Returns:
        Dictionary with configuration examples
    """
    return {
        "development": {
            "strategy": "hybrid",
            "prefer_current_only": True,
            "require_geocoding": True,
            "description": "Balanced approach for development and testing"
        },
        "production_conservative": {
            "strategy": "first_geocoded", 
            "prefer_current_only": True,
            "require_geocoding": True,
            "description": "Minimal change from current behavior"
        },
        "production_enhanced": {
            "strategy": "hybrid",
            "prefer_current_only": True, 
            "require_geocoding": True,
            "description": "Full enhanced logic for production"
        },
        "quality_focused": {
            "strategy": "best_quality",
            "prefer_current_only": True,
            "require_geocoding": True, 
            "description": "Prioritize coordinate quality"
        }
    }
