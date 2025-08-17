"""
Primary Address Selector for CVR Company Table

This module provides intelligent primary address selection for the main company table,
while keeping all addresses in the separate address table.
"""

from enum import Enum
from typing import Any, Dict, List, Optional


class AddressType(Enum):
    """Address type priorities for primary selection."""
    BELIGGENHEDSADRESSE = 1  # Location address (highest priority)
    POSTADRESSE = 2          # Postal address  
    KONTAKTADRESSE = 3       # Contact address


class CoordinateQuality(Enum):
    """Coordinate quality priorities."""
    A = 1  # Highest quality
    B = 2  # Good quality
    C = 3  # Lower quality
    D = 4  # Poor quality


def select_primary_address_for_company_table(
    addresses: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """
    Select the primary address for the main company table.
    
    This function implements intelligent primary address selection that prioritizes:
    1. Current addresses over historical
    2. Successfully geocoded addresses
    3. Business address types (beliggenhedsadresse > postadresse)
    4. Higher coordinate quality (A > B > C > D)
    5. More complete address information
    
    Args:
        addresses: List of address dictionaries from CVR API
        
    Returns:
        The selected primary address dictionary, or None if no suitable address found
    """
    if not addresses:
        return None
    
    # Step 1: Filter to current addresses that are geocoded
    current_geocoded = [
        addr for addr in addresses
        if addr.get('is_current') and addr.get('dawa_enriched')
    ]
    
    # If no current geocoded addresses, fall back to any geocoded addresses
    if not current_geocoded:
        current_geocoded = [
            addr for addr in addresses
            if addr.get('dawa_enriched')
        ]
    
    # If still no geocoded addresses, fall back to current addresses
    if not current_geocoded:
        current_geocoded = [
            addr for addr in addresses
            if addr.get('is_current')
        ]
    
    # If no current addresses, use all addresses
    if not current_geocoded:
        current_geocoded = addresses
    
    if not current_geocoded:
        return None
    
    # Step 2: Score each address and select the best one
    def calculate_address_score(addr: Dict[str, Any]) -> tuple:
        """Calculate score for address prioritization (lower is better)."""
        
        # Address type priority (lower is better)
        addr_type = addr.get('address_type', '').lower()
        if 'beliggenhedsadresse' in addr_type:
            type_priority = AddressType.BELIGGENHEDSADRESSE.value
        elif 'postadresse' in addr_type:
            type_priority = AddressType.POSTADRESSE.value
        elif 'kontakt' in addr_type:
            type_priority = AddressType.KONTAKTADRESSE.value
        else:
            type_priority = 99  # Unknown type gets lowest priority
        
        # Coordinate quality priority (lower is better)
        quality = addr.get('coordinate_quality', 'Z')
        try:
            quality_priority = CoordinateQuality[quality.upper()].value
        except (KeyError, AttributeError):
            quality_priority = 99  # Unknown quality gets lowest priority
        
        # Geocoding status (prefer DAWA over Datavask over none)
        if addr.get('dawa_enriched'):
            geocoding_priority = 1
        elif addr.get('datavask_enriched'):
            geocoding_priority = 2
        else:
            geocoding_priority = 3
        
        # Current status (prefer current)
        current_priority = 1 if addr.get('is_current') else 2
        
        # Completeness score (higher is better, so negate for sorting)
        completeness = 0
        if addr.get('street_name'):
            completeness += 1
        if addr.get('house_number'):
            completeness += 1
        if addr.get('postal_code'):
            completeness += 1
        if addr.get('city'):
            completeness += 1
        if addr.get('municipality_code'):
            completeness += 1
        if addr.get('latitude') and addr.get('longitude'):
            completeness += 2
        
        # Return tuple for sorting (lower values = higher priority)
        return (
            current_priority,      # 1st: Current status
            type_priority,         # 2nd: Address type
            geocoding_priority,    # 3rd: Geocoding quality
            quality_priority,      # 4th: Coordinate quality
            -completeness,         # 5th: Completeness (negated, so higher
                                   #      completeness = lower score)
            addr.get('full_address', '') or ''  # 6th: Alphabetical for consistency
        )
    
    # Sort addresses by score and select the best one
    sorted_addresses = sorted(current_geocoded, key=calculate_address_score)
    return sorted_addresses[0]


def create_primary_address_geometry(primary_address: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create primary address geometry object for company table.
    
    Args:
        primary_address: The selected primary address
        
    Returns:
        Primary address geometry dictionary matching CVR API client format
    """
    if not primary_address:
        return {}
    
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


def enhance_company_data_with_primary_address(company_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enhance company data with intelligently selected primary address.
    
    This function replaces the arbitrary "first address" selection with
    intelligent primary address selection for the company table.
    
    Args:
        company_data: Company data from CVR API
        
    Returns:
        Enhanced company data with improved primary_address_geometry
    """
    addresses = company_data.get('addresses', [])
    if not addresses:
        return company_data
    
    # Select primary address using intelligent logic
    primary_address = select_primary_address_for_company_table(addresses)
    
    if primary_address:
        # Create enhanced company data
        enhanced_data = company_data.copy()
        enhanced_data['primary_address_geometry'] = create_primary_address_geometry(primary_address)
        
        # Add metadata about the selection
        enhanced_data['primary_address_selection'] = {
            'selected_address': primary_address.get('full_address'),
            'address_type': primary_address.get('address_type'),
            'coordinate_quality': primary_address.get('coordinate_quality'),
            'is_current': primary_address.get('is_current'),
            'dawa_enriched': primary_address.get('dawa_enriched'),
            'total_addresses': len(addresses),
            'selection_method': 'intelligent_priority'
        }
        
        return enhanced_data
    
    return company_data


def analyze_address_selection_impact(addresses: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze the impact of different address selection methods.
    
    Args:
        addresses: List of addresses for a company
        
    Returns:
        Analysis showing current vs enhanced selection
    """
    if not addresses:
        return {"error": "No addresses provided"}
    
    # Current method (arbitrary first)
    current_geocoded = [
        addr for addr in addresses
        if addr.get('is_current') and addr.get('dawa_enriched')
    ]
    
    current_selection = current_geocoded[0] if current_geocoded else None
    
    # Enhanced method
    enhanced_selection = select_primary_address_for_company_table(addresses)
    
    # Check if selections differ
    is_different = False
    if current_selection and enhanced_selection:
        is_different = (
            current_selection.get('full_address') != enhanced_selection.get('full_address')
        )
    elif bool(current_selection) != bool(enhanced_selection):
        is_different = True
    
    return {
        "total_addresses": len(addresses),
        "current_and_geocoded": len(current_geocoded),
        "current_selection": {
            "address": current_selection.get('full_address') if current_selection else None,
            "type": current_selection.get('address_type') if current_selection else None,
            "quality": current_selection.get('coordinate_quality') if current_selection else None,
            "method": "arbitrary_first"
        },
        "enhanced_selection": {
            "address": enhanced_selection.get('full_address') if enhanced_selection else None,
            "type": enhanced_selection.get('address_type') if enhanced_selection else None,
            "quality": enhanced_selection.get('coordinate_quality') if enhanced_selection else None,
            "method": "intelligent_priority"
        },
        "selection_differs": is_different,
        "improvement_potential": is_different and len(addresses) > 1
    }
