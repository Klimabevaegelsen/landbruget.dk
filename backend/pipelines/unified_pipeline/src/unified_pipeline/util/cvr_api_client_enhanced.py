"""
Enhanced CVR API Client with intelligent address selection.

This module provides a patched version of the CVR API client that replaces
the arbitrary "first address" selection with intelligent address prioritization.
"""

import os
from typing import Any, Dict, List, Optional

from .cvr_api_client import CVRAPIClient as BaseCVRAPIClient
from .address_selection_integration import get_primary_address_geometry_enhanced


class EnhancedCVRAPIClient(BaseCVRAPIClient):
    """
    Enhanced CVR API client with intelligent primary address selection.
    
    This class extends the base CVR API client to replace the arbitrary
    "first current geocoded address" selection with intelligent logic
    that considers address type, coordinate quality, and completeness.
    """
    
    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        enable_geocoding: bool = True,
        geocode_current_only: bool = True,
        address_selection_strategy: str = "hybrid"
    ):
        """
        Initialize enhanced CVR API client.
        
        Args:
            username: CVR API username
            password: CVR API password  
            enable_geocoding: Whether to enable address geocoding
            geocode_current_only: Whether to geocode only current addresses
            address_selection_strategy: Strategy for primary address selection
                ("first_geocoded", "business_priority", "best_quality", "hybrid")
        """
        super().__init__(username, password, enable_geocoding, geocode_current_only)
        
        self.address_selection_strategy = address_selection_strategy
        
        self.log.info(f"Enhanced CVR API client initialized")
        self.log.info(f"Address selection strategy: {address_selection_strategy}")
    
    def enrich_company_with_geometry(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich company data with geometry using enhanced address selection.
        
        This method overrides the base implementation to use intelligent
        address selection instead of arbitrary "first address" selection.
        
        Args:
            company_data: Company data dictionary with addresses
            
        Returns:
            Company data enriched with primary address geometry
        """
        if not self.enable_geocoding:
            return company_data
            
        try:
            addresses = company_data.get("addresses", [])
            if not addresses:
                return company_data
            
            # Geocode addresses using parent method
            enriched_addresses = []
            
            for address in addresses:
                # Skip historical addresses if configured to do so
                if self.geocode_current_only and not address.get("is_current"):
                    enriched_addresses.append(address)
                    continue
                
                enriched_address = address.copy()
                enriched_address["geocoding_attempted"] = True
                
                # Try DAWA geocoding first if address has adresse_id
                if address.get("adresse_id"):
                    try:
                        geocoded = self.dawa_client.geocode_address_by_id(address["adresse_id"])
                        if geocoded:
                            enriched_address.update({
                                "latitude": geocoded["latitude"],
                                "longitude": geocoded["longitude"],
                                "coordinate_system": geocoded.get("coordinate_system", "WGS84"),
                                "srid": geocoded.get("srid", 4326),
                                "geometry_wkt": self.dawa_client.create_geometry_wkt(
                                    geocoded["latitude"], geocoded["longitude"]
                                ),
                                "geometry_geojson": self.dawa_client.create_geometry_geojson(
                                    geocoded["latitude"], geocoded["longitude"]
                                ),
                                "coordinate_quality": geocoded.get("coordinate_quality"),
                                "coordinate_source": geocoded.get("coordinate_source"),
                                "dawa_enriched": True,
                                "dawa_fetch_timestamp": geocoded.get("dawa_fetch_timestamp"),
                            })
                    except Exception as e:
                        self.log.debug(f"DAWA geocoding failed for {address.get('full_address')}: {e}")
                
                # Fallback to Datavask API if DAWA failed
                if not enriched_address.get("dawa_enriched"):
                    try:
                        full_address = address.get("full_address", "")
                        if address.get("postal_code") and address.get("city"):
                            full_address = f"{full_address}, {address['postal_code']} {address['city']}"
                        
                        if full_address.strip():
                            geocoded = self.datavask_client.geocode_address(full_address)
                            if geocoded:
                                enriched_address.update({
                                    "latitude": geocoded["latitude"],
                                    "longitude": geocoded["longitude"],
                                    "coordinate_system": "WGS84",
                                    "srid": 4326,
                                    "geometry_wkt": self.dawa_client.create_geometry_wkt(
                                        geocoded["latitude"], geocoded["longitude"]
                                    ),
                                    "geometry_geojson": self.dawa_client.create_geometry_geojson(
                                        geocoded["latitude"], geocoded["longitude"]
                                    ),
                                    "coordinate_quality": geocoded.get("coordinate_quality", "C"),
                                    "coordinate_source": "datavask",
                                    "datavask_enriched": True,
                                    "dawa_enriched": False,
                                })
                    except Exception as e:
                        self.log.debug(f"Datavask geocoding failed for {full_address}: {e}")
                
                # Set enrichment status
                if not enriched_address.get("dawa_enriched") and not enriched_address.get("datavask_enriched"):
                    enriched_address["dawa_enriched"] = False
                    enriched_address["datavask_enriched"] = False
                
                enriched_addresses.append(enriched_address)
            
            # Update company data with enriched addresses
            company_data = company_data.copy()
            company_data["addresses"] = enriched_addresses
            
            # ✅ ENHANCED PRIMARY ADDRESS SELECTION
            # Replace the arbitrary "first address" selection with intelligent logic
            primary_geometry = get_primary_address_geometry_enhanced(
                enriched_addresses,
                strategy=self.address_selection_strategy
            )
            
            if primary_geometry:
                company_data["primary_address_geometry"] = primary_geometry
                
                # Log the selection for debugging
                if self.log.level <= 10:  # DEBUG level
                    selected_addr = None
                    for addr in enriched_addresses:
                        if (addr.get("latitude") == primary_geometry.get("latitude") and
                            addr.get("longitude") == primary_geometry.get("longitude")):
                            selected_addr = addr
                            break
                    
                    if selected_addr:
                        self.log.debug(
                            f"Enhanced selection chose: {selected_addr.get('full_address')} "
                            f"(type: {selected_addr.get('address_type')}, "
                            f"quality: {selected_addr.get('coordinate_quality')})"
                        )
            
            return company_data
            
        except Exception as e:
            self.log.error(f"Error enriching company data with enhanced geometry: {e}")
            return company_data
    
    def enrich_pnumber_with_geometry(self, pnumber_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich P-number data with geometry using enhanced address selection.
        
        Args:
            pnumber_data: P-number data dictionary with addresses
            
        Returns:
            P-number data enriched with primary address geometry
        """
        if not self.enable_geocoding:
            return pnumber_data
            
        try:
            addresses = pnumber_data.get("addresses", [])
            if not addresses:
                return pnumber_data
            
            # Use parent method to geocode addresses
            enriched_pnumber = super().enrich_pnumber_with_geometry(pnumber_data)
            enriched_addresses = enriched_pnumber.get("addresses", [])
            
            # ✅ ENHANCED PRIMARY ADDRESS SELECTION for P-numbers too
            primary_geometry = get_primary_address_geometry_enhanced(
                enriched_addresses,
                strategy=self.address_selection_strategy
            )
            
            if primary_geometry:
                enriched_pnumber["primary_address_geometry"] = primary_geometry
            
            return enriched_pnumber
            
        except Exception as e:
            self.log.error(f"Error enriching P-number data with enhanced geometry: {e}")
            return pnumber_data


def create_enhanced_cvr_client(
    address_selection_strategy: str = "hybrid",
    **kwargs
) -> EnhancedCVRAPIClient:
    """
    Factory function to create enhanced CVR API client.
    
    Args:
        address_selection_strategy: Strategy for primary address selection
        **kwargs: Additional arguments passed to CVR API client
        
    Returns:
        Enhanced CVR API client instance
    """
    return EnhancedCVRAPIClient(
        address_selection_strategy=address_selection_strategy,
        **kwargs
    )


# Configuration for different deployment scenarios
ENHANCED_CLIENT_CONFIGS = {
    "development": {
        "address_selection_strategy": "hybrid",
        "description": "Balanced approach for development"
    },
    "production_conservative": {
        "address_selection_strategy": "first_geocoded",
        "description": "Minimal change from current behavior"
    },
    "production_enhanced": {
        "address_selection_strategy": "hybrid",
        "description": "Full enhanced logic for production"
    },
    "quality_focused": {
        "address_selection_strategy": "best_quality",
        "description": "Prioritize coordinate quality"
    },
    "business_focused": {
        "address_selection_strategy": "business_priority",
        "description": "Prioritize business address types"
    }
}
