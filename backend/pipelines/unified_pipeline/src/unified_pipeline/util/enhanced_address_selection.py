"""
Enhanced primary address selection logic for CVR companies.

This module provides improved algorithms for selecting the most appropriate
primary address when a company has multiple addresses.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class AddressSelectionStrategy(Enum):
    """Available strategies for primary address selection."""
    
    FIRST_GEOCODED = "first_geocoded"  # Current behavior: first current+geocoded
    BUSINESS_PRIORITY = "business_priority"  # Enhanced business logic
    BEST_QUALITY = "best_quality"  # Prioritize coordinate quality
    MOST_COMPLETE = "most_complete"  # Most complete address information
    HYBRID = "hybrid"  # Combination of multiple factors


@dataclass
class AddressSelectionConfig:
    """Configuration for address selection algorithm."""
    
    strategy: AddressSelectionStrategy = AddressSelectionStrategy.HYBRID
    prefer_current_only: bool = True
    require_geocoding: bool = True
    address_type_priority: Dict[str, int] = None
    coordinate_quality_priority: Dict[str, int] = None
    
    def __post_init__(self):
        if self.address_type_priority is None:
            self.address_type_priority = {
                "beliggenhedsadresse": 1,  # Location address (highest priority)
                "postadresse": 2,          # Postal address
                "kontaktadresse": 3,       # Contact address
            }
        
        if self.coordinate_quality_priority is None:
            self.coordinate_quality_priority = {
                "A": 1,  # Highest quality
                "B": 2,  # Good quality
                "C": 3,  # Lower quality
                "D": 4,  # Poor quality
            }


class EnhancedAddressSelector:
    """Enhanced primary address selection with multiple strategies."""
    
    def __init__(self, config: AddressSelectionConfig = None):
        """
        Initialize address selector.
        
        Args:
            config: Configuration for address selection
        """
        self.config = config or AddressSelectionConfig()
    
    def select_primary_address(self, addresses: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Select the primary address from a list of addresses.
        
        Args:
            addresses: List of address dictionaries
            
        Returns:
            The selected primary address or None if no suitable address found
        """
        if not addresses:
            return None
        
        # Apply strategy-specific selection
        if self.config.strategy == AddressSelectionStrategy.FIRST_GEOCODED:
            return self._select_first_geocoded(addresses)
        elif self.config.strategy == AddressSelectionStrategy.BUSINESS_PRIORITY:
            return self._select_by_business_priority(addresses)
        elif self.config.strategy == AddressSelectionStrategy.BEST_QUALITY:
            return self._select_by_quality(addresses)
        elif self.config.strategy == AddressSelectionStrategy.MOST_COMPLETE:
            return self._select_most_complete(addresses)
        elif self.config.strategy == AddressSelectionStrategy.HYBRID:
            return self._select_hybrid(addresses)
        else:
            # Fallback to current behavior
            return self._select_first_geocoded(addresses)
    
    def _filter_candidates(self, addresses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter addresses based on basic requirements.
        
        Args:
            addresses: List of address dictionaries
            
        Returns:
            Filtered list of candidate addresses
        """
        candidates = addresses.copy()
        
        # Filter by current status if required
        if self.config.prefer_current_only:
            current_addresses = [addr for addr in candidates if addr.get('is_current')]
            if current_addresses:
                candidates = current_addresses
        
        # Filter by geocoding status if required
        if self.config.require_geocoding:
            geocoded = [addr for addr in candidates if addr.get('dawa_enriched')]
            if geocoded:
                candidates = geocoded
        
        return candidates
    
    def _select_first_geocoded(self, addresses: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Current behavior: select first current and geocoded address.
        
        Args:
            addresses: List of address dictionaries
            
        Returns:
            First address that is current and geocoded, or None
        """
        candidates = [
            addr for addr in addresses
            if addr.get('is_current') and addr.get('dawa_enriched')
        ]
        return candidates[0] if candidates else None
    
    def _select_by_business_priority(
        self, addresses: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Select address based on business logic priority.
        
        Args:
            addresses: List of address dictionaries
            
        Returns:
            Address with highest business priority
        """
        candidates = self._filter_candidates(addresses)
        if not candidates:
            return None
        
        # Sort by address type priority
        candidates.sort(key=lambda addr: (
            self.config.address_type_priority.get(addr.get('address_type', ''), 99),
            addr.get('full_address', '') or ''
        ))
        
        return candidates[0]
    
    def _select_by_quality(self, addresses: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Select address with best coordinate quality.
        
        Args:
            addresses: List of address dictionaries
            
        Returns:
            Address with best coordinate quality
        """
        candidates = self._filter_candidates(addresses)
        if not candidates:
            return None
        
        # Sort by coordinate quality
        candidates.sort(key=lambda addr: (
            self.config.coordinate_quality_priority.get(addr.get('coordinate_quality', 'Z'), 99),
            addr.get('full_address', '') or ''
        ))
        
        return candidates[0]
    
    def _select_most_complete(self, addresses: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Select address with most complete information.
        
        Args:
            addresses: List of address dictionaries
            
        Returns:
            Address with most complete information
        """
        candidates = self._filter_candidates(addresses)
        if not candidates:
            return None
        
        def completeness_score(addr: Dict[str, Any]) -> int:
            """Calculate completeness score for an address."""
            score = 0
            
            # Basic address components
            if addr.get('street_name'):
                score += 1
            if addr.get('house_number'):
                score += 1
            if addr.get('postal_code'):
                score += 1
            if addr.get('city'):
                score += 1
            if addr.get('municipality_code'):
                score += 1
            
            # Additional details
            if addr.get('floor'):
                score += 1
            if addr.get('door'):
                score += 1
            
            # Geocoding information
            if addr.get('latitude') and addr.get('longitude'):
                score += 2  # Coordinates are valuable
            if addr.get('coordinate_quality'):
                score += 1
            
            return score
        
        # Sort by completeness score (descending)
        candidates.sort(key=lambda addr: (
            -completeness_score(addr),  # Higher score first
            addr.get('full_address', '') or ''
        ))
        
        return candidates[0]
    
    def _select_hybrid(self, addresses: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Select address using hybrid approach combining multiple factors.
        
        Args:
            addresses: List of address dictionaries
            
        Returns:
            Address with best overall score
        """
        candidates = self._filter_candidates(addresses)
        if not candidates:
            return None
        
        def hybrid_score(addr: Dict[str, Any]) -> tuple:
            """Calculate hybrid score for an address."""
            
            # Address type priority (lower is better)
            type_priority = self.config.address_type_priority.get(
                addr.get('address_type', ''), 99
            )
            
            # Coordinate quality priority (lower is better)
            quality_priority = self.config.coordinate_quality_priority.get(
                addr.get('coordinate_quality', 'Z'), 99
            )
            
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
            if addr.get('floor'):
                completeness += 1
            if addr.get('door'):
                completeness += 1
            if addr.get('latitude') and addr.get('longitude'):
                completeness += 2
            
            # Prefer more recent addresses (if period_start is available)
            recency_score = 0
            if addr.get('period_start'):
                try:
                    # Simple heuristic: longer period_start string suggests more recent
                    recency_score = -len(addr['period_start'])
                except Exception:
                    recency_score = 0
            
            return (
                type_priority,      # 1st: Address type (lower is better)
                quality_priority,   # 2nd: Coordinate quality (lower is better)
                -completeness,      # 3rd: Completeness (higher is better)
                recency_score,      # 4th: Recency (more recent is better)
                addr.get('full_address', '') or ''  # 5th: Alphabetical for consistency
            )
        
        # Sort by hybrid score
        candidates.sort(key=hybrid_score)
        
        return candidates[0]
    
    def analyze_selection_options(self, addresses: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze what different selection strategies would choose.
        
        Args:
            addresses: List of address dictionaries
            
        Returns:
            Dictionary with analysis results for each strategy
        """
        analysis = {
            "total_addresses": len(addresses),
            "strategy_results": {}
        }
        
        # Test each strategy
        for strategy in AddressSelectionStrategy:
            config = AddressSelectionConfig(strategy=strategy)
            selector = EnhancedAddressSelector(config)
            selected = selector.select_primary_address(addresses)
            
            analysis["strategy_results"][strategy.value] = {
                "selected_address": selected.get('full_address') if selected else None,
                "address_type": selected.get('address_type') if selected else None,
                "coordinate_quality": selected.get('coordinate_quality') if selected else None,
                "is_current": selected.get('is_current') if selected else None,
                "dawa_enriched": selected.get('dawa_enriched') if selected else None,
            }
        
        return analysis


def get_primary_address_with_strategy(
    addresses: List[Dict[str, Any]], 
    strategy: AddressSelectionStrategy = AddressSelectionStrategy.HYBRID
) -> Optional[Dict[str, Any]]:
    """
    Convenience function to get primary address with specified strategy.
    
    Args:
        addresses: List of address dictionaries
        strategy: Selection strategy to use
        
    Returns:
        Selected primary address or None
    """
    config = AddressSelectionConfig(strategy=strategy)
    selector = EnhancedAddressSelector(config)
    return selector.select_primary_address(addresses)
