#!/usr/bin/env python3
"""
Simple test to verify the get_step_input_paths fix works correctly.
"""

import logging
import sys
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from unified_pipeline.gold.cvr_enrichment.shared.config import (
    CVREnrichmentStep,
    get_step_input_paths,
)


def main():
    """Test the path resolution fix."""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
    log = logging.getLogger(__name__)
    
    log.info("🧪 Testing get_step_input_paths fix...")
    
    # Test with the pipeline timestamp from the logs
    date_pattern = "20250815_122017"
    
    try:
        # This should now work with the fix
        input_paths = get_step_input_paths(
            CVREnrichmentStep.ADDRESS_GEOCODING,
            date_pattern,
            bucket="landbrugsdata-raw-data",
            max_days_back=30
        )
        
        log.info(f"✅ Found {len(input_paths)} input paths:")
        for path in input_paths:
            log.info(f"   📁 {path}")
        
        # Test the filtering logic that was failing
        company_input_path = None
        pnumber_input_path = None
        
        for path in input_paths:
            if "cvr_companies" in path.lower():
                company_input_path = path
            elif "cvr_pnumbers" in path.lower():
                pnumber_input_path = path
        
        if company_input_path:
            log.info(f"✅ Company data path found: {company_input_path}")
        else:
            log.warning("❌ No company data path found")
        
        if pnumber_input_path:
            log.info(f"✅ P-number data path found: {pnumber_input_path}")
        else:
            log.warning("❌ No pnumber data path found")
        
        if company_input_path and pnumber_input_path:
            log.info("🎉 Fix verified! Address geocoding should now work correctly.")
        else:
            log.error("❌ Fix verification failed!")
            
    except Exception as e:
        log.error(f"❌ Test failed: {e}")
        raise

if __name__ == "__main__":
    main()
