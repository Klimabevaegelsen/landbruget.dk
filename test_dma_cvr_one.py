#!/usr/bin/env python3
"""
Test CVR extraction with just 1 company to verify the process works.
"""

import json
import os
import subprocess
from datetime import datetime

def test_extract_one_cvr():
    """Test extraction from one company."""
    company_id = "78078154"  # We know this one has a CVR
    timestamp = "20250705_054247"
    gcs_path = f"gs://landbrugsdata-raw-data/bronze/dma/{timestamp}/{timestamp}/{company_id}/complete_data.json"
    
    print(f"🔍 Testing with company {company_id}")
    
    try:
        # Read using gsutil cat
        result = subprocess.run(
            ["gsutil", "cat", gcs_path],
            capture_output=True,
            text=True,
            check=True,
            timeout=30
        )
        
        # Parse JSON
        data = json.loads(result.stdout)
        
        # Extract CVRs
        cvr_numbers = set()
        for section in ["Tilsyn", "Håndhævelser", "Afgørelser"]:
            section_data = data.get(section, [])
            for item in section_data:
                cvr = item.get("cvr")
                if cvr and isinstance(cvr, str) and len(cvr.strip()) == 8 and cvr.strip().isdigit():
                    cvr_numbers.add(cvr.strip())
        
        unique_cvr_numbers = sorted(list(cvr_numbers))
        
        print(f"✅ Found {len(unique_cvr_numbers)} CVRs: {unique_cvr_numbers}")
        
        # Save the result properly
        cvr_data = {
            "pipeline_name": "dma_scraper",
            "timestamp": timestamp,
            "cvr_count": len(unique_cvr_numbers),
            "cvr_numbers": unique_cvr_numbers,
            "generated_at": datetime.now().isoformat(),
            "test_mode": True,
            "companies_processed": 1
        }
        
        # Save locally
        local_path = "cvr_collections/dma_scraper/test"
        os.makedirs(local_path, exist_ok=True)
        
        local_file = f"{local_path}/cvr_numbers_test.json"
        with open(local_file, 'w', encoding='utf-8') as f:
            json.dump(cvr_data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Saved test result to: {local_file}")
        
        # Show the contents
        print(f"📄 File contents:")
        with open(local_file, 'r') as f:
            print(f.read())
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Testing DMA CVR extraction with 1 company")
    print("=" * 50)
    
    success = test_extract_one_cvr()
    
    if success:
        print(f"\n✅ Test successful! The process works correctly.")
        print(f"📝 Ready to run on all 16,151 companies")
    else:
        print(f"\n❌ Test failed! Need to fix issues before full run")