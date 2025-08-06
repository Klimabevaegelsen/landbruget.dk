#!/usr/bin/env python3
"""
Debug script to test CVR extraction from one DMA company.
"""

import json
import subprocess

def test_one_company():
    """Test CVR extraction from one company."""
    company_id = "78078154"
    gcs_path = f"gs://landbrugsdata-raw-data/bronze/dma/20250705_054247/20250705_054247/{company_id}/complete_data.json"
    
    print(f"🔍 Testing CVR extraction from company {company_id}")
    print(f"📁 GCS path: {gcs_path}")
    
    try:
        # Read file from GCS
        print("📥 Reading file from GCS...")
        result = subprocess.run(
            ["gsutil", "cat", gcs_path],
            capture_output=True,
            text=True,
            check=True,
            timeout=30
        )
        
        print("✅ File read successfully")
        
        # Parse JSON
        print("🔄 Parsing JSON...")
        data = json.loads(result.stdout)
        print("✅ JSON parsed successfully")
        
        print(f"📋 Data keys: {list(data.keys())}")
        
        # Check sections
        for section in ["Tilsyn", "Håndhævelser", "Afgørelser"]:
            section_data = data.get(section, [])
            print(f"\n📂 Section '{section}': {len(section_data)} items")
            
            if section_data:
                # Show first item structure
                first_item = section_data[0]
                print(f"   🔍 First item keys: {list(first_item.keys())}")
                
                # Look for CVR fields
                cvr_fields = []
                for key in first_item.keys():
                    if 'cvr' in key.lower():
                        cvr_fields.append(key)
                        print(f"   📋 Found CVR field '{key}': {first_item[key]}")
                
                if not cvr_fields:
                    print(f"   ⚠️ No CVR fields found in {section}")
        
        # Extract CVRs using both possible field names
        all_cvrs = set()
        
        for section in ["Tilsyn", "Håndhævelser", "Afgørelser"]:
            section_data = data.get(section, [])
            for item in section_data:
                # Try both 'cvr' and 'cvr_number'
                for field_name in ['cvr', 'cvr_number']:
                    cvr = item.get(field_name)
                    if cvr and isinstance(cvr, str) and len(cvr.strip()) == 8 and cvr.strip().isdigit():
                        all_cvrs.add(cvr.strip())
                        print(f"   ✅ Found CVR in {section}.{field_name}: {cvr.strip()}")
        
        print(f"\n🎯 Total unique CVRs found: {len(all_cvrs)}")
        if all_cvrs:
            print(f"📋 CVRs: {sorted(all_cvrs)}")
        else:
            print("⚠️ No CVRs found")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_one_company()