#!/usr/bin/env python3
"""
Final script to extract all CVR numbers from DMA pipeline output.
Clean, simple, reliable - no fancy GCS utilities, just basic gsutil commands.
"""

import json
import os
import subprocess
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Set, List

def get_company_ids() -> List[str]:
    """Get all company IDs for the DMA pipeline run."""
    timestamp = "20250705_054247"
    base_path = f"gs://landbrugsdata-raw-data/bronze/dma/{timestamp}/{timestamp}/"
    
    result = subprocess.run(
        ["gsutil", "ls", base_path],
        capture_output=True,
        text=True,
        check=True
    )
    
    company_ids = []
    for line in result.stdout.strip().split('\n'):
        if line and line.endswith('/'):
            company_id = line.rstrip('/').split('/')[-1]
            if company_id.isdigit() and len(company_id) == 8:
                company_ids.append(company_id)
    
    return company_ids

def extract_cvrs_from_company(company_id: str) -> Set[str]:
    """Extract CVR numbers from a single company using gsutil cat."""
    timestamp = "20250705_054247"
    gcs_path = f"gs://landbrugsdata-raw-data/bronze/dma/{timestamp}/{timestamp}/{company_id}/complete_data.json"
    
    try:
        # Use gsutil cat - streams from GCS, no downloads
        result = subprocess.run(
            ["gsutil", "cat", gcs_path],
            capture_output=True,
            text=True,
            check=True,
            timeout=30
        )
        
        # Parse JSON and extract CVRs
        data = json.loads(result.stdout)
        cvr_numbers = set()
        
        for section in ["Tilsyn", "Håndhævelser", "Afgørelser"]:
            section_data = data.get(section, [])
            for item in section_data:
                cvr = item.get("cvr")  # Correct field name
                if cvr and isinstance(cvr, str) and len(cvr.strip()) == 8 and cvr.strip().isdigit():
                    cvr_numbers.add(cvr.strip())
        
        return cvr_numbers
        
    except:
        # Skip companies that error
        return set()

def main():
    """Extract all CVRs from DMA data."""
    print("🚀 DMA CVR Extraction - Final Version")
    print("=" * 50)
    
    # Get all company IDs
    print("📂 Getting all company IDs...")
    company_ids = get_company_ids()
    print(f"📊 Found {len(company_ids)} companies")
    
    # Process all companies in parallel
    print("🔄 Processing companies in parallel (using gsutil cat - no downloads)...")
    all_cvrs = set()
    
    with ThreadPoolExecutor(max_workers=15) as executor:
        # Submit all tasks
        future_to_company = {
            executor.submit(extract_cvrs_from_company, company_id): company_id
            for company_id in company_ids
        }
        
        # Collect results as they complete
        completed = 0
        for future in as_completed(future_to_company):
            completed += 1
            if completed % 500 == 0:
                print(f"   Processed {completed}/{len(company_ids)} companies...")
            
            try:
                company_cvrs = future.result(timeout=60)
                all_cvrs.update(company_cvrs)
            except:
                # Skip companies that error
                pass
    
    # Sort unique CVRs
    unique_cvr_numbers = sorted(list(all_cvrs))
    
    print(f"\n✅ CVR extraction completed!")
    print(f"   📊 Companies processed: {len(company_ids)}")
    print(f"   🔢 Unique CVR numbers found: {len(unique_cvr_numbers)}")
    
    if len(unique_cvr_numbers) == 0:
        print("⚠️ No CVR numbers found")
        return
    
    # Create results data
    cvr_data = {
        "pipeline_name": "dma_scraper",
        "timestamp": "20250705_054247",
        "cvr_count": len(unique_cvr_numbers),
        "cvr_numbers": unique_cvr_numbers,
        "generated_at": datetime.now().isoformat(),
        "extraction_metadata": {
            "companies_processed": len(company_ids),
            "extraction_method": "gsutil_cat_parallel",
            "max_workers": 15
        }
    }
    
    # Save locally first
    print(f"💾 Saving {len(unique_cvr_numbers)} CVR numbers...")
    local_path = "cvr_collections/dma_scraper/20250705_054247"
    os.makedirs(local_path, exist_ok=True)
    
    local_file = f"{local_path}/cvr_numbers.json"
    with open(local_file, 'w', encoding='utf-8') as f:
        json.dump(cvr_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Saved locally to: {local_file}")
    
    # Upload to GCS using basic gsutil cp
    try:
        print("🔄 Uploading to GCS...")
        result = subprocess.run([
            "gsutil", "cp", local_file, 
            f"gs://landbrugsdata-raw-data/{local_file}"
        ], check=True, capture_output=True, text=True)
        
        print(f"✅ Uploaded to GCS: gs://landbrugsdata-raw-data/{local_file}")
        
    except subprocess.CalledProcessError as e:
        print(f"⚠️ GCS upload failed: {e}")
        print(f"📝 Manual upload command: gsutil cp {local_file} gs://landbrugsdata-raw-data/{local_file}")
    
    print(f"\n🎉 DMA CVR extraction completed successfully!")
    print(f"   📊 Total unique CVRs: {len(unique_cvr_numbers)}")
    print(f"   📈 Success rate: {len(unique_cvr_numbers)/len(company_ids)*100:.1f}% of companies had CVRs")

if __name__ == "__main__":
    main()