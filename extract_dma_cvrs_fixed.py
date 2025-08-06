#!/usr/bin/env python3
"""
Fixed script to extract CVR numbers from DMA pipeline output.
Uses the correct field name "cvr" and parallel processing.
"""

import json
import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add the backend to the path for imports
sys.path.append(str(Path(__file__).parent.parent / "backend"))

try:
    from unified_pipeline.util.gcs_access import GCSDataAccess
    from unified_pipeline.util.cvr_collection import save_pipeline_cvr_numbers
    GCS_AVAILABLE = True
    print("✅ GCS utilities available")
except ImportError:
    print("⚠️ GCS utilities not available, will save locally")
    GCS_AVAILABLE = False


def get_company_ids() -> List[str]:
    """Get all company IDs for the DMA pipeline run."""
    timestamp = "20250705_054247"
    base_path = f"gs://landbrugsdata-raw-data/bronze/dma/{timestamp}/{timestamp}/"
    
    try:
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
        
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error listing company directories: {e}")


def extract_cvrs_from_company(company_id: str) -> Set[str]:
    """Extract CVR numbers from a single company using gsutil cat."""
    timestamp = "20250705_054247"
    gcs_path = f"gs://landbrugsdata-raw-data/bronze/dma/{timestamp}/{timestamp}/{company_id}/complete_data.json"
    cvr_numbers = set()
    
    try:
        # Use gsutil cat to read file directly from GCS
        result = subprocess.run(
            ["gsutil", "cat", gcs_path],
            capture_output=True,
            text=True,
            check=True,
            timeout=30
        )
        
        # Parse JSON and extract CVRs
        data = json.loads(result.stdout)
        
        # Extract CVR numbers using the correct field name "cvr"
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


def process_batch_parallel(company_ids: List[str], max_workers: int = 15) -> Set[str]:
    """Process a batch of companies in parallel."""
    all_cvrs = set()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
    
    return all_cvrs


def main():
    """Main execution function."""
    print("🚀 DMA CVR Extraction - Fixed Version")
    print("=" * 50)
    
    try:
        # Get all company IDs
        print("📂 Getting all company IDs...")
        company_ids = get_company_ids()
        print(f"📊 Found {len(company_ids)} companies")
        
        # Process companies
        print("🔄 Processing companies in parallel...")
        all_cvrs = process_batch_parallel(company_ids, max_workers=15)
        
        # Remove duplicates and sort
        unique_cvr_numbers = sorted(list(all_cvrs))
        
        print(f"\n✅ CVR extraction completed!")
        print(f"   📊 Companies processed: {len(company_ids)}")
        print(f"   🔢 Unique CVR numbers found: {len(unique_cvr_numbers)}")
        
        if len(unique_cvr_numbers) == 0:
            print("⚠️ No CVR numbers found")
            return
        
        # Save results
        results = {
            "timestamp": "20250705_054247",
            "companies_processed": len(company_ids),
            "cvr_count": len(unique_cvr_numbers),
            "cvr_numbers": unique_cvr_numbers,
            "generated_at": datetime.now().isoformat()
        }
        
        print(f"💾 Saving {len(unique_cvr_numbers)} CVR numbers...")
        
        if GCS_AVAILABLE:
            try:
                # Use the CVR collection utility
                gcs_path = save_pipeline_cvr_numbers(
                    pipeline_name="dma_scraper",
                    cvr_numbers=unique_cvr_numbers,
                    gcs_access=None,
                    bucket="landbrugsdata-raw-data",
                    timestamp="20250705_054247",
                )
                print(f"✅ Saved to GCS: {gcs_path}")
                
            except Exception as e:
                print(f"❌ GCS save failed: {e}, saving locally...")
                # Fall through to local save
        
        # Save locally as backup or if GCS failed
        if not GCS_AVAILABLE or True:  # Always save locally as backup
            # Save locally
            cvr_data = {
                "pipeline_name": "dma_scraper",
                "timestamp": "20250705_054247",
                "cvr_count": len(unique_cvr_numbers),
                "cvr_numbers": unique_cvr_numbers,
                "generated_at": datetime.now().isoformat()
            }
            
            local_path = "cvr_collections/dma_scraper/20250705_054247"
            os.makedirs(local_path, exist_ok=True)
            
            local_file = f"{local_path}/cvr_numbers.json"
            with open(local_file, 'w', encoding='utf-8') as f:
                json.dump(cvr_data, f, indent=2, ensure_ascii=False)
            
            print(f"💾 Saved locally: {local_file}")
            print(f"🔄 To upload: gsutil cp {local_file} gs://landbrugsdata-raw-data/{local_file}")
        
        print(f"\n🎉 CVR extraction completed successfully!")
        print(f"   📊 Total CVRs: {len(unique_cvr_numbers)}")
        
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()