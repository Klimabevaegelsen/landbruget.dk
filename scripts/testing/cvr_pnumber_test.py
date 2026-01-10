"""
Test script for CVR P-number address discovery and split pipeline.

This script tests the new P-number functionality and pipeline steps locally
with a small sample of CVR numbers to validate the implementation.
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Add the unified pipeline to path
sys.path.append(str(Path(__file__).parent.parent.parent / "backend" / "pipelines" / "unified_pipeline" / "src"))

from unified_pipeline.util.cvr_api_client import CVRAPIClient
from unified_pipeline.gold.cvr_enrichment.shared.config import CVREnrichmentSharedConfig
from unified_pipeline.gold.cvr_enrichment.shared.batch_manager import CVRBatchManager


async def test_pnumber_discovery():
    """Test P-number address discovery functionality."""
    print("🧪 Testing P-number address discovery...")
    
    # Test CVR numbers from the documentation
    test_cvrs = [
        "10103940",  # SIMON PEDERSEN (has P-number 1000000175)
        "25052943",  # FMP Sofiendal ApS (has P-number 1021354151)
        "33065056",  # Another company for testing
    ]
    
    # Initialize CVR API client
    cvr_client = CVRAPIClient(
        enable_geocoding=False,  # Skip geocoding for faster testing
        geocode_current_only=True
    )
    
    print(f"Testing with {len(test_cvrs)} CVR numbers...")
    
    # Test 1: Fetch company data and extract P-numbers
    print("\n1️⃣ Fetching company data and extracting P-numbers...")
    
    company_results = cvr_client.fetch_multiple_companies(
        cvr_numbers=test_cvrs,
        fetch_all_fields=True,
        enrich_with_geometry=False
    )
    
    print(f"Company fetch results: {company_results['summary']}")
    
    # Extract P-numbers from companies
    all_pnumbers = []
    pnumber_to_cvr = {}
    
    for cvr, company_data in company_results["results"].items():
        if company_data:
            pnumbers = cvr_client.get_company_pnumbers(company_data)
            print(f"CVR {cvr} ({company_data.get('company_name', 'Unknown')}): {len(pnumbers)} P-numbers")
            
            for pnumber in pnumbers:
                all_pnumbers.append(pnumber)
                pnumber_to_cvr[pnumber] = cvr
                print(f"  └─ P-number: {pnumber}")
    
    if not all_pnumbers:
        print("⚠️ No P-numbers found in test companies. Trying direct P-number test...")
        # Test with known P-numbers from documentation
        all_pnumbers = ["1000000175", "1021354151"]
        pnumber_to_cvr = {"1000000175": "10103940", "1021354151": "25052943"}
    
    # Test 2: Fetch P-number data
    if all_pnumbers:
        print(f"\n2️⃣ Fetching P-number data for {len(all_pnumbers)} P-numbers...")
        
        pnumber_results = cvr_client.fetch_multiple_pnumbers(
            pnumbers=all_pnumbers,
            fetch_all_fields=True,
            enrich_with_geometry=False
        )
        
        print(f"P-number fetch results: {pnumber_results['summary']}")
        
        # Analyze P-number addresses
        total_addresses = 0
        for pnumber, pnumber_data in pnumber_results["results"].items():
            if pnumber_data:
                addresses = pnumber_data.get("addresses", [])
                parent_cvr = pnumber_to_cvr.get(pnumber, "Unknown")
                
                print(f"\nP-number {pnumber} (parent CVR: {parent_cvr}):")
                print(f"  Unit name: {pnumber_data.get('unit_name', 'N/A')}")
                print(f"  Addresses: {len(addresses)}")
                
                total_addresses += len(addresses)
                
                for i, addr in enumerate(addresses[:3]):  # Show first 3 addresses
                    addr_type = addr.get("address_type", "unknown")
                    full_addr = addr.get("full_address", "N/A")
                    is_current = "✓" if addr.get("is_current") else "○"
                    print(f"    {i+1}. [{addr_type}] {is_current} {full_addr}")
                
                if len(addresses) > 3:
                    print(f"    ... and {len(addresses) - 3} more addresses")
        
        print(f"\n📊 P-number Discovery Summary:")
        print(f"   • Companies tested: {len(test_cvrs)}")
        print(f"   • P-numbers found: {len(all_pnumbers)}")
        print(f"   • Total addresses discovered: {total_addresses}")
        print(f"   • Average addresses per P-number: {total_addresses / len(all_pnumbers) if all_pnumbers else 0:.1f}")
    
    return {
        "companies_tested": len(test_cvrs),
        "companies_successful": company_results["summary"]["successful"],
        "pnumbers_found": len(all_pnumbers),
        "pnumbers_successful": pnumber_results["summary"]["successful"] if all_pnumbers else 0,
        "total_addresses": total_addresses if all_pnumbers else 0,
    }


def test_batch_manager():
    """Test the batch manager functionality."""
    print("\n🧪 Testing batch manager...")
    
    # Create test CVR set
    test_cvrs = {f"{10000000 + i}" for i in range(123)}  # 123 test CVRs
    
    batch_manager = CVRBatchManager(batch_size=25)
    
    # Test batch creation
    batches = batch_manager.create_cvr_batches(test_cvrs)
    
    print(f"Created {len(batches)} batches from {len(test_cvrs)} CVRs")
    
    # Test batch validation
    validation = batch_manager.validate_batch_coverage(test_cvrs, batches)
    
    if validation["is_valid"]:
        print("✅ Batch validation passed")
    else:
        print(f"❌ Batch validation failed: {validation}")
        return False
    
    # Test batch summary
    summary = batch_manager.get_batch_summary(batches)
    print(f"Batch summary: {summary}")
    
    # Test optimal batch calculation
    optimal_batches = batch_manager.calculate_optimal_batch_count(
        total_items=len(test_cvrs),
        max_batch_size=50,
        min_batch_size=10
    )
    
    print(f"Optimal batch count for {len(test_cvrs)} items: {optimal_batches}")
    
    return True


def test_shared_config():
    """Test shared configuration functionality."""
    print("\n🧪 Testing shared configuration...")
    
    # Test default config
    config = CVREnrichmentSharedConfig()
    print(f"Default batch size: {config.default_batch_size}")
    print(f"Enable geocoding: {config.enable_address_geocoding}")
    print(f"Fetch P-numbers: {config.fetch_pnumber_data}")
    
    # Test config with overrides
    config_with_test_limit = CVREnrichmentSharedConfig(
        test_limit=10,
        enable_address_geocoding=False,
        fetch_pnumber_data=True
    )
    
    print(f"Test config with limit: {config_with_test_limit.test_limit}")
    
    return True


def create_mock_collection_data():
    """Create mock collection data for testing pipeline steps."""
    print("\n🧪 Creating mock collection data...")
    
    # Create a small test dataset
    test_cvrs = ["10103940", "25052943", "33065056", "12345678", "87654321"]
    
    collection_data = {
        "all_cvr_numbers": set(test_cvrs),
        "pipeline_sources": {cvr: ["test_pipeline"] for cvr in test_cvrs},
        "collection_summary": {
            "total_cvr_numbers": len(test_cvrs),
            "pipelines_processed": 1,
            "unique_pipelines": ["test_pipeline"]
        }
    }
    
    print(f"Created mock collection with {len(test_cvrs)} CVR numbers")
    return collection_data


async def test_integration():
    """Test integration of P-number discovery with pipeline steps."""
    print("\n🧪 Testing integration with pipeline steps...")
    
    # Create mock data
    collection_data = create_mock_collection_data()
    
    # Test batch creation
    batch_manager = CVRBatchManager(batch_size=2)  # Small batches for testing
    batches = batch_manager.create_cvr_batches(collection_data["all_cvr_numbers"])
    
    print(f"Created {len(batches)} batches for pipeline testing")
    
    # Test first batch with real API calls
    if batches:
        test_batch = batches[0]
        print(f"Testing batch 1 with {len(test_batch)} CVRs: {test_batch}")
        
        # Initialize CVR client
        cvr_client = CVRAPIClient(enable_geocoding=False)
        
        # Simulate company fetching step
        print("  → Simulating company fetching step...")
        company_results = cvr_client.fetch_multiple_companies(
            cvr_numbers=test_batch,
            fetch_all_fields=True,
            enrich_with_geometry=False
        )
        
        # Extract P-numbers
        print("  → Extracting P-numbers...")
        batch_pnumbers = []
        for cvr, company_data in company_results["results"].items():
            if company_data:
                pnumbers = cvr_client.get_company_pnumbers(company_data)
                batch_pnumbers.extend(pnumbers)
                
                # Add processing metadata (like real pipeline would)
                company_data["processing_timestamp"] = datetime.now().isoformat()
                company_data["batch_number"] = 1
                company_data["extracted_pnumbers"] = pnumbers
        
        print(f"  → Found {len(batch_pnumbers)} P-numbers in batch")
        
        # Simulate P-number fetching step
        if batch_pnumbers:
            print("  → Simulating P-number fetching step...")
            pnumber_results = cvr_client.fetch_multiple_pnumbers(
                pnumbers=batch_pnumbers,
                fetch_all_fields=True,
                enrich_with_geometry=False
            )
            
            # Count addresses
            total_addresses = 0
            for pnumber_data in pnumber_results["results"].values():
                if pnumber_data:
                    total_addresses += len(pnumber_data.get("addresses", []))
            
            print(f"  → P-number fetching found {total_addresses} additional addresses")
        
        return {
            "batch_size": len(test_batch),
            "companies_fetched": company_results["summary"]["successful"],
            "pnumbers_found": len(batch_pnumbers),
            "addresses_discovered": total_addresses if batch_pnumbers else 0
        }
    
    return {"error": "No batches created"}


async def main():
    """Main test function."""
    print("🚀 Starting CVR P-number Discovery and Pipeline Split Tests")
    print("=" * 60)
    
    # Check environment
    if not os.getenv("CVR_USERNAME") or not os.getenv("CVR_PASSWORD"):
        print("⚠️ Warning: CVR credentials not found in environment")
        print("   Tests will use hardcoded credentials from memory")
    
    results = {}
    
    try:
        # Test 1: P-number discovery
        results["pnumber_discovery"] = await test_pnumber_discovery()
        
        # Test 2: Batch manager
        results["batch_manager"] = test_batch_manager()
        
        # Test 3: Shared config
        results["shared_config"] = test_shared_config()
        
        # Test 4: Integration test
        results["integration"] = await test_integration()
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Print final results
    print("\n" + "=" * 60)
    print("🎯 Test Results Summary:")
    
    if "pnumber_discovery" in results:
        pn_results = results["pnumber_discovery"]
        print(f"   P-number Discovery:")
        print(f"     • Companies: {pn_results['companies_successful']}/{pn_results['companies_tested']}")
        print(f"     • P-numbers: {pn_results['pnumbers_successful']}/{pn_results['pnumbers_found']}")
        print(f"     • Addresses: {pn_results['total_addresses']}")
    
    if "integration" in results and "error" not in results["integration"]:
        int_results = results["integration"]
        print(f"   Integration Test:")
        print(f"     • Batch size: {int_results['batch_size']}")
        print(f"     • Companies fetched: {int_results['companies_fetched']}")
        print(f"     • P-numbers found: {int_results['pnumbers_found']}")
        print(f"     • Additional addresses: {int_results['addresses_discovered']}")
    
    print(f"   Batch Manager: {'✅' if results.get('batch_manager') else '❌'}")
    print(f"   Shared Config: {'✅' if results.get('shared_config') else '❌'}")
    
    print("\n✅ All tests completed!")
    
    # Save results to file
    results_file = Path("scripts/testing/cvr_pnumber_test_results.json")
    results_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(results_file, "w") as f:
        json.dump({
            "test_timestamp": datetime.now().isoformat(),
            "results": results
        }, f, indent=2, default=str)
    
    print(f"📄 Results saved to: {results_file}")
    
    return True


if __name__ == "__main__":
    asyncio.run(main())
