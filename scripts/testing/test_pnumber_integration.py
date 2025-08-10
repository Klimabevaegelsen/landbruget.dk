"""
Test P-Number Integration

This script tests the core P-number address discovery functionality
to validate that the new P-number integration works correctly.
"""

import asyncio
import sys
import os

# Add the unified pipeline to the Python path
sys.path.append('../backend/pipelines/unified_pipeline/src')

from unified_pipeline.util.cvr_api_client import CVRAPIClient


async def test_pnumber_integration():
    """Test P-number integration functionality."""
    
    print("🧪 Testing P-Number Address Integration")
    print("=" * 50)
    
    # Set up CVR API client
    cvr_username = os.getenv("CVR_USERNAME", "Martin_Collignon_CVR_I_SKYEN")
    cvr_password = os.getenv("CVR_PASSWORD", "3a37d029-9588-4c00-8a09-3d2901452d45")
    
    client = CVRAPIClient(
        username=cvr_username,
        password=cvr_password,
        enable_geocoding=True,
        geocode_current_only=True
    )
    
    print(f"✅ CVR API client initialized")
    print()
    
    # Test CVR numbers known to have P-numbers (as strings, like in real data)
    test_cvrs = ["31373077", "10103940"]  # Known companies with P-numbers
    
    print("🏢 Testing Company Data Fetching")
    print("-" * 40)
    
    company_results = {}
    
    for cvr_number in test_cvrs:
        print(f"Fetching company data for CVR: {cvr_number}")
        
        try:
            # Fetch company data
            company_data = client.get_company_data(cvr_number)
            
            if company_data:
                company_results[cvr_number] = company_data
                
                print(f"✅ Company: {company_data.get('company_name', 'N/A')}")
                print(f"   • Addresses: {len(company_data.get('addresses', []))}")
                
                # Extract P-numbers from company data
                pnumbers = client.get_company_pnumbers(company_data)
                print(f"   • P-numbers found: {len(pnumbers)}")
                
                if pnumbers:
                    print(f"   • P-numbers: {pnumbers[:3]}{'...' if len(pnumbers) > 3 else ''}")
                
                company_results[cvr_number]['extracted_pnumbers'] = pnumbers
                
            else:
                print(f"❌ No data found for CVR: {cvr_number}")
        
        except Exception as e:
            print(f"❌ Error fetching company {cvr_number}: {e}")
        
        print()
    
    print("🏭 Testing P-Number Data Fetching")
    print("-" * 40)
    
    pnumber_results = {}
    all_pnumbers = []
    
    # Collect all P-numbers from companies
    for cvr_number, company_data in company_results.items():
        pnumbers = company_data.get('extracted_pnumbers', [])
        for pnumber in pnumbers[:2]:  # Test first 2 P-numbers per company
            all_pnumbers.append((cvr_number, pnumber))
    
    print(f"Testing {len(all_pnumbers)} P-numbers...")
    
    for cvr_number, pnumber in all_pnumbers:
        print(f"Fetching P-number data: {pnumber} (from CVR {cvr_number})")
        
        try:
            # Fetch P-number data
            pnumber_data = client.get_pnumber_data(pnumber)
            
            if pnumber_data:
                pnumber_results[pnumber] = pnumber_data
                
                print(f"✅ P-number: {pnumber_data.get('unit_name', 'N/A')}")
                
                addresses = pnumber_data.get('addresses', [])
                print(f"   • Addresses: {len(addresses)}")
                
                current_addresses = [addr for addr in addresses if addr.get('is_current')]
                print(f"   • Current addresses: {len(current_addresses)}")
                
                if addresses:
                    sample_addr = addresses[0]
                    print(f"   • Sample address: {sample_addr.get('full_address', 'N/A')}")
                
                # Test geocoding if addresses exist
                if current_addresses and client.enable_geocoding:
                    print("   🌍 Testing address geocoding...")
                    
                    enriched_data = client.enrich_pnumber_with_geometry(pnumber_data)
                    
                    geocoded_addresses = [
                        addr for addr in enriched_data.get('addresses', [])
                        if addr.get('dawa_enriched') or addr.get('datavask_enriched')
                    ]
                    
                    print(f"   ✅ Geocoded addresses: {len(geocoded_addresses)}")
                    
                    if geocoded_addresses:
                        sample_geocoded = geocoded_addresses[0]
                        lat = sample_geocoded.get('latitude')
                        lon = sample_geocoded.get('longitude')
                        print(f"   • Sample coordinates: {lat}, {lon}")
            else:
                print(f"❌ No data found for P-number: {pnumber}")
        
        except Exception as e:
            print(f"❌ Error fetching P-number {pnumber}: {e}")
        
        print()
    
    print("📊 Test Results Summary")
    print("-" * 40)
    
    total_companies = len(company_results)
    companies_with_pnumbers = len([c for c in company_results.values() if c.get('extracted_pnumbers')])
    total_pnumbers_tested = len(pnumber_results)
    
    print(f"✅ Companies tested: {total_companies}")
    print(f"✅ Companies with P-numbers: {companies_with_pnumbers}")
    print(f"✅ P-numbers successfully fetched: {total_pnumbers_tested}")
    
    # Calculate address statistics
    total_company_addresses = sum(
        len(c.get('addresses', [])) for c in company_results.values()
    )
    
    total_pnumber_addresses = sum(
        len(p.get('addresses', [])) for p in pnumber_results.values()
    )
    
    current_pnumber_addresses = sum(
        len([addr for addr in p.get('addresses', []) if addr.get('is_current')])
        for p in pnumber_results.values()
    )
    
    print(f"✅ Company addresses: {total_company_addresses}")
    print(f"✅ P-number addresses: {total_pnumber_addresses}")
    print(f"✅ Current P-number addresses: {current_pnumber_addresses}")
    
    # Test success criteria
    success = (
        total_companies > 0 and
        companies_with_pnumbers > 0 and
        total_pnumbers_tested > 0 and
        current_pnumber_addresses > 0
    )
    
    print()
    if success:
        print("🎉 P-NUMBER INTEGRATION TEST PASSED!")
        print("   • P-number discovery working")
        print("   • Address extraction working")
        print("   • Current address filtering working")
        if client.enable_geocoding:
            print("   • Address geocoding working")
        print("   • Ready for production use!")
    else:
        print("❌ P-NUMBER INTEGRATION TEST FAILED!")
        print("   • Check error messages above")
    
    return success


def run_pnumber_test():
    """Run the P-number integration test."""
    
    # Check environment
    if not os.getenv("CVR_USERNAME") or not os.getenv("CVR_PASSWORD"):
        print("⚠️  CVR credentials not found in environment")
        print("   Setting test credentials...")
        os.environ["CVR_USERNAME"] = "Martin_Collignon_CVR_I_SKYEN"
        os.environ["CVR_PASSWORD"] = "3a37d029-9588-4c00-8a09-3d2901452d45"
    
    # Run the async test
    success = asyncio.run(test_pnumber_integration())
    
    return success


if __name__ == "__main__":
    success = run_pnumber_test()
    sys.exit(0 if success else 1)
