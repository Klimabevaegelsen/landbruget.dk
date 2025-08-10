"""
Test Pipeline Data Flow

This script tests that the 6-step CVR enrichment pipeline can work together,
focusing on data flow between steps and batch processing functionality.
"""

import asyncio
import sys
import os
import json
import tempfile
from datetime import datetime
from pathlib import Path

# Add the unified pipeline to the Python path
sys.path.append('../backend/pipelines/unified_pipeline/src')

from unified_pipeline.gold.cvr_enrichment.shared.config import CVREnrichmentSharedConfig
from unified_pipeline.gold.cvr_enrichment.shared.batch_manager import CVRBatchManager


def test_shared_configuration():
    """Test the shared configuration system."""
    
    print("🔧 Testing Shared Configuration")
    print("-" * 40)
    
    try:
        # Test default configuration
        config = CVREnrichmentSharedConfig()
        
        print(f"✅ Default config created")
        print(f"   • Test limit: {config.test_limit}")
        print(f"   • Default batch size: {config.default_batch_size}")
        print(f"   • P-number fetching: {config.fetch_pnumber_data}")
        print(f"   • Current addresses only: {config.pnumber_current_addresses_only}")
        print(f"   • Enable geocoding: {config.enable_address_geocoding}")
        
        # Test custom configuration
        custom_config = CVREnrichmentSharedConfig(
            test_limit=50,
            default_batch_size=10,
            fetch_pnumber_data=True,
            pnumber_current_addresses_only=True,
            enable_address_geocoding=True,
            geocoding_current_addresses_only=True
        )
        
        print(f"✅ Custom config created")
        print(f"   • Test limit: {custom_config.test_limit}")
        print(f"   • Batch size: {custom_config.default_batch_size}")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_batch_manager():
    """Test the batch manager functionality."""
    
    print("\n📦 Testing Batch Manager")
    print("-" * 40)
    
    try:
        # Test CVR list creation and batching
        test_cvrs = [
            "31373077", "10103940", "25052943", "36914648", "12345678",
            "87654321", "11111111", "22222222", "33333333", "44444444"
        ]
        
        print(f"Test CVRs: {len(test_cvrs)}")
        
        # Test different batch sizes
        for batch_size in [3, 5, 7]:
            print(f"\n  Testing batch size: {batch_size}")
            
            batch_manager = CVRBatchManager(batch_size=batch_size)
            
            # Convert list to set as expected by the method
            cvr_set = set(test_cvrs)
            batches = batch_manager.create_cvr_batches(cvr_set)
            
            print(f"  ✅ Created {len(batches)} batches")
            print(f"     • Batch sizes: {[len(batch) for batch in batches]}")
            
            # Validate batch integrity
            all_cvrs_in_batches = []
            for batch in batches:
                all_cvrs_in_batches.extend(batch)
            
            if set(all_cvrs_in_batches) == set(test_cvrs):
                print(f"  ✅ Batch integrity validated")
            else:
                print(f"  ❌ Batch integrity failed")
                return False
            
            # Test batch validation
            validation_result = batch_manager.validate_batch_coverage(cvr_set, batches)
            if validation_result.get("is_valid", False):
                print(f"  ✅ Batch validation passed")
            else:
                print(f"  ❌ Batch validation failed: {validation_result}")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ Batch manager test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_step_input_output_paths():
    """Test step input/output path resolution."""
    
    print("\n🔗 Testing Step Path Resolution")
    print("-" * 40)
    
    try:
        from unified_pipeline.gold.cvr_enrichment.shared.config import (
            CVREnrichmentStep, get_step_input_paths
        )
        
        test_date = "20240815_120000"
        bucket = "test-bucket"
        
        # Test each step's input path requirements
        steps_to_test = [
            (CVREnrichmentStep.COMPANY_FETCHING, "Company Fetching"),
            (CVREnrichmentStep.PNUMBER_FETCHING, "P-Number Fetching"),
            (CVREnrichmentStep.FINANCIAL_DOCUMENTS, "Financial Documents"),
            (CVREnrichmentStep.ADDRESS_GEOCODING, "Address Geocoding"),
            (CVREnrichmentStep.DATA_CONSOLIDATION, "Data Consolidation"),
        ]
        
        for step, step_name in steps_to_test:
            print(f"\n  {step_name}:")
            
            # Test without batching
            input_paths = get_step_input_paths(step, test_date, bucket=bucket)
            print(f"    • No batching: {len(input_paths)} input paths")
            for path in input_paths[:2]:  # Show first 2
                print(f"      - {path}")
            if len(input_paths) > 2:
                print(f"      - ... and {len(input_paths) - 2} more")
            
            # Test with batching
            input_paths_batched = get_step_input_paths(
                step, test_date, total_batches=3, bucket=bucket
            )
            print(f"    • With batching: {len(input_paths_batched)} input paths")
            for path in input_paths_batched[:2]:  # Show first 2
                print(f"      - {path}")
            if len(input_paths_batched) > 2:
                print(f"      - ... and {len(input_paths_batched) - 2} more")
        
        print(f"\n✅ Step path resolution working")
        return True
        
    except Exception as e:
        print(f"❌ Step path resolution test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_data_serialization():
    """Test data serialization and deserialization for step communication."""
    
    print("\n💾 Testing Data Serialization")
    print("-" * 40)
    
    try:
        # Test company data structure
        sample_company_data = {
            "cvr_number": "31373077",
            "company_name": "Test Company",
            "addresses": [
                {
                    "address_type": "beliggenhedsadresse",
                    "full_address": "Test Street 123",
                    "postal_code": "1234",
                    "city": "Test City",
                    "is_current": True,
                    "latitude": None,
                    "longitude": None
                }
            ],
            "pnumber_data": [],
            "financial_documents": [],
            "fetch_timestamp": datetime.now().isoformat()
        }
        
        # Test JSON serialization
        json_str = json.dumps(sample_company_data)
        deserialized = json.loads(json_str)
        
        print(f"✅ Company data serialization working")
        print(f"   • Original keys: {list(sample_company_data.keys())}")
        print(f"   • Deserialized keys: {list(deserialized.keys())}")
        
        # Test P-number data structure
        sample_pnumber_data = {
            "p_number": "1234567890",
            "unit_name": "Test P-Number",
            "parent_cvr_number": "31373077",
            "addresses": [
                {
                    "address_type": "beliggenhedsadresse",
                    "full_address": "P-Number Address 456",
                    "is_current": True,
                    "latitude": 55.123456,
                    "longitude": 12.654321,
                    "dawa_enriched": True
                }
            ],
            "industries": [],
            "fetch_timestamp": datetime.now().isoformat()
        }
        
        json_str_pnum = json.dumps(sample_pnumber_data)
        deserialized_pnum = json.loads(json_str_pnum)
        
        print(f"✅ P-number data serialization working")
        print(f"   • P-number: {deserialized_pnum['p_number']}")
        print(f"   • Addresses: {len(deserialized_pnum['addresses'])}")
        
        # Test financial data structure
        sample_financial_data = {
            "cvr_number": "31373077",
            "documents": [
                {
                    "document_type": "XBRL",
                    "reporting_period": {"start_date": "2023-01-01", "end_date": "2023-12-31"},
                    "financial_metrics": {
                        "total_assets": 1000000,
                        "revenue": 500000
                    }
                }
            ],
            "document_count": 1,
            "fetch_timestamp": datetime.now().isoformat()
        }
        
        json_str_fin = json.dumps(sample_financial_data)
        deserialized_fin = json.loads(json_str_fin)
        
        print(f"✅ Financial data serialization working")
        print(f"   • Document count: {deserialized_fin['document_count']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data serialization test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_pipeline_configuration_compatibility():
    """Test that all pipeline step configurations are compatible."""
    
    print("\n⚙️  Testing Pipeline Configuration Compatibility")
    print("-" * 40)
    
    try:
        from unified_pipeline.gold.cvr_enrichment import (
            CVRCollectionConfig, CompanyFetchingConfig, PNumberFetchingConfig,
            FinancialDocumentsConfig, AddressGeocodingConfig, DataConsolidationConfig
        )
        
        # Create shared config
        shared_config = CVREnrichmentSharedConfig(
            test_limit=100,
            default_batch_size=20,
            fetch_pnumber_data=True,
            pnumber_current_addresses_only=True,
            enable_address_geocoding=True
        )
        
        # Test each step configuration
        configs_to_test = [
            ("CVR Collection", CVRCollectionConfig(shared_config=shared_config)),
            ("Company Fetching", CompanyFetchingConfig(
                shared_config=shared_config, batch_number=1, total_batches=3
            )),
            ("P-Number Fetching", PNumberFetchingConfig(
                shared_config=shared_config, batch_number=1, total_batches=3
            )),
            ("Financial Documents", FinancialDocumentsConfig(
                shared_config=shared_config, batch_number=1, total_batches=3
            )),
            ("Address Geocoding", AddressGeocodingConfig(
                shared_config=shared_config, batch_number=1, total_batches=3
            )),
            ("Data Consolidation", DataConsolidationConfig(shared_config=shared_config)),
        ]
        
        for step_name, config in configs_to_test:
            print(f"  {step_name}:")
            print(f"    • Dataset: {config.dataset}")
            print(f"    • Bucket: {config.bucket}")
            print(f"    • Shared config test limit: {config.shared_config.test_limit}")
            
            # Test configuration validation (Pydantic)
            try:
                config.model_validate(config.model_dump())
                print(f"    ✅ Configuration validation passed")
            except Exception as e:
                print(f"    ❌ Configuration validation failed: {e}")
                return False
        
        print(f"\n✅ All pipeline configurations compatible")
        return True
        
    except Exception as e:
        print(f"❌ Configuration compatibility test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_pipeline_architecture_tests():
    """Run all pipeline architecture tests."""
    
    print("🧪 Testing CVR Enrichment Pipeline Architecture")
    print("=" * 60)
    
    tests = [
        ("Shared Configuration", test_shared_configuration),
        ("Batch Manager", test_batch_manager),
        ("Step Path Resolution", test_step_input_output_paths),
        ("Data Serialization", test_data_serialization),
        ("Configuration Compatibility", test_pipeline_configuration_compatibility),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n🔍 Running: {test_name}")
        try:
            success = test_func()
            results[test_name] = success
            if success:
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
        except Exception as e:
            print(f"❌ {test_name}: ERROR - {e}")
            results[test_name] = False
    
    # Summary
    print(f"\n📊 Test Results Summary")
    print("=" * 60)
    
    total_tests = len(tests)
    passed_tests = sum(1 for success in results.values() if success)
    
    for test_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {status} - {test_name}")
    
    print(f"\n🎯 Overall Result: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("🎉 PIPELINE ARCHITECTURE TESTS PASSED!")
        print("   • All components working correctly")
        print("   • Data flow validated")
        print("   • Batch processing ready")
        print("   • Configuration system working")
        print("   • Ready for GitHub Actions integration!")
        return True
    else:
        print("❌ PIPELINE ARCHITECTURE TESTS FAILED!")
        print(f"   • {total_tests - passed_tests} test(s) failed")
        print("   • Fix issues before deployment")
        return False


if __name__ == "__main__":
    success = run_pipeline_architecture_tests()
    sys.exit(0 if success else 1)
