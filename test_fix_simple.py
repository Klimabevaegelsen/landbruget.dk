#!/usr/bin/env python3
"""
Simple test to verify the fix by checking the actual code changes.
"""

import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def test_code_changes():
    """Test that the problematic code has been removed."""
    
    logger.info("🧪 Testing that the hardcoded skip logic has been removed...")
    
    # Read the fixed file
    with open('backend/pipelines/chr_pipeline/bronze/data_processing.py', 'r') as f:
        content = f.read()
    
    # Check that the problematic skip conditions are gone
    problematic_patterns = [
        'skipped_reason": "dataset_too_large"',
        'skipped_reason": "auto_chunking_required"',
        'return {.*"reporting_herd_number": reporting_herd,.*"movements": \[\],.*"skipped_reason"'
    ]
    
    issues_found = []
    
    for pattern in problematic_patterns:
        if 'dataset_too_large' in pattern and pattern in content:
            issues_found.append("Found dataset_too_large skip logic")
        elif 'auto_chunking_required' in pattern and pattern in content:
            issues_found.append("Found auto_chunking_required skip logic")
    
    # Check that positive changes are present
    positive_patterns = [
        'will process using volume management chunking',
        'processing will continue'
    ]
    
    missing_positives = []
    for pattern in positive_patterns:
        if pattern not in content:
            missing_positives.append(f"Missing positive pattern: {pattern}")
    
    # Report results
    if issues_found:
        logger.error("❌ FAIL: Found problematic code that should have been removed:")
        for issue in issues_found:
            logger.error(f"  - {issue}")
        return False
    
    if missing_positives:
        logger.error("❌ FAIL: Missing expected positive changes:")
        for missing in missing_positives:
            logger.error(f"  - {missing}")
        return False
    
    logger.info("✅ SUCCESS: Code changes look correct!")
    logger.info("  - Removed hardcoded skip logic for large herds")
    logger.info("  - Added processing continuation logic")
    return True

def test_volume_management_exists():
    """Test that volume management functionality exists."""
    
    logger.info("🔧 Testing that volume management system exists...")
    
    try:
        with open('backend/pipelines/chr_pipeline/bronze/volume_management.py', 'r') as f:
            content = f.read()
        
        required_functions = [
            'def get_optimal_date_range(',
            'def add_high_volume_herd(',
            'def is_high_volume_herd('
        ]
        
        missing_functions = []
        for func in required_functions:
            if func not in content:
                missing_functions.append(func)
        
        if missing_functions:
            logger.error("❌ FAIL: Missing volume management functions:")
            for func in missing_functions:
                logger.error(f"  - {func}")
            return False
        
        logger.info("✅ SUCCESS: Volume management system is in place!")
        return True
        
    except FileNotFoundError:
        logger.error("❌ FAIL: volume_management.py file not found")
        return False

def test_chunking_integration():
    """Test that chunking is integrated in animal_movements.py."""
    
    logger.info("🔄 Testing chunking integration...")
    
    try:
        with open('backend/pipelines/chr_pipeline/bronze/animal_movements.py', 'r') as f:
            content = f.read()
        
        required_patterns = [
            'from .volume_management import get_optimal_date_range',
            'date_ranges = get_optimal_date_range(',
            'Processing herd {herd_number} in {len(date_ranges)} chunks due to high volume'
        ]
        
        missing_patterns = []
        for pattern in required_patterns:
            if pattern not in content:
                missing_patterns.append(pattern)
        
        if missing_patterns:
            logger.error("❌ FAIL: Missing chunking integration:")
            for pattern in missing_patterns:
                logger.error(f"  - {pattern}")
            return False
        
        logger.info("✅ SUCCESS: Chunking integration is in place!")
        return True
        
    except FileNotFoundError:
        logger.error("❌ FAIL: animal_movements.py file not found")
        return False

def main():
    """Run all tests."""
    logger.info("🚀 Starting simple verification of CHR large herd fix...")
    
    tests = [
        ("Code Changes Verification", test_code_changes),
        ("Volume Management System", test_volume_management_exists), 
        ("Chunking Integration", test_chunking_integration),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*60}")
        logger.info(f"Test: {test_name}")
        logger.info('='*60)
        
        try:
            if test_func():
                logger.info(f"✅ {test_name}: PASSED")
                passed += 1
            else:
                logger.error(f"❌ {test_name}: FAILED")
                failed += 1
        except Exception as e:
            logger.error(f"❌ {test_name}: ERROR - {e}")
            failed += 1
    
    logger.info(f"\n{'='*60}")
    logger.info("FINAL RESULTS")
    logger.info('='*60)
    logger.info(f"✅ Passed: {passed}")
    logger.info(f"❌ Failed: {failed}")
    
    if failed == 0:
        logger.info("\n🎉 All tests passed!")
        logger.info("The fix appears to be correctly implemented:")
        logger.info("- Large herds will no longer be skipped")
        logger.info("- Volume management system will handle chunking")
        logger.info("- CHR data loss issue should be resolved")
        return True
    else:
        logger.error(f"\n💥 {failed} tests failed. Please review the issues above.")
        return False

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)