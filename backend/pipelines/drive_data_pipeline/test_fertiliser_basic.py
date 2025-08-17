#!/usr/bin/env python3
"""
Basic test to verify fertiliser integration code structure.
"""

import sys
from pathlib import Path

# Add paths for imports
parent_dir = Path(__file__).parent
sys.path.insert(0, str(parent_dir))

def test_file_structure() -> bool:
    """Test that the fertiliser transformer file exists and has correct structure."""
    print("📁 Testing File Structure")
    
    fertiliser_transformer_path = (
        parent_dir / "silver" / "transformers" / "fertiliser_transformer.py"
    )
    
    if not fertiliser_transformer_path.exists():
        print("❌ Fertiliser transformer file not found")
        return False
    
    print("✅ Fertiliser transformer file exists")
    
    # Check file contents
    with open(fertiliser_transformer_path) as f:
        content = f.read()
    
    required_elements = [
        "class FertiliserTransformer",
        "def can_handle",
        "def transform",
        "def transform_from_content",
        "_harmonize_fertiliser_data",
        "_process_efterafgroeder",
        "_process_gkea",
        "_process_goedningsregnskaber"
    ]
    
    missing_elements = []
    for element in required_elements:
        if element not in content:
            missing_elements.append(element)
    
    if missing_elements:
        print(f"❌ Missing required elements: {missing_elements}")
        return False
    
    print("✅ All required methods present")
    return True

def test_processor_integration() -> bool:
    """Test that the processor file has been updated correctly."""
    print("\n🔗 Testing Processor Integration")
    
    processor_path = parent_dir / "silver" / "processor.py"
    
    if not processor_path.exists():
        print("❌ Silver processor file not found")
        return False
    
    with open(processor_path) as f:
        content = f.read()
    
    # Check for fertiliser transformer import and registration
    required_changes = [
        "from .transformers.fertiliser_transformer import FertiliserTransformer",
        '"Fertiliser": FertiliserTransformer()',
    ]
    
    missing_changes = []
    for change in required_changes:
        if change not in content:
            missing_changes.append(change)
    
    if missing_changes:
        print(f"❌ Missing processor changes: {missing_changes}")
        return False
    
    print("✅ Silver processor correctly updated")
    return True

def test_unified_pipeline_cleanup() -> bool:
    """Test that unified pipeline references have been removed."""
    print("\n🧹 Testing Unified Pipeline Cleanup")
    
    # Check that fertiliser files were removed
    unified_base = parent_dir.parent / "unified_pipeline" / "src" / "unified_pipeline"
    
    removed_files = [
        unified_base / "bronze" / "fertiliser.py",
        unified_base / "silver" / "fertiliser.py"
    ]
    
    for file_path in removed_files:
        if file_path.exists():
            print(f"❌ File should have been removed: {file_path}")
            return False
    
    print("✅ Removed fertiliser pipeline files")
    
    # Check app.py for removed imports
    app_path = unified_base / "app.py" 
    if app_path.exists():
        with open(app_path) as f:
            app_content = f.read()
        
        removed_imports = [
            "from unified_pipeline.silver.fertiliser import",
            "from unified_pipeline.bronze.fertiliser import"
        ]
        
        for import_line in removed_imports:
            if import_line in app_content:
                print(f"❌ Import should have been removed: {import_line}")
                return False
        
        print("✅ Removed fertiliser imports from app.py")
    
    # Check cli.py for removed source
    cli_path = unified_base / "model" / "cli.py"
    if cli_path.exists():
        with open(cli_path) as f:
            cli_content = f.read()
        
        if 'fertiliser = "fertiliser"' in cli_content:
            print("❌ Fertiliser source should have been removed from CLI")
            return False
        
        print("✅ Removed fertiliser source from CLI")
    
    return True

def test_import_structure() -> bool:
    """Test the basic import structure without actually importing."""
    print("\n📦 Testing Import Structure")
    
    # Test that the fertiliser transformer follows the same pattern as other transformers
    transformers_dir = parent_dir / "silver" / "transformers"
    
    # Check other transformers for pattern reference
    base_transformer_path = transformers_dir / "base.py"
    excel_transformer_path = transformers_dir / "excel_transformer.py"
    
    if not base_transformer_path.exists():
        print("❌ Base transformer not found")
        return False
    
    if not excel_transformer_path.exists():
        print("❌ Excel transformer not found for pattern reference")
        return False
    
    # Read base transformer to understand interface
    with open(base_transformer_path) as f:
        f.read()
    
    # Read our fertiliser transformer
    fertiliser_transformer_path = transformers_dir / "fertiliser_transformer.py"
    with open(fertiliser_transformer_path) as f:
        fertiliser_content = f.read()
    
    # Check that fertiliser transformer imports from base
    if "from .base import BaseTransformer" not in fertiliser_content:
        print("❌ Fertiliser transformer doesn't import BaseTransformer")
        return False
    
    # Check that it inherits from BaseTransformer
    if "class FertiliserTransformer(BaseTransformer):" not in fertiliser_content:
        print("❌ Fertiliser transformer doesn't inherit from BaseTransformer")
        return False
    
    print("✅ Fertiliser transformer follows correct inheritance pattern")
    return True

if __name__ == "__main__":
    print("🚀 Running Basic Fertiliser Integration Verification")
    print("=" * 60)
    
    # Run tests
    structure_ok = test_file_structure()
    processor_ok = test_processor_integration()
    cleanup_ok = test_unified_pipeline_cleanup()
    import_ok = test_import_structure()
    
    print("\n" + "=" * 60)
    print("📊 Verification Results:")
    print(f"   - File structure: {'✅ PASS' if structure_ok else '❌ FAIL'}")
    print(f"   - Processor integration: {'✅ PASS' if processor_ok else '❌ FAIL'}")
    print(f"   - Pipeline cleanup: {'✅ PASS' if cleanup_ok else '❌ FAIL'}")
    print(f"   - Import structure: {'✅ PASS' if import_ok else '❌ FAIL'}")
    
    all_passed = structure_ok and processor_ok and cleanup_ok and import_ok
    
    if all_passed:
        print("\n🎉 All verifications PASSED!")
        print("   The fertiliser integration appears to be correctly implemented.")
        print("   Note: Runtime testing requires installing dependencies (duckdb, pydantic, etc.)")
    else:
        print("\n⚠️  Some verifications FAILED.")
        print("   Please fix the issues above before proceeding.")
    
    sys.exit(0 if all_passed else 1)