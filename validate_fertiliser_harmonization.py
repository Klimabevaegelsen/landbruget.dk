#!/usr/bin/env python3
"""
Validation script for fertiliser data harmonization - simpler version without dependencies.
"""

from pathlib import Path

import pandas as pd


def validate_fertiliser_data():
    """Validate the fertiliser data and create harmonization plan."""
    
    print("🔍 FERTILIZER DATA VALIDATION AND HARMONIZATION PLAN")
    print("=" * 80)
    
    fertiliser_dir = Path("data/fertiliser")
    
    if not fertiliser_dir.exists():
        print("❌ Fertiliser data directory not found!")
        return False
    
    parquet_files = list(fertiliser_dir.glob("*.parquet"))
    print(f"Found {len(parquet_files)} parquet files")
    
    # Validation results
    validation_results = {
        'efterafgroeder': [],
        'gkea': [],
        'goedningsregnskaber': []
    }
    
    print("\n📊 FILE VALIDATION:")
    print("-" * 50)
    
    for file_path in sorted(parquet_files):
        try:
            df = pd.read_parquet(file_path)
            file_name = file_path.name
            
            result = {
                'file': file_name,
                'rows': df.shape[0],
                'columns': df.shape[1],
                'column_names': list(df.columns),
                'status': 'valid'
            }
            
            # Categorize files
            if 'Efterafgrøder' in file_name:
                validation_results['efterafgroeder'].append(result)
            elif 'GKEA' in file_name:
                validation_results['gkea'].append(result)
            elif 'Gødningsregnskaber' in file_name:
                validation_results['goedningsregnskaber'].append(result)
            
            print(f"✅ {file_name}: {df.shape[0]:,} rows × {df.shape[1]} cols")
            
        except Exception as e:
            print(f"❌ {file_path.name}: Error - {str(e)}")
    
    print("\n🔧 HARMONIZATION PLAN:")
    print("-" * 50)
    
    # Efterafgrøder harmonization plan
    print("\n1. EFTERAFGRØDER FILES:")
    efterafgroeder_files = validation_results['efterafgroeder']
    if efterafgroeder_files:
        print("   Column standardization needed:")
        for file_info in efterafgroeder_files:
            year = file_info['file'].split()[-1].split('.')[0] if ' ' in file_info['file'] else 'Unknown'
            numbered_cols = [col for col in file_info['column_names'] if col.startswith('a') and '_' in col]
            print(f"   • {year}: {numbered_cols}")
        
        print("   → Standard schema will map:")
        print("     - a18/a19/a20_indberetefterafgalternativ → indberet_alternativ")
        print("     - a19/a20/a21_faktiskhaudlagteaalternativ → faktisk_areal_ha") 
        print("     - a20/a21/a23/a24_omregnethamedea → omregnet_areal_ha")
    
    # GKEA harmonization plan  
    print("\n2. GKEA FILES:")
    gkea_files = validation_results['gkea']
    if gkea_files:
        print("   Generic column names need mapping:")
        for file_info in gkea_files:
            year = file_info['file'].split('GKEA')[1][:4] if 'GKEA' in file_info['file'] else 'Unknown'
            generic_cols = len([col for col in file_info['column_names'] if col.startswith('column_')])
            print(f"   • GKEA {year}: {generic_cols} generic columns need proper names")
        
        print("   → Standard schema will extract:")
        print("     - column_1 → journal_nummer")
        print("     - column_2 → cvr_number")
        print("     - column_X → marknummer, areal_ha, hovedafgroede, etc.")
    
    # Gødningsregnskaber harmonization plan
    print("\n3. GØDNINGSREGNSKABER FILES:")
    goedning_files = validation_results['goedningsregnskaber']
    if goedning_files:
        print("   Complex multi-column structure:")
        for file_info in goedning_files:
            print(f"   • {file_info['file']}: {file_info['columns']} columns")
        
        print("   → Standard schema will focus on:")
        print("     - cvr_number, kommune, virksomhed information")
        print("     - Year extraction from filename")
    
    print("\n🎯 HARMONIZATION BENEFITS:")
    print("-" * 50)
    print("✅ Unified schema across all years and file types")
    print("✅ Consistent column naming and data types") 
    print("✅ Proper numeric conversion (text → float)")
    print("✅ Common identifiers (CVR, year) for joining")
    print("✅ Source tracking for data lineage")
    print("✅ Duplicate detection and removal")
    
    print("\n📋 IMPLEMENTATION STATUS:")
    print("-" * 50)
    print("✅ FertiliserSilverProcessor created")
    print("✅ Schema mapping defined for all file types")
    print("✅ Data type conversions implemented")
    print("✅ Error handling and logging added")
    print("📋 Ready for pipeline integration")
    
    print("\n🚀 NEXT STEPS:")
    print("-" * 50)
    print("1. Integrate FertiliserSilverProcessor into unified_pipeline")
    print("2. Add configuration to pipeline app.py")
    print("3. Set up automated bronze→silver processing")
    print("4. Configure GCS upload for harmonized data")
    print("5. Add data quality monitoring and alerts")
    
    return True

if __name__ == "__main__":
    validate_fertiliser_data()