#!/usr/bin/env python3
"""
Verify column mappings for each year of fertiliser data.
"""

import pandas as pd
from pathlib import Path

def verify_each_year():
    """Check the actual column structure for each year and file type."""
    
    print("🔍 YEAR-BY-YEAR VERIFICATION OF FERTILISER DATA")
    print("=" * 80)
    
    fertiliser_dir = Path("data/fertiliser")
    
    print("\n📋 EFTERAFGRØDER FILES - DETAILED COLUMN ANALYSIS:")
    print("-" * 60)
    
    # Check each Efterafgrøder file
    efterafgroeder_files = [f for f in fertiliser_dir.glob("Efterafgrøder*.parquet")]
    
    for file_path in sorted(efterafgroeder_files):
        try:
            df = pd.read_parquet(file_path)
            year = file_path.name.split()[-1].split('.')[0] if ' ' in file_path.name else 'Unknown'
            
            print(f"\n📅 {year} ({file_path.name}):")
            print(f"   Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
            
            # Show all columns
            print("   All columns:")
            for i, col in enumerate(df.columns, 1):
                print(f"     {i:2}. {col}")
            
            # Find the numbered columns (a18_, a19_, etc.)
            numbered_cols = [col for col in df.columns if col.startswith('a') and '_' in col]
            print(f"   📊 Numbered columns: {numbered_cols}")
            
            # Show sample values for key columns
            if numbered_cols:
                print("   📝 Sample data from numbered columns:")
                for col in numbered_cols[:3]:  # Show first 3 numbered columns
                    sample_vals = df[col].dropna().unique()[:3]
                    print(f"     {col}: {list(sample_vals)}")
                    
        except Exception as e:
            print(f"   ❌ Error reading {file_path.name}: {e}")
    
    print("\n📋 GKEA FILES - DETAILED COLUMN ANALYSIS:")
    print("-" * 60)
    
    # Check each GKEA file
    gkea_files = [f for f in fertiliser_dir.glob("GKEA*.parquet")]
    
    for file_path in sorted(gkea_files):
        try:
            df = pd.read_parquet(file_path)
            year_match = file_path.name.split('GKEA')[1][:4] if 'GKEA' in file_path.name else 'Unknown'
            
            print(f"\n📅 GKEA {year_match} ({file_path.name}):")
            print(f"   Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
            
            # Show first few rows to understand the header structure
            print("   📝 First 5 rows to identify headers:")
            print(df.head(5).to_string())
            
            # Check if row 2 contains the actual headers
            if len(df) > 1:
                row_2 = df.iloc[1]
                potential_headers = [str(val) for val in row_2.values[:10]]
                print(f"   📋 Row 2 (potential headers): {potential_headers}")
                
        except Exception as e:
            print(f"   ❌ Error reading {file_path.name}: {e}")
    
    print("\n📋 GØDNINGSREGNSKABER FILES - DETAILED COLUMN ANALYSIS:")
    print("-" * 60)
    
    # Check each Gødningsregnskaber file
    goedning_files = [f for f in fertiliser_dir.glob("Gødningsregnskaber*.parquet")]
    
    for file_path in sorted(goedning_files):
        try:
            df = pd.read_parquet(file_path)
            
            print(f"\n📅 {file_path.name}:")
            print(f"   Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
            
            # Show key columns that we care about
            key_columns = ['cvr_number', 'kommune', 'vir_navn', 'planaar'] if 'planaar' in df.columns else ['cvr_number', 'kommune', 'vir_navn']
            existing_key_cols = [col for col in key_columns if col in df.columns]
            
            print(f"   📊 Key columns present: {existing_key_cols}")
            
            if existing_key_cols:
                print("   📝 Sample data:")
                sample_data = df[existing_key_cols].head(3)
                print(sample_data.to_string())
                
        except Exception as e:
            print(f"   ❌ Error reading {file_path.name}: {e}")
    
    print("\n⚠️  POTENTIAL MAPPING ISSUES TO VERIFY:")
    print("-" * 60)
    print("1. Efterafgrøder column number shifts - need to verify actual data content")
    print("2. GKEA generic columns - need to map based on actual header row content")  
    print("3. Gødningsregnskaber structure differences between 2022 and 2023")
    print("4. Missing marknummer in some Efterafgrøder years")
    print("5. Data type consistency (numeric vs text)")
    
    return True

if __name__ == "__main__":
    verify_each_year()