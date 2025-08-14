#!/usr/bin/env python3
"""
Detailed analysis of fertilizer data to identify harmonization issues.
"""

import pandas as pd
from pathlib import Path

def analyze_column_inconsistencies():
    """Analyze column naming and structure inconsistencies."""
    
    fertiliser_dir = Path("data/fertiliser")
    parquet_files = list(fertiliser_dir.glob("*.parquet"))
    
    print("🔍 FERTILIZER DATA HARMONIZATION ANALYSIS")
    print("=" * 80)
    
    # Group files by type
    efterafgroeder_files = [f for f in parquet_files if "Efterafgrøder" in f.name]
    gkea_files = [f for f in parquet_files if f.name.startswith("GKEA")]
    goedning_files = [f for f in parquet_files if "Gødningsregnskaber" in f.name]
    
    print(f"\n📊 FILE CATEGORIES:")
    print(f"  • Efterafgrøder files: {len(efterafgroeder_files)}")
    print(f"  • GKEA files: {len(gkea_files)}")
    print(f"  • Gødningsregnskaber files: {len(goedning_files)}")
    
    print(f"\n🚨 MAJOR HARMONIZATION ISSUES IDENTIFIED:")
    
    # Issue 1: Efterafgrøder column naming inconsistency
    print(f"\n1. EFTERAFGRØDER COLUMN NAMING INCONSISTENCY:")
    print("-" * 50)
    for file_path in sorted(efterafgroeder_files):
        df = pd.read_parquet(file_path)
        year = file_path.name.split()[-1].split('.')[0]  # Extract year
        
        # Find the numbered columns (a18_, a19_, etc.)
        numbered_cols = [col for col in df.columns if col.startswith('a') and '_' in col]
        print(f"  {year}: {numbered_cols}")
    
    # Issue 2: GKEA files have generic column names
    print(f"\n2. GKEA FILES HAVE GENERIC COLUMN NAMES:")
    print("-" * 50)
    for file_path in sorted(gkea_files):
        df = pd.read_parquet(file_path)
        year = file_path.name.split('GKEA')[1][:4]  # Extract year
        
        generic_cols = [col for col in df.columns if col.startswith('column_')]
        print(f"  GKEA {year}: {len(generic_cols)} generic columns (column_1, column_2, etc.)")
        
        # Show actual header row if available
        if len(df) > 1:
            second_row = df.iloc[1].values
            meaningful_cols = [str(val) for val in second_row[:10] if str(val) not in ['', 'nan', None]]
            print(f"    → Actual headers: {meaningful_cols}")
    
    # Issue 3: Different schemas across years
    print(f"\n3. SCHEMA INCONSISTENCIES:")
    print("-" * 50)
    
    schemas = {}
    for file_path in sorted(parquet_files):
        df = pd.read_parquet(file_path)
        file_key = file_path.name.split('.')[0]
        schemas[file_key] = {
            'rows': df.shape[0],
            'columns': df.shape[1],
            'column_names': list(df.columns)
        }
    
    # Compare schemas within each category
    print("\nEFTERAFGRØDER schema comparison:")
    for file_path in sorted(efterafgroeder_files):
        df = pd.read_parquet(file_path)
        year = file_path.name.split()[-1].split('.')[0]
        print(f"  {year}: {df.shape[1]} columns, {df.shape[0]:,} rows")
    
    print("\nGKEA schema comparison:")
    for file_path in sorted(gkea_files):
        df = pd.read_parquet(file_path)
        year = file_path.name.split('GKEA')[1][:4]
        print(f"  {year}: {df.shape[1]} columns, {df.shape[0]:,} rows")
    
    # Issue 4: Data type inconsistencies
    print(f"\n4. DATA TYPE ANALYSIS:")
    print("-" * 50)
    
    # Check if numeric fields are stored as strings
    for file_path in efterafgroeder_files:
        df = pd.read_parquet(file_path)
        year = file_path.name.split()[-1].split('.')[0]
        
        # Check columns that should be numeric
        potential_numeric = [col for col in df.columns if any(word in col.lower() for word in ['areal', 'ha', 'omregnet'])]
        
        if potential_numeric:
            print(f"\n  {year} - Potential numeric columns stored as text:")
            for col in potential_numeric:
                sample_vals = df[col].dropna().unique()[:3]
                print(f"    {col}: {sample_vals} (dtype: {df[col].dtype})")

    return schemas

if __name__ == "__main__":
    analyze_column_inconsistencies()