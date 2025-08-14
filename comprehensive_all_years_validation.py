#!/usr/bin/env python3
"""
Comprehensive validation of ALL fertiliser files across ALL years.
"""

import pandas as pd
from pathlib import Path
import re

def comprehensive_validation():
    """Comprehensive validation of every single fertiliser file."""
    
    print("🔍 COMPREHENSIVE ALL-YEARS VALIDATION")
    print("=" * 80)
    
    fertiliser_dir = Path("data/fertiliser")
    
    # Get ALL files
    all_files = list(fertiliser_dir.glob("*.parquet"))
    
    print(f"Total files found: {len(all_files)}")
    for f in sorted(all_files):
        print(f"  - {f.name}")
    
    print(f"\n🧪 TESTING ALL 5 GKEA FILES INDIVIDUALLY")
    print("-" * 60)
    
    # Test ALL 5 GKEA files
    gkea_files = [f for f in all_files if f.name.startswith('GKEA')]
    
    for file_path in sorted(gkea_files):
        print(f"\n📄 {file_path.name}")
        print("=" * 50)
        
        df = pd.read_parquet(file_path)
        
        # Show file structure
        print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        
        # Get headers (row 1, index 1)
        if len(df) > 1:
            headers = df.iloc[1].values
            print("Headers in row 2:")
            for i, header in enumerate(headers[:15]):  # Show first 15
                col_name = df.columns[i]
                print(f"  {col_name}: {header}")
        
        # Test my proposed mappings
        print("\n🔬 Testing My Proposed Mappings:")
        
        # Get sample data (skip first 2 rows)
        if len(df) > 2:
            sample_data = df.iloc[2:7]  # 5 sample rows
            
            # Test journal column (first column should be journal)
            first_col = df.columns[0]
            journal_samples = sample_data[first_col].tolist()
            print(f"  Journal ({first_col}): {journal_samples}")
            
            # Test expected journal pattern
            journal_pattern_valid = any(re.match(r'\d{2}-\d{6,7}', str(val)) for val in journal_samples)
            print(f"    Valid journal pattern: {journal_pattern_valid}")
            
            # Test CVR (column_1)
            if 'column_1' in df.columns:
                cvr_samples = sample_data['column_1'].tolist()
                print(f"  CVR (column_1): {cvr_samples}")
                
                cvr_valid = all(str(val).isdigit() and len(str(val)) == 8 for val in cvr_samples if val)
                print(f"    Valid CVR format: {cvr_valid}")
            
            # Test areas and field numbers - need to identify correct columns
            print(f"  🔍 Looking for area and field number patterns:")
            
            for col in df.columns[1:10]:  # Check columns 1-10
                values = sample_data[col].tolist()
                
                # Check if looks like area (small decimal numbers)
                try:
                    numeric_vals = [float(str(v).replace(',', '.')) for v in values if v and str(v) != '']
                    if numeric_vals:
                        avg_val = sum(numeric_vals) / len(numeric_vals)
                        if 0.1 <= avg_val <= 100:  # Reasonable hectare range
                            print(f"    {col} (AREA candidate): {values} (avg: {avg_val:.2f})")
                except:
                    pass
                
                # Check if looks like field numbers
                field_pattern = any(re.match(r'\d+(-\d+)?$', str(v)) for v in values if v)
                if field_pattern and not all(str(v).replace('.','').replace(',','').isdigit() for v in values if v):
                    print(f"    {col} (FIELD candidate): {values}")
    
    print(f"\n🧪 DEEP DIVE: GØDNINGSREGNSKABER FILES")
    print("-" * 60)
    
    goedning_files = [f for f in all_files if 'Gødningsregnskaber' in f.name]
    
    for file_path in sorted(goedning_files):
        print(f"\n📄 {file_path.name}")
        print("=" * 40)
        
        df = pd.read_parquet(file_path)
        
        print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        
        # Show ALL column names for detailed analysis
        print("All columns:")
        for i, col in enumerate(df.columns):
            print(f"  {i+1:3}. {col}")
            if i >= 30:  # Limit output
                print(f"  ... and {len(df.columns)-31} more columns")
                break
        
        # Check key columns availability
        key_cols = ['cvr_number', 'kommune', 'vir_navn', 'planaar', 'f_planaar']
        available_keys = [col for col in key_cols if col in df.columns]
        print(f"\nKey columns available: {available_keys}")
        
        # Sample data for key columns
        if available_keys:
            print("Sample data:")
            sample = df[available_keys].head(3)
            for col in available_keys:
                values = sample[col].tolist()
                print(f"  {col}: {values}")
        
        # Check for year information
        year_cols = [col for col in df.columns if 'aar' in col.lower() or 'year' in col.lower()]
        if year_cols:
            print(f"\nYear-related columns: {year_cols}")
            for col in year_cols[:3]:  # Show first 3
                sample_vals = df[col].dropna().unique()[:5]
                print(f"  {col}: {list(sample_vals)}")
    
    print(f"\n🧪 EFTERAFGRØDER VERIFICATION (ALL 4 YEARS)")
    print("-" * 60)
    
    efteraf_files = [f for f in all_files if 'Efterafgrøder' in f.name]
    
    for file_path in sorted(efteraf_files):
        year = file_path.name.split()[-1].split('.')[0] if ' ' in file_path.name else 'Unknown'
        print(f"\n📄 {year}: {file_path.name}")
        
        df = pd.read_parquet(file_path)
        print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        
        # Test numeric columns
        numeric_cols = [col for col in df.columns if any(x in col for x in ['areal', 'ha', 'omregnet', 'faktisk'])]
        print(f"Area-related columns: {numeric_cols}")
        
        for col in numeric_cols:
            sample_vals = df[col].head(3).tolist()
            print(f"  {col}: {sample_vals}")
            
            # Test numeric conversion
            try:
                numeric_vals = [float(str(v).replace(',', '.')) for v in sample_vals if v]
                avg_area = sum(numeric_vals) / len(numeric_vals) if numeric_vals else 0
                print(f"    Numeric avg: {avg_area:.2f} ha")
            except Exception as e:
                print(f"    Numeric conversion failed: {e}")

if __name__ == "__main__":
    comprehensive_validation()