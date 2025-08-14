#!/usr/bin/env python3
"""
Deep validation to check if data values actually match the column headers.
This validates that our mappings are correct by examining actual data content.
"""

import pandas as pd
from pathlib import Path
import re

def deep_validate_column_mappings():
    """Deep validate that data values match what headers claim."""
    
    print("🔍 DEEP COLUMN VALIDATION - DATA vs HEADERS")
    print("=" * 80)
    
    fertiliser_dir = Path("data/fertiliser")
    
    print("\n🧪 GKEA FILES - VALIDATING HEADER vs DATA MAPPING")
    print("-" * 60)
    
    # Test our GKEA mappings by checking actual data content
    gkea_mappings = {
        'GKEA2021_Markplan_med_Gødningsoplysninger': {
            'column_1': 'journal_nummer',    # Should be like "21-0002242"
            'column_2': 'cvr_number',        # Should be 8-digit numbers
            'column_6': 'marknummer',        # Should be like "2-0", "3-0"
            'column_7': 'areal_ha',          # Should be numeric areas
            'column_15': 'hovedafgroede',    # Should be crop codes like "252"
            'column_19': 'fosfortal',        # Should be numeric phosphorus values
        },
        'GKEA2022_Markplan_med_Gødningsoplysninger': {
            'column_1': 'journal_nummer',    # Should be like "22-0096499"
            'column_2': 'cvr_number',        # Should be 8-digit numbers
            'column_4': 'marknummer',        # Should be like "1-0", "1-1"
            'column_5': 'areal_ha',          # Should be numeric areas
            'column_12': 'hovedafgroede',    # Should be crop codes
            'column_17': 'fosfortal',        # Should be numeric (my correction)
        },
        'GKEA2023_Markplan_med_Gødningsoplysninger_Aktindsigt': {
            'column_1': 'journal_nummer',    # Should be like "23-0045406"
            'column_2': 'cvr_number',        # Should be 8-digit numbers
            'column_4': 'marknummer',        # Should be like "1", "2", "3"
            'column_5': 'areal_ha',          # Should be numeric areas
            'column_11': 'hovedafgroede',    # Should be crop codes
            'column_15': 'fosfortal',        # Should be numeric (my correction)
        }
    }
    
    for filename, expected_mapping in gkea_mappings.items():
        file_path = fertiliser_dir / f"{filename}.parquet"
        if not file_path.exists():
            continue
            
        print(f"\n📄 {filename}")
        print("-" * 40)
        
        df = pd.read_parquet(file_path)
        
        # Skip header rows - start from row 3 (index 2)
        data_rows = df.iloc[2:7]  # Check first 5 data rows
        
        print("🔬 Validating each mapped column:")
        
        for column_name, expected_content in expected_mapping.items():
            print(f"\n  {column_name} → {expected_content}:")
            
            if column_name in df.columns:
                sample_values = data_rows[column_name].dropna().tolist()
                print(f"    Sample values: {sample_values}")
                
                # Validate data types and patterns
                if expected_content == 'journal_nummer':
                    # Should match pattern like "21-0002242", "22-0096499"
                    valid_pattern = any(re.match(r'\d{2}-\d{6,7}', str(val)) for val in sample_values if val)
                    print(f"    ✅ Journal pattern check: {valid_pattern}")
                    
                elif expected_content == 'cvr_number':
                    # Should be 8-digit numbers
                    valid_cvr = all(str(val).isdigit() and len(str(val)) == 8 for val in sample_values if val)
                    print(f"    ✅ CVR format check: {valid_cvr}")
                    
                elif expected_content == 'marknummer':
                    # Should be field numbers like "2-0", "1-1" or just "1", "2"
                    has_field_pattern = any(re.match(r'\d+(-\d+)?', str(val)) for val in sample_values if val)
                    print(f"    ✅ Field number pattern check: {has_field_pattern}")
                    
                elif expected_content == 'areal_ha':
                    # Should be numeric area values
                    try:
                        numeric_values = [float(str(val).replace(',', '.')) for val in sample_values if val and str(val) != '']
                        avg_area = sum(numeric_values) / len(numeric_values) if numeric_values else 0
                        reasonable_area = 0.1 <= avg_area <= 500  # Reasonable hectare range
                        print(f"    ✅ Numeric area check: {len(numeric_values) > 0}, avg: {avg_area:.2f} ha")
                        print(f"    ✅ Reasonable area range: {reasonable_area}")
                    except:
                        print(f"    ❌ Failed to parse as numeric")
                        
                elif expected_content == 'hovedafgroede':
                    # Should be crop codes (typically numeric)
                    crop_codes = [str(val) for val in sample_values if val and str(val) != '']
                    has_crop_codes = any(str(val).isdigit() for val in crop_codes)
                    print(f"    ✅ Has crop codes: {has_crop_codes}")
                    
                elif expected_content == 'fosfortal':
                    # Should be numeric phosphorus values
                    try:
                        numeric_values = [float(str(val).replace(',', '.')) for val in sample_values if val and str(val) != '']
                        has_numeric = len(numeric_values) > 0
                        avg_value = sum(numeric_values) / len(numeric_values) if numeric_values else 0
                        print(f"    ✅ Numeric phosphorus check: {has_numeric}, avg: {avg_value:.2f}")
                    except:
                        print(f"    ❌ Failed to parse as numeric")
            else:
                print(f"    ❌ Column {column_name} not found!")
    
    print("\n🧪 TESTING POTENTIAL WRONG MAPPINGS")
    print("-" * 60)
    
    # Test if I got fosfortal wrong in 2022 and 2023
    test_files = [
        ('GKEA2022_Markplan_med_Gødningsoplysninger', 'column_16', 'column_17'),
        ('GKEA2023_Markplan_med_Gødningsoplysninger_Aktindsigt', 'column_14', 'column_15')
    ]
    
    for filename, old_mapping, new_mapping in test_files:
        file_path = fertiliser_dir / f"{filename}.parquet"
        if not file_path.exists():
            continue
            
        print(f"\n📄 {filename} - Fosfortal column test:")
        
        df = pd.read_parquet(file_path)
        data_rows = df.iloc[2:10]  # More rows for better test
        
        print(f"  Testing {old_mapping} (old) vs {new_mapping} (new):")
        
        if old_mapping in df.columns:
            old_values = data_rows[old_mapping].dropna().tolist()
            print(f"    {old_mapping}: {old_values}")
            
            # Try to parse as numeric
            try:
                old_numeric = [float(str(val).replace(',', '.')) for val in old_values if val and str(val) != '']
                old_avg = sum(old_numeric) / len(old_numeric) if old_numeric else 0
                print(f"    {old_mapping} numeric avg: {old_avg:.2f}")
            except:
                print(f"    {old_mapping}: Not numeric")
        
        if new_mapping in df.columns:
            new_values = data_rows[new_mapping].dropna().tolist()
            print(f"    {new_mapping}: {new_values}")
            
            # Try to parse as numeric
            try:
                new_numeric = [float(str(val).replace(',', '.')) for val in new_values if val and str(val) != '']
                new_avg = sum(new_numeric) / len(new_numeric) if new_numeric else 0
                print(f"    {new_mapping} numeric avg: {new_avg:.2f}")
            except:
                print(f"    {new_mapping}: Not numeric")
    
    print("\n🧪 EFTERAFGRØDER VALIDATION")
    print("-" * 60)
    
    # Quick validation of Efterafgrøder mappings
    efteraf_files = ['Efterafgrøder 2020.parquet', 'Efterafgrøder 2022.parquet']
    
    for filename in efteraf_files:
        file_path = fertiliser_dir / filename
        if not file_path.exists():
            continue
            
        print(f"\n📄 {filename}")
        
        df = pd.read_parquet(file_path)
        sample_data = df.head(5)
        
        # Check if area columns contain reasonable values
        area_cols = [col for col in df.columns if 'areal' in col.lower() or 'ha' in col.lower()]
        print(f"  Area columns found: {area_cols}")
        
        for col in area_cols:
            values = sample_data[col].tolist()
            print(f"    {col}: {values}")
            
            # Try to convert to numeric
            try:
                numeric_vals = [float(str(val).replace(',', '.')) for val in values if val]
                avg_area = sum(numeric_vals) / len(numeric_vals) if numeric_vals else 0
                print(f"    {col} avg: {avg_area:.2f} ha")
            except:
                print(f"    {col}: Failed to parse as numeric")
    
    return True

if __name__ == "__main__":
    deep_validate_column_mappings()