#!/usr/bin/env python3
"""
Correct GKEA mapping analysis based on actual header row inspection.
"""

import pandas as pd
from pathlib import Path

def analyze_correct_gkea_mappings():
    """Analyze GKEA files by looking at actual headers and matching to data."""
    
    print("🔧 CORRECT GKEA MAPPING ANALYSIS")
    print("=" * 80)
    
    fertiliser_dir = Path("data/fertiliser")
    
    gkea_files = [
        'GKEA2021_Markplan_med_Gødningsoplysninger.parquet',
        'GKEA2022_Markplan_med_Gødningsoplysninger.parquet', 
        'GKEA2023_Markplan_med_Gødningsoplysninger_Aktindsigt.parquet',
        'GKEA2024_Markplan_med_Gødningsoplysninger.parquet'
    ]
    
    for filename in gkea_files:
        file_path = fertiliser_dir / filename
        if not file_path.exists():
            continue
            
        print(f"\n📄 {filename}")
        print("=" * 60)
        
        df = pd.read_parquet(file_path)
        
        # Get the header row (row index 1)
        headers = df.iloc[1].values
        
        # Get some sample data rows (skip first 2 rows)
        data_samples = df.iloc[2:5]
        
        print("📋 HEADER → COLUMN MAPPING:")
        print("Column".ljust(12), "Header".ljust(25), "Sample Data")
        print("-" * 70)
        
        for i, (col_name, header_text) in enumerate(zip(df.columns, headers)):
            if i >= 20:  # Limit to first 20 columns for readability
                break
                
            sample_values = data_samples[col_name].dropna().tolist()[:3]
            sample_str = str(sample_values)[:40] + "..." if len(str(sample_values)) > 40 else str(sample_values)
            
            print(f"{col_name.ljust(12)} {str(header_text).ljust(25)} {sample_str}")
        
        print(f"\n🎯 KEY FIELD IDENTIFICATION:")
        
        # Find Journal Nummer column
        journal_col = None
        cvr_col = None
        marknummer_col = None
        areal_col = None
        
        for i, header_text in enumerate(headers):
            header_str = str(header_text).strip().lower()
            col_name = df.columns[i]
            
            if 'journal' in header_str:
                journal_col = col_name
                sample_data = data_samples[col_name].tolist()
                print(f"  Journal Nummer: {col_name} → {sample_data}")
                
            elif header_str == 'cvr':
                cvr_col = col_name
                sample_data = data_samples[col_name].tolist()
                print(f"  CVR: {col_name} → {sample_data}")
                
            elif 'marknummer' in header_str:
                marknummer_col = col_name
                sample_data = data_samples[col_name].tolist()
                print(f"  Marknummer: {col_name} → {sample_data}")
                
            elif header_str == 'areal':
                areal_col = col_name
                sample_data = data_samples[col_name].tolist()
                print(f"  Areal: {col_name} → {sample_data}")
        
        print(f"\n✅ CORRECT MAPPING FOR {filename}:")
        if journal_col:
            print(f"  '{journal_col}': 'journal_nummer'")
        if cvr_col:
            print(f"  '{cvr_col}': 'cvr_number'")
        if marknummer_col:
            print(f"  '{marknummer_col}': 'marknummer'")
        if areal_col:
            print(f"  '{areal_col}': 'areal_ha'")

if __name__ == "__main__":
    analyze_correct_gkea_mappings()