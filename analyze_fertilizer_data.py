#!/usr/bin/env python3
"""
Analyze fertilizer parquet files to understand their structure and identify harmonization needs.
"""

from pathlib import Path

import pandas as pd


def analyze_parquet_files():
    """Analyze all parquet files in the fertiliser directory."""
    
    fertiliser_dir = Path("data/fertiliser")
    
    if not fertiliser_dir.exists():
        print("Fertiliser directory not found!")
        return
    
    parquet_files = list(fertiliser_dir.glob("*.parquet"))
    
    print(f"Found {len(parquet_files)} parquet files:")
    print("-" * 80)
    
    for file_path in sorted(parquet_files):
        print(f"\n📁 File: {file_path.name}")
        print("=" * 60)
        
        try:
            # Read parquet file
            df = pd.read_parquet(file_path)
            
            print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
            print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
            
            print("\nColumns:")
            for i, col in enumerate(df.columns):
                dtype = str(df[col].dtype)
                null_count = df[col].isnull().sum()
                null_pct = (null_count / len(df) * 100) if len(df) > 0 else 0
                print(f"  {i+1:2}. {col:<40} ({dtype:<12}) - {null_count:,} nulls ({null_pct:.1f}%)")
            
            # Show sample data
            print("\nFirst 3 rows:")
            print(df.head(3).to_string())
            
            # Check for duplicates
            duplicates = df.duplicated().sum()
            if duplicates > 0:
                print(f"\n⚠️  Found {duplicates:,} duplicate rows")
            
            # Check for common date/time columns
            date_cols = [
                col for col in df.columns
                if any(word in col.lower() for word in ['date', 'tid', 'år', 'year'])
            ]
            if date_cols:
                print(f"\nDate-related columns: {date_cols}")
                for col in date_cols[:3]:  # Show first 3
                    unique_vals = df[col].nunique()
                    sample_vals = df[col].dropna().unique()[:5]
                    print(f"  {col}: {unique_vals} unique values, sample: {list(sample_vals)}")
            
        except Exception as e:
            print(f"❌ Error reading {file_path.name}: {e}")
        
        print("\n" + "-" * 80)

if __name__ == "__main__":
    analyze_parquet_files()