#!/usr/bin/env python3
"""
Test script to process a VISA PDF and see what data gets extracted.

Usage:
    python test_visa_pdf.py path/to/visa.pdf
"""

import sys
from pathlib import Path

import pandas as pd

# Add the parent directory to sys.path to enable imports
parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

# Try to import the required modules with better error handling
try:
    import pdfplumber
    import tabula
    print("✅ PDF processing libraries available")
except ImportError as e:
    print(f"❌ Missing PDF processing libraries: {e}")
    print("Install with: uv pip install tabula-py pdfplumber")
    sys.exit(1)

def extract_tables_with_tabula(pdf_path: Path):
    """Extract tables using tabula-py."""
    print("🔍 Extracting tables with tabula...")
    try:
        # Try lattice mode first (good for tables with lines)
        tables = tabula.read_pdf(
            str(pdf_path),
            pages="all",
            multiple_tables=True,
            lattice=True,
        )
        
        if not tables or all(df.empty for df in tables):
            # Try stream mode (good for tables without clear borders)  
            tables = tabula.read_pdf(
                str(pdf_path),
                pages="all", 
                multiple_tables=True,
                stream=True,
            )
        
        # Filter out empty tables
        tables = [df for df in tables if not df.empty and df.shape[0] > 1]
        print(f"✅ Tabula found {len(tables)} tables")
        return tables
        
    except Exception as e:
        print(f"❌ Tabula extraction failed: {e}")
        return []

def extract_tables_with_pdfplumber(pdf_path: Path):
    """Extract tables using pdfplumber."""
    print("🔍 Extracting tables with pdfplumber...")
    tables = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                page_tables = page.extract_tables()
                if page_tables:
                    for table in page_tables:
                        if table and len(table) > 1:  # Skip empty tables
                            # Convert to DataFrame
                            df = pd.DataFrame(table[1:], columns=table[0] if table[0] else None)
                            if not df.empty:
                                df['page_number'] = page_num + 1
                                tables.append(df)
        
        print(f"✅ PDFplumber found {len(tables)} tables")
        return tables
        
    except Exception as e:
        print(f"❌ PDFplumber extraction failed: {e}")
        return []

def test_visa_pdf_extraction(pdf_path: Path) -> None:
    """Test PDF extraction on a VISA file."""
    
    print(f"🧪 Testing PDF extraction on: {pdf_path}")
    
    if not pdf_path.exists():
        print(f"❌ PDF file not found: {pdf_path}")
        return
    
    # Try multiple extraction methods
    all_tables = []
    
    # Method 1: Tabula
    tabula_tables = extract_tables_with_tabula(pdf_path)
    all_tables.extend(tabula_tables)
    
    # Method 2: PDFplumber (if tabula didn't find much)
    if len(all_tables) < 3:
        pdfplumber_tables = extract_tables_with_pdfplumber(pdf_path)
        all_tables.extend(pdfplumber_tables)
    
    if not all_tables:
        print("❌ No tables found in PDF")
        return
    
    print(f"\n🎉 Found {len(all_tables)} total tables")
    
    # Process each table
    for i, df in enumerate(all_tables):
        print(f"\n{'='*60}")
        print(f"📊 TABLE {i+1}")
        print(f"{'='*60}")
        print(f"📏 Shape: {df.shape} (rows x columns)")
        print(f"📋 Columns: {list(df.columns)}")
        
        # Show first few rows
        print("\n📄 First 5 rows:")
        print(df.head().to_string())
        
        # Look for VISA-related columns
        visa_columns = []
        for col in df.columns:
            if col is None:
                continue
            col_lower = str(col).lower()
            if any(term in col_lower for term in ['year', 'år', 'nationality', 'nationalitet', 'permits', 'tilladelse', 'count', 'antal', 'land', 'country']):
                visa_columns.append(col)
        
        if visa_columns:
            print(f"\n🎯 Potential VISA columns found: {visa_columns}")
            print("\n📊 Sample data from VISA columns:")
            print(df[visa_columns].head().to_string())
        else:
            print("\n❓ No obvious VISA columns detected")
        
        print(f"\n{'='*60}\n")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_visa_pdf.py path/to/visa.pdf")
        sys.exit(1)
    
    pdf_path = Path(sys.argv[1])
    test_visa_pdf_extraction(pdf_path)