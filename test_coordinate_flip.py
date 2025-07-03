#!/usr/bin/env python3
"""
Test script to verify ST_FlipCoordinates() fix for H3 polygon conversion
"""

import sys
from pathlib import Path

import duckdb


def test_coordinate_flip():
    """Test if ST_FlipCoordinates resolves H3 conversion issues"""

    # Initialize DuckDB with spatial extensions
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    conn.execute("INSTALL h3")
    conn.execute("LOAD h3")

    # Load sample FVM data from tmp directory
    fvm_file = Path("/tmp/fvm_2022.parquet")
    if not fvm_file.exists():
        print(f"❌ Sample data not found: {fvm_file}")
        return False

    print(f"📁 Loading sample data from {fvm_file}")

    # Load data and test coordinate flip
    conn.execute(f"""
        CREATE TABLE fvm_sample AS 
        SELECT * FROM read_parquet('{fvm_file}')
        LIMIT 5
    """)

    # Test original geometry (should fail)
    print("\n🔍 Testing original geometry (should return 0 cells):")
    result_original = conn.execute("""
        SELECT 
            cvr_number,
            block_id, 
            field_id,
            array_length(h3_polygon_wkt_to_cells(geometry_wkt, 10)) as h3_cell_count_original
        FROM fvm_sample
        WHERE geometry_wkt IS NOT NULL
        LIMIT 3
    """).fetchall()

    for row in result_original:
        print(f"  CVR {row[0]}, Block {row[1]}, Field {row[2]}: {row[3]} H3 cells")

    # Test flipped coordinates (should work)
    print("\n🔄 Testing flipped coordinates (should return >0 cells):")
    result_flipped = conn.execute("""
        SELECT 
            cvr_number,
            block_id,
            field_id, 
            array_length(h3_polygon_wkt_to_cells(ST_AsText(ST_FlipCoordinates(ST_GeomFromText(geometry_wkt))), 10)) as h3_cell_count_flipped
        FROM fvm_sample
        WHERE geometry_wkt IS NOT NULL
        LIMIT 3
    """).fetchall()

    for row in result_flipped:
        print(f"  CVR {row[0]}, Block {row[1]}, Field {row[2]}: {row[3]} H3 cells")

    # Test actual H3 cell IDs with flipped coordinates
    print("\n📍 Sample H3 cell IDs from flipped coordinates:")
    result_cells = conn.execute("""
        SELECT 
            cvr_number,
            block_id,
            field_id,
            h3_polygon_wkt_to_cells(ST_AsText(ST_FlipCoordinates(ST_GeomFromText(geometry_wkt))), 10) as h3_cells
        FROM fvm_sample
        WHERE geometry_wkt IS NOT NULL
        LIMIT 2
    """).fetchall()

    for row in result_cells:
        cells = row[3][:3] if len(row[3]) > 3 else row[3]  # Show first 3 cells
        print(f"  CVR {row[0]}, Block {row[1]}, Field {row[2]}: {cells}...")

    # Check if fix works
    any_cells_found = any(row[4] > 0 for row in result_flipped)

    if any_cells_found:
        print("\n✅ SUCCESS: ST_FlipCoordinates() fix works! H3 cells are now generated.")
        return True
    else:
        print("\n❌ FAILED: ST_FlipCoordinates() did not resolve the issue.")
        return False


if __name__ == "__main__":
    success = test_coordinate_flip()
    sys.exit(0 if success else 1)
