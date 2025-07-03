#!/usr/bin/env python3
"""
Test script to verify coordinate fix for H3 polygon conversion with actual FVM data
"""

import sys

import duckdb


def test_coordinate_fix():
    """Test if coordinate fix resolves H3 conversion issues with real FVM data"""

    # Initialize DuckDB with extensions
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    conn.execute("INSTALL h3 FROM community")
    conn.execute("LOAD h3")

    # Load actual FVM data
    print("📁 Loading FVM data from /tmp/fvm_2022.parquet")
    conn.execute("""
        CREATE TABLE fvm_sample AS 
        SELECT * FROM read_parquet('/tmp/fvm_2022.parquet')
        LIMIT 10
    """)

    # First, let's examine the actual geometry structure
    print("\n🔍 Examining geometry structure:")
    geometry_info = conn.execute("""
        SELECT 
            cvr_number,
            block_id,
            field_id,
            LEFT(geometry_wkt, 100) as wkt_preview
        FROM fvm_sample
        WHERE geometry_wkt IS NOT NULL
        LIMIT 5
    """).fetchall()

    for row in geometry_info:
        print(f"  CVR {row[0]}, Block {row[1]}, Field {row[2]}:")
        print(f"    WKT preview: {row[3]}...")

    # Test original geometry (convert MULTIPOLYGON to POLYGON using regex)
    print("\n🔍 Testing original coordinate order:")
    result_original = conn.execute("""
        SELECT 
            cvr_number,
            block_id, 
            field_id,
            TRY_CAST(array_length(h3_polygon_wkt_to_cells(
                CASE 
                    WHEN geometry_wkt LIKE 'MULTIPOLYGON%'
                    THEN REGEXP_REPLACE(geometry_wkt, '^MULTIPOLYGON \\(\\((.*)\\)\\)$', 'POLYGON (\\1)')
                    ELSE geometry_wkt
                END, 10
            )) AS INTEGER) as h3_cell_count_original
        FROM fvm_sample
        WHERE geometry_wkt IS NOT NULL
        LIMIT 5
    """).fetchall()

    for row in result_original:
        count = row[3] if row[3] is not None else 0
        print(f"  CVR {row[0]}, Block {row[1]}, Field {row[2]}: {count} H3 cells")

    # Test with coordinate flip (convert MULTIPOLYGON to POLYGON and flip coordinates)
    print("\n🔄 Testing flipped coordinates:")
    result_flipped = conn.execute("""
        SELECT 
            cvr_number,
            block_id,
            field_id, 
            TRY_CAST(array_length(h3_polygon_wkt_to_cells(
                ST_AsText(ST_FlipCoordinates(ST_GeomFromText(
                    CASE 
                        WHEN geometry_wkt LIKE 'MULTIPOLYGON%'
                        THEN REGEXP_REPLACE(geometry_wkt, '^MULTIPOLYGON \\(\\((.*)\\)\\)$', 'POLYGON (\\1)')
                        ELSE geometry_wkt
                    END
                ))), 10
            )) AS INTEGER) as h3_cell_count_flipped
        FROM fvm_sample
        WHERE geometry_wkt IS NOT NULL
        LIMIT 5
    """).fetchall()

    for row in result_flipped:
        count = row[3] if row[3] is not None else 0
        print(f"  CVR {row[0]}, Block {row[1]}, Field {row[2]}: {count} H3 cells")

    # Compare results
    original_total = sum(row[3] for row in result_original if row[3] is not None)
    flipped_total = sum(row[3] for row in result_flipped if row[3] is not None)

    print("\n📊 Summary:")
    print(f"  Original coordinate order: {original_total} total H3 cells")
    print(f"  Flipped coordinate order: {flipped_total} total H3 cells")

    if flipped_total > original_total:
        print("\n✅ SUCCESS: Coordinate flip increases H3 cell generation!")
        print("   FVM data appears to be in LAT/LON format, H3 expects LON/LAT")
        return True
    elif original_total > flipped_total:
        print("\n⚠️  NOTICE: Original coordinates work better")
        print("   FVM data might already be in LON/LAT format")
        return True
    else:
        print("\n❌ INCONCLUSIVE: Both approaches return same results")
        return False


if __name__ == "__main__":
    success = test_coordinate_fix()
    sys.exit(0 if success else 1)
