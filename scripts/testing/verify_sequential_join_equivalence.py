#!/usr/bin/env python3
"""
Verify that sequential 2-table joins produce the same results as 3-table joins.

This test ensures that the memory optimization doesn't change the analytical results.
"""

import sys

import duckdb


def test_join_equivalence():
    """Test that sequential joins produce same results as 3-table joins."""

    # Create test connection
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")

    print("🧪 Testing Sequential vs 3-Table Join Equivalence")
    print("=" * 60)

    # Create test data
    print("Creating test spatial data...")

    # Test fields (squares)
    conn.execute("""
        CREATE TABLE test_fields AS
        SELECT 
            'field_' || generate_series as field_id,
            'block_' || generate_series as block_id,
            ST_Buffer(ST_Point(generate_series, generate_series), 1) as geom
        FROM generate_series(1, 20)
    """)

    # Test wetlands (circles overlapping some fields)
    conn.execute("""
        CREATE TABLE test_wetlands AS
        SELECT 
            'wetland_' || generate_series as wetland_id,
            ST_Buffer(ST_Point(generate_series * 1.5, generate_series * 1.5), 0.8) as geom
        FROM generate_series(1, 10)
    """)

    # Test water projects (rectangles overlapping some wetlands)
    conn.execute("""
        CREATE TABLE test_water_projects AS
        SELECT 
            'water_' || generate_series as project_id,
            ST_Buffer(ST_Point(generate_series * 2, generate_series * 2), 0.6) as geom
        FROM generate_series(1, 8)
    """)

    print("✅ Test data created")

    # Method 1: Original 3-table join (memory intensive)
    print("\n🔍 Testing original 3-table join...")

    conn.execute("""
        CREATE TABLE original_3table_result AS
        SELECT 
            f.field_id,
            f.block_id,
            ST_Area(ST_Intersection(ST_Intersection(f.geom, w.geom), wp.geom)) / ST_Area(f.geom) * 100 as wetland_water_projects_share
        FROM test_fields f
        JOIN test_wetlands w ON ST_Intersects(f.geom, w.geom)
        JOIN test_water_projects wp ON ST_Intersects(f.geom, wp.geom)
        WHERE ST_Area(ST_Intersection(ST_Intersection(f.geom, w.geom), wp.geom)) / ST_Area(f.geom) > 0.01
    """)

    original_count = conn.execute("SELECT COUNT(*) FROM original_3table_result").fetchone()[0]
    print(f"✅ Original 3-table join: {original_count} results")

    # Method 2: Sequential 2-table joins (memory efficient)
    print("\n🔍 Testing sequential 2-table joins...")

    # Step 1: Find field-wetland intersections
    conn.execute("""
        CREATE TABLE fields_with_wetlands AS
        SELECT 
            f.field_id,
            f.block_id,
            f.geom as field_geom,
            ST_Intersection(f.geom, w.geom) as wetland_intersection_geom
        FROM test_fields f
        JOIN test_wetlands w ON ST_Intersects(f.geom, w.geom)
    """)

    # Step 2: Find wetland-water project overlaps
    conn.execute("""
        CREATE TABLE sequential_2table_result AS
        SELECT 
            fw.field_id,
            fw.block_id,
            ST_Area(ST_Intersection(fw.wetland_intersection_geom, wp.geom)) / ST_Area(fw.field_geom) * 100 as wetland_water_projects_share
        FROM fields_with_wetlands fw
        JOIN test_water_projects wp ON ST_Intersects(fw.wetland_intersection_geom, wp.geom)
        WHERE ST_Area(ST_Intersection(fw.wetland_intersection_geom, wp.geom)) / ST_Area(fw.field_geom) > 0.01
    """)

    sequential_count = conn.execute("SELECT COUNT(*) FROM sequential_2table_result").fetchone()[0]
    print(f"✅ Sequential 2-table joins: {sequential_count} results")

    # Compare results
    print("\n🔍 Comparing results...")

    if original_count == sequential_count:
        print("✅ Row counts match!")
    else:
        print(f"❌ Row counts differ: {original_count} vs {sequential_count}")
        return False

    # Compare actual values (allowing for small floating point differences)
    conn.execute("""
        CREATE TABLE comparison AS
        SELECT 
            o.field_id,
            o.block_id,
            o.wetland_water_projects_share as original_share,
            s.wetland_water_projects_share as sequential_share,
            ABS(o.wetland_water_projects_share - s.wetland_water_projects_share) as difference
        FROM original_3table_result o
        FULL OUTER JOIN sequential_2table_result s 
            ON o.field_id = s.field_id AND o.block_id = s.block_id
    """)

    # Check for missing results
    missing_in_sequential = conn.execute("""
        SELECT COUNT(*) FROM comparison 
        WHERE sequential_share IS NULL
    """).fetchone()[0]

    missing_in_original = conn.execute("""
        SELECT COUNT(*) FROM comparison 
        WHERE original_share IS NULL
    """).fetchone()[0]

    if missing_in_sequential > 0:
        print(f"❌ {missing_in_sequential} results missing in sequential approach")
        return False

    if missing_in_original > 0:
        print(f"❌ {missing_in_original} results missing in original approach")
        return False

    # Check for significant differences in values
    max_difference = conn.execute("""
        SELECT MAX(difference) FROM comparison 
        WHERE original_share IS NOT NULL AND sequential_share IS NOT NULL
    """).fetchone()[0]

    if max_difference is None:
        max_difference = 0

    print(f"✅ Maximum difference in values: {max_difference:.6f}%")

    if max_difference < 0.001:  # Less than 0.001% difference
        print("✅ Values match within acceptable tolerance!")
        return True
    else:
        print(f"❌ Values differ by more than acceptable tolerance: {max_difference:.6f}%")
        return False


def test_spatial_join_operator():
    """Test that SPATIAL_JOIN operator is being used."""

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")

    print("\n🧪 Testing SPATIAL_JOIN Operator Usage")
    print("=" * 60)

    # Create simple test data
    conn.execute("""
        CREATE TABLE simple_fields AS
        SELECT 
            generate_series as id,
            ST_Point(generate_series, generate_series) as geom
        FROM generate_series(1, 10)
    """)

    conn.execute("""
        CREATE TABLE simple_areas AS
        SELECT 
            generate_series as id,
            ST_Buffer(ST_Point(generate_series * 0.5, generate_series * 0.5), 1.5) as geom
        FROM generate_series(1, 5)
    """)

    # Test with EXPLAIN
    result = conn.execute("""
        EXPLAIN SELECT 
            f.id as field_id,
            a.id as area_id
        FROM simple_fields f
        JOIN simple_areas a ON ST_Intersects(f.geom, a.geom)
    """).fetchall()

    explain_text = "\n".join([str(row) for row in result])

    if "SPATIAL_JOIN" in explain_text:
        print("✅ SPATIAL_JOIN operator is being used")
        return True
    else:
        print("⚠️ SPATIAL_JOIN operator not detected")
        print("Query plan:")
        for row in result:
            print(f"  {row}")
        return False


if __name__ == "__main__":
    print("🚀 Starting Join Equivalence Tests")
    print("=" * 80)

    # Test 1: Spatial join operator
    spatial_join_ok = test_spatial_join_operator()

    # Test 2: Join equivalence
    equivalence_ok = test_join_equivalence()

    print("\n" + "=" * 80)
    print("📊 Test Results Summary:")
    print(f"  SPATIAL_JOIN operator: {'✅ PASS' if spatial_join_ok else '❌ FAIL'}")
    print(f"  Join equivalence: {'✅ PASS' if equivalence_ok else '❌ FAIL'}")

    if spatial_join_ok and equivalence_ok:
        print("\n🎉 All tests passed! Sequential joins are equivalent and optimized.")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed. Please review the results.")
        sys.exit(1)
