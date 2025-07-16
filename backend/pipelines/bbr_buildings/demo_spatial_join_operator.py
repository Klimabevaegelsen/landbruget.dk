#!/usr/bin/env python3
"""
Demonstration of DuckDB Spatial v1.2.2 SPATIAL_JOIN Operator

This script demonstrates how to structure queries to leverage the new SPATIAL_JOIN
operator for massive performance improvements in spatial operations.

Reference: https://github.com/duckdb/duckdb-spatial/pull/545

Key Benefits:
- Creates temporary spatial index on-the-fly
- Uses bounding box intersection for fast filtering
- Only evaluates expensive spatial predicates on potential matches
- Supports: ST_Intersects, ST_Contains, ST_Within, ST_Touches, etc.

Limitations:
- Single spatial join condition only
- INNER/LEFT/RIGHT/OUTER joins supported (not SEMI/ANTI)
- Build side must fit in memory
"""

import duckdb


def create_sample_data():
    """Create sample spatial data for demonstration."""
    print("🏗️  Creating sample spatial data...")

    conn = duckdb.connect()
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")

    # Create sample buildings (points)
    conn.execute("""
        CREATE TABLE buildings AS
        SELECT
            'building_' || row_number() OVER () as building_id,
            ST_Point(x, y) as geometry,
            CASE 
                WHEN random() < 0.3 THEN 'agricultural'
                WHEN random() < 0.7 THEN 'residential' 
                ELSE 'commercial'
            END as building_type,
            random() * 1000 + 50 as building_area_m2
        FROM
            generate_series(0, 1000, 10) r1(x),
            generate_series(0, 1000, 10) r2(y)
        WHERE random() < 0.3  -- Sparse distribution
    """)

    # Create sample agricultural fields (polygons)
    conn.execute("""
        CREATE TABLE agricultural_fields AS
        SELECT
            'field_' || row_number() OVER () as field_id,
            ST_Buffer(ST_Point(x, y), 50) as geometry,
            random() * 10000 + 1000 as field_area_m2,
            CASE 
                WHEN random() < 0.5 THEN 'crop_production'
                ELSE 'livestock'
            END as field_type
        FROM
            generate_series(25, 975, 100) r1(x),
            generate_series(25, 975, 100) r2(y)
    """)

    buildings_count = conn.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]
    fields_count = conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]

    print("✅ Created sample data:")
    print(f"   Buildings: {buildings_count:,}")
    print(f"   Agricultural fields: {fields_count:,}")

    return conn


def demonstrate_spatial_join_operator():
    """Demonstrate the SPATIAL_JOIN operator with proper query structure."""
    print("\n🚀 Demonstrating SPATIAL_JOIN operator...")

    conn = create_sample_data()

    # Query that will trigger SPATIAL_JOIN operator
    spatial_join_query = """
    SELECT 
        b.building_id,
        b.building_type,
        b.building_area_m2,
        f.field_id,
        f.field_type,
        f.field_area_m2,
        ST_Area_Spheroid(ST_Intersection(b.geometry, f.geometry)) as intersection_area_m2
    FROM buildings b
    INNER JOIN agricultural_fields f ON ST_Intersects(b.geometry, f.geometry)
    WHERE ST_IsValid(b.geometry) 
    AND ST_IsValid(f.geometry)
    """

    # Check if SPATIAL_JOIN operator is used
    print("🔍 Checking query plan for SPATIAL_JOIN operator...")
    explain_result = conn.execute(f"EXPLAIN {spatial_join_query}").fetchall()

    spatial_join_used = any("SPATIAL_JOIN" in str(row) for row in explain_result)

    if spatial_join_used:
        print("✅ SPATIAL_JOIN operator detected in query plan!")
        print("🎯 Query is optimized for maximum spatial performance")

        # Show the query plan
        print("\n📋 Query Plan:")
        for row in explain_result:
            if "SPATIAL_JOIN" in str(row):
                print(f"   🔥 {row[0]}")
            else:
                print(f"      {row[0]}")
    else:
        print("❌ SPATIAL_JOIN operator NOT used")
        print("💡 Query may be using standard JOIN with spatial predicate")

    # Execute the query and show results
    print("\n⚡ Executing spatial join...")
    result = conn.execute(spatial_join_query).fetchall()

    print(f"✅ Found {len(result):,} building-field intersections")

    if result:
        print("\n📊 Sample results:")
        for i, row in enumerate(result[:5]):  # Show first 5 results
            (
                building_id,
                building_type,
                building_area,
                field_id,
                field_type,
                field_area,
                intersection_area,
            ) = row
            print(
                f"   {i + 1}. {building_id} ({building_type}) intersects {field_id} ({field_type})"
            )
            print(f"      Intersection area: {intersection_area:.1f} m²")

    conn.close()
    return spatial_join_used


def demonstrate_query_patterns():
    """Show different query patterns that trigger SPATIAL_JOIN operator."""
    print("\n🌟 Demonstrating different SPATIAL_JOIN patterns...")

    conn = create_sample_data()

    # Test different spatial predicates
    spatial_predicates = [
        ("ST_Intersects", "Checks if geometries intersect"),
        ("ST_Contains", "Checks if first geometry contains second"),
        ("ST_Within", "Checks if first geometry is within second"),
        ("ST_Touches", "Checks if geometries touch at boundary"),
        ("ST_Overlaps", "Checks if geometries overlap"),
    ]

    for predicate, description in spatial_predicates:
        print(f"\n🔍 Testing {predicate}: {description}")

        query = f"""
        EXPLAIN SELECT COUNT(*) 
        FROM buildings b
        INNER JOIN agricultural_fields f ON {predicate}(b.geometry, f.geometry)
        """

        try:
            explain_result = conn.execute(query).fetchall()
            spatial_join_used = any("SPATIAL_JOIN" in str(row) for row in explain_result)

            if spatial_join_used:
                print(f"   ✅ {predicate} triggers SPATIAL_JOIN operator")
            else:
                print(f"   ❌ {predicate} does NOT trigger SPATIAL_JOIN operator")

        except Exception as e:
            print(f"   ⚠️  {predicate} failed: {e}")

    conn.close()


def demonstrate_performance_comparison():
    """Compare performance with and without SPATIAL_JOIN operator."""
    print("\n📈 Performance comparison demonstration...")

    conn = create_sample_data()

    # Create larger dataset for meaningful comparison
    print("🏗️  Creating larger dataset for performance test...")
    conn.execute("""
        CREATE TABLE large_buildings AS
        SELECT
            'building_' || row_number() OVER () as building_id,
            ST_Point(x + random() * 10, y + random() * 10) as geometry,
            'test_building' as building_type
        FROM
            generate_series(0, 500, 5) r1(x),
            generate_series(0, 500, 5) r2(y)
    """)

    large_buildings_count = conn.execute("SELECT COUNT(*) FROM large_buildings").fetchone()[0]
    fields_count = conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]

    print("📊 Performance test data:")
    print(f"   Buildings: {large_buildings_count:,}")
    print(f"   Fields: {fields_count:,}")
    print(f"   Potential comparisons: {large_buildings_count * fields_count:,}")

    # Test with optimizers enabled (default behavior)
    print("\n⚡ Testing with DuckDB optimizers (default)...")

    import time

    start_time = time.time()

    result_with_spatial_join = conn.execute("""
        SELECT COUNT(*) 
        FROM large_buildings b
        INNER JOIN agricultural_fields f ON ST_Intersects(b.geometry, f.geometry)
    """).fetchone()[0]

    spatial_join_time = time.time() - start_time

    print(
        f"✅ SPATIAL_JOIN result: {result_with_spatial_join:,} intersections in {spatial_join_time:.2f}s"
    )

    # Note: We can't easily disable the SPATIAL_JOIN operator once it's available
    # This is just a demonstration of the approach

    conn.close()


def main():
    """Run all demonstrations."""
    print("🎯 DuckDB Spatial v1.2.2 SPATIAL_JOIN Operator Demonstration")
    print("=" * 60)

    try:
        # Test 1: Basic SPATIAL_JOIN operator usage
        spatial_join_detected = demonstrate_spatial_join_operator()

        # Test 2: Different spatial predicates
        demonstrate_query_patterns()

        # Test 3: Performance comparison
        demonstrate_performance_comparison()

        # Summary
        print("\n🎉 Demonstration completed!")
        print(f"✅ SPATIAL_JOIN operator available: {'YES' if spatial_join_detected else 'NO'}")

        print("\n📋 Key takeaways for BBR buildings pipeline:")
        print("   🔥 Use INNER JOIN with spatial predicates for SPATIAL_JOIN operator")
        print("   📊 Single spatial condition only (ST_Intersects, ST_Contains, etc.)")
        print("   🎯 Massive performance improvements for large spatial datasets")
        print("   💾 Build side must fit in memory (typically not an issue)")
        print("   🔗 Perfect for buildings × fields, buildings × parcels joins")

        print("\n📖 Reference: https://github.com/duckdb/duckdb-spatial/pull/545")

    except Exception as e:
        print(f"❌ Demonstration failed: {e}")
        print("💡 Make sure DuckDB Spatial v1.2.2+ is installed")


if __name__ == "__main__":
    main()
