#!/usr/bin/env python3
"""
Simplified proximity test to isolate the DuckDB spatial join issue.
"""

from pathlib import Path

import duckdb


def test_simple_proximity() -> None:
    print("🧪 Testing simplified proximity analysis")

    # Setup paths
    data_dir = Path("data_local")
    silver_dir = data_dir / "silver"
    gold_dir = data_dir / "gold"

    # Initialize DuckDB
    conn = duckdb.connect()
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    print("✅ Spatial extensions loaded")

    try:
        # Load a small sample of data
        print("\n📥 Loading sample data...")

        # Load just 100 disaggregation records
        conn.execute(f"""
            CREATE TABLE sample_disaggregation AS 
            SELECT * FROM '{gold_dir / "pesticide_disaggregation_2021_2022.parquet"}'
            LIMIT 100
        """)

        # Load field data
        conn.execute(f"""
            CREATE TABLE sample_fields AS 
            SELECT * FROM '{silver_dir / "fvm_marker_2022.parquet"}'
            WHERE field_uuid IN (SELECT field_uuid FROM sample_disaggregation)
        """)

        # Load just 1000 buildings
        conn.execute(f"""
            CREATE TABLE sample_buildings AS 
            SELECT * FROM '{silver_dir / "joined_buildings.parquet"}'
            WHERE category_group = 'residential' AND address IS NOT NULL
            LIMIT 1000
        """)

        # Check counts
        disagg_count = conn.execute("SELECT COUNT(*) FROM sample_disaggregation").fetchone()[0]
        field_count = conn.execute("SELECT COUNT(*) FROM sample_fields").fetchone()[0]
        building_count = conn.execute("SELECT COUNT(*) FROM sample_buildings").fetchone()[0]

        print(f"✅ Sample disaggregation: {disagg_count} records")
        print(f"✅ Sample fields: {field_count} records")
        print(f"✅ Sample buildings: {building_count} records")

        if field_count == 0:
            print("❌ No matching fields found - cannot test proximity")
            return False

        # Test basic geometry operations first
        print("\n🧪 Testing basic geometry operations...")

        # Check if geometries are valid
        invalid_fields = conn.execute("""
            SELECT COUNT(*) FROM sample_fields WHERE geometry IS NULL
        """).fetchone()[0]

        invalid_buildings = conn.execute("""
            SELECT COUNT(*) FROM sample_buildings WHERE geometry IS NULL
        """).fetchone()[0]

        print(f"Fields with NULL geometry: {invalid_fields}")
        print(f"Buildings with NULL geometry: {invalid_buildings}")

        # Test coordinate transformation
        print("\n🧪 Testing coordinate transformation...")
        conn.execute("""
            CREATE TABLE fields_utm AS
            SELECT 
                field_uuid,
                ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832') as geom_utm
            FROM sample_fields 
            WHERE geometry IS NOT NULL
        """)

        utm_count = conn.execute("SELECT COUNT(*) FROM fields_utm").fetchone()[0]
        print(f"✅ Transformed {utm_count} field geometries to UTM")

        # Test simple distance calculation (no join)
        print("\n🧪 Testing simple distance calculation...")
        result = conn.execute("""
            SELECT COUNT(*) as test_count
            FROM fields_utm f, sample_buildings b
            WHERE ST_DWithin(
                f.geom_utm,
                ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832'),
                100.0
            )
            LIMIT 10
        """).fetchone()[0]

        print(f"✅ Found {result} field-building pairs within 100m (limited to 10)")

        # Now test the actual LEFT JOIN that was failing
        print("\n🧪 Testing LEFT JOIN proximity...")
        conn.execute("""
            CREATE TABLE proximity_test AS
            SELECT 
                f.field_uuid,
                COUNT(b.address) as nearby_buildings
            FROM fields_utm f
            LEFT JOIN sample_buildings b ON ST_DWithin(
                f.geom_utm,
                ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832'),
                100.0
            )
            GROUP BY f.field_uuid
        """)

        prox_count = conn.execute("SELECT COUNT(*) FROM proximity_test").fetchone()[0]
        with_buildings = conn.execute("SELECT COUNT(*) FROM proximity_test WHERE nearby_buildings > 0").fetchone()[0]

        print(f"✅ Proximity analysis completed: {prox_count} fields analyzed")
        print(f"   {with_buildings} fields have nearby residential buildings")

        # Show some results
        sample_results = conn.execute("""
            SELECT field_uuid, nearby_buildings 
            FROM proximity_test 
            WHERE nearby_buildings > 0 
            ORDER BY nearby_buildings DESC 
            LIMIT 5
        """).fetchall()

        if sample_results:
            print("\n📋 Sample results:")
            for field_uuid, count in sample_results:
                print(f"   Field {field_uuid}: {count} buildings within 100m")

        print("\n✅ Simplified proximity test completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        conn.close()


if __name__ == "__main__":
    success = test_simple_proximity()
    if success:
        print("\n🎉 Simple proximity test passed!")
    else:
        print("\n💥 Simple proximity test failed!")
