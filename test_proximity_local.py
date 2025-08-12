#!/usr/bin/env python3
"""
Local test script for pesticide proximity analysis using downloaded data.
This avoids repeated GCS downloads during development.
"""

from pathlib import Path

import duckdb


def test_proximity_analysis():
    print("🧪 Testing pesticide proximity analysis with local data")

    # Setup paths
    data_dir = Path("data_local")
    silver_dir = data_dir / "silver"
    gold_dir = data_dir / "gold"

    # Check files exist
    required_files = [
        silver_dir / "pesticiddata_2021_2022.parquet",
        silver_dir / "fvm_marker_2022.parquet",
        silver_dir / "joined_buildings.parquet",
        silver_dir / "water_typology.parquet",
        gold_dir / "pesticide_disaggregation_2021_2022.parquet",
    ]

    for file_path in required_files:
        if not file_path.exists():
            print(f"❌ Missing file: {file_path}")
            return False
        print(f"✅ Found: {file_path} ({file_path.stat().st_size / 1024 / 1024:.1f}MB)")

    # Initialize DuckDB
    print("\n🔧 Setting up DuckDB...")
    conn = duckdb.connect()
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    print("✅ Spatial extensions loaded")

    # Load data
    print("\n📥 Loading data into DuckDB...")

    # Load disaggregation data
    conn.execute(f"""
        CREATE TABLE current_disaggregation AS 
        SELECT * FROM '{gold_dir / "pesticide_disaggregation_2021_2022.parquet"}'
    """)
    disagg_count = conn.execute("SELECT COUNT(*) FROM current_disaggregation").fetchone()[0]
    print(f"✅ Disaggregation data: {disagg_count:,} records")

    # Load field data
    conn.execute(f"""
        CREATE TABLE data_fvm_marker_2022_silver AS 
        SELECT * FROM '{silver_dir / "fvm_marker_2022.parquet"}'
    """)
    field_count = conn.execute("SELECT COUNT(*) FROM data_fvm_marker_2022_silver").fetchone()[0]
    print(f"✅ Field data: {field_count:,} records")

    # Load buildings data
    conn.execute(f"""
        CREATE TABLE data_bbr_buildings_silver AS 
        SELECT * FROM '{silver_dir / "joined_buildings.parquet"}'
    """)
    building_count = conn.execute("SELECT COUNT(*) FROM data_bbr_buildings_silver").fetchone()[0]
    print(f"✅ Buildings data: {building_count:,} records")

    # Load water data
    conn.execute(f"""
        CREATE TABLE data_water_typology_silver AS 
        SELECT * FROM '{silver_dir / "water_typology.parquet"}'
    """)
    water_count = conn.execute("SELECT COUNT(*) FROM data_water_typology_silver").fetchone()[0]
    print(f"✅ Water data: {water_count:,} records")

    # Check unique fields
    unique_fields = conn.execute("""
        SELECT COUNT(DISTINCT field_uuid) 
        FROM current_disaggregation 
        WHERE field_uuid IS NOT NULL
    """).fetchone()[0]
    print(f"📊 Processing {unique_fields:,} unique fields with pesticide applications")

    # Test proximity analysis step by step
    print("\n🌍 Running proximity analysis...")

    try:
        # Step 1: Create fields with geometry
        print("Step 1: Creating fields with geometry...")
        conn.execute("""
            CREATE OR REPLACE TABLE fields_with_geometry AS
            SELECT DISTINCT
                cd.field_uuid,
                ST_Transform(f.geometry, 'EPSG:4326', 'EPSG:25832') as field_geom_utm
            FROM current_disaggregation cd
            JOIN data_fvm_marker_2022_silver f ON cd.field_uuid = f.field_uuid
            WHERE f.geometry IS NOT NULL
        """)
        geom_count = conn.execute("SELECT COUNT(*) FROM fields_with_geometry").fetchone()[0]
        print(f"✅ Fields with geometry: {geom_count:,}")

        # Step 2: Test residential proximity (simplified)
        print("Step 2: Testing residential proximity...")
        conn.execute("""
            CREATE OR REPLACE TABLE residential_proximity AS
            SELECT 
                fg.field_uuid,
                COUNT(b.address) as residential_count
            FROM fields_with_geometry fg
            LEFT JOIN data_bbr_buildings_silver b ON ST_DWithin(
                fg.field_geom_utm,
                ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832'),
                100.0
            ) AND b.category_group = 'residential'
              AND b.address IS NOT NULL
            GROUP BY fg.field_uuid
            LIMIT 1000
        """)
        res_count = conn.execute("SELECT COUNT(*) FROM residential_proximity").fetchone()[0]
        print(f"✅ Residential proximity (limited): {res_count:,}")

        # Check some sample results
        sample = conn.execute("""
            SELECT field_uuid, residential_count 
            FROM residential_proximity 
            WHERE residential_count > 0 
            LIMIT 5
        """).fetchall()

        if sample:
            print("📋 Sample results:")
            for field_uuid, count in sample:
                print(f"   Field {field_uuid}: {count} residential buildings within 100m")
        else:
            print("ℹ️  No residential buildings found within 100m of sample fields")

        print("\n✅ Proximity analysis test completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Error during proximity analysis: {e}")
        return False

    finally:
        conn.close()


if __name__ == "__main__":
    success = test_proximity_analysis()
    if success:
        print("\n🎉 Local proximity test passed!")
    else:
        print("\n💥 Local proximity test failed!")
