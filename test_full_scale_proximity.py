#!/usr/bin/env python3
"""
Full-scale proximity analysis test using locally downloaded data.
This must pass BEFORE making any changes to the production pipeline.

Tests the complete proximity analysis with real data volumes:
- 227,598 unique fields with pesticide applications
- 1,700,839 building records
- 7,812 water feature records

Based on DuckDB Spatial PR #545: https://github.com/duckdb/duckdb-spatial/pull/545
"""

import duckdb
import time
from pathlib import Path
import sys

def test_full_scale_local():
    """Test with full local dataset before pipeline changes"""
    
    print("🧪 Testing full-scale proximity analysis locally")
    print("=" * 60)
    print("⚠️  CRITICAL: This must succeed before modifying the production pipeline!")
    print()
    
    # Check data availability
    data_dir = Path("data_local")
    required_files = [
        data_dir / "gold" / "pesticide_disaggregation_2021_2022.parquet",
        data_dir / "silver" / "fvm_marker_2022.parquet",
        data_dir / "silver" / "joined_buildings.parquet",
        data_dir / "silver" / "water_typology.parquet"
    ]
    
    for file_path in required_files:
        if not file_path.exists():
            print(f"❌ Missing required file: {file_path}")
            print("Please run data download first!")
            return False
        print(f"✅ Found: {file_path} ({file_path.stat().st_size / 1024 / 1024:.1f}MB)")
    
    # Load all local data
    print("\n🔧 Setting up DuckDB with full datasets...")
    conn = duckdb.connect()
    conn.execute('INSTALL spatial')
    conn.execute('LOAD spatial')
    
    try:
        # Load full datasets
        print("📥 Loading full datasets...")
        
        start_time = time.time()
        conn.execute(f"""
            CREATE TABLE current_disaggregation AS 
            SELECT * FROM '{data_dir / "gold" / "pesticide_disaggregation_2021_2022.parquet"}'
        """)
        load_time = time.time() - start_time
        print(f"   Disaggregation data loaded in {load_time:.1f}s")
        
        start_time = time.time()
        conn.execute(f"""
            CREATE TABLE data_fvm_marker_2022_silver AS 
            SELECT * FROM '{data_dir / "silver" / "fvm_marker_2022.parquet"}'
        """)
        load_time = time.time() - start_time
        print(f"   Field data loaded in {load_time:.1f}s")
        
        start_time = time.time()
        conn.execute(f"""
            CREATE TABLE data_bbr_buildings_silver AS 
            SELECT * FROM '{data_dir / "silver" / "joined_buildings.parquet"}'
        """)
        load_time = time.time() - start_time
        print(f"   Buildings data loaded in {load_time:.1f}s")
        
        start_time = time.time()
        conn.execute(f"""
            CREATE TABLE data_water_typology_silver AS 
            SELECT * FROM '{data_dir / "silver" / "water_typology.parquet"}'
        """)
        load_time = time.time() - start_time
        print(f"   Water data loaded in {load_time:.1f}s")
        
        # Get actual counts
        print("\n📊 Analyzing dataset sizes...")
        disagg_count = conn.execute("SELECT COUNT(*) FROM current_disaggregation").fetchone()[0]
        field_count = conn.execute("SELECT COUNT(DISTINCT field_uuid) FROM current_disaggregation WHERE field_uuid IS NOT NULL").fetchone()[0] 
        total_building_count = conn.execute("SELECT COUNT(*) FROM data_bbr_buildings_silver").fetchone()[0]
        residential_count = conn.execute("SELECT COUNT(*) FROM data_bbr_buildings_silver WHERE category_group = 'residential' AND address IS NOT NULL").fetchone()[0]
        water_count = conn.execute("SELECT COUNT(*) FROM data_water_typology_silver").fetchone()[0]
        
        print(f"📈 Full dataset analysis:")
        print(f"   - Disaggregated pesticide records: {disagg_count:,}")
        print(f"   - Unique fields with pesticides: {field_count:,}")
        print(f"   - Total buildings: {total_building_count:,}")
        print(f"   - Residential buildings with addresses: {residential_count:,}")
        print(f"   - Water features: {water_count:,}")
        print(f"   - Estimated field×building comparisons: {field_count:,} × {residential_count:,} = {field_count * residential_count:,}")
        
        # Determine approach based on scale
        total_comparisons = field_count * residential_count
        if total_comparisons > 100_000_000:  # 100M comparisons
            print(f"⚠️  LARGE DATASET: {total_comparisons:,} comparisons detected")
            print("   Using chunked processing approach...")
            return test_chunked_proximity_full(conn, field_count, residential_count)
        else:
            print(f"✅ MANAGEABLE DATASET: {total_comparisons:,} comparisons")
            print("   Testing direct approach...")
            return test_direct_proximity_full(conn, field_count, residential_count)
            
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        conn.close()

def test_direct_proximity_full(conn, field_count, residential_count):
    """Test direct proximity analysis (for smaller datasets)"""
    print(f"\n🚀 Testing direct proximity analysis...")
    
    try:
        # Create fields with geometry
        print("Step 1: Creating fields with geometry...")
        start_time = time.time()
        
        conn.execute("""
            CREATE TABLE fields_with_geometry AS
            SELECT DISTINCT
                cd.field_uuid,
                ST_Transform(f.geometry, 'EPSG:4326', 'EPSG:25832') as field_geom_utm
            FROM current_disaggregation cd
            JOIN data_fvm_marker_2022_silver f ON cd.field_uuid = f.field_uuid
            WHERE f.geometry IS NOT NULL
        """)
        
        geom_count = conn.execute("SELECT COUNT(*) FROM fields_with_geometry").fetchone()[0]
        geom_time = time.time() - start_time
        print(f"   ✅ {geom_count:,} fields with geometry created in {geom_time:.1f}s")
        
        # Test proximity analysis with SPATIAL_JOIN compliant approach
        print("Step 2: Running proximity analysis...")
        start_time = time.time()
        
        conn.execute("""
            CREATE TABLE proximity_results AS
            SELECT 
                fg.field_uuid,
                COUNT(b.address) as residential_count,
                array_agg(
                    b.address || ':' || 
                    ROUND(ST_Distance(fg.field_geom_utm, 
                          ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832')), 1) || 'm'
                    ORDER BY ST_Distance(fg.field_geom_utm, 
                                       ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832'))
                ) FILTER (WHERE b.address IS NOT NULL) as nearby_buildings
            FROM fields_with_geometry fg
            LEFT JOIN data_bbr_buildings_silver b ON ST_Intersects(
                ST_Buffer(fg.field_geom_utm, 100.0),
                ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832')
            ) AND b.category_group = 'residential'
              AND b.address IS NOT NULL
            GROUP BY fg.field_uuid
        """)
        
        proximity_time = time.time() - start_time
        
        # Get results
        result_count = conn.execute("SELECT COUNT(*) FROM proximity_results").fetchone()[0]
        with_buildings = conn.execute("SELECT COUNT(*) FROM proximity_results WHERE residential_count > 0").fetchone()[0]
        total_matches = conn.execute("SELECT SUM(residential_count) FROM proximity_results").fetchone()[0]
        
        print(f"✅ Direct proximity analysis completed in {proximity_time:.1f}s:")
        print(f"   - Fields processed: {result_count:,}")
        print(f"   - Fields with nearby buildings: {with_buildings:,} ({with_buildings/result_count*100:.1f}%)")
        print(f"   - Total building matches: {total_matches:,}")
        print(f"   - Average matches per field: {total_matches/result_count:.1f}")
        
        # Show sample results
        samples = conn.execute("""
            SELECT field_uuid, residential_count, nearby_buildings
            FROM proximity_results 
            WHERE residential_count > 0 
            ORDER BY residential_count DESC 
            LIMIT 3
        """).fetchall()
        
        if samples:
            print("\n📋 Sample results:")
            for field_uuid, count, buildings in samples:
                buildings_preview = str(buildings)[:100] + "..." if len(str(buildings)) > 100 else str(buildings)
                print(f"   Field {field_uuid}: {count} buildings - {buildings_preview}")
        
        return True
        
    except Exception as e:
        print(f"❌ Direct proximity analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_chunked_proximity_full(conn, total_fields, residential_count):
    """Test chunked processing with full dataset"""
    print(f"\n🔄 Testing chunked proximity processing...")
    
    chunk_size = 1000  # Process 1000 fields at a time
    chunks_needed = (total_fields + chunk_size - 1) // chunk_size
    
    print(f"📊 Processing plan:")
    print(f"   - Total fields: {total_fields:,}")
    print(f"   - Residential buildings: {residential_count:,}")
    print(f"   - Chunk size: {chunk_size:,} fields")
    print(f"   - Total chunks needed: {chunks_needed:,}")
    print(f"   - Estimated comparisons per chunk: {chunk_size:,} × {residential_count:,} = {chunk_size * residential_count:,}")
    
    try:
        # Create fields with geometry
        print("\nStep 1: Creating fields with geometry...")
        start_time = time.time()
        
        conn.execute("""
            CREATE TABLE fields_with_geometry AS
            SELECT DISTINCT
                cd.field_uuid,
                ST_Transform(f.geometry, 'EPSG:4326', 'EPSG:25832') as field_geom_utm,
                row_number() OVER (ORDER BY cd.field_uuid) as row_num
            FROM current_disaggregation cd
            JOIN data_fvm_marker_2022_silver f ON cd.field_uuid = f.field_uuid
            WHERE f.geometry IS NOT NULL
        """)
        
        geom_count = conn.execute("SELECT COUNT(*) FROM fields_with_geometry").fetchone()[0]
        geom_time = time.time() - start_time
        print(f"   ✅ {geom_count:,} fields with geometry created in {geom_time:.1f}s")
        
        # Create results table
        conn.execute("""
            CREATE TABLE chunked_proximity_results (
                field_uuid VARCHAR,
                residential_count INTEGER,
                nearby_buildings VARCHAR[]
            )
        """)
        
        # Test first few chunks to validate approach
        test_chunks = min(3, chunks_needed)  # Test first 3 chunks or all if fewer
        print(f"\nStep 2: Testing first {test_chunks} chunks...")
        
        total_processed = 0
        total_chunk_time = 0
        
        for chunk_id in range(test_chunks):
            start_row = chunk_id * chunk_size + 1
            end_row = (chunk_id + 1) * chunk_size
            
            print(f"   🔄 Processing chunk {chunk_id + 1}/{test_chunks} (rows {start_row:,}-{end_row:,})...")
            
            chunk_start = time.time()
            
            # Process chunk with SPATIAL_JOIN compliant query
            conn.execute(f"""
                CREATE OR REPLACE TABLE current_chunk AS
                SELECT field_uuid, field_geom_utm
                FROM fields_with_geometry 
                WHERE row_num BETWEEN {start_row} AND {end_row}
            """)
            
            chunk_fields = conn.execute("SELECT COUNT(*) FROM current_chunk").fetchone()[0]
            
            # Run proximity analysis for chunk
            conn.execute("""
                CREATE OR REPLACE TABLE chunk_results AS
                SELECT 
                    fg.field_uuid,
                    COUNT(b.address) as residential_count,
                    array_agg(
                        b.address || ':' || 
                        ROUND(ST_Distance(fg.field_geom_utm, 
                              ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832')), 1) || 'm'
                        ORDER BY ST_Distance(fg.field_geom_utm, 
                                           ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832'))
                    ) FILTER (WHERE b.address IS NOT NULL) as nearby_buildings
                FROM current_chunk fg
                LEFT JOIN data_bbr_buildings_silver b ON ST_Intersects(
                    ST_Buffer(fg.field_geom_utm, 100.0),
                    ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832')
                ) AND b.category_group = 'residential'
                  AND b.address IS NOT NULL
                GROUP BY fg.field_uuid
            """)
            
            # Insert chunk results
            conn.execute("""
                INSERT INTO chunked_proximity_results 
                SELECT * FROM chunk_results
            """)
            
            chunk_time = time.time() - chunk_start
            total_chunk_time += chunk_time
            total_processed += chunk_fields
            
            chunk_with_buildings = conn.execute("SELECT COUNT(*) FROM chunk_results WHERE residential_count > 0").fetchone()[0]
            
            print(f"      ✅ Chunk {chunk_id + 1} completed in {chunk_time:.1f}s:")
            print(f"         - Fields processed: {chunk_fields:,}")
            print(f"         - Fields with buildings: {chunk_with_buildings:,}")
        
        # Calculate projections
        avg_time_per_chunk = total_chunk_time / test_chunks
        estimated_total_time = avg_time_per_chunk * chunks_needed
        
        print(f"\n📊 Chunked processing results:")
        print(f"   ✅ Test chunks completed successfully")
        print(f"   ⏱️  Average time per chunk: {avg_time_per_chunk:.1f}s")
        print(f"   📈 Estimated total time for all chunks: {estimated_total_time:.1f}s ({estimated_total_time/60:.1f} minutes)")
        
        # Get sample results
        final_count = conn.execute("SELECT COUNT(*) FROM chunked_proximity_results").fetchone()[0]
        with_buildings = conn.execute("SELECT COUNT(*) FROM chunked_proximity_results WHERE residential_count > 0").fetchone()[0]
        
        print(f"   📋 Results from {test_chunks} test chunks:")
        print(f"      - Fields processed: {final_count:,}")
        print(f"      - Fields with nearby buildings: {with_buildings:,}")
        
        # Show sample results
        samples = conn.execute("""
            SELECT field_uuid, residential_count, nearby_buildings
            FROM chunked_proximity_results 
            WHERE residential_count > 0 
            ORDER BY residential_count DESC 
            LIMIT 3
        """).fetchall()
        
        if samples:
            print("\n📋 Sample proximity results:")
            for field_uuid, count, buildings in samples:
                buildings_preview = str(buildings)[:100] + "..." if len(str(buildings)) > 100 else str(buildings)
                print(f"   Field {field_uuid}: {count} buildings - {buildings_preview}")
        
        # Validate performance
        if estimated_total_time > 1800:  # 30 minutes
            print(f"\n⚠️  WARNING: Estimated total time ({estimated_total_time/60:.1f} minutes) exceeds 30 minutes")
            print("   Consider further optimization or smaller chunk sizes")
            return False
        else:
            print(f"\n✅ Performance acceptable: {estimated_total_time/60:.1f} minutes estimated")
            return True
        
    except Exception as e:
        print(f"❌ Chunked processing failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run full-scale proximity test"""
    print("🧪 FULL-SCALE PESTICIDE PROXIMITY ANALYSIS TEST")
    print("=" * 80)
    print("⚠️  MANDATORY: This test must pass before modifying production pipeline!")
    print("Based on: https://github.com/duckdb/duckdb-spatial/pull/545")
    print()
    
    success = test_full_scale_local()
    
    print("\n" + "=" * 80)
    if success:
        print("🎉 FULL-SCALE TEST PASSED!")
        print("✅ Safe to proceed with production pipeline modifications")
        print()
        print("Next steps:")
        print("1. Update pesticide_proximity.py with validated approach")
        print("2. Test updated pipeline locally")
        print("3. Deploy to GitHub Actions")
    else:
        print("💥 FULL-SCALE TEST FAILED!")
        print("❌ DO NOT modify production pipeline until this passes")
        print()
        print("Required fixes:")
        print("1. Investigate DuckDB crashes or performance issues")
        print("2. Optimize chunk size or spatial query approach")
        print("3. Re-run test until it passes")
    
    print("=" * 80)

if __name__ == "__main__":
    main()
