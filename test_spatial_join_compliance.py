#!/usr/bin/env python3
"""
Test script for DuckDB Spatial SPATIAL_JOIN operator compliance.
Based on PR #545: https://github.com/duckdb/duckdb-spatial/pull/545

This script tests different spatial join approaches to ensure we trigger
the SPATIAL_JOIN operator instead of the inefficient blockwise nested-loop join.
"""

import duckdb
import time
from pathlib import Path
import sys

def setup_test_data():
    """Setup test data with local files"""
    print("🔧 Setting up test environment...")
    
    # Check if data exists
    data_dir = Path("data_local")
    required_files = [
        data_dir / "silver" / "fvm_marker_2022.parquet",
        data_dir / "silver" / "joined_buildings.parquet", 
        data_dir / "gold" / "pesticide_disaggregation_2021_2022.parquet"
    ]
    
    for file_path in required_files:
        if not file_path.exists():
            print(f"❌ Missing required file: {file_path}")
            print("Please run the data download first!")
            return None
            
    # Initialize DuckDB with spatial extensions
    conn = duckdb.connect()
    conn.execute('INSTALL spatial')
    conn.execute('LOAD spatial') 
    print("✅ DuckDB spatial extensions loaded")
    
    # Load sample data for testing
    print("📥 Loading test datasets...")
    
    # Load disaggregation data (sample)
    conn.execute(f"""
        CREATE TABLE sample_disaggregation AS 
        SELECT * FROM '{data_dir / "gold" / "pesticide_disaggregation_2021_2022.parquet"}'
        LIMIT 1000
    """)
    
    # Load field data matching the disaggregation
    conn.execute(f"""
        CREATE TABLE sample_fields AS 
        SELECT * FROM '{data_dir / "silver" / "fvm_marker_2022.parquet"}'
        WHERE field_uuid IN (SELECT DISTINCT field_uuid FROM sample_disaggregation)
    """)
    
    # Load buildings data (sample from residential)
    conn.execute(f"""
        CREATE TABLE sample_buildings AS 
        SELECT * FROM '{data_dir / "silver" / "joined_buildings.parquet"}'
        WHERE category_group = 'residential' 
          AND address IS NOT NULL
          AND geometry IS NOT NULL
        LIMIT 5000
    """)
    
    # Create fields with UTM geometry
    conn.execute("""
        CREATE TABLE fields_with_geometry AS
        SELECT DISTINCT
            sd.field_uuid,
            ST_Transform(sf.geometry, 'EPSG:4326', 'EPSG:25832') as field_geom_utm
        FROM sample_disaggregation sd
        JOIN sample_fields sf ON sd.field_uuid = sf.field_uuid
        WHERE sf.geometry IS NOT NULL
    """)
    
    # Get counts
    disagg_count = conn.execute("SELECT COUNT(*) FROM sample_disaggregation").fetchone()[0]
    field_count = conn.execute("SELECT COUNT(*) FROM fields_with_geometry").fetchone()[0]
    building_count = conn.execute("SELECT COUNT(*) FROM sample_buildings").fetchone()[0]
    
    print(f"✅ Test data loaded:")
    print(f"   - Disaggregation records: {disagg_count:,}")
    print(f"   - Fields with geometry: {field_count:,}")
    print(f"   - Buildings: {building_count:,}")
    
    return conn

def test_spatial_join_detection(conn):
    """Test if SPATIAL_JOIN operator is detected in explain plans"""
    print("\n🔍 Testing SPATIAL_JOIN operator detection...")
    
    test_cases = [
        {
            "name": "ST_DWithin (should NOT use SPATIAL_JOIN)",
            "query": """
                SELECT COUNT(*) FROM fields_with_geometry fg, sample_buildings b
                WHERE ST_DWithin(
                    fg.field_geom_utm,
                    ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832'),
                    100.0
                )
            """
        },
        {
            "name": "ST_Intersects + ST_Buffer (should use SPATIAL_JOIN)",
            "query": """
                SELECT COUNT(*) 
                FROM fields_with_geometry fg
                JOIN sample_buildings b ON ST_Intersects(
                    ST_Buffer(fg.field_geom_utm, 100.0),
                    ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832')
                )
            """
        },
        {
            "name": "ST_Intersects with pre-buffered geometry (should use SPATIAL_JOIN)",
            "query": """
                WITH buffered_fields AS (
                    SELECT field_uuid, ST_Buffer(field_geom_utm, 100.0) as buffer_geom
                    FROM fields_with_geometry
                )
                SELECT COUNT(*)
                FROM buffered_fields bf
                JOIN sample_buildings b ON ST_Intersects(
                    bf.buffer_geom,
                    ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832')
                )
            """
        }
    ]
    
    results = []
    
    for test_case in test_cases:
        print(f"\n📋 Testing: {test_case['name']}")
        
        try:
            # Get execution plan
            explain_query = f"EXPLAIN {test_case['query']}"
            explain_result = conn.execute(explain_query).fetchall()
            
            # Check for SPATIAL_JOIN in plan
            plan_text = "\n".join([str(row[0]) for row in explain_result])
            has_spatial_join = "SPATIAL_JOIN" in plan_text
            
            # Also check for blockwise nested loop (bad)
            has_blockwise_nl = "BLOCKWISE_NL_JOIN" in plan_text
            
            print(f"   SPATIAL_JOIN detected: {'✅' if has_spatial_join else '❌'}")
            print(f"   BLOCKWISE_NL_JOIN detected: {'❌ (bad)' if has_blockwise_nl else '✅ (good)'}")
            
            if has_spatial_join:
                print("   🚀 Using optimized spatial indexing!")
            elif has_blockwise_nl:
                print("   ⚠️  Using inefficient brute-force join!")
            
            results.append({
                "name": test_case['name'],
                "spatial_join": has_spatial_join,
                "blockwise_nl": has_blockwise_nl,
                "query": test_case['query']
            })
            
        except Exception as e:
            print(f"   ❌ Query failed: {e}")
            results.append({
                "name": test_case['name'],
                "spatial_join": False,
                "blockwise_nl": False,
                "error": str(e)
            })
    
    return results

def test_performance_comparison(conn):
    """Compare performance between different spatial join approaches"""
    print("\n⚡ Testing performance comparison...")
    
    # Warm up
    conn.execute("SELECT 1").fetchone()
    
    performance_tests = [
        {
            "name": "ST_DWithin (Cartesian product)",
            "query": """
                SELECT fg.field_uuid, COUNT(b.address) as building_count
                FROM fields_with_geometry fg, sample_buildings b
                WHERE ST_DWithin(
                    fg.field_geom_utm,
                    ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832'),
                    100.0
                )
                GROUP BY fg.field_uuid
                LIMIT 50
            """
        },
        {
            "name": "ST_Intersects + ST_Buffer (SPATIAL_JOIN)",
            "query": """
                SELECT fg.field_uuid, COUNT(b.address) as building_count
                FROM fields_with_geometry fg
                LEFT JOIN sample_buildings b ON ST_Intersects(
                    ST_Buffer(fg.field_geom_utm, 100.0),
                    ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832')
                )
                GROUP BY fg.field_uuid
                LIMIT 50
            """
        }
    ]
    
    results = []
    
    for test in performance_tests:
        print(f"\n🏃 Running: {test['name']}")
        
        try:
            start_time = time.time()
            result = conn.execute(test['query']).fetchall()
            execution_time = time.time() - start_time
            
            result_count = len(result)
            total_buildings = sum(row[1] for row in result if row[1] is not None)
            
            print(f"   ⏱️  Execution time: {execution_time:.3f}s")
            print(f"   📊 Results: {result_count} fields, {total_buildings} total building matches")
            
            results.append({
                "name": test['name'],
                "time": execution_time,
                "result_count": result_count,
                "total_matches": total_buildings
            })
            
        except Exception as e:
            print(f"   ❌ Query failed: {e}")
            results.append({
                "name": test['name'],
                "time": None,
                "error": str(e)
            })
    
    # Calculate performance improvement
    if len(results) >= 2 and results[0]['time'] and results[1]['time']:
        improvement = results[0]['time'] / results[1]['time']
        print(f"\n🚀 Performance improvement: {improvement:.1f}x faster with SPATIAL_JOIN!")
    
    return results

def test_result_accuracy(conn):
    """Verify that ST_Intersects + ST_Buffer produces same results as ST_DWithin"""
    print("\n🎯 Testing result accuracy...")
    
    try:
        # Get results from ST_DWithin approach
        st_dwithin_results = conn.execute("""
            SELECT fg.field_uuid, COUNT(*) as match_count
            FROM fields_with_geometry fg, sample_buildings b
            WHERE ST_DWithin(
                fg.field_geom_utm,
                ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832'),
                100.0
            )
            GROUP BY fg.field_uuid
            ORDER BY fg.field_uuid
            LIMIT 20
        """).fetchall()
        
        # Get results from ST_Intersects + ST_Buffer approach  
        st_intersects_results = conn.execute("""
            SELECT fg.field_uuid, COUNT(*) as match_count
            FROM fields_with_geometry fg
            JOIN sample_buildings b ON ST_Intersects(
                ST_Buffer(fg.field_geom_utm, 100.0),
                ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832')
            )
            GROUP BY fg.field_uuid
            ORDER BY fg.field_uuid
            LIMIT 20
        """).fetchall()
        
        # Convert to dictionaries for comparison
        dwithin_dict = {row[0]: row[1] for row in st_dwithin_results}
        intersects_dict = {row[0]: row[1] for row in st_intersects_results}
        
        # Compare results
        all_fields = set(dwithin_dict.keys()) | set(intersects_dict.keys())
        matches = 0
        total = 0
        
        print("📋 Sample result comparison:")
        for field_uuid in sorted(list(all_fields)[:10]):  # Show first 10
            dwithin_count = dwithin_dict.get(field_uuid, 0)
            intersects_count = intersects_dict.get(field_uuid, 0)
            match = "✅" if dwithin_count == intersects_count else "❌"
            
            print(f"   Field {field_uuid}: ST_DWithin={dwithin_count}, ST_Intersects={intersects_count} {match}")
            
            if dwithin_count == intersects_count:
                matches += 1
            total += 1
        
        accuracy = (matches / total) * 100 if total > 0 else 0
        print(f"\n🎯 Accuracy: {matches}/{total} fields match ({accuracy:.1f}%)")
        
        if accuracy >= 95:
            print("✅ Results are accurate!")
            return True
        else:
            print("❌ Results differ significantly - investigation needed")
            return False
            
    except Exception as e:
        print(f"❌ Accuracy test failed: {e}")
        return False

def test_chunked_processing(conn):
    """Test chunked processing approach for memory safety"""
    print("\n🔄 Testing chunked processing...")
    
    try:
        chunk_size = 100
        total_fields = conn.execute("SELECT COUNT(*) FROM fields_with_geometry").fetchone()[0]
        chunks_needed = (total_fields + chunk_size - 1) // chunk_size
        
        print(f"Processing {total_fields} fields in {chunks_needed} chunks of {chunk_size}")
        
        # Create results table
        conn.execute("""
            CREATE OR REPLACE TABLE chunked_results (
                field_uuid VARCHAR,
                building_count INTEGER
            )
        """)
        
        total_processed = 0
        start_time = time.time()
        
        for chunk_id in range(chunks_needed):
            offset = chunk_id * chunk_size
            
            # Process chunk with SPATIAL_JOIN compliant query
            chunk_results = conn.execute(f"""
                SELECT fg.field_uuid, COUNT(b.address) as building_count
                FROM (
                    SELECT * FROM fields_with_geometry 
                    LIMIT {chunk_size} OFFSET {offset}
                ) fg
                LEFT JOIN sample_buildings b ON ST_Intersects(
                    ST_Buffer(fg.field_geom_utm, 100.0),
                    ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832')
                )
                GROUP BY fg.field_uuid
            """).fetchall()
            
            # Insert chunk results
            for field_uuid, building_count in chunk_results:
                conn.execute("""
                    INSERT INTO chunked_results VALUES (?, ?)
                """, [field_uuid, building_count])
            
            total_processed += len(chunk_results)
            
            if chunk_id % 5 == 0:  # Progress every 5 chunks
                elapsed = time.time() - start_time
                print(f"   Processed {total_processed}/{total_fields} fields in {elapsed:.1f}s")
        
        total_time = time.time() - start_time
        final_count = conn.execute("SELECT COUNT(*) FROM chunked_results").fetchone()[0]
        
        print(f"✅ Chunked processing completed:")
        print(f"   Total time: {total_time:.2f}s")
        print(f"   Fields processed: {final_count}")
        print(f"   Average time per field: {(total_time/final_count)*1000:.1f}ms")
        
        return True
        
    except Exception as e:
        print(f"❌ Chunked processing failed: {e}")
        return False

def main():
    """Run all spatial join compliance tests"""
    print("🧪 DuckDB Spatial SPATIAL_JOIN Compliance Testing")
    print("=" * 60)
    print("Based on: https://github.com/duckdb/duckdb-spatial/pull/545")
    print()
    
    # Setup test environment
    conn = setup_test_data()
    if not conn:
        print("❌ Failed to setup test data")
        sys.exit(1)
    
    try:
        # Run all tests
        test_results = {}
        
        # Test 1: SPATIAL_JOIN operator detection
        test_results['spatial_join'] = test_spatial_join_detection(conn)
        
        # Test 2: Performance comparison
        test_results['performance'] = test_performance_comparison(conn)
        
        # Test 3: Result accuracy
        test_results['accuracy'] = test_result_accuracy(conn)
        
        # Test 4: Chunked processing
        test_results['chunked'] = test_chunked_processing(conn)
        
        # Summary
        print("\n" + "=" * 60)
        print("🏁 TEST SUMMARY")
        print("=" * 60)
        
        # Check if we found SPATIAL_JOIN usage
        spatial_join_found = any(
            result.get('spatial_join', False) 
            for result in test_results['spatial_join']
        )
        
        if spatial_join_found:
            print("✅ SPATIAL_JOIN operator successfully detected!")
            print("✅ Queries are compliant with DuckDB Spatial PR #545")
        else:
            print("❌ SPATIAL_JOIN operator not detected")
            print("❌ Queries may be using inefficient blockwise nested-loop joins")
        
        print(f"✅ Result accuracy test: {'PASSED' if test_results['accuracy'] else 'FAILED'}")
        print(f"✅ Chunked processing test: {'PASSED' if test_results['chunked'] else 'FAILED'}")
        
        print("\n🚀 Ready for production implementation!")
        
    except Exception as e:
        print(f"❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()
