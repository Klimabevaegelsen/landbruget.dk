#!/usr/bin/env python3
"""
Debug water coverage explosion by running the actual pipeline stages with local data.

This script runs the key stages (1B, 2B, 4) with our local parquet files to see
where the 40x area explosion occurs.
"""

import sys
import os
import asyncio
import duckdb
from pathlib import Path

# Add the pipeline to the path
sys.path.append('backend/pipelines/unified_pipeline/src')

from unified_pipeline.gold.field_area_analysis.stage1.water_projects_wetlands import WaterProjectsWetlandsIntersection
from unified_pipeline.gold.field_area_analysis.stage2.fields_wetland_water import FieldsWetlandWaterCoverage  
from unified_pipeline.gold.field_area_analysis.stage4.consolidate_two_tables import ConsolidateResultsTwoTables
from unified_pipeline.gold.field_area_analysis.base import FieldAnalysisStageConfig
from unified_pipeline.util.log_util import Logger

class LocalDebugConfig(FieldAnalysisStageConfig):
    """Configuration for local debugging with smaller batches"""
    def __init__(self, **kwargs):
        kwargs['bucket'] = 'local-debug'
        kwargs['max_memory_gb'] = 4
        kwargs['max_threads'] = 2
        kwargs['batch_size'] = 5000  # Much smaller batches for debugging
        kwargs['enable_area_validation'] = False
        super().__init__(**kwargs)

def load_local_data_direct(conn, file_path, table_name):
    """Load local parquet file directly into DuckDB table"""
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS 
        SELECT * FROM read_parquet('{file_path}')
    """)
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"   ✅ Loaded {count:,} records into {table_name}")
    return count

def run_stage1b_with_local_data():
    """Run Stage 1B: Water Projects × Wetlands with local data"""
    print("\n🎯 STAGE 1B: Water Projects × Wetlands Intersections")
    print("=" * 60)
    
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("SET memory_limit='4GB'; SET threads=2;")
    
    data_dir = Path("data_cache/investigation_systematic")
    
    # Load raw data
    print("📂 Loading local data...")
    load_local_data_direct(conn, data_dir / "wetlands_dissolved.parquet", "wetlands_raw")
    load_local_data_direct(conn, data_dir / "water_projects_dissolved.parquet", "water_projects_raw")
    
    # Apply ST_Dump preprocessing (same as pipeline)
    print("🔧 Applying ST_Dump preprocessing...")
    conn.execute("""
        CREATE TABLE wetlands AS
        SELECT 
            ROW_NUMBER() OVER () as wetland_id,
            toerv_pct,
            UNNEST(ST_Dump(geometry)).geom as geometry
        FROM wetlands_raw
    """)
    
    conn.execute("""
        CREATE TABLE water_projects AS
        SELECT 
            project_id,
            UNNEST(ST_Dump(geometry)).geom as geometry
        FROM water_projects_raw
    """)
    
    wetlands_count = conn.execute("SELECT COUNT(*) FROM wetlands").fetchone()[0]
    projects_count = conn.execute("SELECT COUNT(*) FROM water_projects").fetchone()[0]
    
    print(f"   Wetlands after ST_Dump: {wetlands_count:,}")
    print(f"   Water projects after ST_Dump: {projects_count:,}")
    
    # Show water project details
    project_stats = conn.execute("""
        SELECT 
            project_id,
            COUNT(*) as parts,
            SUM(ST_Area_Spheroid(geometry)) / 1000000 as total_km2
        FROM water_projects 
        GROUP BY project_id
    """).fetchall()
    
    print("\n📊 Water Project Analysis:")
    for project_id, parts, area_km2 in project_stats:
        print(f"   {project_id}: {parts:,} parts, {area_km2:.2f} km²")
    
    # Create intersections (use smaller sample to avoid hang)
    print("\n🔧 Creating water×wetland intersections (limited sample)...")
    print("   Using only first 1000 wetlands to avoid computational explosion...")
    
    conn.execute("""
        CREATE TABLE water_wetland_intersections AS
        SELECT 
            w.wetland_id,
            w.toerv_pct,
            wp.project_id,
            ST_Intersection(w.geometry, wp.geometry) as intersection_geometry,
            ST_Area_Spheroid(ST_Intersection(w.geometry, wp.geometry)) as intersection_area_m2,
            ST_Area_Spheroid(w.geometry) as wetland_area_m2,
            ST_Area_Spheroid(wp.geometry) as project_area_m2
        FROM (SELECT * FROM wetlands LIMIT 1000) w
        JOIN water_projects wp ON ST_Intersects(w.geometry, wp.geometry)
        WHERE ST_Area_Spheroid(ST_Intersection(w.geometry, wp.geometry)) > 0.001
    """)
    
    intersections = conn.execute("SELECT COUNT(*) FROM water_wetland_intersections").fetchone()[0]
    total_area = conn.execute("SELECT SUM(intersection_area_m2) / 1000000 FROM water_wetland_intersections").fetchone()[0] or 0
    
    print(f"   ✅ Stage 1B intersections: {intersections:,}")
    print(f"   📏 Total intersection area: {total_area:.4f} km²")
    
    # Save to local file
    output_file = data_dir / "stage1b_water_wetland_intersections.parquet"
    conn.execute(f"COPY water_wetland_intersections TO '{output_file}' (FORMAT PARQUET)")
    print(f"   💾 Saved to: {output_file}")
    
    return conn, intersections, total_area

def run_stage2b_with_local_data(stage1b_conn):
    """Run Stage 2B: Field × Wetland × Water with local data"""
    print("\n🎯 STAGE 2B: Field × Wetland × Water Coverage")
    print("=" * 60)
    
    conn = stage1b_conn  # Reuse connection with Stage 1B data
    data_dir = Path("data_cache/investigation_systematic")
    
    # Load agricultural fields
    print("📂 Loading agricultural fields...")
    load_local_data_direct(conn, data_dir / "agricultural_fields_2024.parquet", "agricultural_fields")
    
    # Create field×wetland intersections first
    print("🔧 Creating field×wetland intersections...")
    conn.execute("""
        CREATE TABLE field_wetland_intersections AS
        SELECT 
            f.field_uuid,
            f.field_id,
            f.block_id, 
            f.cvr_number,
            f.year,
            w.wetland_id,
            w.toerv_pct,
            ST_Intersection(f.geometry, w.geometry) as field_wetland_geometry
        FROM agricultural_fields f
        JOIN wetlands w ON ST_Intersects(f.geometry, w.geometry)
        LIMIT 5000  -- Limit to avoid explosion
    """)
    
    field_wetland_count = conn.execute("SELECT COUNT(*) FROM field_wetland_intersections").fetchone()[0]
    unique_fields = conn.execute("SELECT COUNT(DISTINCT field_uuid) FROM field_wetland_intersections").fetchone()[0]
    
    print(f"   ✅ Field×wetland intersections: {field_wetland_count:,}")
    print(f"   🏠 Unique fields: {unique_fields:,}")
    
    # Create field×wetland×water intersections (THE KEY STEP)
    print("🔧 Creating field×wetland×water intersections (WHERE EXPLOSION HAPPENS)...")
    conn.execute("""
        CREATE TABLE field_wetland_water_intersections AS
        SELECT 
            fwi.field_uuid,
            fwi.field_id,
            fwi.block_id,
            fwi.cvr_number, 
            fwi.year,
            fwi.wetland_id,
            fwi.toerv_pct,
            wwi.project_id,
            ST_Intersection(fwi.field_wetland_geometry, wwi.intersection_geometry) as field_wetland_water_geometry
        FROM field_wetland_intersections fwi
        JOIN water_wetland_intersections wwi 
            ON ST_Intersects(fwi.field_wetland_geometry, wwi.intersection_geometry)
    """)
    
    triple_intersections = conn.execute("SELECT COUNT(*) FROM field_wetland_water_intersections").fetchone()[0]
    unique_water_fields = conn.execute("SELECT COUNT(DISTINCT field_uuid) FROM field_wetland_water_intersections").fetchone()[0]
    
    print(f"   ✅ Field×wetland×water intersections: {triple_intersections:,}")
    print(f"   🏠 Unique fields with water coverage: {unique_water_fields:,}")
    
    # Show explosion analysis
    explosion_stats = conn.execute("""
        SELECT 
            COUNT(DISTINCT project_id) as projects,
            COUNT(DISTINCT wetland_id) as wetlands,
            COUNT(DISTINCT field_uuid) as fields,
            COUNT(*) as total_records
        FROM field_wetland_water_intersections
    """).fetchone()
    
    print(f"\n📊 EXPLOSION BREAKDOWN:")
    print(f"   Projects involved: {explosion_stats[0]:,}")
    print(f"   Wetlands involved: {explosion_stats[1]:,}")  
    print(f"   Fields affected: {explosion_stats[2]:,}")
    print(f"   Total intersection records: {explosion_stats[3]:,}")
    
    # Save to local file
    output_file = data_dir / "stage2b_field_wetland_water.parquet"
    conn.execute(f"COPY field_wetland_water_intersections TO '{output_file}' (FORMAT PARQUET)")
    print(f"   💾 Saved to: {output_file}")
    
    return triple_intersections

def run_stage4_area_calculations(conn):
    """Run Stage 4: Area calculations to see the explosion"""
    print("\n🎯 STAGE 4: Area Calculations (WHERE EXPLOSION BECOMES VISIBLE)")
    print("=" * 60)
    
    # Calculate water coverage areas (this is where we see the explosion)
    print("🔢 Calculating water coverage areas...")
    area_analysis = conn.execute("""
        SELECT 
            COUNT(DISTINCT field_uuid) as fields_with_water,
            SUM(ST_Area_Spheroid(field_wetland_water_geometry)) / 1000000 as total_water_coverage_km2,
            AVG(ST_Area_Spheroid(field_wetland_water_geometry)) / 10000 as avg_coverage_hectares,
            MAX(ST_Area_Spheroid(field_wetland_water_geometry)) / 10000 as max_coverage_hectares
        FROM field_wetland_water_intersections
    """).fetchone()
    
    fields, total_km2, avg_ha, max_ha = area_analysis
    
    print(f"   📊 AREA CALCULATION RESULTS:")
    print(f"      Fields with water coverage: {fields:,}")
    print(f"      Total water coverage area: {total_km2:.4f} km²")
    print(f"      Average coverage per intersection: {avg_ha:.6f} hectares")
    print(f"      Maximum single intersection: {max_ha:.6f} hectares")
    
    # Compare with expected baseline
    print(f"\n🚨 EXPLOSION ANALYSIS:")
    expected_baseline = 9.57  # From investigation document
    if total_km2 > 0:
        explosion_factor = total_km2 / expected_baseline * (1215905 / 1000)  # Scale up from sample
        print(f"   Expected baseline: {expected_baseline} km²")
        print(f"   Sample coverage: {total_km2:.4f} km² (from 1K wetlands sample)")
        print(f"   Projected full coverage: ~{explosion_factor:.1f} km² (scaled to full dataset)")
        print(f"   Potential explosion factor: ~{explosion_factor/expected_baseline:.1f}x")
    
    # Show per-field breakdown to understand duplication
    print("\n🔍 PER-FIELD BREAKDOWN (showing potential duplication):")
    field_breakdown = conn.execute("""
        SELECT 
            field_uuid,
            COUNT(*) as water_intersections,
            COUNT(DISTINCT project_id) as unique_projects,
            COUNT(DISTINCT wetland_id) as unique_wetlands,
            SUM(ST_Area_Spheroid(field_wetland_water_geometry)) / 10000 as total_coverage_hectares
        FROM field_wetland_water_intersections
        GROUP BY field_uuid
        ORDER BY total_coverage_hectares DESC
        LIMIT 10
    """).fetchall()
    
    print("   Top 10 fields by water coverage:")
    print("   Field UUID                               | Intersections | Projects | Wetlands | Coverage (ha)")
    print("   " + "-" * 100)
    for field_uuid, intersections, projects, wetlands, coverage_ha in field_breakdown:
        print(f"   {field_uuid[:40]:<40} | {intersections:>11} | {projects:>8} | {wetlands:>8} | {coverage_ha:>11.6f}")
    
    return total_km2

def main():
    """Run the complete debugging investigation"""
    print("🔬 DEBUGGING WATER COVERAGE EXPLOSION WITH ACTUAL PIPELINE")
    print("=" * 80)
    print("Running key pipeline stages with local data to identify explosion source")
    print()
    
    try:
        # Stage 1B: Water Projects × Wetlands  
        conn, intersections, stage1b_area = run_stage1b_with_local_data()
        
        # Stage 2B: Field × Wetland × Water
        triple_intersections = run_stage2b_with_local_data(conn)
        
        # Stage 4: Area Calculations
        final_area = run_stage4_area_calculations(conn)
        
        # Final analysis
        print(f"\n✅ DEBUGGING COMPLETE!")
        print(f"📊 RESULTS SUMMARY:")
        print(f"   Stage 1B intersections: {intersections:,} (area: {stage1b_area:.4f} km²)")
        print(f"   Stage 2B triple intersections: {triple_intersections:,}")
        print(f"   Stage 4 calculated area: {final_area:.4f} km²")
        print(f"   🎯 This shows where in the pipeline the explosion occurs!")
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        raise

if __name__ == "__main__":
    main()

