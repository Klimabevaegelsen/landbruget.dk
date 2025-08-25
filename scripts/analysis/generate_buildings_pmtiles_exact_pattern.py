#!/usr/bin/env python3
"""
Generate PMTiles for buildings within 100m of pesticide agriculture.

This script follows the EXACT SAME pattern as the pesticide proximity pipeline:
- Only processes fields from disaggregated pesticide data (not all fields)
- Uses small batches (1000 fields at a time)
- Loads buildings fresh per batch (not globally)
- Uses UTM coordinates for accurate distance calculation
- Filters to residential buildings with addresses
- Returns building geometry instead of just addresses
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
import time

import duckdb
from tqdm import tqdm

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def setup_duckdb() -> duckdb.DuckDBPyConnection:
    """Setup DuckDB with spatial extension (identical to pesticide proximity)."""
    conn = duckdb.connect()
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")

    # Memory optimization settings (following pesticide proximity pattern)
    conn.execute("SET enable_progress_bar = false")
    conn.execute("SET memory_limit = '8GB'")
    conn.execute("SET threads = 4")

    return conn


def load_disaggregated_fields(conn: duckdb.DuckDBPyConnection, disaggregated_file: str, fields_file: str) -> int:
    """Load fields from disaggregated data exactly like proximity pipeline."""
    logger.info("📥 Loading disaggregated pesticide data and fields")

    # Load disaggregated data (like proximity pipeline _load_year_data)
    conn.execute(f"""
        CREATE OR REPLACE TABLE current_disaggregation AS
        SELECT * FROM read_parquet('{disaggregated_file}')
        WHERE field_uuid IS NOT NULL
    """)

    disagg_count = conn.execute("SELECT COUNT(*) FROM current_disaggregation").fetchone()[0]
    unique_fields = conn.execute("SELECT COUNT(DISTINCT field_uuid) FROM current_disaggregation").fetchone()[0]
    logger.info(f"   Loaded {disagg_count:,} disaggregated records")
    logger.info(f"   {unique_fields:,} unique fields with pesticide applications")

    # Load field data
    conn.execute(f"""
        CREATE OR REPLACE TABLE field_data AS
        SELECT * FROM read_parquet('{fields_file}')
        WHERE geometry_wkt IS NOT NULL
    """)

    field_count = conn.execute("SELECT COUNT(*) FROM field_data").fetchone()[0]
    logger.info(f"   Loaded {field_count:,} field records with geometry")

    # Create fields with geometry (EXACT same as proximity pipeline lines 306-313)
    conn.execute("""
        CREATE OR REPLACE TABLE fields_with_geometry AS
        SELECT DISTINCT
            cd.field_uuid,
            ST_GeomFromText(f.geometry_wkt) as field_geom,
            ST_Transform(ST_GeomFromText(f.geometry_wkt), 'EPSG:4326', 'EPSG:25832') as field_geom_utm
        FROM current_disaggregation cd
        JOIN field_data f ON cd.field_uuid = f.field_uuid
        WHERE f.geometry_wkt IS NOT NULL
    """)

    final_count = conn.execute("SELECT COUNT(*) FROM fields_with_geometry").fetchone()[0]
    logger.info(f"   ✅ Created {final_count:,} fields with geometry for proximity analysis")

    return final_count


def load_bbr_buildings(conn: duckdb.DuckDBPyConnection, buildings_file: str):
    """Load BBR buildings as reference table (like proximity pipeline)."""
    logger.info("🏗️  Loading BBR buildings reference")

    # Load all buildings into reference table (like proximity pipeline data_bbr_buildings_silver)
    conn.execute(f"""
        CREATE OR REPLACE TABLE data_bbr_buildings_silver AS
        SELECT
            building_uuid,
            geo_building_polygon as geometry,
            geo_building_centroid,
            building_type,
            building_usage_category,
            inspire_current_use,
            inspire_building_nature,
            inspire_construction_year,
            inspire_floor_area,
            inspire_floors,
            inspire_dwellings,
            address_full as address,
            category_group
        FROM read_parquet('{buildings_file}')
        WHERE geo_building_polygon IS NOT NULL
    """)

    total_buildings = conn.execute("SELECT COUNT(*) FROM data_bbr_buildings_silver").fetchone()[0]
    residential_buildings = conn.execute(
        "SELECT COUNT(*) FROM data_bbr_buildings_silver WHERE category_group = 'residential'"
    ).fetchone()[0]

    logger.info(f"   Loaded {total_buildings:,} total buildings")
    logger.info(f"   {residential_buildings:,} residential buildings available")


def find_buildings_exact_proximity_pattern(conn: duckdb.DuckDBPyConnection, batch_size: int = 1000) -> int:
    """
    Find buildings using EXACT same pattern as pesticide proximity pipeline.

    This replicates the exact logic from pesticide_proximity.py lines 315-412.
    """
    logger.info("🎯 Finding buildings using EXACT pesticide proximity pipeline pattern")

    # Get total counts (identical to pesticide proximity)
    total_fields = conn.execute("SELECT COUNT(*) FROM fields_with_geometry").fetchone()[0]
    total_chunks = (total_fields + batch_size - 1) // batch_size

    logger.info(f"   Processing {total_fields:,} fields in batches of {batch_size:,}")
    logger.info(f"   Total batches: {total_chunks:,}")

    # Initialize results table (similar to proximity pipeline but with geometry)
    conn.execute("""
        CREATE OR REPLACE TABLE building_proximity_results (
            field_uuid VARCHAR,
            building_uuid VARCHAR,
            geometry GEOMETRY,
            building_type VARCHAR,
            building_usage_category VARCHAR,
            inspire_current_use VARCHAR,
            inspire_building_nature VARCHAR,
            inspire_construction_year VARCHAR,
            inspire_floor_area VARCHAR,
            inspire_floors VARCHAR,
            inspire_dwellings VARCHAR,
            address VARCHAR,
            distance_m DOUBLE
        )
    """)

    # Process in chunks (EXACT same pattern as pesticide proximity)
    processed = 0

    # Create progress bar (identical to pesticide proximity)
    pbar = tqdm(
        total=total_fields,
        desc="🏠 Building proximity",
        unit="fields",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} fields [{elapsed}<{remaining}, {rate_fmt}]",
    )

    for chunk_num, offset in enumerate(range(0, total_fields, batch_size), 1):
        chunk_start = time.time()
        logger.info(
            f"🔄 Processing batch {chunk_num}/{total_chunks} "
            f"(fields {offset:,}-{min(offset + batch_size, total_fields):,})"
        )

        # Step 1: Create current batch of fields (IDENTICAL to pesticide proximity lines 352-357)
        conn.execute(f"""
            CREATE OR REPLACE TABLE current_field_batch AS
            SELECT * FROM fields_with_geometry
            ORDER BY field_uuid
            LIMIT {batch_size} OFFSET {offset}
        """)

        # Step 2: Pre-filter buildings per batch (IDENTICAL to proximity pipeline lines 360-369)
        conn.execute("""
            CREATE OR REPLACE TABLE residential_buildings AS
            SELECT
                building_uuid,
                address,
                geometry,
                geo_building_centroid,
                building_type,
                building_usage_category,
                inspire_current_use,
                inspire_building_nature,
                inspire_construction_year,
                inspire_floor_area,
                inspire_floors,
                inspire_dwellings,
                ST_Transform(geo_building_centroid, 'EPSG:4326', 'EPSG:25832') as building_geom_utm
            FROM data_bbr_buildings_silver
            WHERE category_group = 'residential'
              AND address IS NOT NULL
              AND geometry IS NOT NULL
              AND geo_building_centroid IS NOT NULL
        """)

        # Step 3: Spatial join (MODIFIED from proximity pipeline to return geometry instead of formatted strings)
        conn.execute("""
            INSERT INTO building_proximity_results
            SELECT
                f.field_uuid,
                b.building_uuid,
                b.geometry,
                b.building_type,
                b.building_usage_category,
                b.inspire_current_use,
                b.inspire_building_nature,
                b.inspire_construction_year,
                b.inspire_floor_area,
                b.inspire_floors,
                b.inspire_dwellings,
                b.address,
                ROUND(ST_Distance(f.field_geom_utm, b.building_geom_utm), 1) as distance_m
            FROM current_field_batch f
            LEFT JOIN residential_buildings b ON ST_Intersects(
                ST_Buffer(f.field_geom_utm, 100.0),  -- 100m buffer (same as proximity pipeline)
                b.building_geom_utm
            )
            WHERE b.building_uuid IS NOT NULL  -- Only buildings that actually intersect
        """)

        # Statistics and progress (identical to pesticide proximity)
        chunk_time = time.time() - chunk_start
        batch_processed = min(batch_size, total_fields - offset)
        processed += batch_processed

        # Update progress bar (identical to pesticide proximity)
        pbar.update(batch_processed)
        pbar.set_postfix(
            {
                "batch_time": f"{chunk_time:.1f}s",
                "rate": f"{batch_processed / chunk_time:.0f} fields/s",
            }
        )

        logger.info(
            f"✅ Batch {chunk_num} completed in {chunk_time:.2f}s - "
            f"Rate: {batch_processed / chunk_time:.0f} fields/s"
        )

        # Memory cleanup after each batch (following field analysis pattern)
        conn.execute("DROP TABLE IF EXISTS current_field_batch")
        conn.execute("DROP TABLE IF EXISTS residential_buildings")
        if chunk_num % 10 == 0:
            conn.execute("CHECKPOINT")  # Force garbage collection every 10 batches

    pbar.close()

    # Get final count
    unique_buildings = conn.execute("SELECT COUNT(DISTINCT building_uuid) FROM building_proximity_results").fetchone()[
        0
    ]
    total_records = conn.execute("SELECT COUNT(*) FROM building_proximity_results").fetchone()[0]

    logger.info(f"   🎯 Found {unique_buildings:,} unique buildings within 100m of pesticide fields")
    logger.info(f"   📊 Total proximity records: {total_records:,}")

    return unique_buildings


def swap_coordinates(geometry):
    """Swap longitude/latitude coordinates in GeoJSON geometry to fix coordinate inversion."""
    if geometry["type"] == "Point":
        geometry["coordinates"] = [geometry["coordinates"][1], geometry["coordinates"][0]]
    elif geometry["type"] == "LineString":
        geometry["coordinates"] = [[coord[1], coord[0]] for coord in geometry["coordinates"]]
    elif geometry["type"] == "Polygon":
        geometry["coordinates"] = [[[coord[1], coord[0]] for coord in ring] for ring in geometry["coordinates"]]
    elif geometry["type"] == "MultiPolygon":
        geometry["coordinates"] = [
            [[[coord[1], coord[0]] for coord in ring] for ring in polygon] for polygon in geometry["coordinates"]
        ]
    elif geometry["type"] == "MultiPoint":
        geometry["coordinates"] = [[coord[1], coord[0]] for coord in geometry["coordinates"]]
    elif geometry["type"] == "MultiLineString":
        geometry["coordinates"] = [[[coord[1], coord[0]] for coord in line] for line in geometry["coordinates"]]
    return geometry


def convert_to_geojson(conn: duckdb.DuckDBPyConnection, output_file: str):
    """Convert buildings data to GeoJSON format."""
    logger.info("🗺️  Converting proximity buildings to GeoJSON")

    # Use a simpler approach - export to JSON and then convert to GeoJSON
    temp_file = output_file + ".tmp"

    # Export all data as JSON first
    conn.execute(f"""
        COPY (
            SELECT
                ST_AsGeoJSON(geometry) as geometry,
                building_uuid,
                field_uuid,
                building_type,
                building_usage_category,
                inspire_current_use,
                inspire_building_nature,
                inspire_construction_year,
                inspire_floor_area,
                inspire_floors,
                inspire_dwellings,
                address,
                distance_m
            FROM building_proximity_results
        ) TO '{temp_file}' (FORMAT JSON, ARRAY true)
    """)

    # Convert to proper GeoJSON format
    with open(temp_file, "r") as f:
        data = json.load(f)

    features = []
    for row in data:
        # Extract geometry and properties
        geometry_str = row.pop("geometry")
        if isinstance(geometry_str, str):
            geometry = json.loads(geometry_str)
        else:
            geometry = geometry_str  # Already a dict

        # Swap coordinates to fix lng/lat inversion (Denmark coordinates were showing in Indian Ocean)
        geometry = swap_coordinates(geometry)

        properties = row

        feature = {"type": "Feature", "geometry": geometry, "properties": properties}
        features.append(feature)

    feature_collection = {"type": "FeatureCollection", "features": features}

    # Write final GeoJSON
    with open(output_file, "w") as f:
        json.dump(feature_collection, f)

    # Clean up temp file
    os.unlink(temp_file)

    logger.info(f"   Generated GeoJSON with {len(features):,} proximity buildings")
    return len(features)


def generate_pmtiles(geojson_file: str, output_file: str):
    """Generate PMTiles using tippecanoe with building-specific optimization."""
    logger.info(f"🏗️  Generating proximity buildings PMTiles: {output_file}")

    cmd = [
        "tippecanoe",
        "-o",
        output_file,
        "--force",
        "--minimum-zoom=8",
        "--maximum-zoom=18",
        "--base-zoom=14",
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        "--maximum-tile-bytes=200000",
        "--simplification=2",
        "--detect-shared-borders",
        "--reorder",
        "--buffer=32",
        "--layer=buildings",
        geojson_file,
    ]

    logger.info("   Optimized for proximity buildings (point data)")
    logger.info(f"   Command: {' '.join(cmd)}")

    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info("   ✅ Proximity buildings PMTiles generated successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"   ❌ Tippecanoe failed: {e}")
        logger.error(f"   stdout: {e.stdout}")
        logger.error(f"   stderr: {e.stderr}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Generate PMTiles for buildings using exact proximity pipeline pattern"
    )
    parser.add_argument("--disaggregated", required=True, help="Path to disaggregated pesticide parquet file")
    parser.add_argument("--fields", required=True, help="Path to field analysis parquet file with geometry")
    parser.add_argument("--buildings", required=True, help="Path to BBR buildings parquet file")
    parser.add_argument("--output", required=True, help="Output PMTiles file path")

    args = parser.parse_args()

    try:
        logger.info("🏠 Starting buildings PMTiles generation")
        logger.info("   Using EXACT same pattern as pesticide proximity pipeline")
        logger.info(f"Disaggregated input: {args.disaggregated}")
        logger.info(f"Fields input: {args.fields}")
        logger.info(f"Buildings input: {args.buildings}")
        logger.info(f"Output: {args.output}")

        # Setup
        conn = setup_duckdb()

        # Load and process data using EXACT pesticide proximity pattern
        field_count = load_disaggregated_fields(conn, args.disaggregated, args.fields)
        if field_count == 0:
            logger.error("❌ No fields with pesticide applications found")
            sys.exit(1)

        load_bbr_buildings(conn, args.buildings)
        building_count = find_buildings_exact_proximity_pattern(conn, batch_size=1000)

        if building_count == 0:
            logger.error("❌ No buildings found within 100m of pesticide fields")
            sys.exit(1)

        # Generate GeoJSON and PMTiles
        with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as f:
            geojson_file = f.name

        try:
            feature_count = convert_to_geojson(conn, geojson_file)

            if not generate_pmtiles(geojson_file, args.output):
                logger.error("❌ PMTiles generation failed")
                sys.exit(1)

            logger.info("🎉 Proximity buildings PMTiles generation completed successfully!")
            logger.info(f"   Output file: {args.output}")
            logger.info(f"   Total buildings: {building_count}")
            logger.info(f"   File size: {os.path.getsize(args.output) / (1024 * 1024):.2f} MB")

        finally:
            if os.path.exists(geojson_file):
                os.unlink(geojson_file)

    except Exception as e:
        logger.error(f"❌ Error: {e}")
        sys.exit(1)
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
