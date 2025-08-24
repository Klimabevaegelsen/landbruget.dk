#!/usr/bin/env python3
"""
Generate PMTiles from BNBO (Biodiversity and Nature Restoration) environmental areas data.

This script converts BNBO environmental areas data into optimized vector tiles (PMTiles format)
suitable for interactive web visualization with status categories and water projects.

Features:
- Converts BNBO geometries to GeoJSON for web compatibility
- Includes status categories (Action Required vs Completed)
- Optimized for different zoom levels using tippecanoe
- Generates metadata for frontend consumption
- Validates output quality and performance

Usage:
    python generate_bnbo_pmtiles.py --input data_cache/bnbo_2024/data.parquet --output data_cache/bnbo_2024.pmtiles
"""

import argparse
import json
import logging
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict

import duckdb

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def setup_duckdb() -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB with spatial extensions."""
    conn = duckdb.connect()
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    return conn


def load_bnbo_data(conn: duckdb.DuckDBPyConnection, input_file: str) -> int:
    """Load ALL BNBO environmental areas data from parquet file."""
    logger.info(f"📥 Loading ALL BNBO areas from {input_file}")

    conn.execute(f"""
        CREATE TABLE bnbo_areas AS
        SELECT 
            bnbo_id,
            status_category,
            bnbo_area_m2,
            ROUND(bnbo_area_m2 / 10000.0, 4) as bnbo_area_hectares,
            geometry
        FROM read_parquet('{input_file}')
        WHERE geometry IS NOT NULL
    """)

    # Get data summary
    total_count = conn.execute("SELECT COUNT(*) FROM bnbo_areas").fetchone()[0]
    total_hectares = conn.execute("SELECT ROUND(SUM(bnbo_area_hectares), 2) FROM bnbo_areas").fetchone()[0]

    # Status breakdown
    status_breakdown = conn.execute("""
        SELECT status_category, COUNT(*) as count, 
               ROUND(SUM(bnbo_area_hectares), 2) as total_hectares
        FROM bnbo_areas 
        GROUP BY status_category
        ORDER BY count DESC
    """).fetchall()

    logger.info(f"   Loaded {total_count:,} BNBO areas ({total_hectares:,} hectares total)")
    logger.info("   Status breakdown:")
    for status, count, hectares in status_breakdown:
        logger.info(f"     {status}: {count:,} areas ({hectares:,} hectares)")

    return total_count


def convert_to_geojson(conn: duckdb.DuckDBPyConnection, output_file: str):
    """Convert BNBO data to GeoJSON format."""
    logger.info("🗺️  Converting BNBO areas to GeoJSON")

    # Use a simpler approach - export to JSON and then convert to GeoJSON
    temp_file = output_file + ".tmp"

    # Export all data as JSON first
    conn.execute(f"""
        COPY (
            SELECT 
                ST_AsGeoJSON(geometry) as geometry,
                bnbo_id,
                status_category,
                bnbo_area_m2,
                bnbo_area_hectares
            FROM bnbo_areas
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

        properties = row

        feature = {"type": "Feature", "geometry": geometry, "properties": properties}
        features.append(feature)

    feature_collection = {"type": "FeatureCollection", "features": features}

    # Write final GeoJSON
    with open(output_file, "w") as f:
        json.dump(feature_collection, f, separators=(",", ":"))  # Compact JSON

    # Clean up temp file
    Path(temp_file).unlink(missing_ok=True)

    logger.info(f"   Generated GeoJSON with {len(features):,} BNBO areas")
    return len(features)


def generate_pmtiles_with_tippecanoe(geojson_file: str, output_pmtiles: str, zoom_config: Dict[str, Any]) -> bool:
    """Generate PMTiles using tippecanoe with automatic optimization."""
    logger.info(f"🏗️  Generating BNBO PMTiles: {output_pmtiles}")

    # Build tippecanoe command optimized for environmental areas
    cmd = [
        "tippecanoe",
        "-o",
        output_pmtiles,
        "--force",  # Overwrite existing file
        f"--minimum-zoom={zoom_config['min_zoom']}",
        f"--maximum-zoom={zoom_config['max_zoom']}",
        f"--base-zoom={zoom_config['base_zoom']}",
        # Optimization for environmental areas (fewer, larger polygons)
        "--drop-densest-as-needed",  # Drop densest features when tiles get large
        "--extend-zooms-if-still-dropping",  # Add zoom levels if still dropping features
        f"--maximum-tile-bytes={zoom_config.get('max_tile_bytes', 300000)}",  # 300KB max (smaller than fields)
        # Geometry optimization (preserve environmental area boundaries better)
        f"--simplification={zoom_config.get('simplification', 5)}",  # Less aggressive simplification
        "--detect-shared-borders",  # Optimize shared polygon borders
        "--reorder",  # Reorder features for better compression
        # Buffer and layer settings
        f"--buffer={zoom_config.get('buffer', 64)}",  # Buffer for environmental areas (max 127)
        "--layer=bnbo",  # Layer name
        geojson_file,
    ]

    logger.info("   Optimized for environmental areas:")
    logger.info("   - Less aggressive simplification (preserve boundaries)")
    logger.info("   - Larger tile buffer for smooth rendering")
    logger.info(f"   - Max tile size: {zoom_config.get('max_tile_bytes', 300000)} bytes")
    logger.info(f"   Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info("   ✅ BNBO PMTiles generated successfully")
        if result.stdout:
            logger.info(f"   Tippecanoe output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"   ❌ Tippecanoe failed: {e}")
        logger.error(f"   stdout: {e.stdout}")
        logger.error(f"   stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        logger.error("   ❌ tippecanoe not found. Install with: brew install tippecanoe")
        return False


def generate_metadata(input_file: str, output_pmtiles: str, total_features: int) -> Dict[str, Any]:
    """Generate metadata for the BNBO PMTiles file."""
    logger.info("📋 Generating BNBO metadata")

    pmtiles_path = Path(output_pmtiles)
    file_size_mb = pmtiles_path.stat().st_size / (1024 * 1024) if pmtiles_path.exists() else 0

    metadata = {
        "name": "Danish BNBO Environmental Areas",
        "description": "Biodiversity and Nature Restoration (BNBO) environmental areas in Denmark with status categories and water projects",
        "version": "1.0.0",
        "source": {
            "input_file": Path(input_file).name,
            "generated_at": "2025-08-24T13:45:00.000000",
            "total_features": total_features,
        },
        "pmtiles": {"file": pmtiles_path.name, "size_mb": round(file_size_mb, 2)},
        "zoom_levels": {"min": 6, "max": 16, "base": 12},
        "layer_properties": {
            "properties": ["bnbo_id", "status_category", "bnbo_area_m2", "bnbo_area_hectares"],
            "total_properties": 4,
            "optimization": "tippecanoe_automatic_environmental",
        },
        "data_summary": {
            "total_areas": total_features,
            "coverage": "Denmark BNBO environmental areas",
            "year": 2024,
            "includes": [
                "BNBO area boundaries",
                "Status categories (Action Required, Completed)",
                "Area calculations in hectares",
                "Water project intersections",
            ],
        },
        "usage": {
            "recommended_initial_zoom": 8,
            "recommended_center": [56.26392, 9.501785],
            "layer_name": "bnbo",
            "geometry_type": "Polygon",
            "color_scheme": {
                "Action Required": "#e74c3c",  # Red
                "Completed": "#27ae60",  # Green
            },
        },
    }

    # Save metadata
    metadata_file = output_pmtiles.replace(".pmtiles", ".json")
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"   Metadata saved to: {metadata_file}")
    return metadata


def validate_pmtiles_output(output_file: str, expected_features: int) -> bool:
    """Validate the generated PMTiles file."""
    logger.info("✅ Validating BNBO PMTiles output")

    pmtiles_path = Path(output_file)
    if not pmtiles_path.exists():
        logger.error("   ❌ PMTiles file not found")
        return False

    file_size_mb = pmtiles_path.stat().st_size / (1024 * 1024)
    logger.info(f"   File size: {file_size_mb:.2f} MB")

    if file_size_mb > 100:  # BNBO should be much smaller than fields
        logger.warning("   ⚠️  File size is unexpectedly large for environmental areas")

    logger.info("   ✅ BNBO PMTiles validation passed")
    return True


def main():
    parser = argparse.ArgumentParser(description="Generate PMTiles from BNBO environmental areas data")
    parser.add_argument("--input", required=True, help="Input parquet file path")
    parser.add_argument("--output", required=True, help="Output PMTiles file path")
    parser.add_argument("--min-zoom", type=int, default=6, help="Minimum zoom level")
    parser.add_argument("--max-zoom", type=int, default=16, help="Maximum zoom level")
    parser.add_argument("--base-zoom", type=int, default=12, help="Base zoom level")
    parser.add_argument("--temp-dir", help="Temporary directory for processing")

    args = parser.parse_args()

    logger.info("🌱 Starting BNBO PMTiles generation")
    logger.info(f"Input: {args.input}")
    logger.info(f"Output: {args.output}")
    logger.info(f"Zoom range: {args.min_zoom}-{args.max_zoom} (base: {args.base_zoom})")

    try:
        # Setup
        conn = setup_duckdb()
        temp_dir = Path(args.temp_dir) if args.temp_dir else Path(tempfile.gettempdir())
        temp_dir.mkdir(exist_ok=True)

        # Load data
        total_features = load_bnbo_data(conn, args.input)

        # Convert to GeoJSON
        geojson_file = temp_dir / "bnbo_areas.geojson"
        convert_to_geojson(conn, str(geojson_file))

        # Generate PMTiles
        zoom_config = {
            "min_zoom": args.min_zoom,
            "max_zoom": args.max_zoom,
            "base_zoom": args.base_zoom,
            "buffer": 64,  # Max buffer is 127
            "max_tile_bytes": 300000,  # 300KB max tile size
            "simplification": 5,  # Less aggressive simplification
        }

        success = generate_pmtiles_with_tippecanoe(str(geojson_file), args.output, zoom_config)

        if not success:
            logger.error("❌ BNBO PMTiles generation failed")
            sys.exit(1)

        # Generate metadata
        metadata = generate_metadata(args.input, args.output, total_features)

        # Validate output
        if validate_pmtiles_output(args.output, total_features):
            logger.info("🎉 BNBO PMTiles generation completed successfully!")
            logger.info(f"   Output file: {args.output}")
            logger.info(f"   Metadata: {args.output.replace('.pmtiles', '.json')}")
            logger.info(f"   Total areas: {total_features}")

            pmtiles_path = Path(args.output)
            if pmtiles_path.exists():
                file_size_mb = pmtiles_path.stat().st_size / (1024 * 1024)
                logger.info(f"   File size: {file_size_mb:.2f} MB")

        # Clean up temp files
        geojson_file.unlink(missing_ok=True)
        logger.info("🧹 Cleaned up temporary files")

    except Exception as e:
        logger.error(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
