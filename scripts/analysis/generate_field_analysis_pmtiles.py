#!/usr/bin/env python3
"""
Generate PMTiles from comprehensive field analysis data for Kepler.gl visualization.

This script converts the parquet output from recreate_original_csv_structure.py
into optimized vector tiles (PMTiles format) suitable for interactive web visualization.

Features:
- Converts WKT geometry to GeoJSON for web compatibility
- Optimizes properties for different zoom levels
- Generates multi-resolution tiles for smooth zooming
- Creates metadata for frontend consumption
- Validates output quality and performance
"""

import argparse
import json
import logging
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def setup_duckdb() -> duckdb.DuckDBPyConnection:
    """Setup DuckDB with spatial extension."""
    conn = duckdb.connect()

    # Install and load extensions
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")

    # Configure for large data processing
    conn.execute("SET memory_limit='12GB'")
    conn.execute("SET threads=4")
    conn.execute("SET enable_progress_bar=true")

    return conn


def load_field_analysis_data(conn: duckdb.DuckDBPyConnection, input_file: str):
    """Load field analysis data from parquet file."""
    logger.info(f"📥 Loading field analysis data from {input_file}")

    conn.execute(f"""
        CREATE TABLE field_analysis AS
        SELECT * FROM read_parquet('{input_file}')
        WHERE geometry_wkt IS NOT NULL
    """)

    total_count = conn.execute("SELECT COUNT(*) FROM field_analysis").fetchone()[0]
    logger.info(f"   Loaded {total_count:,} fields with geometry")

    return total_count


def get_all_properties(conn: duckdb.DuckDBPyConnection) -> List[str]:
    """
    Get all available properties from the field analysis data.
    Let tippecanoe handle the zoom-level optimization automatically.
    """
    logger.info("📋 Getting all available properties (letting tippecanoe optimize)")

    # Get all column names except geometry_wkt
    available_columns = conn.execute("PRAGMA table_info(field_analysis)").fetchall()
    properties = [col[1] for col in available_columns if col[1] != "geometry_wkt"]

    logger.info(f"   Found {len(properties)} properties to include in PMTiles")
    logger.info("   Tippecanoe will automatically optimize for different zoom levels")

    return properties


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
    """Convert field data to GeoJSON format with all properties."""
    logger.info("🗺️  Converting field analysis data to GeoJSON")

    # Use a simpler approach - export to JSON and then convert to GeoJSON
    temp_file = output_file + ".tmp"

    # Export all data as JSON first (using the main table, not zoom-specific)
    conn.execute(f"""
        COPY (
            SELECT
                ST_AsGeoJSON(ST_GeomFromText(geometry_wkt)) as geometry,
                * EXCLUDE geometry_wkt
            FROM field_analysis
            WHERE geometry_wkt IS NOT NULL
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
        json.dump(feature_collection, f, separators=(",", ":"))  # Compact JSON

    # Clean up temp file
    Path(temp_file).unlink(missing_ok=True)

    logger.info(f"   Generated GeoJSON with {len(features):,} features")
    return len(features)


def generate_pmtiles_with_tippecanoe(geojson_file: str, output_pmtiles: str, zoom_config: Dict[str, Any]) -> bool:
    """Generate PMTiles using tippecanoe with automatic optimization."""
    logger.info(f"🏗️  Generating PMTiles with automatic optimization: {output_pmtiles}")

    # Build tippecanoe command with smart defaults
    cmd = [
        "tippecanoe",
        "-o",
        output_pmtiles,
        "--force",  # Overwrite existing file
        f"--minimum-zoom={zoom_config['min_zoom']}",
        f"--maximum-zoom={zoom_config['max_zoom']}",
        f"--base-zoom={zoom_config['base_zoom']}",
        # Automatic optimization features (recommended approach)
        "--drop-densest-as-needed",  # Drop densest features when tiles get large
        "--extend-zooms-if-still-dropping",  # Add zoom levels if still dropping features
        "--drop-fraction-as-needed",  # Drop fraction of features at low zoom
        f"--maximum-tile-bytes={zoom_config.get('max_tile_bytes', 500000)}",  # 500KB max tile size
        # Geometry optimization
        f"--simplification={zoom_config.get('simplification', 10)}",  # Smart geometry simplification
        "--detect-shared-borders",  # Optimize shared polygon borders
        "--reorder",  # Reorder features for better compression
        # Buffer and layer settings
        f"--buffer={zoom_config.get('buffer', 64)}",  # Tile buffer in pixels
        "--layer=fields",  # Layer name
        geojson_file,
    ]

    logger.info("   Using tippecanoe's automatic optimization:")
    logger.info("   - Drop densest features as needed")
    logger.info("   - Extend zoom levels if still dropping")
    logger.info("   - Smart geometry simplification")
    logger.info(f"   - Max tile size: {zoom_config.get('max_tile_bytes', 500000)} bytes")
    logger.info(f"   Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info("   ✅ PMTiles generated successfully with automatic optimization")
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


def generate_metadata(
    input_file: str, output_pmtiles: str, properties: List[str], total_features: int
) -> Dict[str, Any]:
    """Generate metadata for the PMTiles file."""
    logger.info("📋 Generating metadata")

    pmtiles_path = Path(output_pmtiles)

    metadata = {
        "name": "Danish Agricultural Fields - Comprehensive Analysis",
        "description": "Comprehensive field-level analysis of Danish agriculture with environmental data",
        "version": "1.0.0",
        "source": {
            "input_file": str(Path(input_file).name),
            "generated_at": pd.Timestamp.now().isoformat(),
            "total_features": total_features,
        },
        "pmtiles": {
            "file": str(pmtiles_path.name),
            "size_mb": round(pmtiles_path.stat().st_size / (1024 * 1024), 2) if pmtiles_path.exists() else 0,
        },
        "zoom_levels": {"min": 4, "max": 14, "base": 10},
        "layer_properties": {
            "all_zoom_levels": properties,
            "total_properties": len(properties),
            "optimization": "tippecanoe_automatic",
        },
        "data_summary": {
            "total_fields": total_features,
            "coverage": "All of Denmark",
            "year": 2024,
            "includes": [
                "Field boundaries and areas",
                "Pesticide applications (PFAS, diquat, glyphosate)",
                "Environmental areas (BNBO, wetlands)",
                "Soil type information",
                "Building proximity data",
                "Organic farming status",
                "Crop type information",
            ],
        },
        "usage": {
            "recommended_initial_zoom": 7,
            "recommended_center": [56.26392, 9.501785],  # Center of Denmark
            "layer_name": "fields",
            "geometry_type": "Polygon",
        },
    }

    # Save metadata
    metadata_file = pmtiles_path.with_suffix(".json")
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"   Metadata saved to: {metadata_file}")
    return metadata


def validate_pmtiles_output(pmtiles_file: str, expected_features: int) -> bool:
    """Validate the generated PMTiles file."""
    logger.info("✅ Validating PMTiles output")

    pmtiles_path = Path(pmtiles_file)

    # Check file exists and size
    if not pmtiles_path.exists():
        logger.error("   ❌ PMTiles file not found")
        return False

    file_size_mb = pmtiles_path.stat().st_size / (1024 * 1024)
    logger.info(f"   File size: {file_size_mb:.2f} MB")

    # Basic size validation (should be reasonable for 600k+ features)
    if file_size_mb < 10:
        logger.warning("   ⚠️  File size seems small - check data completeness")
    elif file_size_mb > 500:
        logger.warning("   ⚠️  File size is large - consider optimization")
    else:
        logger.info("   ✅ File size looks reasonable")

    # TODO: Add more sophisticated validation using pmtiles CLI if available

    return True


def main():
    parser = argparse.ArgumentParser(description="Generate PMTiles from comprehensive field analysis data")
    parser.add_argument("--input", required=True, help="Input parquet file from recreate_original_csv_structure.py")
    parser.add_argument("--output", required=True, help="Output PMTiles file path")
    parser.add_argument("--min-zoom", type=int, default=4, help="Minimum zoom level (default: 4)")
    parser.add_argument("--max-zoom", type=int, default=14, help="Maximum zoom level (default: 14)")
    parser.add_argument("--base-zoom", type=int, default=10, help="Base zoom level for optimal detail (default: 10)")
    parser.add_argument("--temp-dir", help="Temporary directory for intermediate files (default: system temp)")

    args = parser.parse_args()

    try:
        logger.info("🚀 Starting PMTiles generation for field analysis data")
        logger.info(f"Input: {args.input}")
        logger.info(f"Output: {args.output}")
        logger.info(f"Zoom range: {args.min_zoom}-{args.max_zoom} (base: {args.base_zoom})")

        # Setup
        conn = setup_duckdb()
        temp_dir = Path(args.temp_dir) if args.temp_dir else Path(tempfile.gettempdir())
        temp_dir.mkdir(exist_ok=True)

        # Load data
        total_features = load_field_analysis_data(conn, args.input)

        # Get all properties (let tippecanoe optimize)
        properties = get_all_properties(conn)

        # Convert to GeoJSON with all properties
        geojson_file = temp_dir / "field_analysis.geojson"
        convert_to_geojson(conn, str(geojson_file))

        # Generate PMTiles
        zoom_config = {
            "min_zoom": args.min_zoom,
            "max_zoom": args.max_zoom,
            "base_zoom": args.base_zoom,
            "buffer": 64,
            "max_tile_bytes": 500000,  # 500KB max tile size
            "simplification": 10,  # Geometry simplification factor
        }

        success = generate_pmtiles_with_tippecanoe(str(geojson_file), args.output, zoom_config)

        if not success:
            logger.error("❌ PMTiles generation failed")
            sys.exit(1)

        # Generate metadata
        metadata = generate_metadata(args.input, args.output, properties, total_features)

        # Validate output
        if validate_pmtiles_output(args.output, total_features):
            logger.info("🎉 PMTiles generation completed successfully!")
            logger.info(f"   Output file: {args.output}")
            logger.info(f"   Metadata: {Path(args.output).with_suffix('.json')}")
            logger.info(f"   Total features: {total_features:,}")
            logger.info(f"   File size: {metadata['pmtiles']['size_mb']} MB")
        else:
            logger.error("❌ PMTiles validation failed")
            sys.exit(1)

        # Cleanup
        if geojson_file.exists():
            geojson_file.unlink()
            logger.info("🧹 Cleaned up temporary files")

    except Exception as e:
        logger.error(f"❌ Error: {e}")
        sys.exit(1)
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
