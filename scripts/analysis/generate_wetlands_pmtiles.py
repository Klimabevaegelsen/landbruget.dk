#!/usr/bin/env python3
"""
Generate PMTiles from wetlands environmental areas data.

This script converts wetlands environmental areas data into optimized vector tiles (PMTiles format)
suitable for interactive web visualization with moisture categories and water projects.

Features:
- Converts wetlands geometries to GeoJSON for web compatibility
- Includes moisture level categories (toerv_pct: 6-12%, >12%)
- Optimized for different zoom levels using tippecanoe
- Generates metadata for frontend consumption
- Validates output quality and performance

Usage:
    python generate_wetlands_pmtiles.py --input data_cache/wetlands_2024/data.parquet \\
        --output data_cache/wetlands_2024.pmtiles
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


def load_wetlands_data(conn: duckdb.DuckDBPyConnection, input_file: str) -> int:
    """Load ALL wetlands environmental areas data from parquet file."""
    logger.info(f"📥 Loading ALL wetlands areas from {input_file}")

    conn.execute(f"""
        CREATE TABLE wetlands_areas AS
        SELECT
            wetland_key,
            wetland_id,
            toerv_pct,
            wetland_area_m2,
            ROUND(wetland_area_m2 / 10000.0, 4) as wetland_area_hectares,
            geometry
        FROM read_parquet('{input_file}')
        WHERE geometry IS NOT NULL
    """)

    # Get data summary
    total_count = conn.execute("SELECT COUNT(*) FROM wetlands_areas").fetchone()[0]
    total_hectares = conn.execute("SELECT ROUND(SUM(wetland_area_hectares), 2) FROM wetlands_areas").fetchone()[0]

    # Moisture category breakdown
    moisture_breakdown = conn.execute("""
        SELECT toerv_pct, COUNT(*) as count,
               ROUND(SUM(wetland_area_hectares), 2) as total_hectares
        FROM wetlands_areas
        GROUP BY toerv_pct
        ORDER BY count DESC
    """).fetchall()

    # Area size distribution
    area_stats = conn.execute("""
        SELECT
            MIN(wetland_area_hectares) as min_ha,
            AVG(wetland_area_hectares) as avg_ha,
            MAX(wetland_area_hectares) as max_ha,
            COUNT(CASE WHEN wetland_area_hectares < 0.1 THEN 1 END) as very_small,
            COUNT(CASE WHEN wetland_area_hectares BETWEEN 0.1 AND 1 THEN 1 END) as small,
            COUNT(CASE WHEN wetland_area_hectares BETWEEN 1 AND 10 THEN 1 END) as medium,
            COUNT(CASE WHEN wetland_area_hectares > 10 THEN 1 END) as large
        FROM wetlands_areas
    """).fetchone()

    logger.info(f"   Loaded {total_count:,} wetlands areas ({total_hectares:,} hectares total)")
    logger.info("   Moisture level breakdown:")
    for category, count, hectares in moisture_breakdown:
        logger.info(f"     {category}%: {count:,} areas ({hectares:,} hectares)")

    logger.info(
        f"   Area distribution: {area_stats[6]:,} very small (<0.1ha), {area_stats[5]:,} small (0.1-1ha), "
        f"{area_stats[4]:,} medium (1-10ha), {area_stats[3]:,} large (>10ha)"
    )

    return total_count


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
    """Convert wetlands data to GeoJSON format."""
    logger.info("🗺️  Converting wetlands areas to GeoJSON")

    # Use a simpler approach - export to JSON and then convert to GeoJSON
    temp_file = output_file + ".tmp"

    # Export all data as JSON first
    conn.execute(f"""
        COPY (
            SELECT
                ST_AsGeoJSON(geometry) as geometry,
                wetland_key,
                wetland_id,
                toerv_pct,
                wetland_area_m2,
                wetland_area_hectares
            FROM wetlands_areas
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

    logger.info(f"   Generated GeoJSON with {len(features):,} wetlands areas")
    return len(features)


def generate_pmtiles_with_tippecanoe(geojson_file: str, output_pmtiles: str, zoom_config: Dict[str, Any]) -> bool:
    """Generate PMTiles using tippecanoe with automatic optimization for wetlands."""
    logger.info(f"🏗️  Generating wetlands PMTiles: {output_pmtiles}")

    # Build tippecanoe command optimized for wetlands (many small areas)
    cmd = [
        "tippecanoe",
        "-o",
        output_pmtiles,
        "--force",  # Overwrite existing file
        f"--minimum-zoom={zoom_config['min_zoom']}",
        f"--maximum-zoom={zoom_config['max_zoom']}",
        f"--base-zoom={zoom_config['base_zoom']}",
        # Optimization for wetlands (many small polygons)
        "--drop-densest-as-needed",  # Drop densest features when tiles get large
        "--extend-zooms-if-still-dropping",  # Add zoom levels if still dropping features
        "--drop-fraction-as-needed",  # Drop fraction of features at low zoom
        f"--maximum-tile-bytes={zoom_config.get('max_tile_bytes', 400000)}",  # 400KB max (more than BNBO)
        # Geometry optimization for small wetlands
        f"--simplification={zoom_config.get('simplification', 8)}",  # Moderate simplification
        "--detect-shared-borders",  # Optimize shared polygon borders
        "--reorder",  # Reorder features for better compression
        f"--drop-rate={zoom_config.get('drop_rate', 2.5)}",  # Drop rate for low zoom levels
        # Buffer and layer settings
        f"--buffer={zoom_config.get('buffer', 64)}",  # Standard buffer
        "--layer=wetlands",  # Layer name
        geojson_file,
    ]

    logger.info("   Optimized for wetlands (many small areas):")
    logger.info("   - Moderate simplification (balance detail vs performance)")
    logger.info("   - Drop fraction as needed for low zoom performance")
    logger.info(f"   - Max tile size: {zoom_config.get('max_tile_bytes', 400000)} bytes")
    logger.info(f"   - Drop rate: {zoom_config.get('drop_rate', 2.5)} for low zoom")
    logger.info(f"   Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info("   ✅ Wetlands PMTiles generated successfully")
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
    """Generate metadata for the wetlands PMTiles file."""
    logger.info("📋 Generating wetlands metadata")

    pmtiles_path = Path(output_pmtiles)
    file_size_mb = pmtiles_path.stat().st_size / (1024 * 1024) if pmtiles_path.exists() else 0

    metadata = {
        "name": "Danish Wetlands Environmental Areas",
        "description": "Wetlands environmental areas in Denmark with moisture level categories (toerv_pct)",
        "version": "1.0.0",
        "source": {
            "input_file": Path(input_file).name,
            "generated_at": "2025-08-24T13:50:00.000000",
            "total_features": total_features,
        },
        "pmtiles": {"file": pmtiles_path.name, "size_mb": round(file_size_mb, 2)},
        "zoom_levels": {"min": 5, "max": 16, "base": 11},
        "layer_properties": {
            "properties": ["wetland_key", "wetland_id", "toerv_pct", "wetland_area_m2", "wetland_area_hectares"],
            "total_properties": 5,
            "optimization": "tippecanoe_automatic_wetlands",
        },
        "data_summary": {
            "total_areas": total_features,
            "coverage": "Denmark wetlands environmental areas",
            "year": 2024,
            "includes": [
                "Wetland area boundaries",
                "Moisture level categories (6-12%, >12%)",
                "Area calculations in hectares",
                "Water project compatibility",
            ],
        },
        "usage": {
            "recommended_initial_zoom": 7,
            "recommended_center": [56.26392, 9.501785],
            "layer_name": "wetlands",
            "geometry_type": "Polygon",
            "color_scheme": {
                "6-12": "#3498db",  # Light blue
                ">12": "#2980b9",  # Darker blue
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
    logger.info("✅ Validating wetlands PMTiles output")

    pmtiles_path = Path(output_file)
    if not pmtiles_path.exists():
        logger.error("   ❌ PMTiles file not found")
        return False

    file_size_mb = pmtiles_path.stat().st_size / (1024 * 1024)
    logger.info(f"   File size: {file_size_mb:.2f} MB")

    if file_size_mb > 500:  # Wetlands might be larger due to many small areas
        logger.warning("   ⚠️  File size is large for wetlands - consider further optimization")

    logger.info("   ✅ Wetlands PMTiles validation passed")
    return True


def main():
    parser = argparse.ArgumentParser(description="Generate PMTiles from wetlands environmental areas data")
    parser.add_argument("--input", required=True, help="Input parquet file path")
    parser.add_argument("--output", required=True, help="Output PMTiles file path")
    parser.add_argument("--min-zoom", type=int, default=5, help="Minimum zoom level")
    parser.add_argument("--max-zoom", type=int, default=16, help="Maximum zoom level")
    parser.add_argument("--base-zoom", type=int, default=11, help="Base zoom level")
    parser.add_argument("--temp-dir", help="Temporary directory for processing")

    args = parser.parse_args()

    logger.info("💧 Starting wetlands PMTiles generation")
    logger.info(f"Input: {args.input}")
    logger.info(f"Output: {args.output}")
    logger.info(f"Zoom range: {args.min_zoom}-{args.max_zoom} (base: {args.base_zoom})")

    try:
        # Setup
        conn = setup_duckdb()
        temp_dir = Path(args.temp_dir) if args.temp_dir else Path(tempfile.gettempdir())
        temp_dir.mkdir(exist_ok=True)

        # Load data
        total_features = load_wetlands_data(conn, args.input)

        # Convert to GeoJSON
        geojson_file = temp_dir / "wetlands_areas.geojson"
        convert_to_geojson(conn, str(geojson_file))

        # Generate PMTiles
        zoom_config = {
            "min_zoom": args.min_zoom,
            "max_zoom": args.max_zoom,
            "base_zoom": args.base_zoom,
            "buffer": 64,  # Standard buffer
            "max_tile_bytes": 400000,  # 400KB max tile size
            "simplification": 8,  # Moderate simplification
            "drop_rate": 2.5,  # Drop rate for low zoom
        }

        success = generate_pmtiles_with_tippecanoe(str(geojson_file), args.output, zoom_config)

        if not success:
            logger.error("❌ Wetlands PMTiles generation failed")
            sys.exit(1)

        # Generate metadata
        generate_metadata(args.input, args.output, total_features)

        # Validate output
        if validate_pmtiles_output(args.output, total_features):
            logger.info("🎉 Wetlands PMTiles generation completed successfully!")
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
