"""Shared utilities for PMTiles generation."""

import asyncio
import json
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class TippecanoeRunner:
    """Utility class for running tippecanoe commands."""

    def __init__(self, temp_dir: str = "/tmp"):
        """Initialize the tippecanoe runner.

        Args:
            temp_dir: Directory for temporary files
        """
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    async def generate_pmtiles(
        self,
        geojson_path: str,
        output_path: str,
        layer_name: str,
        max_zoom: int = 14,
        min_zoom: int = 0,
        buffer: int = 64,
        simplification: int = 10,
        additional_args: Optional[List[str]] = None,
    ) -> bool:
        """Generate PMTiles from GeoJSON using tippecanoe.

        Args:
            geojson_path: Path to input GeoJSON file
            output_path: Path for output PMTiles file
            layer_name: Name of the layer
            max_zoom: Maximum zoom level
            min_zoom: Minimum zoom level
            buffer: Buffer size
            simplification: Simplification level
            additional_args: Additional tippecanoe arguments

        Returns:
            True if successful, False otherwise
        """
        try:
            # Build tippecanoe command
            cmd = [
                "tippecanoe",
                f"--output={output_path}",
                f"--maximum-zoom={max_zoom}",
                f"--minimum-zoom={min_zoom}",
                f"--buffer={buffer}",
                f"--simplification={simplification}",
                f"--layer={layer_name}",
                "--detect-shared-borders",
                "--extend-zooms-if-still-dropping",
                "--force",  # Overwrite existing files
                geojson_path,
            ]

            # Add additional arguments if provided
            if additional_args:
                cmd.extend(additional_args)

            logger.info(f"Running tippecanoe: {' '.join(cmd)}")

            # Run tippecanoe
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                logger.info(f"Successfully generated PMTiles: {output_path}")

                # Log file size
                if os.path.exists(output_path):
                    size_mb = os.path.getsize(output_path) / (1024 * 1024)
                    logger.info(f"PMTiles file size: {size_mb:.1f} MB")

                return True
            else:
                logger.error(f"Tippecanoe failed with return code {process.returncode}")
                logger.error(f"stdout: {stdout.decode()}")
                logger.error(f"stderr: {stderr.decode()}")
                return False

        except Exception as e:
            logger.error(f"Error running tippecanoe: {e}")
            return False

    def check_tippecanoe_available(self) -> bool:
        """Check if tippecanoe is available on the system.

        Returns:
            True if tippecanoe is available, False otherwise
        """
        try:
            result = subprocess.run(
                ["tippecanoe", "--version"], capture_output=True, text=True, timeout=10
            )

            if result.returncode == 0:
                logger.info(f"Tippecanoe version: {result.stdout.strip()}")
                return True
            else:
                logger.error("Tippecanoe not available")
                return False

        except Exception as e:
            logger.error(f"Error checking tippecanoe availability: {e}")
            return False


class GeoJSONWriter:
    """Utility class for writing GeoJSON files from DuckDB query results."""

    @staticmethod
    async def write_geojson_from_query(
        duckdb_conn,
        query: str,
        output_path: str,
        properties_columns: Optional[List[str]] = None,
    ) -> bool:
        """Write GeoJSON file from a DuckDB query result.

        Args:
            duckdb_conn: DuckDB connection
            query: SQL query that returns geometry and properties
            output_path: Path for output GeoJSON file
            properties_columns: List of columns to include as properties
                (if None, include all non-geometry)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Execute query
            result = duckdb_conn.execute(query).fetchall()
            columns = [desc[0] for desc in duckdb_conn.description]

            if not result:
                logger.warning(f"Query returned no results for {output_path}")
                return False

            # Find geometry column
            geometry_col = None
            for col in columns:
                if col.lower() in ["geometry", "geom", "wkt", "geometry_wkt"]:
                    geometry_col = col
                    break

            if not geometry_col:
                logger.error(f"No geometry column found in query results for {output_path}")
                return False

            # Determine property columns
            if properties_columns is None:
                properties_columns = [col for col in columns if col != geometry_col]

            # Build GeoJSON
            features = []
            geometry_col_idx = columns.index(geometry_col)

            skipped_count = 0
            geometry_errors = []

            for row in result:
                # Parse geometry (assuming WKT format)
                geometry_wkt = row[geometry_col_idx]
                if not geometry_wkt:
                    skipped_count += 1
                    continue

                # Handle geometry - could be WKT, WKB, or already GeoJSON string
                if isinstance(geometry_wkt, str):
                    try:
                        # Try to parse as GeoJSON first (from ST_AsGeoJSON)
                        geometry = json.loads(geometry_wkt)
                    except json.JSONDecodeError:
                        # Fall back to WKT parsing
                        geometry = GeoJSONWriter._wkt_to_geojson_geometry(geometry_wkt)
                        if not geometry and len(geometry_errors) < 5:
                            geometry_errors.append(
                                f"Failed to parse WKT: {str(geometry_wkt)[:100]}..."
                            )
                else:
                    # Handle WKB or other formats
                    geometry = GeoJSONWriter._wkt_to_geojson_geometry(str(geometry_wkt))
                    if not geometry and len(geometry_errors) < 5:
                        geometry_errors.append(
                            f"Failed to parse non-string geometry: {type(geometry_wkt)}"
                        )

                if not geometry:
                    skipped_count += 1
                    continue

                # Build properties
                properties = {}
                for prop_col in properties_columns:
                    if prop_col in columns:
                        col_idx = columns.index(prop_col)
                        properties[prop_col] = row[col_idx]

                # Create feature
                feature = {"type": "Feature", "geometry": geometry, "properties": properties}
                features.append(feature)

            # Create GeoJSON FeatureCollection
            geojson = {"type": "FeatureCollection", "features": features}

            # DEBUG: Log coordinate bounds from actual GeoJSON
            if features:
                try:
                    # Get first feature's first coordinate to check format
                    first_feature = features[0]
                    first_coord = first_feature["geometry"]["coordinates"]
                    if first_feature["geometry"]["type"] == "MultiPolygon":
                        sample_coord = first_coord[0][0][0]  # [lon, lat]
                    elif first_feature["geometry"]["type"] == "Polygon":
                        sample_coord = first_coord[0][0]  # [lon, lat]
                    else:
                        sample_coord = first_coord  # Point [lon, lat]

                    logger.info(
                        f"DEBUG: First coordinate in GeoJSON: "\n                        f"[{sample_coord[0]:.6f}, {sample_coord[1]:.6f}]"
                    )
                    if sample_coord[0] > 50:
                        logger.error("❌ GeoJSON has lat,lon order (WRONG) - first value > 50")
                    else:
                        logger.info("✅ GeoJSON has lon,lat order (CORRECT) - first value < 50")
                except Exception as coord_debug_error:
                    logger.warning(f"Could not debug GeoJSON coordinates: {coord_debug_error}")

            # Write to file
            with open(output_path, "w") as f:
                json.dump(geojson, f, separators=(",", ":"))  # Compact format

            logger.info(f"Wrote {len(features)} features to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Error writing GeoJSON to {output_path}: {e}")
            return False

    @staticmethod
    def _wkt_to_geojson_geometry(wkt: str) -> Optional[Dict[str, Any]]:
        """Convert WKT to GeoJSON geometry (simplified implementation).

        Args:
            wkt: Well-Known Text geometry string

        Returns:
            GeoJSON geometry dictionary or None if conversion fails
        """
        try:
            # Handle both WKT strings and WKB bytes
            if isinstance(wkt, bytes):
                # This is likely WKB (Well-Known Binary), not WKT
                # For now, skip these - we should use DuckDB's ST_AsGeoJSON instead
                logger.debug("Skipping WKB geometry - should use ST_AsGeoJSON from DuckDB")
                return None

            # This is a very basic WKT parser - in production, use a proper library like Shapely
            wkt = wkt.strip().upper()

            if wkt.startswith("POINT"):
                # Extract coordinates from POINT(x y)
                coords_str = wkt[wkt.find("(") + 1 : wkt.rfind(")")]
                x, y = map(float, coords_str.split())
                return {"type": "Point", "coordinates": [x, y]}

            elif wkt.startswith("POLYGON"):
                # This is a simplified polygon parser
                # In production, use a proper WKT library
                logger.warning("Polygon WKT parsing is simplified - consider using Shapely")
                return {
                    "type": "Polygon",
                    "coordinates": [[]],  # Placeholder
                }

            else:
                logger.warning(f"Unsupported WKT geometry type: {wkt[:20]}...")
                return None

        except Exception as e:
            logger.error(f"Error parsing WKT: {e}")
            return None


class FileManager:
    """Utility class for managing temporary files and directories."""

    def __init__(self, temp_dir: str):
        """Initialize the file manager.

        Args:
            temp_dir: Base temporary directory
        """
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.temp_files = []
        self.temp_dirs = []

    def create_temp_file(self, suffix: str = "", prefix: str = "pmtiles_") -> str:
        """Create a temporary file and track it for cleanup.

        Args:
            suffix: File suffix/extension
            prefix: File prefix

        Returns:
            Path to temporary file
        """
        temp_file = tempfile.NamedTemporaryFile(
            suffix=suffix, prefix=prefix, dir=str(self.temp_dir), delete=False
        )
        temp_file.close()

        self.temp_files.append(temp_file.name)
        return temp_file.name

    def create_temp_dir(self, prefix: str = "pmtiles_") -> str:
        """Create a temporary directory and track it for cleanup.

        Args:
            prefix: Directory prefix

        Returns:
            Path to temporary directory
        """
        temp_dir = tempfile.mkdtemp(prefix=prefix, dir=str(self.temp_dir))
        self.temp_dirs.append(temp_dir)
        return temp_dir

    def cleanup(self):
        """Clean up all temporary files and directories."""
        # Clean up temporary files
        for temp_file in self.temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    logger.debug(f"Removed temporary file: {temp_file}")
            except Exception as e:
                logger.warning(f"Failed to remove temporary file {temp_file}: {e}")

        # Clean up temporary directories
        for temp_dir in self.temp_dirs:
            try:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                    logger.debug(f"Removed temporary directory: {temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to remove temporary directory {temp_dir}: {e}")

        self.temp_files.clear()
        self.temp_dirs.clear()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.cleanup()
