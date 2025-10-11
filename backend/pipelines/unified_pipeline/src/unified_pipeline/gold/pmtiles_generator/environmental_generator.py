"""Environmental Layers PMTiles Generator."""

import logging
import os
from datetime import datetime
from typing import Dict, Optional

import duckdb

from .config import PMTilesGeneratorConfig
from .data_loader import PMTilesDataLoader
from .utils import FileManager, GeoJSONWriter, TippecanoeRunner

logger = logging.getLogger(__name__)


class EnvironmentalLayersPMTilesGenerator:
    """Generates environmental layers PMTiles (BNBO, wetlands, water projects)."""

    def __init__(
        self,
        config: PMTilesGeneratorConfig,
        data_loader: PMTilesDataLoader,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ):
        """Initialize the environmental layers generator.

        Args:
            config: PMTiles generator configuration
            data_loader: Data loader instance
            duckdb_conn: DuckDB connection
        """
        self.config = config
        self.data_loader = data_loader
        self.conn = duckdb_conn
        self.tippecanoe = TippecanoeRunner(self.config.temp_dir)

    async def generate_all_environmental_pmtiles(self) -> Dict[str, Optional[str]]:
        """Generate all environmental layer PMTiles.

        Returns:
            Dictionary mapping layer names to PMTiles file paths (None if failed)
        """
        logger.info("Generating all environmental layer PMTiles")

        # Load environmental layers data
        environmental_layers = await self.data_loader.load_environmental_layers()

        results = {}

        # Generate BNBO areas PMTiles
        if "bnbo_status" in environmental_layers:
            results["bnbo_areas"] = await self._generate_bnbo_pmtiles(
                environmental_layers["bnbo_status"]
            )

        # Generate wetlands PMTiles
        if "wetlands" in environmental_layers:
            results["wetlands_all"] = await self._generate_wetlands_pmtiles(
                environmental_layers["wetlands"]
            )

        # Generate water projects PMTiles
        if "water_projects" in environmental_layers:
            results["water_projects"] = await self._generate_water_projects_pmtiles(
                environmental_layers["water_projects"]
            )

        # Log summary
        successful = sum(1 for path in results.values() if path is not None)
        logger.info(f"Generated {successful}/{len(results)} environmental layer PMTiles")

        return results

    async def _generate_bnbo_pmtiles(self, table_name: str) -> Optional[str]:
        """Generate BNBO areas PMTiles.

        Args:
            table_name: Name of the BNBO status table

        Returns:
            Path to generated PMTiles file or None if failed
        """
        logger.info("Generating BNBO areas PMTiles")

        with FileManager(self.config.temp_dir) as file_manager:
            try:
                # Generate GeoJSON
                geojson_path = file_manager.create_temp_file(
                    suffix=".geojson", prefix="bnbo_areas_"
                )

                success = await self._export_bnbo_geojson(table_name, geojson_path)
                if not success:
                    logger.error("Failed to export BNBO GeoJSON")
                    return None

                # Generate PMTiles
                pmtiles_path = file_manager.create_temp_file(
                    suffix=".pmtiles", prefix="bnbo_areas_"
                )

                success = await self.tippecanoe.generate_pmtiles(
                    geojson_path=geojson_path,
                    output_path=pmtiles_path,
                    layer_name="bnbo",
                    max_zoom=12,  # Lower zoom for environmental layers
                    min_zoom=0,
                    buffer=32,  # Smaller buffer for environmental layers
                    simplification=5,  # More aggressive simplification
                    additional_args=["--drop-densest-as-needed"],
                )

                if not success:
                    logger.error("Failed to generate BNBO PMTiles")
                    return None

                # Move to final location
                final_path = os.path.join(
                    os.path.dirname(self.config.temp_dir), "bnbo_areas.pmtiles"
                )

                import shutil

                shutil.copy2(pmtiles_path, final_path)

                # Log file size
                if os.path.exists(final_path):
                    size_mb = os.path.getsize(final_path) / (1024 * 1024)
                    logger.info(f"Generated BNBO areas PMTiles: {size_mb:.1f} MB")

                return final_path

            except Exception as e:
                logger.error(f"Error generating BNBO PMTiles: {e}")
                return None

    async def _export_bnbo_geojson(self, table_name: str, output_path: str) -> bool:
        """Export BNBO status data as GeoJSON.

        Args:
            table_name: Name of the BNBO status table
            output_path: Path for output GeoJSON file

        Returns:
            True if successful, False otherwise
        """
        try:
            query = f"""
            SELECT
                status_category,
                CASE
                    WHEN status_category = 'Action Required' THEN '#ff4444'
                    WHEN status_category = 'Completed' THEN '#44ff44'
                    ELSE '#888888'
                END as color,
                CASE
                    WHEN status_category = 'Action Required' THEN 'Kræver handling'
                    WHEN status_category = 'Completed' THEN 'Gennemført'
                    ELSE 'Ukendt status'
                END as status_danish,
                ST_AsGeoJSON(ST_FlipCoordinates(geometry)) as geometry
            FROM {table_name}
            WHERE geometry IS NOT NULL
            """

            return await GeoJSONWriter.write_geojson_from_query(
                self.conn, query, output_path, ["status_category", "color", "status_danish"]
            )

        except Exception as e:
            logger.error(f"Error exporting BNBO GeoJSON: {e}")
            return False

    async def _generate_wetlands_pmtiles(self, table_name: str) -> Optional[str]:
        """Generate wetlands PMTiles.

        Args:
            table_name: Name of the wetlands table

        Returns:
            Path to generated PMTiles file or None if failed
        """
        logger.info("Generating wetlands PMTiles")

        with FileManager(self.config.temp_dir) as file_manager:
            try:
                # Generate GeoJSON
                geojson_path = file_manager.create_temp_file(suffix=".geojson", prefix="wetlands_")

                success = await self._export_wetlands_geojson(table_name, geojson_path)
                if not success:
                    logger.error("Failed to export wetlands GeoJSON")
                    return None

                # Generate PMTiles
                pmtiles_path = file_manager.create_temp_file(suffix=".pmtiles", prefix="wetlands_")

                success = await self.tippecanoe.generate_pmtiles(
                    geojson_path=geojson_path,
                    output_path=pmtiles_path,
                    layer_name="wetlands",
                    max_zoom=12,
                    min_zoom=0,
                    buffer=32,
                    simplification=5,
                    additional_args=["--drop-densest-as-needed", "--drop-fraction-as-needed"],
                )

                if not success:
                    logger.error("Failed to generate wetlands PMTiles")
                    return None

                # Move to final location
                final_path = os.path.join(
                    os.path.dirname(self.config.temp_dir),
                    f"wetlands_all_{datetime.now().year}.pmtiles",
                )

                import shutil

                shutil.copy2(pmtiles_path, final_path)

                # Log file size
                if os.path.exists(final_path):
                    size_mb = os.path.getsize(final_path) / (1024 * 1024)
                    logger.info(f"Generated wetlands PMTiles: {size_mb:.1f} MB")

                    if size_mb > self.config.max_environmental_layer_size_mb:
                        logger.warning(
                            f"Wetlands PMTiles size ({size_mb:.1f} MB) exceeds target "
                            f"({self.config.max_environmental_layer_size_mb} MB)"
                        )

                return final_path

            except Exception as e:
                logger.error(f"Error generating wetlands PMTiles: {e}")
                return None

    async def _export_wetlands_geojson(self, table_name: str, output_path: str) -> bool:
        """Export wetlands data as GeoJSON.

        Args:
            table_name: Name of the wetlands table
            output_path: Path for output GeoJSON file

        Returns:
            True if successful, False otherwise
        """
        try:
            query = f"""
            SELECT
                wetland_id,
                toerv_pct,
                CASE
                    WHEN toerv_pct = '>12' THEN '#003d82'
                    WHEN toerv_pct = '6-12' THEN '#0066cc'
                    WHEN toerv_pct = '3-6' THEN '#3399ff'
                    WHEN toerv_pct = '<3' THEN '#66ccff'
                    ELSE '#cccccc'
                END as color,
                CASE
                    WHEN toerv_pct = '>12' THEN 'Meget høj tørv (>12%)'
                    WHEN toerv_pct = '6-12' THEN 'Høj tørv (6-12%)'
                    WHEN toerv_pct = '3-6' THEN 'Moderat tørv (3-6%)'
                    WHEN toerv_pct = '<3' THEN 'Lav tørv (<3%)'
                    ELSE 'Ukendt tørv indhold'
                END as toerv_description,
                ST_AsGeoJSON(ST_FlipCoordinates(geometry)) as geometry
            FROM {table_name}
            WHERE geometry IS NOT NULL
            ORDER BY wetland_id
            """

            return await GeoJSONWriter.write_geojson_from_query(
                self.conn,
                query,
                output_path,
                ["wetland_id", "toerv_pct", "color", "toerv_description"],
            )

        except Exception as e:
            logger.error(f"Error exporting wetlands GeoJSON: {e}")
            return False

    async def _generate_water_projects_pmtiles(self, table_name: str) -> Optional[str]:
        """Generate water projects PMTiles.

        Args:
            table_name: Name of the water projects table

        Returns:
            Path to generated PMTiles file or None if failed
        """
        logger.info("Generating water projects PMTiles")

        with FileManager(self.config.temp_dir) as file_manager:
            try:
                # Generate GeoJSON
                geojson_path = file_manager.create_temp_file(
                    suffix=".geojson", prefix="water_projects_"
                )

                success = await self._export_water_projects_geojson(table_name, geojson_path)
                if not success:
                    logger.error("Failed to export water projects GeoJSON")
                    return None

                # Generate PMTiles
                pmtiles_path = file_manager.create_temp_file(
                    suffix=".pmtiles", prefix="water_projects_"
                )

                success = await self.tippecanoe.generate_pmtiles(
                    geojson_path=geojson_path,
                    output_path=pmtiles_path,
                    layer_name="water_projects",
                    max_zoom=12,
                    min_zoom=0,
                    buffer=32,
                    simplification=5,
                    additional_args=["--drop-densest-as-needed"],
                )

                if not success:
                    logger.error("Failed to generate water projects PMTiles")
                    return None

                # Move to final location
                final_path = os.path.join(
                    os.path.dirname(self.config.temp_dir),
                    f"water_projects_{datetime.now().year}.pmtiles",
                )

                import shutil

                shutil.copy2(pmtiles_path, final_path)

                # Log file size
                if os.path.exists(final_path):
                    size_mb = os.path.getsize(final_path) / (1024 * 1024)
                    logger.info(f"Generated water projects PMTiles: {size_mb:.1f} MB")

                return final_path

            except Exception as e:
                logger.error(f"Error generating water projects PMTiles: {e}")
                return None

    async def _export_water_projects_geojson(self, table_name: str, output_path: str) -> bool:
        """Export water projects data as GeoJSON.

        Args:
            table_name: Name of the water projects table
            output_path: Path for output GeoJSON file

        Returns:
            True if successful, False otherwise
        """
        try:
            query = f"""
            SELECT
                project_id,
                feature_count,
                dissolved_at::varchar as dissolved_at,
                '#4a90e2' as color,
                'Vandprojekt' as project_type_danish,
                ST_AsGeoJSON(ST_FlipCoordinates(geometry)) as geometry
            FROM {table_name}
            WHERE geometry IS NOT NULL
            """

            return await GeoJSONWriter.write_geojson_from_query(
                self.conn,
                query,
                output_path,
                ["project_id", "feature_count", "dissolved_at", "color", "project_type_danish"],
            )

        except Exception as e:
            logger.error(f"Error exporting water projects GeoJSON: {e}")
            return False
