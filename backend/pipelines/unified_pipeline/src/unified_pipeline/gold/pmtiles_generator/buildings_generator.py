"""Buildings Proximity PMTiles Generator."""

import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

import duckdb

from .config import PMTilesGeneratorConfig
from .data_loader import PMTilesDataLoader
from .utils import FileManager, GeoJSONWriter, TippecanoeRunner

logger = logging.getLogger(__name__)


class BuildingsProximityPMTilesGenerator:
    """Generates buildings proximity PMTiles for residential and public service buildings."""

    def __init__(
        self,
        config: PMTilesGeneratorConfig,
        data_loader: PMTilesDataLoader,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ):
        """Initialize the buildings proximity generator.

        Args:
            config: PMTiles generator configuration
            data_loader: Data loader instance
            duckdb_conn: DuckDB connection
        """
        self.config = config
        self.data_loader = data_loader
        self.conn = duckdb_conn
        self.tippecanoe = TippecanoeRunner(self.config.temp_dir)

    async def generate_buildings_proximity_pmtiles(self) -> Optional[str]:
        """Generate buildings proximity PMTiles.

        Returns:
            Path to generated PMTiles file or None if failed
        """
        logger.info("Generating buildings proximity PMTiles")

        with FileManager(self.config.temp_dir) as file_manager:
            try:
                # Load environmental layers to get buildings data
                environmental_layers = await self.data_loader.load_environmental_layers()

                if "bbr_buildings" not in environmental_layers:
                    logger.error("BBR buildings data not available")
                    return None

                buildings_table = environmental_layers["bbr_buildings"]

                # Generate GeoJSON
                geojson_path = file_manager.create_temp_file(
                    suffix=".geojson", prefix="buildings_proximity_"
                )

                success = await self._export_buildings_geojson(buildings_table, geojson_path)
                if not success:
                    logger.error("Failed to export buildings GeoJSON")
                    return None

                # Generate PMTiles
                pmtiles_path = file_manager.create_temp_file(
                    suffix=".pmtiles", prefix="buildings_proximity_"
                )

                success = await self.tippecanoe.generate_pmtiles(
                    geojson_path=geojson_path,
                    output_path=pmtiles_path,
                    layer_name="buildings",
                    max_zoom=14,  # Higher zoom for building points
                    min_zoom=8,  # Don't show at very low zoom levels
                    buffer=64,
                    simplification=1,  # Minimal simplification for points
                    additional_args=self._get_buildings_tippecanoe_args(),
                )

                if not success:
                    logger.error("Failed to generate buildings proximity PMTiles")
                    return None

                # Move to final location
                final_path = os.path.join(
                    os.path.dirname(self.config.temp_dir),
                    f"buildings_proximity_{datetime.now().year}.pmtiles",
                )

                import shutil

                shutil.copy2(pmtiles_path, final_path)

                # Log file size
                if os.path.exists(final_path):
                    size_mb = os.path.getsize(final_path) / (1024 * 1024)
                    logger.info(f"Generated buildings proximity PMTiles: {size_mb:.1f} MB")

                return final_path

            except Exception as e:
                logger.error(f"Error generating buildings proximity PMTiles: {e}")
                return None

    async def _export_buildings_geojson(self, table_name: str, output_path: str) -> bool:
        """Export buildings data as GeoJSON with proximity-relevant attributes.

        Args:
            table_name: Name of the buildings table
            output_path: Path for output GeoJSON file

        Returns:
            True if successful, False otherwise
        """
        try:
            query = f"""
            SELECT
                building_uuid,
                category_group,
                building_type,
                building_usage_category,
                inspire_current_use,
                address,
                address_full,
                building_floor_area_sqm,
                inspire_construction_year,
                inspire_floors,
                inspire_dwellings,
                CASE
                    WHEN category_group = 'residential' THEN '#ff6b6b'
                    WHEN category_group = 'publicServices' THEN '#45b7d1'
                    WHEN category_group = 'agricultural' THEN '#4ecdc4'
                    ELSE '#888888'
                END as color,
                CASE
                    WHEN category_group = 'residential' THEN 'Boligbyggeri'
                    WHEN category_group = 'publicServices' THEN 'Offentlig service'
                    WHEN category_group = 'agricultural' THEN 'Landbrugsbyggeri'
                    ELSE 'Andet'
                END as category_danish,
                CASE
                    WHEN category_group = 'residential' THEN 1
                    WHEN category_group = 'publicServices' THEN 2
                    WHEN category_group = 'agricultural' THEN 3
                    ELSE 4
                END as priority,
                ST_AsGeoJSON(ST_FlipCoordinates(geo_building_centroid)) as geometry
            FROM {table_name}
            WHERE category_group IN ('residential', 'publicServices', 'agricultural')
                AND geo_building_centroid IS NOT NULL
            ORDER BY category_group, building_uuid
            """

            property_columns = [
                "building_uuid",
                "category_group",
                "building_type",
                "building_usage_category",
                "inspire_current_use",
                "address",
                "address_full",
                "building_floor_area_sqm",
                "inspire_construction_year",
                "inspire_floors",
                "inspire_dwellings",
                "color",
                "category_danish",
                "priority",
            ]

            return await GeoJSONWriter.write_geojson_from_query(
                self.conn, query, output_path, property_columns
            )

        except Exception as e:
            logger.error(f"Error exporting buildings GeoJSON: {e}")
            return False

    def _get_buildings_tippecanoe_args(self) -> List[str]:
        """Get additional tippecanoe arguments for buildings proximity.

        Returns:
            List of additional arguments
        """
        args = [
            "--drop-densest-as-needed",
            "--cluster-distance=10",  # Cluster nearby buildings
            "--cluster-maxzoom=10",  # Stop clustering at zoom 10
        ]

        # Add attribute-specific settings
        args.extend(
            [
                "--attribute-type=building_floor_area_sqm:float",
                "--attribute-type=inspire_floors:int",
                "--attribute-type=inspire_dwellings:int",
                "--attribute-type=priority:int",
            ]
        )

        return args

    async def generate_residential_buildings_pmtiles(self) -> Optional[str]:
        """Generate PMTiles specifically for residential buildings.

        Returns:
            Path to generated PMTiles file or None if failed
        """
        logger.info("Generating residential buildings PMTiles")

        with FileManager(self.config.temp_dir) as file_manager:
            try:
                # Load environmental layers to get buildings data
                environmental_layers = await self.data_loader.load_environmental_layers()

                if "bbr_buildings" not in environmental_layers:
                    logger.error("BBR buildings data not available")
                    return None

                buildings_table = environmental_layers["bbr_buildings"]

                # Generate GeoJSON for residential buildings only
                geojson_path = file_manager.create_temp_file(
                    suffix=".geojson", prefix="residential_buildings_"
                )

                success = await self._export_residential_buildings_geojson(
                    buildings_table, geojson_path
                )
                if not success:
                    logger.error("Failed to export residential buildings GeoJSON")
                    return None

                # Generate PMTiles
                pmtiles_path = file_manager.create_temp_file(
                    suffix=".pmtiles", prefix="residential_buildings_"
                )

                success = await self.tippecanoe.generate_pmtiles(
                    geojson_path=geojson_path,
                    output_path=pmtiles_path,
                    layer_name="residential_buildings",
                    max_zoom=14,
                    min_zoom=8,
                    buffer=64,
                    simplification=1,
                    additional_args=[
                        "--drop-densest-as-needed",
                        "--cluster-distance=15",
                        "--cluster-maxzoom=11",
                        "--attribute-type=building_floor_area_sqm:float",
                        "--attribute-type=inspire_dwellings:int",
                    ],
                )

                if not success:
                    logger.error("Failed to generate residential buildings PMTiles")
                    return None

                # Move to final location
                final_path = os.path.join(
                    os.path.dirname(self.config.temp_dir), "residential_buildings.pmtiles"
                )

                import shutil

                shutil.copy2(pmtiles_path, final_path)

                # Log file size
                if os.path.exists(final_path):
                    size_mb = os.path.getsize(final_path) / (1024 * 1024)
                    logger.info(f"Generated residential buildings PMTiles: {size_mb:.1f} MB")

                return final_path

            except Exception as e:
                logger.error(f"Error generating residential buildings PMTiles: {e}")
                return None

    async def _export_residential_buildings_geojson(
        self, table_name: str, output_path: str
    ) -> bool:
        """Export residential buildings data as GeoJSON.

        Args:
            table_name: Name of the buildings table
            output_path: Path for output GeoJSON file

        Returns:
            True if successful, False otherwise
        """
        try:
            query = f"""
            SELECT
                building_uuid,
                building_type,
                address,
                address_full,
                building_floor_area_sqm,
                inspire_construction_year,
                inspire_floors,
                inspire_dwellings,
                '#ff6b6b' as color,
                'Boligbyggeri' as category_danish,
                geo_building_centroid as geometry
            FROM {table_name}
            WHERE category_group = 'residential'
                AND geo_building_centroid IS NOT NULL
            ORDER BY building_uuid
            """

            property_columns = [
                "building_uuid",
                "building_type",
                "address",
                "address_full",
                "building_floor_area_sqm",
                "inspire_construction_year",
                "inspire_floors",
                "inspire_dwellings",
                "color",
                "category_danish",
            ]

            return await GeoJSONWriter.write_geojson_from_query(
                self.conn, query, output_path, property_columns
            )

        except Exception as e:
            logger.error(f"Error exporting residential buildings GeoJSON: {e}")
            return False

    async def generate_educational_facilities_pmtiles(self) -> Optional[str]:
        """Generate PMTiles specifically for educational facilities.

        Returns:
            Path to generated PMTiles file or None if failed
        """
        logger.info("Generating educational facilities PMTiles")

        with FileManager(self.config.temp_dir) as file_manager:
            try:
                # Load environmental layers to get buildings data
                environmental_layers = await self.data_loader.load_environmental_layers()

                if "bbr_buildings" not in environmental_layers:
                    logger.error("BBR buildings data not available")
                    return None

                buildings_table = environmental_layers["bbr_buildings"]

                # Generate GeoJSON for educational facilities
                geojson_path = file_manager.create_temp_file(
                    suffix=".geojson", prefix="educational_facilities_"
                )

                success = await self._export_educational_facilities_geojson(
                    buildings_table, geojson_path
                )
                if not success:
                    logger.error("Failed to export educational facilities GeoJSON")
                    return None

                # Generate PMTiles
                pmtiles_path = file_manager.create_temp_file(
                    suffix=".pmtiles", prefix="educational_facilities_"
                )

                success = await self.tippecanoe.generate_pmtiles(
                    geojson_path=geojson_path,
                    output_path=pmtiles_path,
                    layer_name="educational_facilities",
                    max_zoom=14,
                    min_zoom=6,  # Show educational facilities at lower zoom levels
                    buffer=64,
                    simplification=1,
                    additional_args=[
                        "--drop-densest-as-needed",
                        "--attribute-type=building_floor_area_sqm:float",
                    ],
                )

                if not success:
                    logger.error("Failed to generate educational facilities PMTiles")
                    return None

                # Move to final location
                final_path = os.path.join(
                    os.path.dirname(self.config.temp_dir), "educational_facilities.pmtiles"
                )

                import shutil

                shutil.copy2(pmtiles_path, final_path)

                # Log file size
                if os.path.exists(final_path):
                    size_mb = os.path.getsize(final_path) / (1024 * 1024)
                    logger.info(f"Generated educational facilities PMTiles: {size_mb:.1f} MB")

                return final_path

            except Exception as e:
                logger.error(f"Error generating educational facilities PMTiles: {e}")
                return None

    async def _export_educational_facilities_geojson(
        self, table_name: str, output_path: str
    ) -> bool:
        """Export educational facilities data as GeoJSON.

        Args:
            table_name: Name of the buildings table
            output_path: Path for output GeoJSON file

        Returns:
            True if successful, False otherwise
        """
        try:
            query = f"""
            SELECT
                building_uuid,
                building_type,
                building_usage_category,
                inspire_current_use,
                address,
                address_full,
                building_floor_area_sqm,
                inspire_construction_year,
                '#45b7d1' as color,
                'Uddannelsesinstitution' as category_danish,
                geo_building_centroid as geometry
            FROM {table_name}
            WHERE category_group = 'publicServices'
                AND (
                    building_usage_category ILIKE '%skole%' OR
                    building_usage_category ILIKE '%uddannelse%' OR
                    inspire_current_use ILIKE '%education%' OR
                    building_type ILIKE '%skole%'
                )
                AND geo_building_centroid IS NOT NULL
            ORDER BY building_uuid
            """

            property_columns = [
                "building_uuid",
                "building_type",
                "building_usage_category",
                "inspire_current_use",
                "address",
                "address_full",
                "building_floor_area_sqm",
                "inspire_construction_year",
                "color",
                "category_danish",
            ]

            return await GeoJSONWriter.write_geojson_from_query(
                self.conn, query, output_path, property_columns
            )

        except Exception as e:
            logger.error(f"Error exporting educational facilities GeoJSON: {e}")
            return False

    async def generate_all_building_pmtiles(self) -> Dict[str, Optional[str]]:
        """Generate building-related PMTiles (only the one needed by frontend).

        Returns:
            Dictionary mapping layer names to PMTiles file paths (None if failed)
        """
        logger.info("Generating building-related PMTiles")

        results = {}

        # Generate only the buildings proximity PMTiles that the frontend uses
        results["buildings_proximity"] = await self.generate_buildings_proximity_pmtiles()

        # Log summary
        successful = sum(1 for path in results.values() if path is not None)
        logger.info(f"Generated {successful}/{len(results)} building-related PMTiles")

        return results
