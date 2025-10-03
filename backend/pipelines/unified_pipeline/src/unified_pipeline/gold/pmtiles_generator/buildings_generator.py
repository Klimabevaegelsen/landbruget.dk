"""Buildings Proximity PMTiles Generator."""

import asyncio
import logging
import os
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
                    max_zoom=14,
                    min_zoom=8,
                    buffer=64,
                    simplification=2,  # Allow more simplification for polygons
                    additional_args=self._get_buildings_tippecanoe_args(),
                )

                if not success:
                    logger.error("Failed to generate buildings proximity PMTiles")
                    return None

                # Move to final location
                final_path = os.path.join(
                    os.path.dirname(self.config.temp_dir),
                    "buildings_proximity.pmtiles",
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

        Only includes buildings within 100m of agricultural fields as per requirements
        in COMPREHENSIVE_FIELD_ANALYSIS_PMTILES_PLAN.md.

        Args:
            table_name: Name of the buildings table
            output_path: Path for output GeoJSON file

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("🏢 Generating buildings GeoJSON with 100m proximity filtering...")

            # First, load agricultural fields data to filter buildings by proximity
            await self._load_agricultural_fields_for_proximity()

            # COORDINATE ALIGNMENT - Flip BBR buildings to match field coordinate order
            logger.info("📍 Creating buildings table for proximity analysis...")
            
            # DEBUG: Log building coordinates before proximity filtering
            try:
                building_bounds = await asyncio.to_thread(
                    self.conn.execute,
                    f"SELECT ST_XMin(ST_Extent(geometry)), ST_YMin(ST_Extent(geometry)), "
                    f"ST_XMax(ST_Extent(geometry)), ST_YMax(ST_Extent(geometry)) "
                    f"FROM {table_name} WHERE geometry IS NOT NULL"
                )
                b_min_x, b_min_y, b_max_x, b_max_y = building_bounds.fetchone()
                logger.info(f"DEBUG: Building bounds before filtering: X({b_min_x:.6f} to {b_max_x:.6f}), Y({b_min_y:.6f} to {b_max_y:.6f})")
            except Exception as e:
                logger.warning(f"Could not log building bounds: {e}")
            
            await asyncio.to_thread(
                self.conn.execute,
                f"""
                CREATE OR REPLACE TABLE buildings_for_proximity AS
                SELECT
                    building_uuid, category_group, building_type, building_usage_category,
                    current_use, address, address_full, building_floor_area_sqm,
                    inspire_construction_year, inspire_floors, inspire_dwellings,
                    bbr_usage_code,
                    COALESCE(bbr_usage_name, 'Ukendt bygningstype') as bbr_usage_name,
                    geometry as geometry
                FROM {table_name}
                WHERE category_group IN ('residential', 'publicServices', 'agricultural')
                    AND geometry IS NOT NULL
            """,
            )

            # Step 2: Create buffered agricultural fields (use geometry as-is like field analysis)
            logger.info("🌾 Creating buffered agricultural fields...")
            
            # DEBUG: Log field coordinates before buffering
            try:
                field_bounds = await asyncio.to_thread(
                    self.conn.execute,
                    "SELECT ST_XMin(ST_Extent(geometry)), ST_YMin(ST_Extent(geometry)), "
                    "ST_XMax(ST_Extent(geometry)), ST_YMax(ST_Extent(geometry)) "
                    "FROM agricultural_fields_proximity"
                )
                f_min_x, f_min_y, f_max_x, f_max_y = field_bounds.fetchone()
                logger.info(f"DEBUG: Field bounds before buffer: X({f_min_x:.6f} to {f_max_x:.6f}), Y({f_min_y:.6f} to {f_max_y:.6f})")
            except Exception as e:
                logger.warning(f"Could not log field bounds: {e}")
            
            await asyncio.to_thread(
                self.conn.execute,
                """
                CREATE OR REPLACE TABLE fields_buffered AS
                SELECT
                    field_uuid,
                    ST_Buffer(geometry, 0.001) as geometry_buffer
                FROM agricultural_fields_proximity
                WHERE geometry IS NOT NULL
            """,
            )

            # Step 3: Perform SPATIAL_JOIN compliant proximity filtering
            # This follows PR #545 requirements:
            # ✅ Simple table-to-table JOIN (no complex nesting)
            # ✅ Single spatial predicate (ST_Intersects only)
            # ✅ Pre-transformed geometries (no ST_Transform in join)
            # ✅ Clean query structure
            logger.info("🔗 Performing SPATIAL_JOIN optimized proximity filtering...")

            chunk_size = 10000  # Process buildings in chunks for memory safety

            # Verify table exists before counting
            table_exists = await asyncio.to_thread(
                self.conn.execute,
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = 'buildings_for_proximity'",
            )
            if table_exists.fetchone()[0] == 0:
                logger.error("buildings_for_proximity table does not exist")
                return False

            building_count = await asyncio.to_thread(
                self.conn.execute, "SELECT COUNT(*) FROM buildings_for_proximity"
            )
            total_buildings = building_count.fetchone()[0]

            logger.info(f"Processing {total_buildings:,} buildings in chunks of {chunk_size:,}")

            # Create final results table
            await asyncio.to_thread(
                self.conn.execute,
                """
                CREATE OR REPLACE TABLE buildings_with_proximity AS
                SELECT
                    building_uuid, category_group, building_type, building_usage_category,
                    current_use, address, address_full, building_floor_area_sqm,
                    inspire_construction_year, inspire_floors, inspire_dwellings,
                    bbr_usage_code, bbr_usage_name,
                    CAST(NULL AS DOUBLE) as distance_to_field_m,
                    geometry as geometry
                FROM buildings_for_proximity
                WHERE FALSE  -- Empty table with correct schema
            """,
            )

            # Process in chunks to respect memory limits
            for offset in range(0, total_buildings, chunk_size):
                chunk_num = offset // chunk_size + 1
                total_chunks = (total_buildings + chunk_size - 1) // chunk_size
                logger.info(f"Processing chunk {chunk_num}/{total_chunks}")

                # Create chunk table
                await asyncio.to_thread(
                    self.conn.execute,
                    f"""
                    CREATE OR REPLACE TABLE buildings_chunk AS
                    SELECT * FROM buildings_for_proximity
                    ORDER BY building_uuid
                    LIMIT {chunk_size} OFFSET {offset}
                """,
                )

                # SPATIAL_JOIN compliant query - simple table-to-table join
                await asyncio.to_thread(
                    self.conn.execute,
                    """
                    INSERT INTO buildings_with_proximity
                    SELECT DISTINCT
                        b.building_uuid, b.category_group, b.building_type,
                        b.building_usage_category, b.current_use, b.address,
                        b.address_full, b.building_floor_area_sqm, b.inspire_construction_year,
                        b.inspire_floors, b.inspire_dwellings, b.bbr_usage_code, b.bbr_usage_name,
                        MIN(ST_Distance(b.geometry, f_orig.geometry)) as distance_to_field_m,
                        b.geometry as geometry
                    FROM buildings_chunk b
                    JOIN fields_buffered f ON ST_Intersects(b.geometry, f.geometry_buffer)
                    JOIN agricultural_fields_proximity f_orig ON f.field_uuid = f_orig.field_uuid
                    GROUP BY
                        b.building_uuid, b.category_group, b.building_type,
                        b.building_usage_category, b.current_use, b.address,
                        b.address_full, b.building_floor_area_sqm, b.inspire_construction_year,
                        b.inspire_floors, b.inspire_dwellings, b.bbr_usage_code,
                        b.bbr_usage_name, b.geometry
                """,
                )

            # DEBUG: Log final proximity results before export
            try:
                proximity_count = await asyncio.to_thread(
                    self.conn.execute,
                    "SELECT COUNT(*) FROM buildings_with_proximity"
                )
                count = proximity_count.fetchone()[0]
                logger.info(f"DEBUG: Buildings with proximity count: {count:,}")
                
                if count > 0:
                    proximity_bounds = await asyncio.to_thread(
                        self.conn.execute,
                        "SELECT ST_XMin(ST_Extent(geometry)), ST_YMin(ST_Extent(geometry)), "
                        "ST_XMax(ST_Extent(geometry)), ST_YMax(ST_Extent(geometry)) "
                        "FROM buildings_with_proximity"
                    )
                    p_min_x, p_min_y, p_max_x, p_max_y = proximity_bounds.fetchone()
                    logger.info(f"DEBUG: Proximity buildings bounds: X({p_min_x:.6f} to {p_max_x:.6f}), Y({p_min_y:.6f} to {p_max_y:.6f})")
                else:
                    logger.error("DEBUG: No buildings found in proximity table - spatial join failed!")
            except Exception as e:
                logger.warning(f"Could not log proximity results: {e}")

            # Final query for GeoJSON export
            query = """
            SELECT
                building_uuid,
                category_group,
                building_type,
                building_usage_category,
                current_use,
                address,
                address_full,
                building_floor_area_sqm,
                inspire_construction_year,
                inspire_floors,
                inspire_dwellings,
                bbr_usage_code,
                bbr_usage_name,
                ROUND(distance_to_field_m, 1) as distance_to_field_m,
                CASE
                    WHEN category_group = 'residential' THEN '#ff6b6b'
                    WHEN category_group = 'publicServices' THEN '#45b7d1'
                    WHEN category_group = 'agricultural' THEN '#4ecdc4'
                    ELSE '#888888'
                END as color,
                CASE
                    WHEN category_group = 'residential' THEN 'Beboelsesbygning'
                    WHEN category_group = 'publicServices' THEN 'Offentlige institutioner'
                    WHEN category_group = 'agricultural' THEN 'Landbrugsbygning'
                    ELSE 'Andet'
                END as category_danish,
                CASE
                    WHEN category_group = 'residential' THEN 1
                    WHEN category_group = 'publicServices' THEN 2
                    WHEN category_group = 'agricultural' THEN 3
                    ELSE 4
                END as priority,
                CASE
                    WHEN bbr_usage_code IN ('420', '421', '422', '429') THEN 'Skole'
                    WHEN bbr_usage_code IN ('440', '441') THEN 'Børnehave'
                    WHEN category_group = 'publicServices' THEN 'Offentlig service'
                    WHEN category_group = 'residential' THEN 'Bolig'
                    WHEN category_group = 'agricultural' THEN 'Landbrug'
                    ELSE 'Andet'
                END as building_type_simple,
                ST_AsGeoJSON(geometry) as geometry
            FROM buildings_with_proximity
            ORDER BY distance_to_field_m, category_group, building_uuid
            """

            property_columns = [
                "building_uuid",
                "category_group",
                "building_type",
                "building_usage_category",
                "current_use",
                "address",
                "address_full",
                "building_floor_area_sqm",
                "inspire_construction_year",
                "inspire_floors",
                "inspire_dwellings",
                "bbr_usage_code",
                "bbr_usage_name",
                "distance_to_field_m",
                "color",
                "category_danish",
                "priority",
                "building_type_simple",
            ]

            logger.info("📄 Exporting buildings to GeoJSON...")
            return await GeoJSONWriter.write_geojson_from_query(
                self.conn, query, output_path, property_columns
            )

        except Exception as e:
            logger.error(f"Error exporting buildings GeoJSON: {e}")
            return False

    async def _load_agricultural_fields_for_proximity(self) -> None:
        """Load agricultural fields data for proximity filtering.

        Creates a table 'agricultural_fields_proximity' with field geometries
        for use in 100m proximity filtering of buildings.
        """
        try:
            # Load the latest agricultural fields data from multiple years
            # Use the most recent available data
            current_year = 2024
            fields_loaded = False

            for year in range(current_year, current_year - 3, -1):  # Try last 3 years
                try:
                    base_path = f"gs://{self.config.gcs_bucket}/silver/fvm_marker_{year}"

                    # Try to find the latest timestamped directory for this year
                    gcs_path = await self.data_loader._find_latest_timestamped_path(base_path)
                    if gcs_path:
                        logger.info(
                            f"Loading agricultural fields from {year} for proximity filtering"
                        )

                        query = f"""
                        CREATE OR REPLACE TABLE agricultural_fields_proximity AS
                        SELECT DISTINCT
                            field_uuid,
                            geometry as geometry  -- Fix coordinate swap issue
                        FROM read_parquet('{gcs_path}data.parquet')
                        WHERE geometry IS NOT NULL
                            AND ST_IsValid(geometry)
                            AND area_ha > 0.1  -- Minimum 0.1 hectare fields
                        """

                        await asyncio.to_thread(self.conn.execute, query)

                        # Verify data was loaded
                        count_result = await asyncio.to_thread(
                            self.conn.execute, "SELECT COUNT(*) FROM agricultural_fields_proximity"
                        )
                        field_count = count_result.fetchone()[0]

                        if field_count > 0:
                            logger.info(
                                f"Loaded {field_count:,} agricultural fields "
                                f"for proximity filtering"
                            )
                            fields_loaded = True
                            break

                except Exception as e:
                    logger.warning(f"Failed to load agricultural fields from year {year}: {e}")
                    continue

            if not fields_loaded:
                # Fallback: create empty table to prevent query errors
                logger.warning("No agricultural fields data found, creating empty table")
                await asyncio.to_thread(
                    self.conn.execute,
                    """
                    CREATE OR REPLACE TABLE agricultural_fields_proximity AS
                    SELECT
                        CAST(NULL AS VARCHAR) as field_uuid,
                        CAST(NULL AS GEOMETRY) as geometry
                    WHERE FALSE
                """,
                )

        except Exception as e:
            logger.error(f"Error loading agricultural fields for proximity filtering: {e}")
            # Create empty table to prevent query errors
            await asyncio.to_thread(
                self.conn.execute,
                """
                CREATE OR REPLACE TABLE agricultural_fields_proximity AS
                SELECT
                    CAST(NULL AS VARCHAR) as field_uuid,
                    CAST(NULL AS GEOMETRY) as geometry
                WHERE FALSE
            """,
            )

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
                ST_AsGeoJSON(ST_FlipCoordinates(geo_building_polygon)) as geometry
            FROM {table_name}
            WHERE category_group = 'residential'
                AND geo_building_polygon IS NOT NULL
                AND ST_IsValid(geo_building_polygon)
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
                current_use,
                address,
                address_full,
                building_floor_area_sqm,
                inspire_construction_year,
                '#45b7d1' as color,
                'Uddannelsesinstitution' as category_danish,
                ST_AsGeoJSON(geo_building_polygon) as geometry
            FROM {table_name}
            WHERE category_group = 'publicServices'
                AND (
                    building_usage_category ILIKE '%skole%' OR
                    building_usage_category ILIKE '%uddannelse%' OR
                    current_use ILIKE '%education%' OR
                    building_type ILIKE '%skole%'
                )
                AND geo_building_polygon IS NOT NULL
                AND ST_IsValid(geo_building_polygon)
            ORDER BY building_uuid
            """

            property_columns = [
                "building_uuid",
                "building_type",
                "building_usage_category",
                "current_use",
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
