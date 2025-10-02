"""Field Analysis PMTiles Generator."""

import asyncio
import logging
import os
from typing import Dict, List, Optional

import duckdb

from .config import PMTilesGeneratorConfig
from .data_loader import PMTilesDataLoader
from .utils import FileManager, GeoJSONWriter, TippecanoeRunner

logger = logging.getLogger(__name__)


class FieldAnalysisPMTilesGenerator:
    """Generates field analysis PMTiles with integrated data sources."""

    def __init__(
        self,
        config: PMTilesGeneratorConfig,
        data_loader: PMTilesDataLoader,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ):
        """Initialize the field analysis generator.

        Args:
            config: PMTiles generator configuration
            data_loader: Data loader instance
            duckdb_conn: DuckDB connection
        """
        self.config = config
        self.data_loader = data_loader
        self.conn = duckdb_conn
        self.tippecanoe = TippecanoeRunner(self.config.temp_dir)

    async def generate_field_analysis_pmtiles(self, year: int) -> Optional[str]:
        """Generate field analysis PMTiles for a specific year.

        Args:
            year: Target year

        Returns:
            Path to generated PMTiles file or None if failed
        """
        logger.info(f"Generating field analysis PMTiles for year {year}")

        with FileManager(self.config.temp_dir) as file_manager:
            try:
                # Load and integrate data
                integrated_table = await self.data_loader.load_and_integrate_field_data(year)
                if not integrated_table:
                    logger.error(f"Failed to load integrated data for year {year}")
                    return None

                # Generate GeoJSON
                geojson_path = file_manager.create_temp_file(
                    suffix=".geojson", prefix=f"field_analysis_{year}_"
                )

                success = await self._export_field_analysis_geojson(
                    integrated_table, geojson_path, year
                )
                if not success:
                    logger.error(f"Failed to export GeoJSON for year {year}")
                    return None

                # Generate PMTiles
                pmtiles_path = file_manager.create_temp_file(
                    suffix=".pmtiles", prefix=f"field_analysis_{year}_"
                )

                success = await self.tippecanoe.generate_pmtiles(
                    geojson_path=geojson_path,
                    output_path=pmtiles_path,
                    layer_name="field_analysis",
                    max_zoom=self.config.tippecanoe_max_zoom,
                    min_zoom=self.config.tippecanoe_min_zoom,
                    buffer=self.config.tippecanoe_buffer,
                    simplification=self.config.tippecanoe_simplification,
                    additional_args=self._get_field_analysis_tippecanoe_args(),
                )

                if not success:
                    logger.error(f"Failed to generate PMTiles for year {year}")
                    return None

                # Check file size
                if os.path.exists(pmtiles_path):
                    size_mb = os.path.getsize(pmtiles_path) / (1024 * 1024)
                    logger.info(f"Generated field analysis PMTiles for {year}: {size_mb:.1f} MB")

                    if size_mb > self.config.max_field_analysis_size_mb:
                        logger.warning(
                            f"PMTiles file size ({size_mb:.1f} MB) exceeds target "
                            f"({self.config.max_field_analysis_size_mb} MB)"
                        )

                # Move to final location (outside temp directory)
                final_path = os.path.join(
                    os.path.dirname(self.config.temp_dir), f"field_analysis_{year}.pmtiles"
                )

                # Copy file to final location
                import shutil

                shutil.copy2(pmtiles_path, final_path)

                logger.info(f"Field analysis PMTiles saved to: {final_path}")
                return final_path

            except Exception as e:
                logger.error(f"Error generating field analysis PMTiles for year {year}: {e}")
                return None

    async def _export_field_analysis_geojson(
        self, table_name: str, output_path: str, year: int
    ) -> bool:
        """Export integrated field data as GeoJSON.

        Args:
            table_name: Name of the integrated data table
            output_path: Path for output GeoJSON file
            year: Target year

        Returns:
            True if successful, False otherwise
        """
        try:
            # Build the export query with all available fields
            query = self._build_field_analysis_query(table_name, year)

            # Execute query and get column information
            result = await asyncio.to_thread(self.conn.execute, query)
            rows = result.fetchall()
            columns = [desc[0] for desc in self.conn.description]

            if not rows:
                logger.warning(f"No field data found for year {year}")
                return False

            # Find geometry column
            geometry_col = None
            for col in columns:
                if col.lower() in ["geometry", "geom", "wkt", "geometry_wkt"]:
                    geometry_col = col
                    break

            if not geometry_col:
                logger.error("No geometry column found in field data")
                return False

            # Get property columns (exclude geometry)
            property_columns = [col for col in columns if col != geometry_col]

            # Write GeoJSON using utility
            success = await GeoJSONWriter.write_geojson_from_query(
                self.conn, query, output_path, property_columns
            )

            if success:
                logger.info(f"Exported {len(rows):,} field features to GeoJSON")

            return success

        except Exception as e:
            logger.error(f"Error exporting field analysis GeoJSON: {e}")
            return False

    def _build_field_analysis_query(self, table_name: str, year: int) -> str:
        """Build the SQL query for field analysis data export.

        Args:
            table_name: Name of the integrated data table
            year: Target year

        Returns:
            SQL query string
        """
        # Base fields (always available) - mapped to frontend expectations
        base_fields = [
            "field_uuid",
            "field_id as mark_id",  # Frontend expects mark_id
            "block_id as markblok_id",  # Frontend expects markblok_id
            "cvr_number",
            "year as field_year",  # Frontend expects field_year
            "area_ha * 100 as area_hectares",  # Convert ha to hectares for frontend
            "crop_name",
            "crop_code",
            "is_organic",
            "municipality as kommune",  # Frontend expects kommune
        ]

        # Environmental fields (if available)
        environmental_fields = [
            "field_bnbo_coverage_pct",
            "field_bnbo_water_coverage_pct",
            "bnbo_status_categories",
            "bnbo_action_required_hectares",
            "bnbo_completed_hectares",
            "field_wetland_coverage_pct",
            "field_wetland_water_coverage_pct",
            "field_soil_coverage_pct",
        ]

        # Production fields (if available)
        production_fields = ["yield_estimate_hkg_ha", "production_estimate_hkg", "dst_regions"]

        # Pesticide fields (if available) - mapped to frontend expectations
        pesticide_fields = [
            "pesticide_applications as total_pesticide_applications",
            "pesticides_used",
            "residential_buildings_formatted as residential_buildings_proximity",
            "educational_facilities_formatted as educational_facilities_proximity",
            "water_distance_formatted as water_distance_proximity",
            "avg_match_confidence",
        ]

        # NLES5 fields (if available)
        nles5_fields = [
            "nitrogen_washout_kg_ha",
            "total_nitrogen_washout_kg",
            "nles5_soil_type",
            "nles5_crop_code",
            "nles5_data_quality",
        ]

        # Check which columns exist in the table
        try:
            # Get table schema
            schema_result = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            available_columns = {row[0] for row in schema_result}

            # Filter fields to only those available
            selected_fields = []

            # Always include base fields that exist
            for field in base_fields:
                if field in available_columns:
                    selected_fields.append(field)

            # Add optional fields if they exist
            for field_group in [
                environmental_fields,
                production_fields,
                pesticide_fields,
                nles5_fields,
            ]:
                for field in field_group:
                    if field in available_columns:
                        selected_fields.append(field)

            # Always include geometry last - convert to GeoJSON format with coordinate swap
            if "geometry" in available_columns:
                # Use ST_FlipCoordinates to ensure lon,lat order for PMTiles compatibility
                selected_fields.append("ST_AsGeoJSON(ST_FlipCoordinates(geometry)) as geometry")
                logger.info(
                    "Geometry column found and added (converted to GeoJSON with coordinate swap)"
                )

                # DEBUG: Log coordinate bounds to verify swap worked
                try:
                    bounds_result = self.conn.execute(
                        f"SELECT ST_XMin(ST_Extent(geometry)), ST_YMin(ST_Extent(geometry)), "
                        f"ST_XMax(ST_Extent(geometry)), ST_YMax(ST_Extent(geometry)) "
                        f"FROM {table_name}"
                    )
                    min_x, min_y, max_x, max_y = bounds_result.fetchone()
                    logger.info(
                        f"DEBUG: Original bounds: X({min_x:.6f} to {max_x:.6f}), "
                        f"Y({min_y:.6f} to {max_y:.6f})"
                    )

                    bounds_flipped = self.conn.execute(
                        f"SELECT ST_XMin(ST_Extent(ST_FlipCoordinates(geometry))), "
                        f"ST_YMin(ST_Extent(ST_FlipCoordinates(geometry))), "
                        f"ST_XMax(ST_Extent(ST_FlipCoordinates(geometry))), "
                        f"ST_YMax(ST_Extent(ST_FlipCoordinates(geometry))) "
                        f"FROM {table_name}"
                    )
                    flip_min_x, flip_min_y, flip_max_x, flip_max_y = bounds_flipped.fetchone()
                    logger.info(
                        f"DEBUG: Flipped bounds: X({flip_min_x:.6f} to {flip_max_x:.6f}), "\n                        f"Y({flip_min_y:.6f} to {flip_max_y:.6f})"
                    )
                except Exception as debug_error:
                    logger.warning(f"Could not log coordinate bounds: {debug_error}")
            else:
                logger.error("No geometry column found! This will cause 0 features.")

        except Exception as e:
            logger.warning(f"Could not determine table schema, using base fields: {e}")
            selected_fields = base_fields + [
                "ST_AsGeoJSON(ST_FlipCoordinates(geometry)) as geometry"
            ]

        # Build query
        fields_str = ",\n    ".join(selected_fields)

        query = f"""
        SELECT
            {fields_str}
        FROM {table_name}
        WHERE geometry IS NOT NULL
        ORDER BY field_uuid
        """

        return query

    def _get_field_analysis_tippecanoe_args(self) -> List[str]:
        """Get additional tippecanoe arguments for field analysis.

        Returns:
            List of additional arguments
        """
        args = [
            "--detect-shared-borders",
            "--extend-zooms-if-still-dropping",
            "--drop-densest-as-needed",
            "--drop-fraction-as-needed",
        ]

        # Add attribute-specific settings
        args.extend(
            [
                "--attribute-type=area_ha:float",
                "--attribute-type=year:int",
                "--attribute-type=is_organic:bool",
                "--attribute-type=pesticide_applications:int",
                "--attribute-type=avg_match_confidence:float",
            ]
        )

        # Add environmental data types if available
        if self.config.include_environmental_analysis:
            args.extend(
                [
                    "--attribute-type=field_bnbo_coverage_pct:float",
                    "--attribute-type=field_wetland_coverage_pct:float",
                    "--attribute-type=field_soil_coverage_pct:float",
                    "--attribute-type=bnbo_action_required_hectares:float",
                    "--attribute-type=bnbo_completed_hectares:float",
                ]
            )

        # Add production data types if available
        if self.config.include_production_data:
            args.extend(
                [
                    "--attribute-type=yield_estimate_hkg_ha:float",
                    "--attribute-type=production_estimate_hkg:float",
                ]
            )

        # Add NLES5 data types if available
        if self.config.include_nles5_data:
            args.extend(
                [
                    "--attribute-type=nitrogen_washout_kg_ha:float",
                    "--attribute-type=total_nitrogen_washout_kg:float",
                ]
            )

        return args

    async def generate_multiple_years(self, years: List[int]) -> Dict[int, Optional[str]]:
        """Generate field analysis PMTiles for multiple years.

        Args:
            years: List of target years

        Returns:
            Dictionary mapping years to PMTiles file paths (None if failed)
        """
        logger.info(f"Generating field analysis PMTiles for {len(years)} years: {years}")

        results = {}

        if self.config.enable_parallel_processing and len(years) > 1:
            # Process years in parallel (limited by max_parallel_years)
            semaphore = asyncio.Semaphore(self.config.max_parallel_years)

            async def process_year_with_semaphore(year: int) -> tuple[int, Optional[str]]:
                async with semaphore:
                    result = await self.generate_field_analysis_pmtiles(year)
                    return year, result

            # Create tasks for all years
            tasks = [process_year_with_semaphore(year) for year in years]

            # Execute tasks and collect results
            task_results = await asyncio.gather(*tasks, return_exceptions=True)

            for task_result in task_results:
                if isinstance(task_result, Exception):
                    logger.error(f"Error in parallel processing: {task_result}")
                else:
                    year, pmtiles_path = task_result
                    results[year] = pmtiles_path
        else:
            # Process years sequentially
            for year in years:
                pmtiles_path = await self.generate_field_analysis_pmtiles(year)
                results[year] = pmtiles_path

        # Log summary
        successful = sum(1 for path in results.values() if path is not None)
        logger.info(f"Generated field analysis PMTiles for {successful}/{len(years)} years")

        return results
