"""
PMTiles generator for H3 PFAS exposure analysis.

This module generates PMTiles files directly from DuckDB tables in the H3 pipeline,
providing optimized vector tiles for frontend visualization.
"""

import os
import subprocess
import tempfile
from pathlib import Path

import duckdb
from loguru import logger

from ..config import H3SpatialConfig


class H3PMTilesGenerator:
    """Generates PMTiles from H3 PFAS analysis results."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, config: H3SpatialConfig, gcs_access=None):
        self.conn = conn
        self.config = config
        self.gcs_access = gcs_access
        self.log = logger.bind(component="H3PMTilesGenerator")

    def generate_pmtiles_for_year(self, results_table: str, year: int) -> str | None:
        """Generate PMTiles for a specific year and resolution."""
        self.log.info(
            f"🗺️ Generating PMTiles for year {year}, resolution {self.config.h3_resolution}"
        )

        try:
            # Check if tippecanoe is available
            if not self._check_tippecanoe():
                self.log.warning("⚠️ Tippecanoe not available, skipping PMTiles generation")
                return None

            # Clean up any existing files first
            self._cleanup_temp_files_for_year(year)

            # Get data count
            count = self.conn.execute(f"SELECT COUNT(*) FROM {results_table}").fetchone()[0]
            if count == 0:
                self.log.warning(f"⚠️ No data in {results_table}, skipping PMTiles generation")
                return None

            self.log.info(f"📊 Processing {count:,} H3 cells for PMTiles generation")

            # Create GeoJSON
            geojson_path = self._create_geojson(results_table, year)
            if not geojson_path:
                return None

            # Generate PMTiles
            pmtiles_path = self._generate_pmtiles(geojson_path, year)
            if not pmtiles_path:
                # Clean up GeoJSON on PMTiles generation failure
                if geojson_path and os.path.exists(geojson_path):
                    os.remove(geojson_path)
                return None

            # Upload to GCS if available
            if self.gcs_access:
                gcs_path = self._upload_pmtiles_to_gcs(pmtiles_path, year)
                if gcs_path:
                    self.log.info(f"✅ PMTiles uploaded to: {gcs_path}")
                    # Clean up GeoJSON file after successful upload
                    if geojson_path and os.path.exists(geojson_path):
                        os.remove(geojson_path)
                    return gcs_path
                else:
                    # Clean up files on upload failure
                    if geojson_path and os.path.exists(geojson_path):
                        os.remove(geojson_path)
                    return None
            else:
                self.log.info(f"✅ PMTiles generated locally: {pmtiles_path}")
                # Clean up GeoJSON file even for local generation to save space
                if geojson_path and os.path.exists(geojson_path):
                    os.remove(geojson_path)
                return pmtiles_path

        except Exception as e:
            self.log.error(f"❌ PMTiles generation failed for year {year}: {e}")
            # Clean up temporary files on error
            self._cleanup_temp_files_for_year(year)
            return None

    def generate_kommune_pmtiles_for_year(self, results_table: str, year: int) -> str | None:
        """Generate PMTiles for kommune (municipality) data."""
        self.log.info(f"🏛️ Generating kommune PMTiles for year {year}")

        try:
            # Check if tippecanoe is available
            if not self._check_tippecanoe():
                self.log.warning("⚠️ Tippecanoe not available, skipping kommune PMTiles generation")
                return None

            # Clean up any existing files first
            self._cleanup_temp_files_for_year(year)

            # Get data count
            count = self.conn.execute(f"SELECT COUNT(*) FROM {results_table}").fetchone()[0]
            if count == 0:
                self.log.warning(
                    f"⚠️ No data in {results_table}, skipping kommune PMTiles generation"
                )
                return None

            self.log.info(f"📊 Processing {count:,} kommuner for PMTiles generation")

            # Create GeoJSON for kommune data
            geojson_path = self._create_kommune_geojson(results_table, year)
            if not geojson_path:
                return None

            # Generate PMTiles
            pmtiles_path = self._generate_kommune_pmtiles(geojson_path, year)
            if not pmtiles_path:
                # Clean up GeoJSON on PMTiles generation failure
                if geojson_path and os.path.exists(geojson_path):
                    os.remove(geojson_path)
                return None

            # Upload to GCS if available
            if self.gcs_access:
                gcs_path = self._upload_kommune_pmtiles_to_gcs(pmtiles_path, year)
                if gcs_path:
                    self.log.info(f"✅ Kommune PMTiles uploaded to: {gcs_path}")
                    # Clean up GeoJSON file after successful upload
                    if geojson_path and os.path.exists(geojson_path):
                        os.remove(geojson_path)
                    return gcs_path
                else:
                    # Clean up files on upload failure
                    if geojson_path and os.path.exists(geojson_path):
                        os.remove(geojson_path)
                    return None
            else:
                self.log.info(f"✅ Kommune PMTiles generated locally: {pmtiles_path}")
                # Clean up GeoJSON file even for local generation to save space
                if geojson_path and os.path.exists(geojson_path):
                    os.remove(geojson_path)
                return pmtiles_path

        except Exception as e:
            self.log.error(f"❌ Kommune PMTiles generation failed for year {year}: {e}")
            # Clean up temporary files on error
            self._cleanup_temp_files_for_year(year)
            return None

    def _check_tippecanoe(self) -> bool:
        """Check if tippecanoe is installed."""
        try:
            result = subprocess.run(["tippecanoe", "--version"], capture_output=True, text=True)
            return result.returncode == 0
        except FileNotFoundError:
            return False

    def _create_geojson(self, results_table: str, year: int) -> str | None:
        """Create GeoJSON from H3 results table."""
        try:
            # Create temporary file for GeoJSON
            temp_dir = Path(tempfile.gettempdir())
            geojson_path = temp_dir / f"h3_pfas_{year}_res{self.config.h3_resolution}.geojson"

            # Clean up any existing file first
            if geojson_path.exists():
                geojson_path.unlink()

            self.log.info(f"📄 Creating GeoJSON: {geojson_path}")

            # Determine column names from the table schema
            schema = self.conn.execute(f"DESCRIBE {results_table}").fetchall()
            column_names = [row[0] for row in schema]

            # Map to standardized names
            h3_col = self._find_column(column_names, ["h3_cell", "h3_id"])
            pfas_col = self._find_column(
                column_names, ["total_pfas_containing_active_ingredient_grams", "pfas_grams"]
            )
            pesticide_col = self._find_column(
                column_names, ["total_pesticide_belastning", "pesticide_load"]
            )
            applications_col = self._find_column(
                column_names, ["total_pesticide_applications", "applications"]
            )
            field_count_col = self._find_column(column_names, ["unique_field_count", "field_count"])
            coverage_col = self._find_column(
                column_names, ["actual_coverage_ratio", "coverage_ratio"]
            )

            if not h3_col:
                self.log.error("❌ No H3 cell column found in results table")
                return None

            self.log.info(f"Using columns: H3={h3_col}, PFAS={pfas_col}, Pesticide={pesticide_col}")

            # Build values with null safety
            pfas_value = f"COALESCE({pfas_col}, 0)" if pfas_col else "0"
            pesticide_value = f"COALESCE({pesticide_col}, 0)" if pesticide_col else "0"
            applications_value = f"COALESCE({applications_col}, 0)" if applications_col else "0"
            field_count_value = f"COALESCE({field_count_col}, 0)" if field_count_col else "0"
            coverage_value = f"COALESCE({coverage_col}, 0)" if coverage_col else "0"

            # Generate GeoJSON with H3 geometries
            self.conn.execute(f"""
                COPY (
                    SELECT 
                        'Feature' as type,
                        ST_AsGeoJSON(ST_GeomFromText(h3_cell_to_boundary_wkt({h3_col})))::JSON as geometry,
                        json_object(
                            'h3_id', CAST({h3_col} AS VARCHAR),
                            'year', {year},
                            'resolution', {self.config.h3_resolution},
                            'pfas_grams', ROUND({pfas_value}, 3),
                            'pesticide_load', ROUND({pesticide_value}, 3),
                            'applications', {applications_value},
                            'field_count', {field_count_value},
                            'coverage', ROUND({coverage_value}, 3),
                            
                            -- Calculate intensity per hectare
                            'pfas_intensity', CASE 
                                WHEN h3_cell_area_ha > 0 THEN ROUND({pfas_value} / h3_cell_area_ha, 3)
                                ELSE 0 
                            END,
                            'pesticide_intensity', CASE 
                                WHEN h3_cell_area_ha > 0 THEN ROUND({pesticide_value} / h3_cell_area_ha, 3)
                                ELSE 0 
                            END,
                            
                            -- Zoom classification for level-of-detail rendering
                            'zoom_class', CASE 
                                WHEN {pfas_value} > 100 THEN 'high'
                                WHEN {pfas_value} > 10 THEN 'medium'
                                WHEN {pfas_value} > 0 THEN 'low'
                                ELSE 'none'
                            END,
                            
                            -- Color coding for visualization
                            'pfas_color', CASE 
                                WHEN {pfas_value} > 100 THEN '#d73027'
                                WHEN {pfas_value} > 50 THEN '#f46d43'
                                WHEN {pfas_value} > 10 THEN '#fdae61'
                                WHEN {pfas_value} > 1 THEN '#fee08b'
                                WHEN {pfas_value} > 0 THEN '#e6f598'
                                ELSE '#abdda4'
                            END
                        ) as properties
                    FROM {results_table}
                    WHERE {pfas_value} > 0  -- Only include cells with PFAS data
                    ORDER BY {pfas_value} DESC
                ) TO '{geojson_path}' (FORMAT JSON, ARRAY true)
            """)

            if geojson_path.exists():
                size_mb = geojson_path.stat().st_size / (1024 * 1024)
                self.log.info(f"✅ GeoJSON created: {size_mb:.1f} MB")
                return str(geojson_path)
            else:
                self.log.error("❌ GeoJSON file was not created")
                return None

        except Exception as e:
            self.log.error(f"❌ GeoJSON creation failed: {e}")
            return None

    def _find_column(self, column_names: list[str], candidates: list[str]) -> str | None:
        """Find the first matching column name from candidates."""
        for candidate in candidates:
            for col in column_names:
                if candidate.lower() in col.lower():
                    return col
        return None

    def _generate_pmtiles(self, geojson_path: str, year: int) -> str | None:
        """Generate PMTiles using tippecanoe."""
        try:
            temp_dir = Path(tempfile.gettempdir())
            pmtiles_path = temp_dir / f"h3_pfas_{year}_res{self.config.h3_resolution}.pmtiles"

            # Clean up any existing PMTiles file first
            if pmtiles_path.exists():
                pmtiles_path.unlink()

            self.log.info(f"🔧 Generating PMTiles: {pmtiles_path}")

            # Adjust zoom levels based on H3 resolution
            # Higher H3 resolution = more detailed data = higher zoom levels
            min_zoom = max(4, self.config.h3_resolution - 3)
            max_zoom = min(14, self.config.h3_resolution + 2)
            base_zoom = self.config.h3_resolution

            cmd = [
                "tippecanoe",
                "--output",
                str(pmtiles_path),
                "--force",  # Overwrite existing
                "--layer",
                f"h3_pfas_{year}_res{self.config.h3_resolution}",
                "--minimum-zoom",
                str(min_zoom),
                "--maximum-zoom",
                str(max_zoom),
                "--base-zoom",
                str(base_zoom),
                "--drop-densest-as-needed",  # Handle dense areas
                "--extend-zooms-if-still-dropping",
                "--attribute-type",
                "pfas_grams:float",
                "--attribute-type",
                "pesticide_load:float",
                "--attribute-type",
                "pfas_intensity:float",
                "--attribute-type",
                "pesticide_intensity:float",
                "--attribute-type",
                "applications:int",
                "--attribute-type",
                "field_count:int",
                "--attribute-type",
                "coverage:float",
                "--attribute-type",
                "year:int",
                "--attribute-type",
                "resolution:int",
                geojson_path,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            # Clean up GeoJSON to save space
            os.remove(geojson_path)

            if pmtiles_path.exists():
                size_mb = pmtiles_path.stat().st_size / (1024 * 1024)
                self.log.info(f"✅ PMTiles generated: {size_mb:.1f} MB")
                return str(pmtiles_path)
            else:
                self.log.error("❌ PMTiles file was not created")
                return None

        except subprocess.CalledProcessError as e:
            self.log.error(f"❌ Tippecanoe failed: {e.stderr}")
            return None
        except Exception as e:
            self.log.error(f"❌ PMTiles generation failed: {e}")
            return None

    def _upload_pmtiles_to_gcs(self, pmtiles_path: str, year: int) -> str | None:
        """Upload PMTiles to GCS."""
        try:
            import shutil
            from datetime import datetime

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            gcs_path = f"gs://{self.config.bucket}/gold/pmtiles/h3_pfas_{year}_res{self.config.h3_resolution}/{timestamp}/h3_pfas_{year}_res{self.config.h3_resolution}.pmtiles"

            self.log.info(f"☁️ Uploading PMTiles to: {gcs_path}")

            # Upload using GCS access - use the correct streaming method
            with open(pmtiles_path, "rb") as src:
                with self.gcs_access.fs.open(gcs_path, "wb") as dst:
                    shutil.copyfileobj(src, dst)

            # Clean up local file after successful upload
            if os.path.exists(pmtiles_path):
                os.remove(pmtiles_path)

            return gcs_path

        except Exception as e:
            self.log.error(f"❌ PMTiles upload failed: {e}")
            # Clean up local file on error too
            if os.path.exists(pmtiles_path):
                try:
                    os.remove(pmtiles_path)
                except Exception:
                    pass
            return None

    def _create_kommune_geojson(self, results_table: str, year: int) -> str | None:
        """Create GeoJSON from kommune results table."""
        try:
            # Create temporary file for GeoJSON
            temp_dir = Path(tempfile.gettempdir())
            geojson_path = temp_dir / f"kommune_pfas_{year}.geojson"

            # Clean up any existing file first
            if geojson_path.exists():
                geojson_path.unlink()

            self.log.info(f"📄 Creating kommune GeoJSON: {geojson_path}")

            # Check if we have geometry data in the results table
            schema = self.conn.execute(f"DESCRIBE {results_table}").fetchall()
            column_names = [row[0] for row in schema]

            has_geometry = any(col.lower() in ["geometry", "geom", "wkt"] for col in column_names)

            if not has_geometry:
                self.log.warning(
                    "⚠️ No geometry column found in results table, using centroids as fallback"
                )
                # Use centroids as fallback
                geometry_select = """
                    json_object(
                        'type', 'Point',
                        'coordinates', json_array(kommune_centroid_x, kommune_centroid_y)
                    )
                """
                geometry_condition = (
                    "kommune_centroid_x IS NOT NULL AND kommune_centroid_y IS NOT NULL"
                )
            else:
                # Use actual kommune geometries - need to join with kommune_boundaries table
                self.log.info("✅ Using actual kommune polygon geometries")
                geometry_select = """
                    json_extract(ST_AsGeoJSON(kb.geometry), '$.geometry')
                """
                geometry_condition = "kb.geometry IS NOT NULL"

            # Create GeoJSON with proper kommune geometries
            query = f"""
                COPY (
                    SELECT 
                        'Feature' as type,
                        {geometry_select} as geometry,
                        json_object(
                            'kommune_code', r.kommune_code,
                            'kommune_name', r.kommune_name,
                            'region_code', r.region_code,
                            'year', {year},
                            'kommune_area_ha', ROUND(COALESCE(r.kommune_area_ha, 0), 2),
                            'agricultural_area_ha', ROUND(COALESCE(r.total_agricultural_area_ha, 0), 2),
                            'agricultural_coverage_pct', ROUND(COALESCE(r.agricultural_coverage_pct, 0), 1),
                            'pfas_grams', ROUND(COALESCE(r.total_pfas_containing_active_ingredient_grams, 0), 3),
                            'pesticide_load', ROUND(COALESCE(r.total_pesticide_belastning, 0), 3),
                            'pfas_intensity', ROUND(COALESCE(r.pfas_containing_active_ingredient_intensity_grams_per_ha, 0), 3),
                            'pesticide_intensity', ROUND(COALESCE(r.pesticide_belastning_per_ha, 0), 3),
                            'field_count', COALESCE(r.unique_field_count, 0),
                            'company_count', COALESCE(r.unique_company_count, 0),
                            'applications', COALESCE(r.total_pesticide_applications, 0),
                            'pfas_applications', COALESCE(r.pfas_containing_applications, 0),
                            'crop_diversity', COALESCE(r.crop_diversity, 0),
                            'crop_types', COALESCE(r.crop_types, ''),
                            
                            -- Classification for visualization
                            'pfas_class', CASE 
                                WHEN COALESCE(r.total_pfas_containing_active_ingredient_grams, 0) > 10000 THEN 'very_high'
                                WHEN COALESCE(r.total_pfas_containing_active_ingredient_grams, 0) > 1000 THEN 'high'
                                WHEN COALESCE(r.total_pfas_containing_active_ingredient_grams, 0) > 100 THEN 'medium'
                                WHEN COALESCE(r.total_pfas_containing_active_ingredient_grams, 0) > 0 THEN 'low'
                                ELSE 'none'
                            END,
                            
                            -- Color coding for visualization
                            'pfas_color', CASE 
                                WHEN COALESCE(r.total_pfas_containing_active_ingredient_grams, 0) > 10000 THEN '#8c2d04'
                                WHEN COALESCE(r.total_pfas_containing_active_ingredient_grams, 0) > 1000 THEN '#d73027'
                                WHEN COALESCE(r.total_pfas_containing_active_ingredient_grams, 0) > 100 THEN '#f46d43'
                                WHEN COALESCE(r.total_pfas_containing_active_ingredient_grams, 0) > 10 THEN '#fdae61'
                                WHEN COALESCE(r.total_pfas_containing_active_ingredient_grams, 0) > 0 THEN '#fee08b'
                                ELSE '#e6f598'
                            END
                        ) as properties
                    FROM {results_table} r
                    {"LEFT JOIN kommune_boundaries kb ON r.kommune_code = kb.kommune_code" if has_geometry else ""}
                    WHERE {geometry_condition}
                    ORDER BY COALESCE(r.total_pfas_containing_active_ingredient_grams, 0) DESC
                ) TO '{geojson_path}' (FORMAT JSON, ARRAY true)
            """

            self.conn.execute(query)

            if geojson_path.exists():
                size_mb = geojson_path.stat().st_size / (1024 * 1024)
                self.log.info(f"✅ Kommune GeoJSON created: {size_mb:.1f} MB")
                return str(geojson_path)
            else:
                self.log.error("❌ Kommune GeoJSON file was not created")
                return None

        except Exception as e:
            self.log.error(f"❌ Kommune GeoJSON creation failed: {e}")
            return None

    def _generate_kommune_pmtiles(self, geojson_path: str, year: int) -> str | None:
        """Generate PMTiles for kommune data using tippecanoe."""
        try:
            temp_dir = Path(tempfile.gettempdir())
            pmtiles_path = temp_dir / f"kommune_pfas_{year}.pmtiles"

            # Clean up any existing PMTiles file first
            if pmtiles_path.exists():
                pmtiles_path.unlink()

            self.log.info(f"🔧 Generating kommune PMTiles: {pmtiles_path}")

            # Kommune polygon data needs different zoom levels and optimization
            min_zoom = 3  # Country level - can see all of Denmark
            max_zoom = 12  # Municipality level - detailed boundaries
            base_zoom = 6  # Regional level - good balance

            cmd = [
                "tippecanoe",
                "--output",
                str(pmtiles_path),
                "--force",  # Overwrite existing
                "--layer",
                f"kommune_pfas_{year}",
                "--minimum-zoom",
                str(min_zoom),
                "--maximum-zoom",
                str(max_zoom),
                "--base-zoom",
                str(base_zoom),
                "--simplification",
                "10",  # Polygon simplification for performance
                "--buffer",
                "5",  # Buffer for polygon edges
                "--polygon-buffer",
                "1",  # Additional polygon buffer
                "--drop-densest-as-needed",  # Handle overlapping polygons
                "--extend-zooms-if-still-dropping",
                "--coalesce-densest-as-needed",  # Merge dense polygons
                "--detect-shared-borders",  # Optimize shared municipality borders
                "--attribute-type",
                "pfas_grams:float",
                "--attribute-type",
                "pesticide_load:float",
                "--attribute-type",
                "pfas_intensity:float",
                "--attribute-type",
                "pesticide_intensity:float",
                "--attribute-type",
                "agricultural_area_ha:float",
                "--attribute-type",
                "agricultural_coverage_pct:float",
                "--attribute-type",
                "kommune_area_ha:float",
                "--attribute-type",
                "field_count:int",
                "--attribute-type",
                "company_count:int",
                "--attribute-type",
                "applications:int",
                "--attribute-type",
                "pfas_applications:int",
                "--attribute-type",
                "crop_diversity:int",
                "--attribute-type",
                "year:int",
                "--attribute-type",
                "kommune_code:int",
                geojson_path,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            # Clean up GeoJSON to save space
            os.remove(geojson_path)

            if pmtiles_path.exists():
                size_mb = pmtiles_path.stat().st_size / (1024 * 1024)
                self.log.info(f"✅ Kommune PMTiles generated: {size_mb:.1f} MB")
                return str(pmtiles_path)
            else:
                self.log.error("❌ Kommune PMTiles file was not created")
                return None

        except subprocess.CalledProcessError as e:
            self.log.error(f"❌ Tippecanoe failed for kommune data: {e.stderr}")
            return None
        except Exception as e:
            self.log.error(f"❌ Kommune PMTiles generation failed: {e}")
            return None

    def _upload_kommune_pmtiles_to_gcs(self, pmtiles_path: str, year: int) -> str | None:
        """Upload kommune PMTiles to GCS."""
        try:
            import shutil
            from datetime import datetime

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            gcs_path = f"gs://{self.config.bucket}/gold/pmtiles/kommune_pfas_{year}/{timestamp}/kommune_pfas_{year}.pmtiles"

            self.log.info(f"☁️ Uploading kommune PMTiles to: {gcs_path}")

            # Upload using GCS access - use the correct streaming method
            with open(pmtiles_path, "rb") as src:
                with self.gcs_access.fs.open(gcs_path, "wb") as dst:
                    shutil.copyfileobj(src, dst)

            # Clean up local file after successful upload
            if os.path.exists(pmtiles_path):
                os.remove(pmtiles_path)

            return gcs_path

        except Exception as e:
            self.log.error(f"❌ Kommune PMTiles upload failed: {e}")
            # Clean up local file on error too
            if os.path.exists(pmtiles_path):
                try:
                    os.remove(pmtiles_path)
                except Exception:
                    pass
            return None

    def generate_pmtiles_for_multiple_years(self, years_data: dict[int, str]) -> dict[int, str]:
        """Generate PMTiles for multiple years.

        Args:
            years_data: Dict mapping year to results table name

        Returns:
            Dict mapping year to PMTiles path (GCS or local)
        """
        results = {}

        for year, table_name in years_data.items():
            pmtiles_path = self.generate_pmtiles_for_year(table_name, year)
            if pmtiles_path:
                results[year] = pmtiles_path

        self.log.info(f"✅ Generated PMTiles for {len(results)}/{len(years_data)} years")
        return results

    def _cleanup_temp_files_for_year(self, year: int) -> None:
        """Clean up temporary files created during PMTiles generation for a specific year."""
        try:
            temp_dir = Path(tempfile.gettempdir())

            # PMTiles-specific cleanup patterns - expanded for comprehensive cleanup
            patterns = [
                # Year-specific H3 patterns
                f"h3_pfas_{year}_*.geojson",
                f"h3_pfas_{year}_*.pmtiles",
                f"h3_pfas_{year}_res*.geojson",
                f"h3_pfas_{year}_res*.pmtiles",
                # Year-specific kommune patterns
                f"kommune_pfas_{year}.geojson",
                f"kommune_pfas_{year}.pmtiles",
                f"kommune_pfas_{year}_*.geojson",
                f"kommune_pfas_{year}_*.pmtiles",
                # Tippecanoe temporary files
                f"tippecanoe_*_{year}_*",
                f"*_{year}_tippecanoe*",
                f"tippecanoe_{year}_*",
                # Resolution-specific patterns
                f"*_res{self.config.h3_resolution}_{year}*",
                f"*_{year}_res{self.config.h3_resolution}*",
                # General PMTiles artifacts
                f"*_{year}_*.mbtiles",
                f"*_{year}_*.geojsonl",
                f"*_{year}_*.jsonl",
                # Processing artifacts for the year
                f"processing_{year}_*",
                f"analysis_{year}_*",
                f"results_{year}_*",
            ]

            files_cleaned = 0
            for pattern in patterns:
                for file_path in temp_dir.glob(pattern):
                    try:
                        if file_path.exists():
                            file_path.unlink()
                            files_cleaned += 1
                    except Exception as e:
                        self.log.debug(f"Could not remove {file_path}: {e}")

            if files_cleaned > 0:
                self.log.debug(
                    f"🧹 PMTiles generator cleaned up {files_cleaned} temporary files for year {year}"
                )

        except Exception as e:
            self.log.debug(f"PMTiles cleanup warning for year {year}: {e}")
