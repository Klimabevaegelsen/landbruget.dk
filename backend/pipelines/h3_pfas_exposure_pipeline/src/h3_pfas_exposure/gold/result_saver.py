"""
Result saving utilities for H3 PFAS exposure analysis.
"""

from datetime import datetime

import duckdb
from loguru import logger

from ..config import H3SpatialConfig
from .pmtiles_generator import H3PMTilesGenerator


class H3ResultSaver:
    """Handles saving analysis results to GCS."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, config: H3SpatialConfig, gcs_access=None):
        self.conn = conn
        self.config = config
        self.gcs_access = gcs_access
        self.log = logger.bind(component="H3ResultSaver")

        # Initialize PMTiles generator
        self.pmtiles_generator = H3PMTilesGenerator(conn, config, gcs_access)

    def save_year_results_kepler_compatible(self, results_table: str, year: int) -> int:
        """Save results to GCS with Kepler.gl compatibility fixes."""
        self.log.info(
            f"💾 Saving Kepler.gl-compatible H3 pesticide exposure results for year {year} (resolution {self.config.h3_resolution}) to GCS"
        )

        # Create Kepler.gl compatible version by converting BigInt columns to regular numbers
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE final_results_kepler_{year} AS
            SELECT
                -- Convert H3 cell to string format with correct column name for Kepler.gl H3 layer auto-detection
                CAST(h3_cell AS VARCHAR) as h3_id,
                CAST(center_lat AS DOUBLE) as center_lat,
                CAST(center_lon AS DOUBLE) as center_lon,
                CAST(h3_cell_area_ha AS DOUBLE) as h3_cell_area_ha,
                CAST(total_intersection_area_ha AS DOUBLE) as total_intersection_area_ha,
                CAST(actual_coverage_ratio AS DOUBLE) as actual_coverage_ratio,

                -- Convert BigInt counts to regular integers (32-bit max: 2.1 billion)
                CAST(unique_field_count AS INTEGER) as unique_field_count,
                CAST(total_pesticide_applications AS INTEGER) as total_pesticide_applications,
                CAST(pfas_containing_applications AS INTEGER) as pfas_containing_applications,
                CAST(diquat_containing_applications AS INTEGER) as diquat_containing_applications,
                CAST(glyphosate_containing_applications AS INTEGER) as glyphosate_containing_applications,
                CAST(crop_diversity AS INTEGER) as crop_diversity,

                -- Active ingredient exposure metrics as doubles
                CAST(total_pfas_containing_active_ingredient_grams AS DOUBLE) as total_pfas_containing_active_ingredient_grams,
                CAST(total_diquat_containing_active_ingredient_grams AS DOUBLE) as total_diquat_containing_active_ingredient_grams,
                CAST(total_glyphosate_containing_active_ingredient_grams AS DOUBLE) as total_glyphosate_containing_active_ingredient_grams,
                -- Pesticide load metrics as doubles
                CAST(total_pesticide_belastning AS DOUBLE) as total_pesticide_belastning,
                CAST(total_pfas_pesticide_belastning AS DOUBLE) as total_pfas_pesticide_belastning,
                CAST(total_diquat_pesticide_belastning AS DOUBLE) as total_diquat_pesticide_belastning,
                CAST(total_glyphosate_pesticide_belastning AS DOUBLE) as total_glyphosate_pesticide_belastning,
                -- Intensity metrics (grams per hectare)
                CAST(pfas_containing_active_ingredient_intensity_grams_per_ha AS DOUBLE) as pfas_containing_active_ingredient_intensity_grams_per_ha,
                CAST(diquat_containing_active_ingredient_intensity_grams_per_ha AS DOUBLE) as diquat_containing_active_ingredient_intensity_grams_per_ha,
                CAST(glyphosate_containing_active_ingredient_intensity_grams_per_ha AS DOUBLE) as glyphosate_containing_active_ingredient_intensity_grams_per_ha,

                -- String fields (no conversion needed)
                crop_types,

                -- Timestamp as string for better compatibility
                CAST(created_at AS VARCHAR) as created_at
            FROM {results_table}
            ORDER BY h3_cell
        """)

        # Create output path for Kepler-compatible version with resolution in filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path_kepler = f"gs://{self.config.bucket}/gold/h3_pesticide_{year}_res{self.config.h3_resolution}/{timestamp}/h3_pesticide_{year}_res{self.config.h3_resolution}_kepler.parquet"

        # Upload Kepler-compatible table
        self.gcs_access.upload_from_duckdb_table(f"final_results_kepler_{year}", output_path_kepler)

        # Also save the original version with BigInt columns
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE final_results_{year} AS
            SELECT
                *,
                CURRENT_TIMESTAMP as created_at
            FROM {results_table}
            ORDER BY h3_cell
        """)

        output_path_original = f"gs://{self.config.bucket}/gold/h3_pesticide_{year}_res{self.config.h3_resolution}/{timestamp}/h3_pesticide_{year}_res{self.config.h3_resolution}.parquet"
        self.gcs_access.upload_from_duckdb_table(f"final_results_{year}", output_path_original)

        # Get count for return
        count = self.conn.execute(f"SELECT COUNT(*) FROM final_results_{year}").fetchone()[0]

        self.log.info(
            f"✅ Saved {count:,} H3 pesticide exposure records for year {year} (resolution {self.config.h3_resolution})"
        )
        self.log.info(f"   📊 Original format: {output_path_original}")
        self.log.info(f"   🗺️  Kepler.gl compatible: {output_path_kepler}")

        # Generate PMTiles for frontend visualization
        self.log.info(f"🗺️ Generating PMTiles for year {year}...")
        pmtiles_path = self.pmtiles_generator.generate_pmtiles_for_year(
            f"final_results_{year}", year
        )
        if pmtiles_path:
            self.log.info(f"   🎯 PMTiles: {pmtiles_path}")
        else:
            self.log.warning(f"   ⚠️  PMTiles generation skipped for year {year}")

        return count

    def save_kommune_results(self, results_table: str, year: int) -> int:
        """Save kommune-level results to GCS."""
        self.log.info(f"💾 Saving kommune-level pesticide exposure results for year {year} to GCS")

        # Create final results table
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE final_kommune_results_{year} AS
            SELECT
                kommune_code,
                kommune_name,
                region_code,
                CAST(kommune_area_ha AS DOUBLE) as kommune_area_ha,
                CAST(kommune_centroid_x AS DOUBLE) as kommune_centroid_x,
                CAST(kommune_centroid_y AS DOUBLE) as kommune_centroid_y,
                CAST(total_agricultural_area_ha AS DOUBLE) as total_agricultural_area_ha,
                CAST(unique_field_count AS INTEGER) as unique_field_count,
                CAST(unique_company_count AS INTEGER) as unique_company_count,
                CAST(avg_field_coverage_ratio AS DOUBLE) as avg_field_coverage_ratio,
                CAST(max_field_coverage_ratio AS DOUBLE) as max_field_coverage_ratio,
                CAST(min_field_coverage_ratio AS DOUBLE) as min_field_coverage_ratio,
                CAST(crop_diversity AS INTEGER) as crop_diversity,
                crop_types,
                -- Active ingredient totals
                CAST(total_pfas_containing_active_ingredient_grams AS DOUBLE) as total_pfas_containing_active_ingredient_grams,
                CAST(total_diquat_containing_active_ingredient_grams AS DOUBLE) as total_diquat_containing_active_ingredient_grams,
                CAST(total_glyphosate_containing_active_ingredient_grams AS DOUBLE) as total_glyphosate_containing_active_ingredient_grams,
                -- Pesticide load totals
                CAST(total_pesticide_belastning AS DOUBLE) as total_pesticide_belastning,
                CAST(total_pfas_pesticide_belastning AS DOUBLE) as total_pfas_pesticide_belastning,
                CAST(total_diquat_pesticide_belastning AS DOUBLE) as total_diquat_pesticide_belastning,
                CAST(total_glyphosate_pesticide_belastning AS DOUBLE) as total_glyphosate_pesticide_belastning,
                -- Application counts
                CAST(total_pesticide_applications AS INTEGER) as total_pesticide_applications,
                CAST(pfas_containing_applications AS INTEGER) as pfas_containing_applications,
                CAST(diquat_containing_applications AS INTEGER) as diquat_containing_applications,
                CAST(glyphosate_containing_applications AS INTEGER) as glyphosate_containing_applications,
                -- Unique product counts
                CAST(unique_pfas_products AS INTEGER) as unique_pfas_products,
                CAST(unique_diquat_products AS INTEGER) as unique_diquat_products,
                CAST(unique_glyphosate_products AS INTEGER) as unique_glyphosate_products,
                CAST(unique_pesticide_products AS INTEGER) as unique_pesticide_products,
                -- Intensity metrics (grams per hectare)
                CAST(pfas_containing_active_ingredient_intensity_grams_per_ha AS DOUBLE) as pfas_containing_active_ingredient_intensity_grams_per_ha,
                CAST(diquat_containing_active_ingredient_intensity_grams_per_ha AS DOUBLE) as diquat_containing_active_ingredient_intensity_grams_per_ha,
                CAST(glyphosate_containing_active_ingredient_intensity_grams_per_ha AS DOUBLE) as glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                -- Pesticide load intensity metrics
                CAST(pesticide_belastning_per_ha AS DOUBLE) as pesticide_belastning_per_ha,
                CAST(pfas_pesticide_belastning_per_ha AS DOUBLE) as pfas_pesticide_belastning_per_ha,
                CAST(diquat_pesticide_belastning_per_ha AS DOUBLE) as diquat_pesticide_belastning_per_ha,
                CAST(glyphosate_pesticide_belastning_per_ha AS DOUBLE) as glyphosate_pesticide_belastning_per_ha,
                CAST(agricultural_coverage_pct AS DOUBLE) as agricultural_coverage_pct,
                CAST(created_at AS VARCHAR) as created_at
            FROM {results_table}
            ORDER BY total_pfas_containing_active_ingredient_grams DESC
        """)

        # Create output paths
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path_parquet = f"gs://{self.config.bucket}/gold/kommune_pesticide_{year}/{timestamp}/kommune_pesticide_{year}.parquet"
        output_path_csv = f"gs://{self.config.bucket}/gold/kommune_pesticide_{year}/{timestamp}/kommune_pesticide_{year}.csv"

        # Upload to GCS in both formats
        self.gcs_access.upload_from_duckdb_table(
            f"final_kommune_results_{year}", output_path_parquet
        )
        self.gcs_access.upload_from_duckdb_table(f"final_kommune_results_{year}", output_path_csv)

        # Get count for return
        count = self.conn.execute(f"SELECT COUNT(*) FROM final_kommune_results_{year}").fetchone()[
            0
        ]

        self.log.info(f"✅ Saved {count:,} kommune pesticide exposure records for year {year}")
        self.log.info(f"   📊 Parquet format: {output_path_parquet}")
        self.log.info(f"   📄 CSV format: {output_path_csv}")

        # Generate PMTiles for frontend visualization
        self.log.info(f"🗺️ Generating kommune PMTiles for year {year}...")
        pmtiles_path = self.pmtiles_generator.generate_kommune_pmtiles_for_year(
            f"final_kommune_results_{year}", year
        )
        if pmtiles_path:
            self.log.info(f"   🎯 PMTiles: {pmtiles_path}")
        else:
            self.log.warning(f"   ⚠️  PMTiles generation skipped for year {year}")

        return count

    def save_local_results(self, results_table: str, year: int, local_data_dir):
        """Save results to local file."""
        if not local_data_dir:
            return

        output_dir = local_data_dir / "results"
        output_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save Kepler.gl compatible version
        output_path_kepler = (
            output_dir
            / f"h3_pesticide_{year}_res{self.config.h3_resolution}_{timestamp}_kepler.parquet"
        )

        # Create Kepler.gl compatible version
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {results_table}_kepler AS
            SELECT
                -- Convert H3 cell to string format with correct column name for Kepler.gl H3 layer auto-detection
                CAST(h3_cell AS VARCHAR) as h3_id,
                CAST(center_lat AS DOUBLE) as center_lat,
                CAST(center_lon AS DOUBLE) as center_lon,
                CAST(h3_cell_area_ha AS DOUBLE) as h3_cell_area_ha,
                CAST(total_intersection_area_ha AS DOUBLE) as total_intersection_area_ha,
                CAST(actual_coverage_ratio AS DOUBLE) as actual_coverage_ratio,

                -- Convert BigInt counts to regular integers for Kepler.gl compatibility
                CAST(unique_field_count AS INTEGER) as unique_field_count,
                CAST(total_pesticide_applications AS INTEGER) as total_pesticide_applications,
                CAST(pfas_containing_applications AS INTEGER) as pfas_containing_applications,
                CAST(diquat_containing_applications AS INTEGER) as diquat_containing_applications,
                CAST(glyphosate_containing_applications AS INTEGER) as glyphosate_containing_applications,
                CAST(crop_diversity AS INTEGER) as crop_diversity,

                -- Active ingredient exposure metrics as doubles
                CAST(total_pfas_containing_active_ingredient_grams AS DOUBLE) as total_pfas_containing_active_ingredient_grams,
                CAST(total_diquat_containing_active_ingredient_grams AS DOUBLE) as total_diquat_containing_active_ingredient_grams,
                CAST(total_glyphosate_containing_active_ingredient_grams AS DOUBLE) as total_glyphosate_containing_active_ingredient_grams,
                -- Pesticide load metrics as doubles
                CAST(total_pesticide_belastning AS DOUBLE) as total_pesticide_belastning,
                CAST(total_pfas_pesticide_belastning AS DOUBLE) as total_pfas_pesticide_belastning,
                CAST(total_diquat_pesticide_belastning AS DOUBLE) as total_diquat_pesticide_belastning,
                CAST(total_glyphosate_pesticide_belastning AS DOUBLE) as total_glyphosate_pesticide_belastning,
                -- Intensity metrics (grams per hectare)
                CAST(pfas_containing_active_ingredient_intensity_grams_per_ha AS DOUBLE) as pfas_containing_active_ingredient_intensity_grams_per_ha,
                CAST(diquat_containing_active_ingredient_intensity_grams_per_ha AS DOUBLE) as diquat_containing_active_ingredient_intensity_grams_per_ha,
                CAST(glyphosate_containing_active_ingredient_intensity_grams_per_ha AS DOUBLE) as glyphosate_containing_active_ingredient_intensity_grams_per_ha,

                -- String fields
                crop_types,

                -- Timestamp as string
                CAST(created_at AS VARCHAR) as created_at
            FROM {results_table}
            ORDER BY h3_cell
        """)

        self.log.info(f"💾 Saving Kepler.gl-compatible results to {output_path_kepler}")
        self.conn.execute(f"COPY {results_table}_kepler TO '{output_path_kepler}' (FORMAT PARQUET)")

        # Also save original version
        output_path_original = (
            output_dir / f"h3_pesticide_{year}_res{self.config.h3_resolution}_{timestamp}.parquet"
        )
        self.log.info(f"💾 Saving original results to {output_path_original}")
        self.conn.execute(f"COPY {results_table} TO '{output_path_original}' (FORMAT PARQUET)")

        if output_path_kepler.exists() and output_path_original.exists():
            kepler_size = output_path_kepler.stat().st_size / (1024 * 1024)
            original_size = output_path_original.stat().st_size / (1024 * 1024)
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {results_table}").fetchone()[0]

            self.log.info(f"✅ Saved {row_count:,} rows of pesticide exposure data")
            self.log.info(f"   📊 Original format: {original_size:.1f} MB")
            self.log.info(f"   🗺️  Kepler.gl compatible: {kepler_size:.1f} MB")
            self.log.info("   🎯 Use the *_kepler.parquet file for Kepler.gl visualization")
