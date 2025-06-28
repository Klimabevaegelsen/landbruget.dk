"""
Field Area Analysis Gold Layer

This module implements the gold layer processor for field area analysis.
It performs comprehensive spatial analysis of agricultural fields against multiple datasets
including properties, soil types, BNBO status, wetlands, and water projects.

Migrated from the standalone field_area_analysis_pipeline to the unified pipeline architecture.
"""

import os
import tempfile
from typing import Any, Dict, Optional

import duckdb
import pandas as pd
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger


class FieldAreaAnalysisGoldConfig(BaseJobConfig):
    """Configuration for Field Area Analysis gold layer."""

    name: str = "Field Area Analysis Gold"
    dataset: str = "field_area_analysis"
    type: str = "gold"
    description: str = "Comprehensive spatial analysis of agricultural fields"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    # Input silver datasets
    agricultural_fields_dataset: str = "agricultural_fields"
    properties_dataset: str = "property_cadastral_merged"
    soil_types_dataset: str = "soil_types"
    bnbo_status_dataset: str = "bnbo_status_dissolved"
    wetlands_dataset: str = "wetlands_dissolved"
    water_projects_dataset: str = "water_projects_dissolved"

    # Processing configuration
    batch_size: int = 2500  # Increased batch size for better performance with 16GB RAM
    memory_limit: str = "12GB"  # Use 75% of available 16GB RAM for optimal performance
    thread_count: int = 4  # Use all available CPU cores

    # Quality thresholds
    min_area_threshold: float = 0.01  # Minimum area share to include (1%)

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class FieldAreaAnalysisGold(BaseSource[FieldAreaAnalysisGoldConfig], GoldJobInterface):
    """
    Gold layer processor for field area analysis.

    Performs comprehensive spatial analysis of agricultural fields against multiple datasets
    to create analytics-ready spatial intersection results.
    """

    def __init__(self, config: FieldAreaAnalysisGoldConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)
        self.log = Logger.get_logger()

        # Initialize DuckDB connection for spatial operations
        self.conn = duckdb.connect()
        self._configure_duckdb()

    def _configure_duckdb(self):
        """Configure DuckDB for optimal spatial operations."""
        self.conn.execute(f"SET memory_limit='{self.config.memory_limit}'")
        self.conn.execute(f"SET threads={self.config.thread_count}")
        self.conn.execute(f"SET max_memory='{self.config.memory_limit}'")

        # Install and load extensions for spatial operations
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")

        self.log.info("✅ DuckDB Spatial configured - Field Area Analysis Gold Layer")
        self.log.info(
            f"   Memory: {self.config.memory_limit}, Threads: {self.config.thread_count}, Batch size: {self.config.batch_size:,}"
        )

    def _load_silver_data_streaming(self, silver_data: Optional[Dict[str, Any]]) -> Dict[str, str]:
        """Load silver data paths for streaming processing, avoiding memory loading of large datasets."""

        dataset_paths = {}
        required_datasets = [
            self.config.agricultural_fields_dataset,
            self.config.properties_dataset,
            self.config.soil_types_dataset,
            self.config.bnbo_status_dataset,
            self.config.wetlands_dataset,
            self.config.water_projects_dataset,
        ]

        # Large datasets that should be streamed directly to DuckDB
        large_datasets = {self.config.properties_dataset}

        for dataset_name in required_datasets:
            if silver_data and dataset_name in silver_data and dataset_name not in large_datasets:
                # Use in-memory data for smaller datasets only
                self.log.info(f"Using in-memory silver data for {dataset_name}")
                dataset_paths[dataset_name] = silver_data[dataset_name]
            else:
                # Get file paths for streaming processing
                self.log.info(f"Setting up streaming for {dataset_name}")
                try:
                    files = self.gcs_util.list_files(
                        bucket_name=self.config.bucket,
                        prefix=f"silver/{dataset_name}/",
                    )
                    if files:
                        latest_file = max(files, key=lambda x: x.time_created)
                        dataset_paths[dataset_name] = (
                            f"gs://{self.config.bucket}/{latest_file.name}"
                        )
                        self.log.info(f"Found {dataset_name}: {latest_file.name}")
                    else:
                        self.log.warning(f"No data found for {dataset_name}")
                        dataset_paths[dataset_name] = None
                except Exception as e:
                    self.log.error(f"Error finding {dataset_name}: {e}")
                    dataset_paths[dataset_name] = None

        return dataset_paths

    def _load_reference_data_into_duckdb_streaming(self, dataset_paths: Dict[str, Any]):
        """Load reference datasets into DuckDB tables using streaming for large datasets."""

        self.log.info("Loading reference datasets into DuckDB with streaming...")

        # Load properties - STREAM LARGE DATASET DIRECTLY
        properties_path = dataset_paths.get(self.config.properties_dataset)
        if (
            properties_path
            and isinstance(properties_path, str)
            and properties_path.startswith("gs://")
        ):
            self.log.info("🏠 Streaming property cadastral data from GCS...")

            # Download to temporary file
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
                temp_path = tmp_file.name

            try:
                bucket_name = properties_path.split("/")[2]
                blob_name = "/".join(properties_path.split("/")[3:])
                self.gcs_util.download_file(bucket_name, blob_name, temp_path)

                # Load directly into DuckDB without pandas
                self.conn.execute(f"""
                    CREATE TABLE properties AS
                    SELECT 
                        bfe_number,
                        ST_GeomFromWKB(geometry) as geom
                    FROM read_parquet('{temp_path}')
                    WHERE bfe_number IS NOT NULL
                """)

                property_count = self.conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
                self.log.info(f"    ✅ Streamed {property_count:,} properties directly to DuckDB")

            finally:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)

        elif properties_path is not None:
            # Handle in-memory data
            self.log.info(
                f"🏠 Loading property cadastral data from memory ({len(properties_path)} records)..."
            )
            self.conn.register("properties_df", properties_path)
            self.conn.execute("""
                CREATE TABLE properties AS
                SELECT 
                    bfe_number,
                    geometry as geom
                FROM properties_df
                WHERE bfe_number IS NOT NULL
            """)
        else:
            self.log.warning("No property data available - creating empty table")
            self.conn.execute("CREATE TABLE properties (bfe_number VARCHAR, geom GEOMETRY)")

        # Load other datasets - these are typically smaller and can be loaded normally
        for dataset_name, dataset_data in dataset_paths.items():
            if dataset_name == self.config.properties_dataset:
                continue  # Already handled above

            if dataset_data is None:
                self._create_empty_table_for_dataset(dataset_name)
                continue

            if isinstance(dataset_data, str) and dataset_data.startswith("gs://"):
                # Stream smaller datasets if needed
                self._load_dataset_from_gcs(dataset_name, dataset_data)
            else:
                # Load from memory
                self._load_dataset_from_memory(dataset_name, dataset_data)

    def _create_empty_table_for_dataset(self, dataset_name: str):
        """Create empty tables for missing datasets."""
        if dataset_name == self.config.soil_types_dataset:
            self.log.warning("No soil data available - creating empty table")
            self.conn.execute(
                "CREATE TABLE soil_types (soil_description VARCHAR, soil_code VARCHAR, geom GEOMETRY)"
            )
        elif dataset_name == self.config.bnbo_status_dataset:
            self.log.warning("No BNBO data available - creating empty table")
            self.conn.execute("CREATE TABLE bnbo_areas (status_category VARCHAR, geom GEOMETRY)")
        elif dataset_name == self.config.wetlands_dataset:
            self.log.warning("No wetlands data available - creating empty table")
            self.conn.execute("CREATE TABLE wetlands (wetland_id VARCHAR, geom GEOMETRY)")
        elif dataset_name == self.config.water_projects_dataset:
            self.log.warning("No water projects data available - creating empty table")
            self.conn.execute("CREATE TABLE water_projects (project_id VARCHAR, geom GEOMETRY)")

    def _load_dataset_from_gcs(self, dataset_name: str, gcs_path: str):
        """Load dataset from GCS path into DuckDB."""

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            temp_path = tmp_file.name

        try:
            bucket_name = gcs_path.split("/")[2]
            blob_name = "/".join(gcs_path.split("/")[3:])
            self.gcs_util.download_file(bucket_name, blob_name, temp_path)

            if dataset_name == self.config.soil_types_dataset:
                self.conn.execute(f"""
                    CREATE TABLE soil_types AS
                    SELECT 
                        soil_description,
                        soil_code,
                        ST_GeomFromWKB(geometry) as geom
                    FROM read_parquet('{temp_path}')
                """)
                count = self.conn.execute("SELECT COUNT(*) FROM soil_types").fetchone()[0]
                self.log.info(f"    ✅ Loaded {count:,} soil areas from GCS")

            elif dataset_name == self.config.bnbo_status_dataset:
                self.conn.execute(f"""
                    CREATE TABLE bnbo_areas AS
                    SELECT 
                        status_category,
                        ST_GeomFromWKB(geometry) as geom
                    FROM read_parquet('{temp_path}')
                """)
                count = self.conn.execute("SELECT COUNT(*) FROM bnbo_areas").fetchone()[0]
                self.log.info(f"    ✅ Loaded {count:,} BNBO areas from GCS")

            elif dataset_name == self.config.wetlands_dataset:
                self.conn.execute(f"""
                    CREATE TABLE wetlands AS
                    SELECT 
                        wetland_id,
                        ST_GeomFromWKB(geometry) as geom
                    FROM read_parquet('{temp_path}')
                """)
                count = self.conn.execute("SELECT COUNT(*) FROM wetlands").fetchone()[0]
                self.log.info(f"    ✅ Loaded {count:,} wetlands from GCS")

            elif dataset_name == self.config.water_projects_dataset:
                self.conn.execute(f"""
                    CREATE TABLE water_projects AS
                    SELECT 
                        project_id,
                        ST_GeomFromWKB(geometry) as geom
                    FROM read_parquet('{temp_path}')
                """)
                count = self.conn.execute("SELECT COUNT(*) FROM water_projects").fetchone()[0]
                self.log.info(f"    ✅ Loaded {count:,} water projects from GCS")

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def _load_dataset_from_memory(self, dataset_name: str, dataset_data):
        """Load dataset from memory into DuckDB."""
        if dataset_name == self.config.soil_types_dataset:
            self.log.info(f"🌱 Loading soil types from memory ({len(dataset_data)} records)...")
            self.conn.register("soil_df", dataset_data)
            self.conn.execute("""
                CREATE TABLE soil_types AS
                SELECT 
                    soil_description,
                    soil_code,
                    geometry as geom
                FROM soil_df
            """)
        elif dataset_name == self.config.bnbo_status_dataset:
            self.log.info(
                f"🛡️ Loading BNBO status areas from memory ({len(dataset_data)} records)..."
            )
            self.conn.register("bnbo_df", dataset_data)
            self.conn.execute("""
                CREATE TABLE bnbo_areas AS
                SELECT 
                    status_category,
                    geometry as geom
                FROM bnbo_df
            """)
        elif dataset_name == self.config.wetlands_dataset:
            self.log.info(f"🌊 Loading wetlands from memory ({len(dataset_data)} records)...")
            self.conn.register("wetlands_df", dataset_data)
            self.conn.execute("""
                CREATE TABLE wetlands AS
                SELECT 
                    wetland_id,
                    geometry as geom
                FROM wetlands_df
            """)
        elif dataset_name == self.config.water_projects_dataset:
            self.log.info(f"💧 Loading water projects from memory ({len(dataset_data)} records)...")
            self.conn.register("water_df", dataset_data)
            self.conn.execute("""
                CREATE TABLE water_projects AS
                SELECT 
                    project_id,
                    geometry as geom
                FROM water_df
            """)

    def _process_field_batch_spatial(self, fields_batch: pd.DataFrame) -> pd.DataFrame:
        """Process field batch with spatial analysis using unified pipeline data."""

        # Register the fields batch with DuckDB
        self.conn.register("fields_batch_df", fields_batch)

        # Load the field batch - only essential fields
        self.conn.execute("""
            CREATE OR REPLACE TABLE current_fields AS
            SELECT 
                field_id,
                block_id,
                cvr_number,
                geometry as geom
            FROM fields_batch_df
        """)

        actual_fields = self.conn.execute("SELECT COUNT(*) FROM current_fields").fetchone()[0]
        self.log.info(f"    📋 Processing {actual_fields:,} fields...")

        # Property analysis - SINGLE spatial join condition only (DuckDB Spatial v1.2.2 limitation)
        self.log.info("    🔍 Analyzing property ownership...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_property_intersections AS
            SELECT 
                f.field_id,
                f.block_id,
                p.bfe_number,
                f.geom as field_geom,
                p.geom as property_geom
            FROM current_fields f
            JOIN properties p ON ST_Intersects(f.geom, p.geom)
        """)

        # Calculate area shares separately (not in JOIN due to single condition limitation)
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE field_property_shares AS
            SELECT 
                field_id,
                block_id,
                bfe_number,
                ST_Area(ST_Intersection(field_geom, property_geom)) / ST_Area(field_geom) * 100 as area_share
            FROM field_property_intersections
            WHERE ST_Area(ST_Intersection(field_geom, property_geom)) / ST_Area(field_geom) > {self.config.min_area_threshold}
        """)

        # Soil analysis - SINGLE spatial join condition only
        self.log.info("    🌱 Analyzing soil types...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_soil_intersections AS
            SELECT 
                f.field_id,
                f.block_id,
                s.soil_code,
                s.soil_description,
                f.geom as field_geom,
                s.geom as soil_geom
            FROM current_fields f
            JOIN soil_types s ON ST_Intersects(f.geom, s.geom)
        """)

        # Calculate soil area shares separately
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE field_soil_shares AS
            SELECT 
                field_id,
                block_id,
                soil_code,
                soil_description,
                ST_Area(ST_Intersection(field_geom, soil_geom)) / ST_Area(field_geom) * 100 as area_share
            FROM field_soil_intersections
            WHERE ST_Area(ST_Intersection(field_geom, soil_geom)) / ST_Area(field_geom) > {self.config.min_area_threshold}
        """)

        # BNBO analysis - SINGLE spatial join condition only
        self.log.info("    🛡️ Analyzing BNBO status...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_bnbo_intersections AS
            SELECT 
                f.field_id,
                f.block_id,
                b.status_category,
                f.geom as field_geom,
                b.geom as bnbo_geom
            FROM current_fields f
            JOIN bnbo_areas b ON ST_Intersects(f.geom, b.geom)
        """)

        # Calculate BNBO area shares separately
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE field_bnbo_shares AS
            SELECT 
                field_id,
                block_id,
                status_category,
                ST_Area(ST_Intersection(field_geom, bnbo_geom)) / ST_Area(field_geom) * 100 as area_share
            FROM field_bnbo_intersections
            WHERE ST_Area(ST_Intersection(field_geom, bnbo_geom)) / ST_Area(field_geom) > {self.config.min_area_threshold}
        """)

        # Wetlands analysis - SINGLE spatial join condition only
        self.log.info("    🌊 Analyzing wetlands...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_wetland_intersections AS
            SELECT 
                f.field_id,
                f.block_id,
                f.geom as field_geom,
                w.geom as wetland_geom
            FROM current_fields f
            JOIN wetlands w ON ST_Intersects(f.geom, w.geom)
        """)

        # Calculate wetland area shares with proper union aggregation
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_wetland_shares AS
            SELECT 
                field_id,
                block_id,
                ST_Area(ST_Union_Agg(ST_Intersection(field_geom, wetland_geom))) / ST_Area(field_geom) * 100 as wetland_area_share
            FROM field_wetland_intersections
            GROUP BY field_id, block_id, field_geom
        """)

        # Water projects analysis - SINGLE spatial join condition only
        self.log.info("    💧 Analyzing water projects...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_water_intersections AS
            SELECT 
                f.field_id,
                f.block_id,
                f.geom as field_geom,
                wp.geom as water_geom
            FROM current_fields f
            JOIN water_projects wp ON ST_Intersects(f.geom, wp.geom)
        """)

        # Calculate water projects area shares with proper union aggregation
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_water_projects_shares AS
            SELECT 
                field_id,
                block_id,
                ST_Area(ST_Union_Agg(ST_Intersection(field_geom, water_geom))) / ST_Area(field_geom) * 100 as water_projects_area_share
            FROM field_water_intersections
            GROUP BY field_id, block_id, field_geom
        """)

        # Wetland-water projects overlap analysis - TWO separate spatial joins (limitation compliance)
        self.log.info("    🌊💧 Analyzing wetland-water projects overlap...")

        # First: Get field-wetland intersections (already done above)
        # Second: Get those intersections that also intersect with water projects
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetland_water_intersections AS
            SELECT 
                fwi.field_id,
                fwi.block_id,
                fwi.field_geom,
                ST_Intersection(fwi.field_geom, fwi.wetland_geom) as field_wetland_intersection,
                wp.geom as water_geom
            FROM field_wetland_intersections fwi
            JOIN water_projects wp ON ST_Intersects(ST_Intersection(fwi.field_geom, fwi.wetland_geom), wp.geom)
        """)

        # Calculate wetland-water overlap shares
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_wetland_water_overlap AS
            SELECT 
                wwi.field_id,
                wwi.block_id,
                ST_Area(ST_Union_Agg(ST_Intersection(wwi.field_wetland_intersection, wwi.water_geom))) / 
                ST_Area(ST_Union_Agg(wwi.field_wetland_intersection)) * 100 as wetland_water_projects_share
            FROM wetland_water_intersections wwi
            GROUP BY wwi.field_id, wwi.block_id
        """)

        # BNBO-water projects overlap analysis - TWO separate spatial joins
        self.log.info("    🛡️💧 Analyzing BNBO-water projects overlap...")

        # Get BNBO intersections that also intersect with water projects
        self.conn.execute("""
            CREATE OR REPLACE TABLE bnbo_water_intersections AS
            SELECT 
                fbi.field_id,
                fbi.block_id,
                fbi.status_category,
                fbi.field_geom,
                ST_Intersection(fbi.field_geom, fbi.bnbo_geom) as field_bnbo_intersection,
                wp.geom as water_geom
            FROM field_bnbo_intersections fbi
            JOIN water_projects wp ON ST_Intersects(ST_Intersection(fbi.field_geom, fbi.bnbo_geom), wp.geom)
        """)

        # Calculate BNBO-water overlap shares by status category
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_bnbo_water_overlap AS
            SELECT 
                field_id,
                block_id,
                status_category,
                ST_Area(ST_Intersection(field_bnbo_intersection, water_geom)) / 
                ST_Area(field_bnbo_intersection) * 100 as bnbo_water_projects_share
            FROM bnbo_water_intersections
            WHERE ST_Area(field_bnbo_intersection) > 0
        """)

        # Create JSON aggregation tables using DuckDB
        self.log.info("    🔗 Creating JSON aggregations...")

        # Property shares JSON
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_property_json AS
            SELECT 
                field_id,
                block_id,
                COALESCE('{' || string_agg('"' || bfe_number || '":' || area_share, ',') || '}', '{}') as property_area_shares
            FROM field_property_shares
            GROUP BY field_id, block_id
        """)

        # Soil shares JSON
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_soil_json AS
            SELECT 
                field_id,
                block_id,
                COALESCE('{' || string_agg('"' || soil_code || '":' || area_share, ',') || '}', '{}') as soil_area_shares
            FROM field_soil_shares
            GROUP BY field_id, block_id
        """)

        # BNBO shares JSON
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_bnbo_json AS
            SELECT 
                field_id,
                block_id,
                COALESCE('{' || string_agg('"' || status_category || '":' || area_share, ',') || '}', '{}') as bnbo_area_shares
            FROM field_bnbo_shares
            GROUP BY field_id, block_id
        """)

        # BNBO-water overlap shares JSON
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_bnbo_water_json AS
            SELECT 
                field_id,
                block_id,
                COALESCE('{' || string_agg('"' || status_category || '":' || bnbo_water_projects_share, ',') || '}', '{}') as bnbo_water_projects_shares
            FROM field_bnbo_water_overlap
            GROUP BY field_id, block_id
        """)

        # Combine all results into final table using DuckDB
        self.log.info("    🔗 Combining all results...")
        results = self.conn.execute("""
            SELECT 
                f.field_id,
                f.block_id,
                f.cvr_number,
                COALESCE(fw.wetland_area_share, 0) as wetland_area_share,
                COALESCE(fwo.wetland_water_projects_share, 0) as wetland_water_projects_share,
                COALESCE(fwp.water_projects_area_share, 0) as water_projects_area_share,
                COALESCE(fpj.property_area_shares, '{}') as property_area_shares,
                COALESCE(fsj.soil_area_shares, '{}') as soil_area_shares,
                COALESCE(fbj.bnbo_area_shares, '{}') as bnbo_area_shares,
                COALESCE(fbwj.bnbo_water_projects_shares, '{}') as bnbo_water_projects_shares
            FROM current_fields f
            LEFT JOIN field_wetland_shares fw ON f.field_id = fw.field_id AND f.block_id = fw.block_id
            LEFT JOIN field_wetland_water_overlap fwo ON f.field_id = fwo.field_id AND f.block_id = fwo.block_id
            LEFT JOIN field_water_projects_shares fwp ON f.field_id = fwp.field_id AND f.block_id = fwp.block_id
            LEFT JOIN field_property_json fpj ON f.field_id = fpj.field_id AND f.block_id = fpj.block_id
            LEFT JOIN field_soil_json fsj ON f.field_id = fsj.field_id AND f.block_id = fsj.block_id
            LEFT JOIN field_bnbo_json fbj ON f.field_id = fbj.field_id AND f.block_id = fbj.block_id
            LEFT JOIN field_bnbo_water_json fbwj ON f.field_id = fbwj.field_id AND f.block_id = fbwj.block_id
        """).df()

        return results

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """
        Run the field area analysis gold processing.

        Args:
            silver_data: Optional dictionary of silver datasets for in-memory processing
        """
        self.log.info("Starting Field Area Analysis Gold processing")

        try:
            # Load silver data paths for streaming processing
            dataset_paths = self._load_silver_data_streaming(silver_data)

            # Check if we have the required agricultural fields data
            agricultural_fields_path = dataset_paths.get(self.config.agricultural_fields_dataset)
            if agricultural_fields_path is None:
                self.log.error(
                    "No agricultural fields data available - cannot proceed with analysis"
                )
                return

            # Load agricultural fields data (smaller dataset, can be loaded into memory)
            if isinstance(agricultural_fields_path, str) and agricultural_fields_path.startswith(
                "gs://"
            ):
                # Load from GCS
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
                    temp_path = tmp_file.name

                try:
                    bucket_name = agricultural_fields_path.split("/")[2]
                    blob_name = "/".join(agricultural_fields_path.split("/")[3:])
                    self.gcs_util.download_file(bucket_name, blob_name, temp_path)
                    agricultural_fields = pd.read_parquet(temp_path)
                finally:
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
            else:
                # Use in-memory data
                agricultural_fields = agricultural_fields_path

            self.log.info(f"Loaded {len(agricultural_fields):,} agricultural fields")

            # Load reference data into DuckDB using streaming approach
            self._load_reference_data_into_duckdb_streaming(dataset_paths)

            # Process fields in batches
            all_results = []
            batch_size = self.config.batch_size
            total_batches = (len(agricultural_fields) + batch_size - 1) // batch_size

            self.log.info(
                f"Processing {len(agricultural_fields):,} fields in {total_batches} batches of {batch_size}"
            )

            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min((batch_idx + 1) * batch_size, len(agricultural_fields))
                batch = agricultural_fields.iloc[start_idx:end_idx]

                self.log.info(
                    f"Processing batch {batch_idx + 1}/{total_batches} ({len(batch)} fields)"
                )

                # Process the batch
                batch_results = self._process_field_batch_spatial(batch)
                all_results.append(batch_results)

                # Progress update
                progress = (batch_idx + 1) / total_batches * 100
                self.log.info(f"    ⏱️ Batch completed | Progress: {progress:.1f}%")

            # Combine all results
            self.log.info(f"🔗 Combining {len(all_results)} batches...")
            final_results = pd.concat(all_results, ignore_index=True)

            # Log summary statistics
            total_fields = len(final_results)
            fields_with_wetlands = (final_results["wetland_area_share"] > 0).sum()
            fields_with_water_projects = (final_results["water_projects_area_share"] > 0).sum()
            fields_with_properties = (final_results["property_area_shares"] != "{}").sum()
            fields_with_soil = (final_results["soil_area_shares"] != "{}").sum()
            fields_with_bnbo = (final_results["bnbo_area_shares"] != "{}").sum()

            self.log.info("Field Area Analysis summary:")
            self.log.info(f"  Total fields analyzed: {total_fields:,}")
            self.log.info(
                f"  Fields with wetlands: {fields_with_wetlands:,} ({fields_with_wetlands / total_fields * 100:.1f}%)"
            )
            self.log.info(
                f"  Fields with water projects: {fields_with_water_projects:,} ({fields_with_water_projects / total_fields * 100:.1f}%)"
            )
            self.log.info(
                f"  Fields with property data: {fields_with_properties:,} ({fields_with_properties / total_fields * 100:.1f}%)"
            )
            self.log.info(
                f"  Fields with soil data: {fields_with_soil:,} ({fields_with_soil / total_fields * 100:.1f}%)"
            )
            self.log.info(
                f"  Fields with BNBO data: {fields_with_bnbo:,} ({fields_with_bnbo / total_fields * 100:.1f}%)"
            )

            # Save to gold layer
            self._save_data(final_results, self.config.dataset, self.config.bucket, stage="gold")

            self.log.info("Field Area Analysis Gold processing completed successfully")

        except Exception as e:
            self.log.error(f"Field Area Analysis Gold processing failed: {e}")
            raise

        finally:
            if hasattr(self, "conn"):
                self.conn.close()
