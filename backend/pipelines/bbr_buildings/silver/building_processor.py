"""BBR Building processor for silver layer processing."""

import logging
from pathlib import Path
from typing import Any

import duckdb


# Try to import optimized GCS access with fallback
def _get_optimized_gcs_access():
    """Get optimized GCS access with robust import handling."""
    try:
        from unified_pipeline.util.gcs_access import GCSDataAccess

        logging.info("✅ Successfully imported optimized GCSDataAccess for BBR buildings")
        return GCSDataAccess
    except ImportError as e:
        logging.warning(f"⚠️ Could not import optimized GCSDataAccess: {e}")
        return None


OptimizedGCSDataAccess = _get_optimized_gcs_access()


class BuildingProcessor:
    """Process BBR building data in the silver layer."""

    def __init__(self, settings, logger: logging.Logger) -> None:
        """Initialize the building processor."""
        self.settings = settings
        self.logger = logger
        self.conn = None

    def _get_connection(self):
        """Get or create a DuckDB connection with spatial extension."""
        if self.conn is None:
            self.conn = duckdb.connect(":memory:")
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")
        return self.conn

    def process_buildings_from_data(self, bronze_data: dict[str, Any], output_dir: Path) -> None:
        """Process buildings from in-memory bronze data."""
        self.logger.info("Processing buildings from in-memory bronze data")

        # Extract data from bronze result
        if not bronze_data or "data" not in bronze_data:
            raise ValueError("Invalid bronze data structure")

        data = bronze_data["data"]
        output_dir_path = Path(data.get("output_dir", ""))

        if not output_dir_path.exists():
            raise ValueError(f"Bronze output directory not found: {output_dir_path}")

        # Process the buildings from the bronze output directory
        self._process_buildings_directory(output_dir_path, output_dir)

    def process_buildings(self, input_dir: Path, output_dir: Path) -> None:
        """Process buildings from disk-based bronze data."""
        self.logger.info(f"Processing buildings from disk: {input_dir}")

        if not input_dir.exists():
            raise ValueError(f"Input directory not found: {input_dir}")

        self._process_buildings_directory(input_dir, output_dir)

    def _process_buildings_directory(self, input_dir: Path, output_dir: Path) -> None:
        """Process buildings from a bronze output directory."""
        conn = self._get_connection()

        # Look for the expected bronze output files
        joined_buildings_file = input_dir / "joined_buildings.geoparquet"
        inspire_attributes_file = input_dir / "inspire_attributes.parquet"

        if not joined_buildings_file.exists():
            raise ValueError(f"Expected joined buildings file not found: {joined_buildings_file}")

        self.logger.info(f"Loading joined buildings from: {joined_buildings_file}")

        # Load the joined buildings data
        conn.execute(f"""
            CREATE OR REPLACE TABLE joined_buildings AS
            SELECT * FROM read_parquet('{joined_buildings_file}')
        """)

        # Load INSPIRE attributes if available
        if inspire_attributes_file.exists():
            self.logger.info(f"Loading INSPIRE attributes from: {inspire_attributes_file}")
            conn.execute(f"""
                CREATE OR REPLACE TABLE inspire_attributes AS
                SELECT * FROM read_parquet('{inspire_attributes_file}')
            """)

            # Join with INSPIRE attributes if both tables exist
            # Fix: Cast both UUID types to VARCHAR for proper join
            conn.execute("""
                CREATE OR REPLACE TABLE enriched_buildings AS
                SELECT 
                    jb.*,
                    ia.current_use,
                    ia.building_nature,
                    ia.construction_year,
                    ia.floor_area,
                    ia.floors,
                    ia.dwellings,
                    ia.address,
                    ia.bbr_usage_code,
                    ia.category_group
                FROM joined_buildings jb
                LEFT JOIN inspire_attributes ia ON jb.BBRUUID::VARCHAR = ia.building_uuid::VARCHAR
            """)

            processing_table = "enriched_buildings"
        else:
            self.logger.warning("INSPIRE attributes file not found, processing without enrichment")
            processing_table = "joined_buildings"

        # Apply silver layer transformations
        self.logger.info("Applying silver layer transformations...")

        conn.execute(f"""
            CREATE OR REPLACE TABLE processed_buildings AS
            SELECT 
                BBRUUID as building_uuid,
                geometry as geo_building_polygon,
                ST_Centroid(geometry) as geo_building_centroid,
                bygningstype as building_type,
                building_area_m2 as building_floor_area_sqm,
                join_status,
                CASE 
                    WHEN current_use IN ('individualResidence', 'collectiveResidence', 'twoDwellings') THEN 'residential'
                    WHEN current_use = 'agriculture' THEN 'agricultural'
                    WHEN current_use = 'publicServices' THEN 'educational'
                    ELSE 'other'
                END as building_usage_category,
                current_use as inspire_current_use,
                building_nature as inspire_building_nature,
                construction_year as inspire_construction_year,
                floor_area as inspire_floor_area,
                floors as inspire_floors,
                dwellings as inspire_dwellings,
                address as address_full,
                -- Pesticide proximity pipeline compatibility
                address as inspire_address,
                bbr_usage_code,
                category_group as inspire_category_group,
                CURRENT_DATE as last_updated
            FROM {processing_table}
            WHERE ST_IsValid(geometry)
            AND building_area_m2 > 0
        """)

        # Get processing statistics
        stats = conn.execute("""
            SELECT 
                COUNT(*) as total_buildings,
                COUNT(DISTINCT building_uuid) as unique_buildings,
                AVG(building_floor_area_sqm) as avg_floor_area,
                COUNT(*) FILTER (WHERE building_usage_category = 'residential') as residential_count,
                COUNT(*) FILTER (WHERE building_usage_category = 'agricultural') as agricultural_count,
                COUNT(*) FILTER (WHERE building_usage_category = 'educational') as educational_count
            FROM processed_buildings
        """).fetchone()

        self.logger.info("Silver layer processing results:")
        self.logger.info(f"  Total buildings: {stats[0]:,}")
        self.logger.info(f"  Unique buildings: {stats[1]:,}")
        self.logger.info(f"  Average floor area: {stats[2]:.1f} m²")
        self.logger.info(f"  Residential: {stats[3]:,}")
        self.logger.info(f"  Agricultural: {stats[4]:,}")
        self.logger.info(f"  Educational: {stats[5]:,}")

        # Save processed buildings
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "buildings_processed.geoparquet"

        # 🚀 ENHANCED: Try native GCS export first if available
        gcs_export_success = False
        if OptimizedGCSDataAccess:
            try:
                gcs_access = OptimizedGCSDataAccess()
                timestamp = Path(output_dir).name  # Extract timestamp from output directory
                gcs_path = f"gs://landbrugsdata-raw-data/silver/bbr_buildings/{timestamp}/buildings_processed.geoparquet"

                # Use native GCS export with server-side compression
                gcs_access.export_to_gcs_native(
                    connection=conn,
                    table_name="processed_buildings",
                    gcs_path=gcs_path,
                    compression="zstd",
                    query="SELECT * FROM processed_buildings ORDER BY building_floor_area_sqm DESC",
                )

                self.logger.info(f"✅ Native GCS export successful: {gcs_path}")
                gcs_export_success = True
            except Exception as e:
                self.logger.warning(f"Native GCS export failed, using local export: {e}")

        # Always create local file as well (for compatibility)
        conn.execute(f"""
            COPY (
                SELECT * FROM processed_buildings
                ORDER BY building_floor_area_sqm DESC
            ) TO '{output_file}' (FORMAT PARQUET)
        """)

        export_location = (
            f"GCS and local: {output_file}" if gcs_export_success else f"local: {output_file}"
        )
        self.logger.info(f"Saved processed buildings to: {export_location}")

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
