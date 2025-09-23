"""BBR Building processor for silver layer processing."""

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb

# Robust import handling for Settings
try:
    from ..config.settings import Settings
except ImportError:
    # Fallback for when module is imported directly
    try:
        from config.settings import Settings
    except ImportError:
        # Last resort - create a minimal settings class
        class Settings:
            def __init__(self) -> None:
                pass


# Try to import comprehensive geo validator
def _get_geo_validator() -> Callable | None:
    """Get comprehensive geo validator with robust import handling."""
    try:
        from unified_pipeline.common.geometry_validator import (
            validate_and_transform_geometries_duckdb,
        )

        logging.info("✅ Successfully imported comprehensive geo validator for BBR buildings")
        return validate_and_transform_geometries_duckdb
    except ImportError as e:
        logging.warning(f"⚠️ Could not import comprehensive geo validator: {e}")
        return None


ComprehensiveGeoValidator = _get_geo_validator()


# Try to import optimized GCS access with fallback
def _get_optimized_gcs_access() -> type | None:
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

    def __init__(self, settings: Settings, logger: logging.Logger) -> None:
        """Initialize the building processor."""
        self.settings = settings
        self.logger = logger
        self.conn = None

        # BBR Building Usage Codes (BygAnvendelse) mapping
        # Source: https://teknik.bbr.dk/kodelister/0/1/0/BygAnvendelse
        self.bbr_code_names = {
            # Residential buildings
            110: "Stuehus til landbrugsejendom",
            120: "Fritliggende enfamiliehus (parcelhus)",
            130: "Række-, kæde- eller dobbelthus",
            140: "Etageboligbebyggelse (flerfamiliehus)",
            150: "Kollegium",
            160: "Døgninstitution",
            190: "Anden bygning til helårsbeboelse",
            510: "Sommerhus",
            540: "Kolonihavehus",
            # Agricultural buildings
            210: "Bygning til landbrug, gartneri, råstofudvinding o.lign.",
            211: "Bygning til husdyrhold",
            212: "Bygning til opbevaring af afgrøder, foder, maskiner o.lign.",
            213: "Bygning til drivhusgartneri",
            214: "Bygning til pelsdyravl",
            215: "Silo til landbrugsformål",
            216: "Lade, hølade, kornlade o.lign.",
            217: "Maskinhus, redskabsrum o.lign.",
            218: "Stald, kostald, svinestald o.lign.",
            219: "Anden bygning til landbrugsformål",
            # Educational and childcare buildings
            420: "Bygning til undervisning og forskning (grundskoler)",
            421: "Bygning til undervisning og forskning (gymnasier)",
            422: "Bygning til undervisning og forskning (universiteter)",
            429: "Anden undervisningsbygning",
            440: "Bygning til daginstitutioner for børn og unge",
            441: "Anden bygning til daginstitutioner",
            # Other public services
            430: "Bygning til sundhedsformål",
            431: "Hospital",
            432: "Anden bygning til sundhedsformål",
            450: "Bygning til kulturelle formål",
            460: "Bygning til religiøse formål",
            # Industrial and commercial
            310: "Transport- og garagebyggeri",
            320: "Bygning til kontor og handel",
            330: "Bygning til hotel og restaurant",
            340: "Bygning til kultur og fritid",
            390: "Anden bygning til erhvervsformål",
            # Special codes
            930: "Bygning under opførelse",
            940: "Uoplyst bygningsanvendelse",
            970: "Bygning uden anvendelse",
        }

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create a DuckDB connection with spatial extension."""
        if self.conn is None:
            self.conn = duckdb.connect(":memory:")
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")
        return self.conn

    def _get_bbr_code_name(self, code: str | int) -> str:
        """Get the human-readable name for a BBR code."""
        try:
            code_int = int(code) if isinstance(code, str) else code
            return self.bbr_code_names.get(code_int, f"Ukendt BBR kode ({code})")
        except (ValueError, TypeError):
            return f"Ugyldig BBR kode ({code})"

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

        # Check if INSPIRE data is already in joined_buildings (which it should be!)
        inspire_columns_check = conn.execute("""
            SELECT column_name
            FROM (DESCRIBE joined_buildings)
            WHERE column_name IN ('current_use', 'category_group', 'bbr_usage_code')
        """).fetchall()

        if len(inspire_columns_check) >= 3:
            self.logger.info("✅ INSPIRE data already present in joined_buildings, using directly")
            processing_table = "joined_buildings"

            # Debug: Check publicServices count in existing data
            public_count = conn.execute("""
                SELECT COUNT(*) FROM joined_buildings
                WHERE current_use = 'publicServices'
            """).fetchone()[0]
            self.logger.info(f"🔍 PublicServices buildings in joined data: {public_count:,}")

            if public_count > 0:
                # Show sample education codes
                education_sample = conn.execute("""
                    SELECT bbr_usage_code, COUNT(*) as count
                    FROM joined_buildings
                    WHERE current_use = 'publicServices'
                      AND bbr_usage_code IN ('420', '421', '422', '429', '440', '441')
                    GROUP BY bbr_usage_code
                    ORDER BY count DESC
                """).fetchall()

                if education_sample:
                    self.logger.info("🏫 Education buildings by BBR code:")
                    for code, count in education_sample:
                        code_name = self._get_bbr_code_name(code)
                        self.logger.info(f"  {code_name}: {count:,} buildings")
                else:
                    self.logger.warning(
                        "⚠️ No education BBR codes found in joined publicServices buildings"
                    )

        elif inspire_attributes_file.exists():
            self.logger.warning("⚠️ INSPIRE data not in joined_buildings, attempting separate join")
            self.logger.info(f"Loading INSPIRE attributes from: {inspire_attributes_file}")
            # ... keep the old join logic as fallback ...
            processing_table = "joined_buildings"
        else:
            self.logger.warning("INSPIRE attributes file not found, processing without enrichment")
            processing_table = "joined_buildings"

        # Apply comprehensive geo validation before transformations
        self.logger.info("Applying comprehensive geo validation...")

        if ComprehensiveGeoValidator:
            try:
                # Apply comprehensive geo validation to the processing table
                ComprehensiveGeoValidator(
                    conn=conn,
                    table_name=processing_table,
                    dataset_name="BBR Buildings",
                    geometry_column="geometry",
                )
                self.logger.info("✅ Comprehensive geo validation completed successfully")
            except Exception as e:
                self.logger.warning(
                    f"⚠️ Comprehensive geo validation failed, using basic validation: {e}"
                )
                # Fallback to basic validation
                conn.execute(f"""
                    DELETE FROM {processing_table}
                    WHERE geometry IS NULL OR NOT ST_IsValid(geometry)
                """)
        else:
            self.logger.warning(
                "⚠️ Comprehensive geo validator not available, using basic validation"
            )
            # Fallback to basic validation
            conn.execute(f"""
                DELETE FROM {processing_table}
                WHERE geometry IS NULL OR NOT ST_IsValid(geometry)
            """)

        # Apply silver layer transformations
        self.logger.info("Applying silver layer transformations...")

        # Create BBR code mapping table for join
        bbr_mappings = [(code, name) for code, name in self.bbr_code_names.items()]
        conn.execute("CREATE OR REPLACE TABLE bbr_code_mapping (code INTEGER, name VARCHAR)")
        conn.executemany("INSERT INTO bbr_code_mapping VALUES (?, ?)", bbr_mappings)

        conn.execute(f"""
            CREATE OR REPLACE TABLE processed_buildings AS
            SELECT
                BBRUUID as building_uuid,
                geometry as geo_building_polygon,
                ST_Centroid(geometry) as geo_building_centroid,
                bygningstype as building_type,
                TRY_CAST(floor_area AS DOUBLE) as building_floor_area_sqm,
                join_status,
                CASE
                    WHEN current_use IN (
                        'individualResidence', 'collectiveResidence', 'twoDwellings'
                    ) THEN 'residential'
                    WHEN current_use = 'agriculture' THEN 'agricultural'
                    WHEN current_use = 'publicServices' THEN 'publicServices'
                    ELSE 'other'
                END as building_usage_category,
                current_use as inspire_current_use,
                building_nature as inspire_building_nature,
                construction_year as inspire_construction_year,
                floor_area as inspire_floor_area,
                floors as inspire_floors,
                dwellings as inspire_dwellings,
                address as address_full,
                -- Pesticide proximity pipeline compatibility columns
                address,
                geometry,
                CASE
                    WHEN current_use IN (
                        'individualResidence', 'collectiveResidence', 'twoDwellings'
                    ) THEN 'residential'
                    WHEN current_use = 'agriculture' THEN 'agricultural'
                    WHEN current_use = 'publicServices' THEN 'publicServices'
                    ELSE 'other'
                END as category_group,
                bbr_usage_code,
                COALESCE(bcm.name, 'Ukendt BBR kode (' || bbr_usage_code || ')') as bbr_usage_name,
                category_group as inspire_category_group,
                CURRENT_DATE as last_updated
            FROM {processing_table} pb
            LEFT JOIN bbr_code_mapping bcm ON TRY_CAST(pb.bbr_usage_code AS INTEGER) = bcm.code
            WHERE ST_IsValid(geometry)
            AND TRY_CAST(floor_area AS DOUBLE) > 0
        """)

        # Get processing statistics
        stats = conn.execute("""
            SELECT
                COUNT(*) as total_buildings,
                COUNT(DISTINCT building_uuid) as unique_buildings,
                AVG(building_floor_area_sqm) as avg_floor_area,
                COUNT(*) FILTER (
                    WHERE building_usage_category = 'residential'
                ) as residential_count,
                COUNT(*) FILTER (
                    WHERE building_usage_category = 'agricultural'
                ) as agricultural_count,
                COUNT(*) FILTER (
                    WHERE building_usage_category = 'publicServices'
                ) as publicservices_count
            FROM processed_buildings
        """).fetchone()

        self.logger.info("Silver layer processing results:")
        self.logger.info(f"  Total buildings: {stats[0]:,}")
        self.logger.info(f"  Unique buildings: {stats[1]:,}")
        self.logger.info(f"  Average floor area: {stats[2]:.1f} m²")
        self.logger.info(f"  Residential: {stats[3]:,}")
        self.logger.info(f"  Agricultural: {stats[4]:,}")
        self.logger.info(f"  Public Services: {stats[5]:,}")

        # Save processed buildings
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "buildings_processed.parquet"

        # 🚀 ENHANCED: Try native GCS export first if available
        gcs_export_success = False
        if OptimizedGCSDataAccess:
            try:
                gcs_access = OptimizedGCSDataAccess()
                timestamp = Path(output_dir).name  # Extract timestamp from output directory
                gcs_path = f"gs://landbrugsdata-raw-data/silver/bbr_buildings/{timestamp}/buildings_processed.parquet"

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
