"""Geospatial validator for Silver layer using DuckDB-spatial."""

import sys
from pathlib import Path
from typing import Any

# Add common module to path for CRS utilities
sys.path.insert(0, str(Path(__file__).resolve().parents[5]))
from common.crs_utils import DANISH_UTM, WGS84, detect_crs_from_bounds  # noqa: F401

# Handle imports for both standalone and package usage
try:
    from ...utils.logging import get_logger
    from ..duckdb_base import DuckDBProcessor
except ImportError:
    import logging

    def get_logger() -> logging.Logger:
        return logging.getLogger(__name__)

    from silver.duckdb_base import DuckDBProcessor
from .base import BaseValidator, ValidationResult

logger = get_logger()


class GeospatialValidator(BaseValidator, DuckDBProcessor):
    """Validate and standardize geospatial data using DuckDB-spatial."""

    def __init__(
        self,
        geometry_column: str = "geometry",
        target_crs: str = "EPSG:4326",
        validate_geometry: bool = True,
        auto_fix_geometry: bool = True,
    ) -> None:
        BaseValidator.__init__(self)
        DuckDBProcessor.__init__(self)

        self.geometry_column = geometry_column
        self.target_crs = target_crs
        self.validate_geometry = validate_geometry
        self.auto_fix_geometry = auto_fix_geometry

        logger.info("Initialized GeospatialValidator (DuckDB-spatial)")

    def validate(self, table_name_or_data: Any) -> ValidationResult:
        """Validate geospatial data.

        Args:
            table_name_or_data: DuckDB table name (str) or data to register.
        """
        result = ValidationResult(is_valid=True)

        try:
            table_name = self._ensure_table(table_name_or_data, "geo_validation_data")

            columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            column_names = [col[0] for col in columns_info]
            if self.geometry_column not in column_names:
                self.add_error(result, f"Geometry column '{self.geometry_column}' not found")
                return result

            spatial_table = f"{table_name}_spatial"
            try:
                self.conn.execute(f"""
                    CREATE TABLE {spatial_table} AS
                    SELECT
                        * EXCLUDE {self.geometry_column},
                        ST_GeomFromText({self.geometry_column}) as {self.geometry_column}
                    FROM {table_name}
                    WHERE {self.geometry_column} IS NOT NULL AND {self.geometry_column} != ''
                """)
            except Exception:
                try:
                    self.conn.execute(f"""
                        CREATE TABLE {spatial_table} AS
                        SELECT * FROM {table_name}
                        WHERE {self.geometry_column} IS NOT NULL
                    """)
                except Exception as e:
                    self.add_error(result, f"Failed to create spatial table: {e!s}")
                    return result

            null_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM {spatial_table}
                WHERE {self.geometry_column} IS NULL
            """).fetchone()[0]
            if null_count > 0:
                total_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                self.add_warning(result, f"Found {null_count}/{total_count} null geometries")

            if self.validate_geometry:
                self._check_geometry_validity(spatial_table, result)

            self._check_crs(spatial_table, result)

            self.conn.execute(f"DROP TABLE IF EXISTS {spatial_table}")
            logger.info("DuckDB-spatial validation completed")

        except Exception as e:
            self.add_error(result, f"DuckDB-spatial validation failed: {e!s}")

        return result

    def standardize(self, table_name_or_data: Any) -> str:
        """Standardize the geospatial data using DuckDB-spatial.

        Returns the name of a new DuckDB table with EPSG:4326 geometries.
        """
        source_table = self._ensure_table(table_name_or_data, "geo_standardization_source")
        result_table = f"{source_table}_standardized"

        columns_info = self.conn.execute(f"DESCRIBE {source_table}").fetchall()
        column_names = [col[0] for col in columns_info]
        if self.geometry_column not in column_names:
            raise ValueError(f"Geometry column '{self.geometry_column}' not found")

        self.conn.execute(f"""
            CREATE TABLE {result_table} AS
            SELECT
                * EXCLUDE {self.geometry_column},
                CASE
                    WHEN {self.geometry_column} IS NULL THEN NULL
                    WHEN ST_IsValid(ST_GeomFromText({self.geometry_column})) THEN
                        ST_Transform(
                            ST_GeomFromText({self.geometry_column}),
                            'EPSG:25832', '{self.target_crs}'
                        )
                    ELSE
                        ST_Transform(
                            ST_MakeValid(ST_GeomFromText({self.geometry_column})),
                            'EPSG:25832', '{self.target_crs}'
                        )
                END as {self.geometry_column}
            FROM {source_table}
        """)

        valid_count = self.conn.execute(f"""
            SELECT COUNT(*) FROM {result_table}
            WHERE {self.geometry_column} IS NOT NULL AND ST_IsValid({self.geometry_column})
        """).fetchone()[0]
        total_count = self.conn.execute(f"""
            SELECT COUNT(*) FROM {result_table}
            WHERE {self.geometry_column} IS NOT NULL
        """).fetchone()[0]
        logger.info(f"Standardized {valid_count}/{total_count} geometries using DuckDB-spatial")
        return result_table

    def _ensure_table(self, table_name_or_data: Any, default_name: str) -> str:
        """Return a DuckDB table name — registering pandas/geo data if needed."""
        if isinstance(table_name_or_data, str):
            return table_name_or_data
        self.register_table(table_name_or_data, default_name)
        return default_name

    def _check_geometry_validity(self, spatial_table: str, result: ValidationResult) -> None:
        try:
            invalid_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM {spatial_table}
                WHERE {self.geometry_column} IS NOT NULL
                    AND NOT ST_IsValid({self.geometry_column})
            """).fetchone()[0]
        except Exception as e:
            self.add_warning(result, f"Could not validate geometries: {e!s}")
            return

        if invalid_count == 0:
            return

        self.add_error(result, f"Found {invalid_count} invalid geometries")

        # DuckDB spatial lacks ST_IsValidReason, so fall back to shapely for
        # the ≤5 already-invalid geometries we report in detail. Narrow seam.
        if invalid_count <= 5:
            invalid_details = self.conn.execute(f"""
                SELECT ROW_NUMBER() OVER () as row_num,
                       ST_AsText({self.geometry_column}) as geom_wkt
                FROM {spatial_table}
                WHERE {self.geometry_column} IS NOT NULL
                    AND NOT ST_IsValid({self.geometry_column})
                LIMIT 5
            """).fetchall()

            try:
                from shapely import wkt
                from shapely.validation import explain_validity
            except ImportError:
                for row_num, _ in invalid_details:
                    self.add_error(result, f"Invalid geometry at row {row_num}")
                return

            for row_num, geom_wkt in invalid_details:
                try:
                    reason = explain_validity(wkt.loads(geom_wkt))
                    self.add_error(result, f"Invalid geometry at row {row_num}: {reason}")
                except Exception:
                    self.add_error(
                        result,
                        f"Invalid geometry at row {row_num}: Could not determine reason",
                    )

    def _check_crs(self, spatial_table: str, result: ValidationResult) -> None:
        try:
            row = self.conn.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE {self.geometry_column} IS NOT NULL) as geom_count,
                    MIN(ST_XMin({self.geometry_column})) as min_x,
                    MAX(ST_XMax({self.geometry_column})) as max_x,
                    MIN(ST_YMin({self.geometry_column})) as min_y,
                    MAX(ST_YMax({self.geometry_column})) as max_y
                FROM {spatial_table}
                WHERE {self.geometry_column} IS NOT NULL
            """).fetchone()
        except Exception as e:
            logger.warning(f"CRS detection failed: {e!s}. Assuming {DANISH_UTM}.")
            self.add_warning(
                result,
                f"CRS validation: Detection failed, assuming {DANISH_UTM}, "
                f"will transform to {self.target_crs}",
            )
            return

        geom_count, min_x, max_x, min_y, max_y = row
        if geom_count == 0:
            return
        if min_x is None:
            self.add_warning(
                result,
                f"CRS validation: No valid bounds found, assuming {DANISH_UTM}, "
                f"will transform to {self.target_crs}",
            )
            return
        detected_crs, coord_order = detect_crs_from_bounds(min_x, max_x, min_y, max_y)

        if detected_crs:
            logger.info(
                f"Detected CRS: {detected_crs} ({coord_order}) from bounds "
                f"X=[{min_x:.2f}, {max_x:.2f}], Y=[{min_y:.2f}, {max_y:.2f}]"
            )
            if detected_crs != DANISH_UTM:
                self.add_warning(
                    result,
                    f"CRS detection: Detected {detected_crs} but expected {DANISH_UTM}. "
                    f"Will assume {DANISH_UTM} and transform to {self.target_crs}",
                )
            else:
                self.add_warning(
                    result,
                    f"CRS validation: Detected {detected_crs}, will transform to {self.target_crs}",
                )
        else:
            logger.warning(
                f"Could not detect CRS from bounds X=[{min_x:.2f}, {max_x:.2f}], "
                f"Y=[{min_y:.2f}, {max_y:.2f}]. Assuming {DANISH_UTM}."
            )
            self.add_warning(
                result,
                f"CRS validation: Could not detect CRS from bounds, "
                f"assuming {DANISH_UTM}, will transform to {self.target_crs}",
            )
