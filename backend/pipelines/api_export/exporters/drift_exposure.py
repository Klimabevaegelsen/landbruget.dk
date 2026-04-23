"""Pesticide drift exposure exporter.

Surfaces the per-building drift exposure percentile computed in
`pesticide_drift_exposure` (Rautmann + wind-rose) so the frontend can show
"Top 1/5/10/25/50% mest eksponeret" / "Under gennemsnit" on address pages.

Output:
- pesticides/drift-exposure/index.json — national reference + tile shard metadata
- pesticides/drift-exposure/tiles/{z}/{x}/{y}.json — buildings in a slippy-map tile

Per-tile record (compact):
  {uid, lat, lng, pct, dose}
  - uid:  BBR building_uuid
  - lat/lng: centroid, EPSG:4326, 6 decimals (~11 cm precision)
  - pct:  exposure_percentile rounded to 1 decimal
  - dose: total_drift_dose_kg rounded to 4 decimals

Data source (R2):
- gold/pesticide_drift_exposure_{y}_{y+1}/{ts}/pesticide_drift_exposure_{y}_{y+1}.parquet
"""

import os
import re

import duckdb
from common.crs_utils import (
    DANISH_UTM,
    DENMARK_BOUNDS_UTM,
    DENMARK_BOUNDS_WGS84,
    WGS84,
    detect_crs_from_bounds,
    sql_transform_to_wgs84,
)
from common.logging_utils import get_pipeline_logger

from exporters.base import BaseExporter

logger = get_pipeline_logger("api_export.drift_exposure")

BUCKET = os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET") or "landbruget-data"
DRIFT_TILE_ZOOM = 12


class DriftExposureExporter(BaseExporter):
    """Export spatially sharded drift exposure JSON for address lookups."""

    def _ensure_spatial_loaded(self) -> None:
        """Ensure DuckDB spatial functions are available for geometry joins."""
        try:
            self.conn.execute("LOAD spatial")
        except duckdb.Error:
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")

    def _normalized_wgs84_geometry_expr(self, table_name: str, geometry_expr: str) -> str:
        """Return a geometry SQL expression normalized to lon/lat WGS84.

        The live DAGI and drift-export inputs are not consistent:
        - some files are proper WGS84 lon/lat,
        - some are WGS84 with flipped axes,
        - some are EPSG:25832 coordinates stored without usable CRS metadata.
        Normalize defensively here before any spatial join.
        """
        counts = self.conn.execute(
            f"""
            WITH sample AS (
                SELECT ST_Centroid({geometry_expr}) AS centroid
                FROM {table_name}
                WHERE {geometry_expr} IS NOT NULL
                LIMIT 10000
            )
            SELECT
                SUM(
                    CASE
                        WHEN ST_X(centroid) BETWEEN {DENMARK_BOUNDS_WGS84["min_x"]} AND {DENMARK_BOUNDS_WGS84["max_x"]}
                         AND ST_Y(centroid) BETWEEN {DENMARK_BOUNDS_WGS84["min_y"]} AND {DENMARK_BOUNDS_WGS84["max_y"]}
                        THEN 1
                        ELSE 0
                    END
                ) AS lon_lat_count,
                SUM(
                    CASE
                        WHEN ST_X(centroid) BETWEEN {DENMARK_BOUNDS_WGS84["min_y"]} AND {DENMARK_BOUNDS_WGS84["max_y"]}
                         AND ST_Y(centroid) BETWEEN {DENMARK_BOUNDS_WGS84["min_x"]} AND {DENMARK_BOUNDS_WGS84["max_x"]}
                        THEN 1
                        ELSE 0
                    END
                ) AS lat_lon_swapped_count,
                SUM(
                    CASE
                        WHEN ST_X(centroid) BETWEEN {DENMARK_BOUNDS_UTM["min_x"]} AND {DENMARK_BOUNDS_UTM["max_x"]}
                         AND ST_Y(centroid) BETWEEN {DENMARK_BOUNDS_UTM["min_y"]} AND {DENMARK_BOUNDS_UTM["max_y"]}
                        THEN 1
                        ELSE 0
                    END
                ) AS utm_count
            FROM sample
            """
        ).fetchone()

        lon_lat_count, lat_lon_swapped_count, utm_count = counts
        detected_crs: str | None = None
        coord_order = "unknown"

        if lon_lat_count or lat_lon_swapped_count or utm_count:
            best_count = max(lon_lat_count or 0, lat_lon_swapped_count or 0, utm_count or 0)
            if best_count == (lon_lat_count or 0):
                detected_crs = WGS84
                coord_order = "lon_lat"
            elif best_count == (lat_lon_swapped_count or 0):
                detected_crs = WGS84
                coord_order = "lat_lon_swapped"
            else:
                detected_crs = DANISH_UTM
                coord_order = "easting_northing"

        bounds = self.conn.execute(
            f"""
            SELECT
                MIN(ST_XMin({geometry_expr})),
                MAX(ST_XMax({geometry_expr})),
                MIN(ST_YMin({geometry_expr})),
                MAX(ST_YMax({geometry_expr}))
            FROM {table_name}
            WHERE {geometry_expr} IS NOT NULL
            """
        ).fetchone()

        if not bounds or bounds[0] is None:
            raise ValueError(f"No valid geometries found in {table_name}.{geometry_expr}")

        if detected_crs is None:
            detected_crs, coord_order = detect_crs_from_bounds(*bounds)
        if detected_crs is None:
            min_x, max_x, min_y, max_y = bounds
            raise ValueError(
                f"Cannot detect CRS for {table_name}.{geometry_expr}: "
                f"X=[{min_x:.3f}, {max_x:.3f}], Y=[{min_y:.3f}, {max_y:.3f}]"
            )

        normalized_expr = geometry_expr
        if detected_crs == WGS84 and coord_order == "lat_lon_swapped":
            logger.warning(
                f"{table_name}.{geometry_expr} appears lat/lon-swapped WGS84; flipping coordinates"
            )
            normalized_expr = f"ST_FlipCoordinates({geometry_expr})"

        if detected_crs == DANISH_UTM:
            logger.info(
                f"{table_name}.{geometry_expr} detected as {DANISH_UTM}; transforming to WGS84"
            )
            return sql_transform_to_wgs84(normalized_expr, DANISH_UTM)

        logger.info(f"{table_name}.{geometry_expr} detected as {WGS84} ({coord_order})")
        return normalized_expr

    def export(self) -> dict:
        stats: dict = {"files_written": 0}

        drift_path = self._latest_drift_parquet()
        if not drift_path:
            logger.warning("No pesticide_drift_exposure parquet found on R2")
            return stats

        try:
            self.load_parquet_table(drift_path, "drift_exposure")
        except Exception:
            logger.exception("Failed to load source parquets")
            return stats

        self._ensure_spatial_loaded()

        drift_geom_expr = self._normalized_wgs84_geometry_expr("drift_exposure", "geometry")

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE drift_by_tile AS
            SELECT
                building_uuid,
                address,
                ST_Y(ST_Centroid(geom_wgs84)) AS lat,
                ST_X(ST_Centroid(geom_wgs84)) AS lng,
                exposure_percentile,
                total_drift_dose_kg,
                pesticide_year,
                CAST(
                    FLOOR(((ST_X(ST_Centroid(geom_wgs84)) + 180.0) / 360.0) * POW(2, {DRIFT_TILE_ZOOM}))
                    AS BIGINT
                ) AS tile_x,
                CAST(
                    FLOOR(
                        (
                            1.0 - LN(
                                TAN(RADIANS(ST_Y(ST_Centroid(geom_wgs84)))) +
                                (1.0 / COS(RADIANS(ST_Y(ST_Centroid(geom_wgs84)))))
                            ) / PI()
                        ) / 2.0 * POW(2, {DRIFT_TILE_ZOOM})
                    )
                    AS BIGINT
                ) AS tile_y
            FROM (
                SELECT
                    *,
                    {drift_geom_expr} AS geom_wgs84
                FROM drift_exposure
                WHERE building_uuid IS NOT NULL
                  AND geometry IS NOT NULL
            ) d
        """)

        summary = self.conn.execute("""
            SELECT
                COUNT(*)::BIGINT AS total_buildings,
                AVG(total_drift_dose_kg) AS national_avg_drift_dose_kg,
                MAX(pesticide_year) AS pesticide_year,
                COUNT(DISTINCT (tile_x, tile_y))::BIGINT AS tile_count
            FROM drift_by_tile
        """).fetchone()
        total_buildings, national_avg, pesticide_year, tile_count = summary

        if not total_buildings:
            logger.warning("drift_by_tile is empty")
            return stats

        tile_rows = self.query_to_dicts("""
            SELECT tile_x, tile_y, COUNT(*)::BIGINT AS building_count
            FROM drift_by_tile
            GROUP BY tile_x, tile_y
            ORDER BY tile_x, tile_y
        """)

        for row in tile_rows:
            tile_x = int(row["tile_x"])
            tile_y = int(row["tile_y"])
            buildings = self.query_to_dicts(f"""
                SELECT
                    building_uuid AS uid,
                    ROUND(lat, 6)::DOUBLE AS lat,
                    ROUND(lng, 6)::DOUBLE AS lng,
                    ROUND(exposure_percentile, 1)::DOUBLE AS pct,
                    ROUND(total_drift_dose_kg, 4)::DOUBLE AS dose
                FROM drift_by_tile
                WHERE tile_x = {tile_x}
                  AND tile_y = {tile_y}
                ORDER BY uid
            """)
            self.write_json(
                buildings,
                f"pesticides/drift-exposure/tiles/{DRIFT_TILE_ZOOM}/{tile_x}/{tile_y}.json",
            )
            stats["files_written"] += 1

        index = {
            "pesticide_year": int(pesticide_year) if pesticide_year is not None else None,
            "national_avg_drift_dose_kg": (
                round(float(national_avg), 6) if national_avg is not None else None
            ),
            "building_count": int(total_buildings),
            "tile_zoom": DRIFT_TILE_ZOOM,
            "tile_count": int(tile_count),
        }
        self.write_json(index, "pesticides/drift-exposure/index.json")
        stats["files_written"] += 1
        stats["building_count"] = int(total_buildings)
        stats["tile_count"] = len(tile_rows)

        logger.info(
            f"Drift exposure: {total_buildings:,} buildings across "
            f"{len(tile_rows)} tiles at z{DRIFT_TILE_ZOOM}; national_avg={national_avg}"
        )
        return stats

    def _latest_drift_parquet(self) -> str | None:
        pattern = f"{self._r2_bucket}/gold/pesticide_drift_exposure_*/*/*.parquet"
        try:
            files = self.r2_fs.glob(pattern)
        except Exception:
            logger.exception("Failed to list drift exposure files on R2")
            return None

        # Pick the newest start-year, and within that the newest timestamp.
        best_year = -1
        best_path = None
        for f in files:
            m = re.search(r"pesticide_drift_exposure_(\d{4})_(\d{4})", f)
            if not m:
                continue
            year = int(m.group(1))
            if year > best_year or (year == best_year and f > (best_path or "")):
                best_year = year
                best_path = f
        return f"r2://{best_path}" if best_path else None
