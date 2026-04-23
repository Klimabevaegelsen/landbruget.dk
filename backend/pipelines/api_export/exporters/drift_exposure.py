"""Pesticide drift exposure exporter.

Surfaces the per-building drift exposure percentile computed in
`pesticide_drift_exposure` (Rautmann + wind-rose) so the frontend can show
"Top 1/5/10/25/50% mest eksponeret" / "Under gennemsnit" on address pages.

Output:
- pesticides/drift-exposure/index.json — national reference + kommune list
- pesticides/drift-exposure/{kommunekode}.json — array of buildings in that kommune

Per-kommune record (compact):
  {uid, lat, lng, pct, dose}
  - uid:  BBR building_uuid
  - lat/lng: centroid, EPSG:4326, 6 decimals (~11 cm precision)
  - pct:  exposure_percentile rounded to 1 decimal
  - dose: total_drift_dose_kg rounded to 4 decimals

Data sources (R2):
- gold/pesticide_drift_exposure_{y}_{y+1}/{ts}/pesticide_drift_exposure_{y}_{y+1}.parquet
- silver/dagi_kommuner/{ts}/data.parquet  (for kommunekode spatial join)
"""

import os
import re

import duckdb
from common.logging_utils import get_pipeline_logger

from exporters.base import BaseExporter

logger = get_pipeline_logger("api_export.drift_exposure")

BUCKET = os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET") or "landbruget-data"


class DriftExposureExporter(BaseExporter):
    """Export per-kommune drift exposure JSON for address lookups."""

    def _ensure_spatial_loaded(self) -> None:
        """Ensure DuckDB spatial functions are available for geometry joins."""
        try:
            self.conn.execute("LOAD spatial")
        except duckdb.Error:
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")

    def export(self) -> dict:
        stats: dict = {"files_written": 0}

        drift_path = self._latest_drift_parquet()
        if not drift_path:
            logger.warning("No pesticide_drift_exposure parquet found on R2")
            return stats

        kommuner_path = self._latest_dagi_kommuner_parquet()
        if not kommuner_path:
            logger.warning("No silver/dagi_kommuner parquet found on R2")
            return stats

        try:
            self.load_parquet_table(drift_path, "drift_exposure")
            self.load_parquet_table(kommuner_path, "dagi_kommuner")
        except Exception:
            logger.exception("Failed to load source parquets")
            return stats

        self._ensure_spatial_loaded()

        # Spatial join: building centroid → kommune polygon → kommunekode.
        # Both sides are EPSG:4326 (see pesticide_drift_exposure._save_year_results
        # and dagi_kommuner baseline metrics).
        self.conn.execute("""
            CREATE OR REPLACE TABLE drift_by_kommune AS
            SELECT
                d.building_uuid,
                d.address,
                ST_Y(ST_Centroid(d.geometry)) AS lat,
                ST_X(ST_Centroid(d.geometry)) AS lng,
                d.exposure_percentile,
                d.total_drift_dose_kg,
                d.pesticide_year,
                k.code AS kommunekode
            FROM drift_exposure d
            LEFT JOIN dagi_kommuner k
              ON ST_Within(ST_Centroid(d.geometry), k.geometry)
            WHERE d.building_uuid IS NOT NULL
              AND d.geometry IS NOT NULL
        """)

        summary = self.conn.execute("""
            SELECT
                COUNT(*)::BIGINT AS total_buildings,
                COUNT(*) FILTER (WHERE kommunekode IS NULL)::BIGINT AS unmatched,
                AVG(total_drift_dose_kg) AS national_avg_drift_dose_kg,
                MAX(pesticide_year) AS pesticide_year
            FROM drift_by_kommune
        """).fetchone()
        total_buildings, unmatched, national_avg, pesticide_year = summary

        if not total_buildings:
            logger.warning("drift_by_kommune is empty")
            return stats

        if unmatched:
            logger.warning(
                f"{unmatched:,}/{total_buildings:,} buildings fell outside any kommune polygon"
            )

        # Per-kommune files
        kommune_rows = self.query_to_dicts("""
            SELECT kommunekode, COUNT(*)::BIGINT AS building_count
            FROM drift_by_kommune
            WHERE kommunekode IS NOT NULL
            GROUP BY kommunekode
            ORDER BY kommunekode
        """)

        for row in kommune_rows:
            code = str(row["kommunekode"])
            buildings = self.query_to_dicts(f"""
                SELECT
                    building_uuid AS uid,
                    ROUND(lat, 6)::DOUBLE AS lat,
                    ROUND(lng, 6)::DOUBLE AS lng,
                    ROUND(exposure_percentile, 1)::DOUBLE AS pct,
                    ROUND(total_drift_dose_kg, 4)::DOUBLE AS dose
                FROM drift_by_kommune
                WHERE kommunekode = '{code}'
                ORDER BY uid
            """)
            self.write_json(buildings, f"pesticides/drift-exposure/{code}.json")
            stats["files_written"] += 1

        # Index file
        index = {
            "pesticide_year": int(pesticide_year) if pesticide_year is not None else None,
            "national_avg_drift_dose_kg": (
                round(float(national_avg), 6) if national_avg is not None else None
            ),
            "building_count": int(total_buildings),
            "unmatched_count": int(unmatched),
            "kommunekoder": [str(r["kommunekode"]) for r in kommune_rows],
        }
        self.write_json(index, "pesticides/drift-exposure/index.json")
        stats["files_written"] += 1
        stats["building_count"] = int(total_buildings)
        stats["kommune_count"] = len(kommune_rows)

        logger.info(
            f"Drift exposure: {total_buildings:,} buildings across "
            f"{len(kommune_rows)} kommuner; national_avg={national_avg}"
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

    def _latest_dagi_kommuner_parquet(self) -> str | None:
        pattern = f"{self._r2_bucket}/silver/dagi_kommuner/*/data.parquet"
        try:
            files = sorted(self.r2_fs.glob(pattern))
        except Exception:
            logger.exception("Failed to list DAGI kommuner files on R2")
            return None
        return f"r2://{files[-1]}" if files else None
