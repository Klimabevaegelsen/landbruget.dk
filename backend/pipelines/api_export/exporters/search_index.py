"""Search index exporter — generates a client-side searchable company index.

Replaces: Supabase edge function /functions/v1/search

Output: search/index.json (~1.5MB gzipped for ~46k companies)

Frontend loads this once into memory and filters client-side with Fuse.js,
which is faster than the current 300ms debounce + server round-trip.
"""

import os

from common.logging_utils import get_pipeline_logger

from exporters.base import BaseExporter

logger = get_pipeline_logger("api_export.search")

BUCKET = os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET") or "landbruget-data"


class SearchIndexExporter(BaseExporter):
    """Generate search/index.json from company data."""

    def export(self) -> dict:
        parquet_path = self.latest_r2_parquet("gold/cvr_enrichment_companies")
        if not parquet_path:
            logger.error("Failed to resolve latest companies parquet on R2")
            return {"files_written": 0, "companies": 0}

        try:
            self.load_parquet_table(parquet_path, "companies_search")
        except Exception:
            logger.exception(f"Failed to load companies from {parquet_path}")
            return {"files_written": 0, "companies": 0}

        # Extract minimal fields for search — columns match silver/cvr_companies schema
        rows = self.query_to_dicts("""
            SELECT
                cvr_number AS cvr,
                company_name AS name,
                current_municipality_name AS municipality,
                company_type_description AS type
            FROM companies_search
            WHERE company_name IS NOT NULL
            ORDER BY cvr
        """)

        # Clean up nulls to save bytes
        for row in rows:
            for key in list(row.keys()):
                if row[key] is None:
                    del row[key]

        index = {
            "version": "1.0",
            "count": len(rows),
            "companies": rows,
        }

        self.write_json(index, "search/index.json")
        logger.info(f"Search index: {len(rows):,} companies")

        return {"files_written": 1, "companies": len(rows)}
