"""
Bulk GeoDanmark Buildings Fetcher via GraphQL API

Downloads all buildings from Datafordeleren's GEODKV GraphQL register
using cursor-based pagination, saving to GeoParquet format.

Replaces the WFS-based BulkGeoDanmarkFetcher since the WFS endpoint
(wfs.datafordeler.dk/GeoDanmarkVektor/...) is no longer working.

GraphQL API details:
  - Endpoint: https://graphql.datafordeler.dk/GEODKV/v2?apiKey=<key>
  - Entity: GEODKV_Bygning (37 fields)
  - Geometry: SpatialPolygonZEpsg25832Type → { wkt } returns WKT POLYGON Z(...)
  - Pagination: cursor-based, first (max 1000) + after
  - Bitemporal: registreringstid required
  - BBRUUID links to BBR_Bygning.id_lokalId
"""

import contextlib
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import requests
from common.logging_utils import get_pipeline_logger
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = get_pipeline_logger(__name__)

# Fields to fetch — covers all useful attributes from GEODKV_Bygning
BUILDING_FIELDS = """
  id_lokalId
  BBRUUID
  bygningstype
  status
  geometristatus
  BBRaktion
  maalestedBygning
  metode3D
  overlapBygning
  synligBygning
  underMinimumBygning
  dataansvar
  registreringFra
  registreringTil
  virkningFra
  geometri {
    wkt
  }
"""

# Max page size allowed by Datafordeleren GraphQL API
MAX_PAGE_SIZE = 1000

# Explicit column types for read_json — prevents type inference variability
# across pages/batches (e.g. BBRaktion inferred as VARCHAR vs JSON, or
# registreringTil as TIMESTAMP vs VARCHAR depending on first record seen).
BUILDING_COLUMNS = {
    "id_lokalId": "VARCHAR",
    "BBRUUID": "VARCHAR",
    "bygningstype": "VARCHAR",
    "status": "VARCHAR",
    "geometristatus": "VARCHAR",
    "BBRaktion": "VARCHAR",
    "maalestedBygning": "VARCHAR",
    "metode3D": "VARCHAR",
    "overlapBygning": "VARCHAR",
    "synligBygning": "VARCHAR",
    "underMinimumBygning": "VARCHAR",
    "dataansvar": "VARCHAR",
    "registreringFra": "VARCHAR",
    "registreringTil": "VARCHAR",
    "virkningFra": "VARCHAR",
    "geometri_wkt": "VARCHAR",
}

# Column list for explicit SELECT during combine (cast everything to VARCHAR)
_COMBINE_COLUMNS = [c for c in BUILDING_COLUMNS if c != "geometri_wkt"]


class BulkGeoDanmarkGraphQLFetcher:
    def __init__(self, api_key: str, endpoint_base: str | None = None) -> None:
        self.api_key = api_key
        self.endpoint_base = (
            endpoint_base
            or os.getenv("GEODKV_GRAPHQL_ENDPOINT")
            or "https://graphql.datafordeler.dk/GEODKV/v2"
        )
        self.session = self._create_session()
        self.output_dir = Path("data")
        self.output_dir.mkdir(exist_ok=True)
        self.now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Initialize DuckDB with spatial extension
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        session.mount("https://", adapter)
        return session

    def _graphql_query(self, query: str) -> dict:
        """Execute a GraphQL query with error handling."""
        resp = self.session.post(
            self.endpoint_base,
            params={"apiKey": self.api_key},
            headers={"Content-Type": "application/json; charset=utf-8"},
            json={"query": query},
            timeout=120,
        )
        if not resp.ok:
            raise RuntimeError(f"GraphQL request failed: HTTP {resp.status_code}")
        data = resp.json()
        if "errors" in data:
            error_msgs = [e.get("message", "?") for e in data["errors"]]
            raise RuntimeError(f"GraphQL errors: {error_msgs}")
        return data

    def fetch_buildings_page(
        self, cursor: str | None = None, page_size: int = MAX_PAGE_SIZE
    ) -> tuple[list[dict], bool, str | None]:
        """
        Fetch a page of buildings.

        Returns:
            (nodes, has_next_page, end_cursor)
        """
        after_clause = f', after: "{cursor}"' if cursor else ""
        query = f"""
        {{
          GEODKV_Bygning(
            registreringstid: "{self.now}",
            first: {page_size}{after_clause}
          ) {{
            nodes {{
              {BUILDING_FIELDS}
            }}
            pageInfo {{
              hasNextPage
              endCursor
            }}
          }}
        }}
        """
        data = self._graphql_query(query)
        result = data["data"]["GEODKV_Bygning"]
        nodes = result["nodes"]
        page_info = result["pageInfo"]
        return nodes, page_info["hasNextPage"], page_info.get("endCursor")

    def _nodes_to_duckdb_table(self, nodes: list[dict], table_name: str) -> int:
        """
        Convert GraphQL nodes to a DuckDB table with proper geometry.

        Returns the number of rows inserted.
        """
        if not nodes:
            return 0

        # Extract flat records + WKT geometry
        records = []
        for node in nodes:
            record = {}
            for key, value in node.items():
                if key == "geometri" and isinstance(value, dict):
                    record["geometri_wkt"] = value.get("wkt")
                else:
                    record[key] = value
            records.append(record)

        # Write records as newline-delimited JSON for DuckDB to ingest
        import json as _json
        import tempfile

        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        tmp_path = None
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name
            for rec in records:
                tmp.write(_json.dumps(rec) + "\n")

        col_spec = ", ".join(f"'{k}': '{v}'" for k, v in BUILDING_COLUMNS.items())
        self.conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT
                * EXCLUDE (geometri_wkt),
                TRY(ST_GeomFromText(geometri_wkt)) AS geometri
            FROM read_json('{tmp_path}', columns={{{col_spec}}})
        """)

        Path(tmp_path).unlink(missing_ok=True)

        return self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    def bulk_download_buildings(self, batch_size: int = 10) -> None:
        """
        Download all GeoDanmark buildings using cursor-based pagination.

        Args:
            batch_size: Number of pages (of 1000 each) to accumulate before saving
                        to an intermediate GeoParquet file.
        """
        logger.info("Starting bulk download via GraphQL API")
        logger.info(f"Page size: {MAX_PAGE_SIZE}, save every {batch_size} pages")

        cursor = None
        page_num = 0
        total_buildings = 0
        batch_tables = []
        intermediate_batch_num = 0

        progress_bar = tqdm(desc="Downloading pages", unit="page", ncols=100, disable=False)

        try:
            while True:
                progress_bar.set_description(f"Page {page_num + 1} (total: {total_buildings:,})")

                max_page_retries = 5
                last_fetch_error = None
                for attempt in range(1, max_page_retries + 1):
                    try:
                        nodes, has_next, end_cursor = self.fetch_buildings_page(cursor)
                        last_fetch_error = None
                        break
                    except Exception as e:
                        last_fetch_error = e
                        if attempt == max_page_retries:
                            break
                        wait = min(60, 5 * attempt)
                        logger.warning(
                            f"Failed to fetch page {page_num + 1} "
                            f"(attempt {attempt}/{max_page_retries}): {e}. "
                            f"Retrying in {wait}s..."
                        )
                        time.sleep(wait)
                else:
                    nodes, has_next, end_cursor = [], False, None

                if last_fetch_error is not None:
                    raise RuntimeError(
                        f"Failed to fetch page {page_num + 1} after {max_page_retries} attempts"
                    ) from last_fetch_error

                if not nodes:
                    if page_num == 0 and total_buildings == 0:
                        raise RuntimeError("GraphQL API returned no buildings on the first page")
                    logger.info("No more buildings — reached end of dataset")
                    break

                # Parse nodes into DuckDB table
                table_name = f"page_{page_num}"
                row_count = self._nodes_to_duckdb_table(nodes, table_name)
                total_buildings += row_count
                batch_tables.append(table_name)

                progress_bar.set_postfix(
                    {"buildings": f"{total_buildings:,}", "page_rows": row_count}
                )
                progress_bar.update(1)

                # Save intermediate results
                if len(batch_tables) >= batch_size:
                    try:
                        self._save_intermediate_results(batch_tables, intermediate_batch_num)
                    except RuntimeError:
                        logger.error(
                            f"Batch {intermediate_batch_num} lost ({len(batch_tables)} pages). "
                            "Continuing download."
                        )
                    batch_tables = []
                    intermediate_batch_num += 1

                if not has_next:
                    logger.info("hasNextPage=false — reached end of dataset")
                    break

                cursor = end_cursor
                page_num += 1

                # Rate limiting — be respectful
                time.sleep(0.2)

        finally:
            progress_bar.close()

        # Save remaining
        if batch_tables:
            try:
                self._save_intermediate_results(batch_tables, intermediate_batch_num)
            except RuntimeError:
                logger.error(
                    f"Final batch {intermediate_batch_num} lost ({len(batch_tables)} pages)."
                )

        logger.info(f"Completed: {page_num + 1} pages, {total_buildings:,} buildings")

        # Combine intermediate files
        self._combine_intermediate_files()

    def _save_intermediate_results(self, table_names: list, batch_num: int) -> None:
        """Save accumulated page tables to an intermediate GeoParquet file."""
        if not table_names:
            return

        union_query = " UNION ALL ".join(f"SELECT * FROM {t}" for t in table_names)
        output_file = self.output_dir / f"geodanmark_buildings_batch_{batch_num:04d}.geoparquet"

        max_retries = 3
        last_error = None

        try:
            for attempt in range(max_retries):
                try:
                    self.conn.execute(f"""
                        COPY ({union_query})
                        TO '{output_file}' (FORMAT PARQUET)
                    """)

                    count = self.conn.execute(f"SELECT COUNT(*) FROM ({union_query})").fetchone()[0]
                    logger.info(f"Saved {count:,} buildings to {output_file}")
                    last_error = None
                    break

                except Exception as e:
                    last_error = e
                    logger.warning(
                        f"Save attempt {attempt + 1}/{max_retries} failed for batch {batch_num}: {e}"
                    )
                    # Remove potentially corrupted file
                    if output_file.exists():
                        output_file.unlink()
                    if attempt < max_retries - 1:
                        time.sleep(2)

            if last_error is not None:
                raise RuntimeError(f"Failed to save batch {batch_num}") from last_error

        finally:
            # Always free memory regardless of save outcome
            for table_name in table_names:
                with contextlib.suppress(Exception):
                    self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    def _combine_intermediate_files(self) -> None:
        """Combine all intermediate GeoParquet files into final output."""
        intermediate_files = sorted(self.output_dir.glob("geodanmark_buildings_batch_*.geoparquet"))

        if not intermediate_files:
            raise RuntimeError("No intermediate GeoDanmark files were produced")

        logger.info(f"Validating {len(intermediate_files)} intermediate files")

        valid_files = []
        skipped_files = []

        for f in intermediate_files:
            # Quick size check — Parquet header alone is ~100+ bytes
            if f.stat().st_size < 100:
                logger.warning(f"Skipping {f.name}: file too small ({f.stat().st_size} bytes)")
                skipped_files.append(f)
                continue

            # Validate DuckDB can read it
            try:
                count = self.conn.execute(f"SELECT COUNT(*) FROM read_parquet('{f}')").fetchone()[0]
                if count == 0:
                    logger.warning(f"Skipping {f.name}: contains 0 rows")
                    skipped_files.append(f)
                    continue
                valid_files.append(str(f))
            except Exception as e:
                logger.warning(f"Skipping corrupted file {f.name}: {e}")
                skipped_files.append(f)

        if skipped_files:
            logger.warning(
                f"Skipped {len(skipped_files)}/{len(intermediate_files)} corrupted/empty files"
            )

        if not valid_files:
            raise RuntimeError("All intermediate files are corrupted — no data to combine")

        logger.info(f"Combining {len(valid_files)} valid intermediate files")

        try:
            # Read each file individually, casting all text columns to VARCHAR
            # to resolve type conflicts (e.g. BBRaktion stored as JSON in some
            # files and VARCHAR in others — DuckDB's read_parquet union_by_name
            # cannot reconcile these).
            union_parts = []
            for _i, f in enumerate(valid_files):
                cols = self.conn.execute(
                    f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('{f}'))"
                ).fetchall()
                select_parts = []
                for col_name, col_type in cols:
                    if col_name in _COMBINE_COLUMNS and col_type != "VARCHAR":
                        select_parts.append(f'TRY_CAST("{col_name}" AS VARCHAR) AS "{col_name}"')
                    else:
                        select_parts.append(f'"{col_name}"')
                select_clause = ", ".join(select_parts)
                union_parts.append(f"SELECT {select_clause} FROM read_parquet('{f}')")

            union_query = " UNION ALL ".join(union_parts)

            self.conn.execute(f"""
                COPY ({union_query})
                TO '{self.output_dir}/geodanmark_buildings_complete.geoparquet'
                (FORMAT PARQUET)
            """)

            count = self.conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{self.output_dir}/geodanmark_buildings_complete.geoparquet')"
            ).fetchone()[0]

            logger.info(
                f"Combined {count:,} buildings into geodanmark_buildings_complete.geoparquet"
            )

            # Clean up all intermediate files (including skipped ones)
            for f in intermediate_files:
                f.unlink()
            logger.info("Cleaned up intermediate files")

        except Exception as e:
            logger.error(f"Error combining files: {e}")
            raise

    def __del__(self) -> None:
        if hasattr(self, "conn"):
            self.conn.close()


def main() -> None:
    """Main function to run bulk download via GraphQL."""
    api_key = os.getenv("DATAFORDELER_GRAPHQL_API_KEY")
    if not api_key:
        logger.error("DATAFORDELER_GRAPHQL_API_KEY environment variable required")
        return

    fetcher = BulkGeoDanmarkGraphQLFetcher(api_key)
    logger.info("Starting full bulk download via GraphQL")
    fetcher.bulk_download_buildings(batch_size=10)


if __name__ == "__main__":
    main()
