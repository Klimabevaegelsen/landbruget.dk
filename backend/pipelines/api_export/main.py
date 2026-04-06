#!/usr/bin/env python3
"""API Export Pipeline — Generate JSON API files from gold-layer parquet and upload to R2.

Replaces Supabase edge functions + materialized views with pre-computed static JSON.

Usage:
    python main.py                          # Export all, upload to R2
    python main.py --local /tmp/api_export  # Export to local dir for testing
    python main.py --only homepage          # Export specific module
    python main.py --dry-run                # Show what would be exported

Environment Variables:
    R2_BUCKET: R2 bucket name (default: landbruget-data)
    R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY: R2 credentials
    GCS_BUCKET: Source GCS bucket (default: landbruget-data)
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import duckdb
from dotenv import load_dotenv

# Add backend root to path for common imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from common.storage.filesystem import setup_duckdb_cloud_auth
from exporters.company_profiles import CompanyProfilesExporter
from exporters.homepage import HomepageExporter
from exporters.municipalities import MunicipalitiesExporter
from exporters.pesticides import PesticidesExporter
from exporters.search_index import SearchIndexExporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("api_export")

EXPORTERS = {
    "homepage": HomepageExporter,
    "search": SearchIndexExporter,
    "municipalities": MunicipalitiesExporter,
    "pesticides": PesticidesExporter,
    "companies": CompanyProfilesExporter,
}


def create_connection() -> duckdb.DuckDBPyConnection:
    """Create DuckDB connection with cloud storage auth."""
    conn = duckdb.connect()
    conn.execute("SET memory_limit = '4GB'")
    conn.execute("SET threads = 4")

    if not setup_duckdb_cloud_auth(conn):
        logger.warning("No cloud auth configured — will only work with local files")

    return conn


def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Generate JSON API files for R2")
    parser.add_argument("--local", type=str, help="Output to local directory instead of R2")
    parser.add_argument("--only", type=str, help=f"Run only specific exporter: {','.join(EXPORTERS)}")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be exported")
    parser.add_argument("--bucket", type=str, help="Override GCS source bucket")
    args = parser.parse_args()

    if args.bucket:
        os.environ["GCS_BUCKET"] = args.bucket

    # Determine which exporters to run
    if args.only:
        names = [n.strip() for n in args.only.split(",")]
        for name in names:
            if name not in EXPORTERS:
                logger.error(f"Unknown exporter: {name}. Available: {','.join(EXPORTERS)}")
                return 1
        to_run = {name: EXPORTERS[name] for name in names}
    else:
        to_run = EXPORTERS

    if args.dry_run:
        logger.info(f"Would run exporters: {', '.join(to_run)}")
        return 0

    # Create DuckDB connection with cloud auth
    conn = create_connection()
    output_dir = args.local

    total_start = time.time()
    results = {}
    failed = []

    for name, exporter_cls in to_run.items():
        logger.info(f"{'=' * 60}")
        logger.info(f"Running exporter: {name}")
        logger.info(f"{'=' * 60}")
        start = time.time()

        try:
            exporter = exporter_cls(conn=conn, output_dir=output_dir)
            stats = exporter.export()
            elapsed = time.time() - start
            results[name] = {**stats, "duration_seconds": round(elapsed, 2)}
            logger.info(f"✓ {name} completed in {elapsed:.1f}s: {stats}")
        except Exception:
            elapsed = time.time() - start
            logger.exception(f"✗ {name} failed after {elapsed:.1f}s")
            failed.append(name)

    total_elapsed = time.time() - total_start
    logger.info(f"{'=' * 60}")
    logger.info(f"API Export complete in {total_elapsed:.1f}s")
    logger.info(f"  Succeeded: {', '.join(results.keys()) or 'none'}")
    if failed:
        logger.error(f"  Failed: {', '.join(failed)}")
    logger.info(f"{'=' * 60}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
