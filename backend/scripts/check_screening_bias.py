#!/usr/bin/env python3
# ruff: noqa: T201
"""
Check screening bias: for key substances, find the earliest analysis dates
and count negative vs positive tests over time.

If a substance has many negative tests (tested but not detected) before the
first positive, that eliminates the "first-testing-not-first-detection" concern.

Usage:
    cd backend
    python3 scripts/check_screening_bias.py
"""

import logging
import os
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("screening_bias")

BUCKET = "landbruget-data"

# Key substances from Paper 1 (pesticide correlation)
PAPER1_SUBSTANCES = [
    "1,2,4-Triazol",
    "4-Chlor-2-methylphenol",
    "Bentazon",
    "(Aminomethyl)phosphonsyre",  # AMPA
    "Glyphosat",
    "2,4-Dichlorphenol",
    "MCPA",
    "Ethylenthiourea",
    "Diuron",
]

# PFAS substances
PFAS_SUBSTANCES = [
    "PFOS",
    "PFOA",
    "PFHxS",
    "PFNA",
    "Trifluoreddikesyre",
]

DETECTION_THRESHOLD = 0.015


def setup_r2(conn):
    """Configure DuckDB for R2 access using env vars."""
    key_id = os.getenv("R2_ACCESS_KEY_ID")
    secret = os.getenv("R2_SECRET_ACCESS_KEY")
    account_id = os.getenv("R2_ACCOUNT_ID")

    if not all([key_id, secret, account_id]):
        # Try loading from .env file
        env_file = Path(__file__).parent.parent / "pipelines" / "unified_pipeline" / "src" / "unified_pipeline" / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    if k and v:
                        os.environ[k] = v
            key_id = os.getenv("R2_ACCESS_KEY_ID")
            secret = os.getenv("R2_SECRET_ACCESS_KEY")
            account_id = os.getenv("R2_ACCOUNT_ID")

    if not all([key_id, secret, account_id]):
        raise RuntimeError("R2 credentials not found. Set R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID")

    conn.execute("INSTALL httpfs; LOAD httpfs")
    conn.execute(f"""
        CREATE SECRET r2_secret (
            TYPE s3,
            KEY_ID '{key_id}',
            SECRET '{secret}',
            ENDPOINT '{account_id}.r2.cloudflarestorage.com',
            URL_STYLE 'path',
            REGION 'auto'
        )
    """)
    log.info("R2 auth configured")


def analyze_pesticides(conn):
    """Analyze pesticide substance testing history."""
    log.info("Loading clean pesticide data from R2...")

    # First check what's available
    try:
        schema = conn.execute(f"""
            SELECT column_name, data_type
            FROM (DESCRIBE SELECT * FROM read_parquet('s3://{BUCKET}/silver/geus_clean_pesticides/**/*.parquet') LIMIT 0)
        """).fetchall()
        log.info(f"Pesticide columns: {[c[0] for c in schema]}")
    except Exception as e:
        log.error(f"Cannot read pesticide data: {e}")
        return

    # Run the screening bias analysis for each substance
    for substance in PAPER1_SUBSTANCES:
        safe = substance.replace("'", "''")
        try:
            results = conn.execute(f"""
                SELECT
                    year,
                    COUNT(*) AS total_analyses,
                    SUM(CASE WHEN maengde > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END) AS detections,
                    SUM(CASE WHEN maengde <= {DETECTION_THRESHOLD} THEN 1 ELSE 0 END) AS non_detections,
                    ROUND(100.0 * SUM(CASE WHEN maengde > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END) / COUNT(*), 1) AS det_pct,
                    COUNT(DISTINCT dgu_nr) AS n_wells
                FROM read_parquet('s3://{BUCKET}/silver/geus_clean_pesticides/**/*.parquet')
                WHERE stof_tekst = '{safe}'
                GROUP BY year
                ORDER BY year
            """).fetchall()
        except Exception:
            # Try ILIKE match
            try:
                results = conn.execute(f"""
                    SELECT
                        year,
                        COUNT(*) AS total_analyses,
                        SUM(CASE WHEN maengde > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END) AS detections,
                        SUM(CASE WHEN maengde <= {DETECTION_THRESHOLD} THEN 1 ELSE 0 END) AS non_detections,
                        ROUND(100.0 * SUM(CASE WHEN maengde > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END) / COUNT(*), 1) AS det_pct,
                        COUNT(DISTINCT dgu_nr) AS n_wells
                    FROM read_parquet('s3://{BUCKET}/silver/geus_clean_pesticides/**/*.parquet')
                    WHERE stof_tekst ILIKE '%{safe}%'
                    GROUP BY year
                    ORDER BY year
                """).fetchall()
            except Exception as e2:
                print(f"\n--- {substance}: ERROR - {e2} ---")
                continue

        if not results:
            print(f"\n--- {substance}: NOT FOUND IN DATASET ---")
            continue

        print(f"\n--- {substance} ---")
        print(f"{'Year':>6} | {'Total':>8} | {'Detected':>8} | {'Not Det.':>8} | {'Det%':>6} | {'Wells':>6}")
        print("-" * 60)

        first_test_year = None
        first_detect_year = None
        neg_before = 0

        for yr, total, det, nondet, pct, wells in results:
            if yr is None:
                continue
            yr = int(yr)
            print(f"{yr:>6} | {total:>8,} | {det:>8,} | {nondet:>8,} | {pct:>5.1f}% | {wells:>6}")

            if first_test_year is None:
                first_test_year = yr
            if det > 0 and first_detect_year is None:
                first_detect_year = yr
            if first_detect_year is None:
                neg_before += nondet

        print()
        print(f"  First test year: {first_test_year}")
        print(f"  First detection year: {first_detect_year}")
        if first_detect_year and first_test_year:
            gap = first_detect_year - first_test_year
            if gap > 0:
                print(f"  Years of negative-only testing before first detection: {gap}")
                print(f"  Negative tests before first detection: {neg_before:,}")
                print("  → SCREENING BIAS RISK: LOW")
            elif gap == 0 and neg_before > 0:
                print(f"  Negative tests in same year as first detection: {neg_before}")
                print("  → SCREENING BIAS RISK: LOW (negatives exist in first year)")
            else:
                print("  → SCREENING BIAS RISK: CHECK CAREFULLY")


def analyze_pfas(conn):
    """Analyze PFAS substance testing history."""
    log.info("\nLoading clean all-parameters data from R2 (PFAS)...")

    try:
        schema = conn.execute(f"""
            SELECT column_name
            FROM (DESCRIBE SELECT * FROM read_parquet('s3://{BUCKET}/silver/geus_clean_all/**/*.parquet') LIMIT 0)
        """).fetchall()
        col_names = [c[0] for c in schema]
        log.info(f"All-params columns: {col_names}")
    except Exception as e:
        log.error(f"Cannot read all-params data: {e}")
        return

    # Determine the right column names
    name_col = "stof_tekst" if "stof_tekst" in col_names else "kort_navn"
    val_col = "maengde" if "maengde" in col_names else "concentration"
    year_col = "year" if "year" in col_names else None

    for substance in PFAS_SUBSTANCES:
        safe = substance.replace("'", "''")
        try:
            results = conn.execute(f"""
                SELECT
                    {year_col or "EXTRACT(YEAR FROM sample_date)::INT"} AS yr,
                    COUNT(*) AS total_analyses,
                    SUM(CASE WHEN {val_col} > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END) AS detections,
                    SUM(CASE WHEN {val_col} <= {DETECTION_THRESHOLD} THEN 1 ELSE 0 END) AS non_detections,
                    ROUND(100.0 * SUM(CASE WHEN {val_col} > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS det_pct,
                    COUNT(DISTINCT dgu_nr) AS n_wells
                FROM read_parquet('s3://{BUCKET}/silver/geus_clean_all/**/*.parquet')
                WHERE {name_col} ILIKE '%{safe}%'
                GROUP BY yr
                ORDER BY yr
            """).fetchall()
        except Exception as e:
            print(f"\n--- {substance} (PFAS): ERROR - {e} ---")
            continue

        if not results:
            print(f"\n--- {substance} (PFAS): NOT FOUND ---")
            continue

        print(f"\n--- {substance} (PFAS) ---")
        print(f"{'Year':>6} | {'Total':>8} | {'Detected':>8} | {'Not Det.':>8} | {'Det%':>6} | {'Wells':>6}")
        print("-" * 60)

        first_test_year = None
        first_detect_year = None
        neg_before = 0

        for yr, total, det, nondet, pct, wells in results:
            if yr is None:
                continue
            yr = int(yr)
            print(f"{yr:>6} | {total:>8,} | {det:>8,} | {nondet:>8,} | {pct:>5.1f}% | {wells:>6}")

            if first_test_year is None:
                first_test_year = yr
            if det > 0 and first_detect_year is None:
                first_detect_year = yr
            if first_detect_year is None:
                neg_before += nondet

        print()
        print(f"  First test year: {first_test_year}")
        print(f"  First detection year: {first_detect_year}")
        if first_detect_year and first_test_year:
            gap = first_detect_year - first_test_year
            if gap > 0:
                print(f"  Years of negative-only testing before first detection: {gap}")
                print(f"  Negative tests before first detection: {neg_before:,}")
                print("  → SCREENING BIAS RISK: LOW")
            elif gap == 0 and neg_before > 0:
                print(f"  Negative tests in same year as first detection: {neg_before}")
                print("  → SCREENING BIAS RISK: LOW (negatives exist in first year)")
            else:
                print("  → SCREENING BIAS RISK: CHECK CAREFULLY")


def main():
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial")

    setup_r2(conn)

    print("\n" + "=" * 100)
    print("SCREENING BIAS ANALYSIS — Paper 1: Pesticide Correlation")
    print("=" * 100)
    analyze_pesticides(conn)

    print("\n" + "=" * 100)
    print("SCREENING BIAS ANALYSIS — Paper 2: PFAS Correlation")
    print("=" * 100)
    analyze_pfas(conn)

    conn.close()


if __name__ == "__main__":
    main()
