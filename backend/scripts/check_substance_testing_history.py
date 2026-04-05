#!/usr/bin/env python3
"""
Check whether metabolite detections are a surveillance artifact.

For each key substance from the groundwater correlation paper, answers:
  1. When did it first appear in the GEUS dataset? (proxy for when testing started)
  2. How has the number of tested boreholes changed over time?
  3. Is the detection rate stable, or does it spike when testing expands?
  4. What monitoring programmes contribute the detections?

Uses silver/geus_clean_pesticides (sample-level clean dataset with monitoring flags)
The clean dataset (Deliverable 2) includes monitoring flags directly.

Usage:
    cd backend && source venv/bin/activate
    python scripts/check_substance_testing_history.py
"""

import argparse
import logging
import os
import sys
import tempfile
from pathlib import Path

import duckdb
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("testing_history")

BUCKET = "landbruget-data"
DETECTION_THRESHOLD = 0.015

# Key substances from the paper (GEUS stof_tekst names)
KEY_SUBSTANCES = [
    # Metabolites (paper's top findings)
    "1,2,4-Triazol",
    "4-Chlor-2-methylphenol",
    "(Aminomethyl)phosphonsyre",  # AMPA
    "2,4-Dichlorphenol",
    # Parent compounds
    "Bentazon",
    "Glyphosat",
    "MCPA",
    # Emerging substances from Supplementary S3.10
    "Azoxystrobinsyre",
    "Metazachlor OA",
    "Dimethachlor ESA",
]


def setup_storage(conn: duckdb.DuckDBPyConnection) -> None:
    """Register cloud storage credentials with DuckDB."""
    r2_key = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret = os.getenv("R2_SECRET_ACCESS_KEY")
    r2_account = os.getenv("R2_ACCOUNT_ID")

    if r2_key and r2_secret and r2_account:
        endpoint = f"https://{r2_account}.r2.cloudflarestorage.com"
        conn.execute(f"""
            CREATE OR REPLACE SECRET (
                TYPE S3,
                KEY_ID '{r2_key}',
                SECRET '{r2_secret}',
                ENDPOINT '{endpoint.replace("https://", "")}',
                URL_STYLE 'path',
                REGION 'auto'
            )
        """)
        log.info("Configured R2 storage")
    else:
        log.warning("No R2 credentials found — set R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID")
        sys.exit(1)


def load_silver(conn: duckdb.DuckDBPyConnection) -> int:
    """Load silver GEUS data."""
    log.info("Loading silver/geus_clean_pesticides...")
    conn.execute(f"""
        CREATE TABLE geus AS
        SELECT * FROM read_parquet('s3://{BUCKET}/silver/geus_clean_pesticides/**/*.parquet')
    """)
    n = conn.execute("SELECT COUNT(*) FROM geus").fetchone()[0]
    log.info(f"  Loaded {n:,} records")
    return n


def load_bronze_monitoring_flags(conn: duckdb.DuckDBPyConnection) -> bool:
    """Optionally load bronze .rds to get monitoring type flags.

    Returns True if bronze data with monitoring flags is available.
    """
    log.info("Attempting to load bronze data for monitoring type flags...")
    try:
        import pyreadr
    except ImportError:
        log.warning("  pyreadr not installed — skipping bronze analysis")
        return False

    # Find latest bronze manifest
    try:
        import s3fs

        r2_key = os.getenv("R2_ACCESS_KEY_ID")
        r2_secret = os.getenv("R2_SECRET_ACCESS_KEY")
        r2_account = os.getenv("R2_ACCOUNT_ID")
        endpoint = f"https://{r2_account}.r2.cloudflarestorage.com"

        fs = s3fs.S3FileSystem(
            key=r2_key,
            secret=r2_secret,
            endpoint_url=endpoint,
        )

        # List bronze directories to find manifest
        bronze_path = f"{BUCKET}/bronze/geus_dataverse_pesticides"
        dirs = sorted(fs.ls(bronze_path))
        if not dirs:
            log.warning("  No bronze data found")
            return False

        # Find the .rds file
        latest_dir = dirs[-1]
        files = fs.ls(latest_dir)
        rds_files = [f for f in files if f.endswith("AM_pest.rds")]
        if not rds_files:
            log.warning(f"  No AM_pest.rds in {latest_dir}")
            return False

        # Download and read
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".rds")
        os.close(tmp_fd)
        fs.get(rds_files[0], tmp_path)
        log.info(f"  Downloaded bronze RDS ({Path(tmp_path).stat().st_size / 1e6:.1f} MB)")

        result = pyreadr.read_r(tmp_path)
        df = next(iter(result.values()))
        Path(tmp_path).unlink()

        # Check for monitoring columns
        monitoring_cols = [
            c for c in df.columns if c in ("chemicalMonitoring", "investigativeMonitoring", "surveillanceMonitoring")
        ]
        if not monitoring_cols:
            log.warning(f"  Bronze data has no monitoring type columns. Columns: {list(df.columns)}")
            return False

        log.info(f"  Bronze monitoring columns found: {monitoring_cols}")

        # Register and create table with key columns
        conn.register("bronze_df", df)
        conn.execute("""
            CREATE TABLE bronze AS
            SELECT
                CAST(PROEVEAAR AS INTEGER) as year,
                STOFNAVN as stof_tekst,
                CAST(AM AS DOUBLE) as maengde,
                DGUNR as dgu_nr,
                DATATYPE as data_type,
                CAST(chemicalMonitoring AS BOOLEAN) as chemical_monitoring,
                CAST(investigativeMonitoring AS BOOLEAN) as investigative_monitoring,
                CAST(surveillanceMonitoring AS BOOLEAN) as surveillance_monitoring
            FROM bronze_df
            WHERE PROEVEAAR >= 1980 AND PROEVEAAR <= 2030
        """)
        n = conn.execute("SELECT COUNT(*) FROM bronze").fetchone()[0]
        log.info(f"  Bronze table: {n:,} records with monitoring flags")
        return True

    except Exception as e:
        log.warning(f"  Failed to load bronze: {e}")
        return False


def analysis_1_first_appearance(conn: duckdb.DuckDBPyConnection) -> None:
    """When did each substance first appear in the dataset?"""
    print("\n" + "=" * 80)  # noqa: T201
    print("ANALYSIS 1: First appearance of each substance in GEUS dataset")  # noqa: T201
    print("(Proxy for when routine testing began)")  # noqa: T201
    print("=" * 80)  # noqa: T201

    placeholders = ", ".join(f"'{s}'" for s in KEY_SUBSTANCES)
    rows = conn.execute(f"""
        SELECT
            stof_tekst,
            MIN(year) as first_year,
            MAX(year) as last_year,
            COUNT(DISTINCT year) as n_years,
            COUNT(*) as total_analyses,
            COUNT(DISTINCT dgu_nr) as total_boreholes,
            SUM(CASE WHEN maengde > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END) as total_detections,
            ROUND(100.0 * SUM(CASE WHEN maengde > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END)
                  / COUNT(*), 1) as overall_det_rate
        FROM geus
        WHERE stof_tekst IN ({placeholders})
        GROUP BY stof_tekst
        ORDER BY first_year, stof_tekst
    """).fetchall()

    print(  # noqa: T201
        f"\n{'Substance':<30} {'First':>5} {'Last':>5} {'Years':>5} "
        f"{'Analyses':>10} {'Boreholes':>10} {'Detections':>10} {'Det%':>6}"
    )
    print("-" * 92)  # noqa: T201
    for r in rows:
        print(f"{r[0]:<30} {r[1]:>5} {r[2]:>5} {r[3]:>5} {r[4]:>10,} {r[5]:>10,} {r[6]:>10,} {r[7]:>5.1f}%")  # noqa: T201

    # Check which substances are NOT in the data at all
    found = {r[0] for r in rows}
    missing = [s for s in KEY_SUBSTANCES if s not in found]
    if missing:
        print(f"\n  NOT FOUND in dataset: {missing}")  # noqa: T201


def analysis_2_testing_rampup(conn: duckdb.DuckDBPyConnection) -> None:
    """How has the number of boreholes tested changed over time per substance?"""
    print("\n" + "=" * 80)  # noqa: T201
    print("ANALYSIS 2: Testing ramp-up — boreholes tested per year")  # noqa: T201
    print("(Shows when labs expanded their analytical panels)")  # noqa: T201
    print("=" * 80)  # noqa: T201

    for substance in KEY_SUBSTANCES:
        safe = substance.replace("'", "''")
        rows = conn.execute(f"""
            SELECT
                year,
                COUNT(*) as n_analyses,
                COUNT(DISTINCT dgu_nr) as n_boreholes,
                SUM(CASE WHEN maengde > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END) as n_detections,
                ROUND(100.0 * SUM(CASE WHEN maengde > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 1) as det_rate
            FROM geus
            WHERE stof_tekst = '{safe}'
            GROUP BY year
            ORDER BY year
        """).fetchall()

        if not rows:
            print(f"\n  {substance}: NO DATA")  # noqa: T201
            continue

        print(f"\n  {substance}:")  # noqa: T201
        print(f"  {'Year':>6} {'Analyses':>10} {'Boreholes':>10} {'Detections':>10} {'Det%':>7}")  # noqa: T201
        print(f"  {'-' * 49}")  # noqa: T201

        # Show key years: first 3, then every 5th year, then last 3
        show_indices = set()
        # First 3
        for i in range(min(3, len(rows))):
            show_indices.add(i)
        # Last 3
        for i in range(max(0, len(rows) - 3), len(rows)):
            show_indices.add(i)
        # Every 5th year
        for i, r in enumerate(rows):
            if r[0] % 5 == 0:
                show_indices.add(i)
        # 2015-2018 transition period (application → detection window)
        for i, r in enumerate(rows):
            if 2015 <= r[0] <= 2020:
                show_indices.add(i)

        prev_shown = -1
        for _i, r in enumerate(sorted(show_indices)):
            if r > prev_shown + 1 and prev_shown >= 0:
                print(f"  {'...':>6}")  # noqa: T201
            row = rows[r]
            print(f"  {row[0]:>6} {row[1]:>10,} {row[2]:>10,} {row[3]:>10,} {row[4]:>6.1f}%")  # noqa: T201
            prev_shown = r


def analysis_3_detection_vs_testing_expansion(conn: duckdb.DuckDBPyConnection) -> None:
    """Is detection rate stable as testing expands, or does it spike with new testing?"""
    print("\n" + "=" * 80)  # noqa: T201
    print("ANALYSIS 3: Detection rate vs testing expansion (2010-2025)")  # noqa: T201
    print("(If detection rate is STABLE as boreholes increase → real signal)")  # noqa: T201
    print("(If detection rate SPIKES when testing starts → surveillance artifact)")  # noqa: T201
    print("=" * 80)  # noqa: T201

    for substance in KEY_SUBSTANCES:
        safe = substance.replace("'", "''")
        rows = conn.execute(f"""
            SELECT
                year,
                COUNT(DISTINCT dgu_nr) as n_boreholes,
                ROUND(100.0 * SUM(CASE WHEN maengde > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 2) as det_rate
            FROM geus
            WHERE stof_tekst = '{safe}'
              AND year >= 2010
            GROUP BY year
            ORDER BY year
        """).fetchall()

        if len(rows) < 3:
            continue

        years = [r[0] for r in rows]
        boreholes = [r[1] for r in rows]
        det_rates = [r[2] for r in rows]

        # Correlation between n_boreholes and detection rate
        if np.std(boreholes) > 0 and np.std(det_rates) > 0:
            from scipy.stats import spearmanr

            rho, p = spearmanr(boreholes, det_rates)
            verdict = ""
            if p < 0.05 and rho > 0.3:
                verdict = "⚠️  MORE TESTING → MORE DETECTIONS (surveillance artifact risk)"
            elif p < 0.05 and rho < -0.3:
                verdict = "✓  MORE TESTING → LOWER DET RATE (dilution = real signal)"
            else:
                verdict = "—  No clear relationship"

            print(f"\n  {substance}:")  # noqa: T201
            print(f"    Boreholes: {min(boreholes):,} ({min(years)}) → {max(boreholes):,} ({max(years)})")  # noqa: T201
            print(f"    Det rate:  {det_rates[0]:.1f}% ({years[0]}) → {det_rates[-1]:.1f}% ({years[-1]})")  # noqa: T201
            print(f"    Spearman(n_boreholes, det_rate): rho={rho:.3f}, p={p:.3f}")  # noqa: T201
            print(f"    {verdict}")  # noqa: T201


def analysis_4_monitoring_type(conn: duckdb.DuckDBPyConnection) -> None:
    """What monitoring programmes contribute the detections?

    With the clean dataset (Deliverable 2), monitoring flags are available directly
    in silver — no need to load bronze data separately.
    """
    # Check if monitoring flags are available in the geus table
    has_monitoring = conn.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name = 'geus' AND column_name = 'chemical_monitoring'
    """).fetchone()[0]

    if not has_monitoring:
        print("\n" + "=" * 80)  # noqa: T201
        print("ANALYSIS 4: Monitoring programme breakdown — SKIPPED (no monitoring flags)")  # noqa: T201
        print("Use the clean dataset (silver/geus_clean_pesticides) to include monitoring flags")  # noqa: T201
        print("=" * 80)  # noqa: T201
        return

    print("\n" + "=" * 80)  # noqa: T201
    print("ANALYSIS 4: Monitoring programme breakdown")  # noqa: T201
    print("(investigative = targeted follow-up → weaker evidence)")  # noqa: T201
    print("(surveillance/chemical = routine → stronger evidence)")  # noqa: T201
    print("=" * 80)  # noqa: T201

    for substance in KEY_SUBSTANCES:
        safe = substance.replace("'", "''")

        # All analyses for this substance
        all_rows = conn.execute(f"""
            SELECT
                COALESCE(data_type, 'UNKNOWN') as dtype,
                chemical_monitoring,
                investigative_monitoring,
                surveillance_monitoring,
                COUNT(*) as n,
                SUM(CASE WHEN maengde > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END) as n_det
            FROM geus
            WHERE stof_tekst = '{safe}'
              AND year >= 2018
            GROUP BY data_type, chemical_monitoring, investigative_monitoring, surveillance_monitoring
            ORDER BY n DESC
        """).fetchall()

        if not all_rows:
            continue

        total_n = sum(r[4] for r in all_rows)
        total_det = sum(r[5] for r in all_rows)

        print(f"\n  {substance} (2018+: {total_n:,} analyses, {total_det:,} detections):")  # noqa: T201
        print(f"    {'DataType':<12} {'Chem':>5} {'Invest':>7} {'Surv':>5} {'N':>8} {'Det':>6} {'Det%':>6}")  # noqa: T201
        print(f"    {'-' * 55}")  # noqa: T201
        for r in all_rows[:8]:  # Top 8 combinations
            det_pct = 100.0 * r[5] / r[4] if r[4] > 0 else 0
            print(f"    {r[0]!s:<12} {r[1]!s:>5} {r[2]!s:>7} {r[3]!s:>5} {r[4]:>8,} {r[5]:>6,} {det_pct:>5.1f}%")  # noqa: T201

        # Summary: what % of detections come from investigative monitoring?
        invest_det = sum(r[5] for r in all_rows if r[2])
        routine_det = sum(r[5] for r in all_rows if not r[2])
        if total_det > 0:
            print(f"    → Investigative: {invest_det}/{total_det} detections ({100 * invest_det / total_det:.0f}%)")  # noqa: T201
            print(f"    → Routine:       {routine_det}/{total_det} detections ({100 * routine_det / total_det:.0f}%)")  # noqa: T201


def analysis_5_data_type_breakdown(conn: duckdb.DuckDBPyConnection) -> None:
    """Breakdown by DATATYPE (GRUMO vs VF etc) — available in silver."""
    print("\n" + "=" * 80)  # noqa: T201
    print("ANALYSIS 5: Data type breakdown (GRUMO=routine, VF=vulnerable zones)")  # noqa: T201
    print("(available from silver data)")  # noqa: T201
    print("=" * 80)  # noqa: T201

    for substance in KEY_SUBSTANCES:
        safe = substance.replace("'", "''")
        rows = conn.execute(f"""
            SELECT
                COALESCE(data_type, 'UNKNOWN') as dtype,
                COUNT(*) as n,
                SUM(CASE WHEN maengde > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END) as n_det,
                COUNT(DISTINCT dgu_nr) as n_boreholes,
                ROUND(100.0 * SUM(CASE WHEN maengde > {DETECTION_THRESHOLD} THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 1) as det_rate
            FROM geus
            WHERE stof_tekst = '{safe}'
              AND year >= 2018
            GROUP BY data_type
            ORDER BY n DESC
        """).fetchall()

        if not rows:
            continue

        print(f"\n  {substance} (2018+):")  # noqa: T201
        print(f"    {'Type':<20} {'Analyses':>10} {'Boreholes':>10} {'Detections':>10} {'Det%':>6}")  # noqa: T201
        print(f"    {'-' * 60}")  # noqa: T201
        for r in rows:
            print(f"    {r[0]:<20} {r[1]:>10,} {r[3]:>10,} {r[2]:>10,} {r[4]:>5.1f}%")  # noqa: T201


def main():
    parser = argparse.ArgumentParser(description="Check substance testing history in GEUS data")
    parser.add_argument("--with-bronze", action="store_true", help="Load bronze .rds to get monitoring type flags")
    args = parser.parse_args()

    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial")

    setup_storage(conn)
    load_silver(conn)

    if args.with_bronze:
        load_bronze_monitoring_flags(conn)

    analysis_1_first_appearance(conn)
    analysis_2_testing_rampup(conn)
    analysis_3_detection_vs_testing_expansion(conn)
    analysis_4_monitoring_type(conn)
    analysis_5_data_type_breakdown(conn)

    conn.close()
    print("\n✓ Done")  # noqa: T201


if __name__ == "__main__":
    main()
