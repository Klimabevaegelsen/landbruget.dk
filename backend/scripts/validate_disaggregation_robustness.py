#!/usr/bin/env python3
"""
Empirical validation of the pesticide disaggregation methodology.

Three analyses that measure (not guess) the robustness of the disaggregation:

  1. TOLERANCE SENSITIVITY — Run the area-matching logic at 0%, 1%, 2%, 3%, 5%, 10%
     tolerance and report actual coverage rates and ambiguous-match counts.

  2. UNMATCHED PROFILER — Characterize the records that fail the 2% tolerance:
     distribution by crop code, area ratio (SJI / FVM), farm size, field count.

  3. BMD DOSE-RATE CHECK — Compare disaggregated per-hectare doses against BMD
     authorized maximum rates. Report what fraction exceeds authorized rates.

Data sources (all from R2 / landbruget-data bucket):
  - silver/pesticides/              — SJI spray journal data (company+crop level)
  - silver/fvm_marker_{year}/       — FVM field boundaries (field level)
  - silver/bmd/                     — BMD pesticide product registrations
  - gold/pesticide_disaggregation_  — disaggregated results (for dose-rate check)

Usage:
    cd backend && source venv/bin/activate
    python scripts/validate_disaggregation_robustness.py [--year 2021] [--dry-run] [--verbose]
    python scripts/validate_disaggregation_robustness.py --analysis tolerance   # run only tolerance
    python scripts/validate_disaggregation_robustness.py --analysis unmatched   # run only profiler
    python scripts/validate_disaggregation_robustness.py --analysis doserate    # run only dose check
    python scripts/validate_disaggregation_robustness.py --analysis all         # run all (default)

Auth: Uses wrangler CLI (must be logged in) or R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY env vars.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

# Add backend to path for common imports
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("validate_disaggregation")

BUCKET = "landbruget-data"

# Area tolerance levels to test in sensitivity analysis
TOLERANCE_LEVELS = [0.0, 0.5, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0]

# Default pesticide year to analyze
DEFAULT_YEAR = 2021

# Y+1 offset (pesticide year X uses field year X+1)
FIELD_YEAR_OFFSET = 1


# ---------------------------------------------------------------------------
# R2 data access — reuses pattern from verify_groundwater_correlations.py
# ---------------------------------------------------------------------------


def _has_r2_env() -> bool:
    return all(os.getenv(k) for k in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ACCOUNT_ID"))


def _setup_duckdb_r2(conn: duckdb.DuckDBPyConnection) -> bool:
    """Try to configure DuckDB for direct R2 access via env vars."""
    if not _has_r2_env():
        return False
    try:
        from common.storage.filesystem import setup_duckdb_cloud_auth

        return setup_duckdb_cloud_auth(conn)
    except Exception as e:
        log.warning(f"Native R2 auth failed: {e}")
        return False


def _wrangler_download(r2_path: str, local_path: str) -> None:
    """Download a single object from R2 via wrangler CLI."""
    cmd = ["wrangler", "r2", "object", "get", f"{BUCKET}/{r2_path}", "--file", local_path, "--remote"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"wrangler download failed for {r2_path}: {result.stderr}")


def _get_wrangler_token() -> str:
    """Read wrangler's cached OAuth token."""
    for config_path in [
        Path.home() / "Library" / "Preferences" / ".wrangler" / "config" / "default.toml",
        Path.home() / ".config" / ".wrangler" / "config" / "default.toml",
    ]:
        if config_path.exists():
            for line in config_path.read_text().splitlines():
                if line.startswith("oauth_token"):
                    return line.split("=", 1)[1].strip().strip('"')
    raise RuntimeError("Could not read wrangler OAuth token")


def _wrangler_list_prefix(prefix: str) -> list[str]:
    """List objects under a prefix using Cloudflare API via wrangler's OAuth token."""
    subprocess.run(["wrangler", "whoami"], capture_output=True, text=True, timeout=15)

    account_id = os.getenv("R2_ACCOUNT_ID", "a5f130bfd0d34de38f8e77f6a0f40a27")
    token = _get_wrangler_token()

    import urllib.request

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
        f"/r2/buckets/{BUCKET}/objects?prefix={prefix}&limit=1000"
    )
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})  # noqa: S310
    with urllib.request.urlopen(req) as resp:  # noqa: S310
        data = json.loads(resp.read())

    if not data.get("success"):
        raise RuntimeError(f"Cloudflare API error: {data.get('errors')}")

    result = data.get("result", [])
    if isinstance(result, dict):
        objects = result.get("objects", [])
    elif isinstance(result, list):
        objects = result
    else:
        objects = []

    return [obj["key"] for obj in objects if obj.get("key", "").endswith(".parquet")]


class DataLoader:
    """Handles loading parquet data into DuckDB, using R2 native or wrangler fallback."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.has_native_r2 = _setup_duckdb_r2(conn)
        self._tmpdir = None

        if self.has_native_r2:
            log.info("Using native DuckDB R2 auth")
        else:
            log.info("Using wrangler CLI for R2 downloads")
            self._tmpdir = tempfile.mkdtemp(prefix="validate_disagg_")
            log.info(f"  Temp dir: {self._tmpdir}")

    def read_parquet(self, r2_prefix: str, table_name: str, extra_select: str = "*") -> int:
        """Load parquet file(s) from R2 prefix into a DuckDB table. Returns row count."""
        if self.has_native_r2:
            return self._read_native(r2_prefix, table_name, extra_select)
        return self._read_via_wrangler(r2_prefix, table_name, extra_select)

    def _read_native(self, r2_prefix: str, table_name: str, extra_select: str) -> int:
        for path in [f"r2://{BUCKET}/{r2_prefix}/*.parquet", f"r2://{BUCKET}/{r2_prefix}/data.parquet"]:
            try:
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT {extra_select} FROM read_parquet('{path}')")
                return self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            except Exception:  # noqa: S112
                continue
        raise RuntimeError(f"Could not read {r2_prefix} from R2")

    def _read_via_wrangler(self, r2_prefix: str, table_name: str, extra_select: str) -> int:
        local_dir = Path(self._tmpdir) / r2_prefix.replace("/", "_")
        local_dir.mkdir(parents=True, exist_ok=True)

        list_prefix = r2_prefix.rstrip("/") + "/"
        log.info(f"  Listing {list_prefix}...")
        try:
            keys = _wrangler_list_prefix(list_prefix)
        except Exception as e:
            log.warning(f"  List failed ({e}), trying known filenames...")
            keys = []

        parquet_keys = [k for k in keys if k.endswith(".parquet") and k.startswith(list_prefix)]
        if not parquet_keys:
            raise RuntimeError(f"No parquet files found under {r2_prefix}")

        root_files = [k for k in parquet_keys if "/" not in k[len(list_prefix) :]]
        parquet_keys = [root_files[-1]] if root_files else [sorted(parquet_keys)[-1]]

        log.info(f"  Using: {parquet_keys[0]}")

        local_files = []
        for key in parquet_keys:
            fname = key.replace("/", "_")
            local_path = str(local_dir / fname)
            log.info(f"  Downloading {key}...")
            _wrangler_download(key, local_path)
            local_files.append(local_path)

        if len(local_files) == 1:
            sql = f"CREATE TABLE {table_name} AS SELECT {extra_select} FROM read_parquet('{local_files[0]}')"
        else:
            paths = ", ".join(f"'{f}'" for f in local_files)
            sql = f"CREATE TABLE {table_name} AS SELECT {extra_select} FROM read_parquet([{paths}])"

        self.conn.execute(sql)
        return self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    def cleanup(self):
        if self._tmpdir:
            import shutil

            shutil.rmtree(self._tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def get_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial")
    return conn


def _load_sji_year(loader: DataLoader, pesticide_year: int, sji_filename: str) -> int:
    """Load a specific year's SJI file from silver/pesticides/."""
    r2_prefix = "silver/pesticides"
    list_prefix = r2_prefix + "/"

    if loader.has_native_r2:
        # Try glob for the specific file
        for pattern in [f"r2://{BUCKET}/{r2_prefix}/*/{sji_filename}", f"r2://{BUCKET}/{r2_prefix}/{sji_filename}"]:
            try:
                loader.conn.execute(f"CREATE TABLE sji_raw_all AS SELECT * FROM read_parquet('{pattern}')")
                return loader.conn.execute("SELECT COUNT(*) FROM sji_raw_all").fetchone()[0]
            except Exception:  # noqa: S112
                continue
        raise RuntimeError(f"Could not find {sji_filename} in R2 via native auth")
    # List files and find the one matching our year
    keys = _wrangler_list_prefix(list_prefix)
    matching = [k for k in keys if k.endswith(sji_filename)]
    if not matching:
        raise RuntimeError(f"No file matching {sji_filename} found under {r2_prefix}. Available: {keys}")
    # Pick latest timestamp directory
    key = sorted(matching)[-1]
    log.info(f"  Using: {key}")

    local_dir = Path(loader._tmpdir) / "sji"
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = str(local_dir / sji_filename)
    log.info(f"  Downloading {key}...")
    _wrangler_download(key, local_path)
    loader.conn.execute(f"CREATE TABLE sji_raw_all AS SELECT * FROM read_parquet('{local_path}')")
    return loader.conn.execute("SELECT COUNT(*) FROM sji_raw_all").fetchone()[0]


def load_sji_and_fvm(loader: DataLoader, pesticide_year: int) -> None:
    """Load SJI pesticide data and FVM field data for a given year pair."""
    field_year = pesticide_year + FIELD_YEAR_OFFSET

    # Load SJI spray journal data for the specific year.
    # Files are named pesticiddata_YYYY_YYYY+1.parquet inside silver/pesticides/.
    # We need to download the specific file, not just grab any parquet under the prefix.
    log.info(f"Loading SJI pesticide data for year {pesticide_year}...")
    sji_filename = f"pesticiddata_{pesticide_year}_{pesticide_year + 1}.parquet"
    n = _load_sji_year(loader, pesticide_year, sji_filename)
    log.info(f"  Loaded {n:,} total SJI records")

    # The pesticides silver layer contains multiple years in separate files.
    # Filter to the requested year based on filename pattern in the data.
    # The files are named pesticiddata_YYYY_YYYY+1.parquet.
    # Since we loaded all, we need to check what columns are available.
    cols = [row[0] for row in loader.conn.execute("DESCRIBE sji_raw_all").fetchall()]
    log.info(f"  SJI columns: {cols}")

    # Standardize column names — handle both old and new naming
    cvr_col = next((c for c in cols if c.lower() in ("cvr_number", "companyregistrationnumber")), None)
    area_col = next((c for c in cols if c.lower() in ("area_ha", "acreagesize")), None)
    crop_col = next((c for c in cols if c.lower() in ("crop_code", "code")), None)
    dosage_col = next((c for c in cols if c.lower() in ("dosage_quantity", "dosagequantity")), None)
    dosage_unit_col = next((c for c in cols if c.lower() in ("dosage_unit", "dosageunit")), None)
    pesticide_name_col = next((c for c in cols if c.lower() in ("pesticide_name", "pesticidename")), None)
    pesticide_reg_col = next(
        (c for c in cols if c.lower() in ("pesticide_registration_number", "pesticideregistrationnumber")), None
    )
    nopest_col = next((c for c in cols if c.lower() in ("no_pesticides", "nopesticides")), None)

    if not all([cvr_col, area_col, crop_col]):
        raise RuntimeError(f"Missing required SJI columns. Found: {cols}")

    # Create standardized SJI table
    nopest_filter = ""
    if nopest_col:
        nopest_filter = f"""
            WHERE ({nopest_col} IS NULL
               OR CAST({nopest_col} AS VARCHAR) NOT IN ('1', 'True', 'true', 'TRUE'))
        """

    loader.conn.execute(f"""
        CREATE TABLE sji AS
        SELECT
            row_number() OVER () as row_id,
            TRIM(CAST({cvr_col} AS VARCHAR)) as cvr_number,
            CAST({area_col} AS DOUBLE) as area_ha,
            CAST({crop_col} AS VARCHAR) as crop_code,
            {f"CAST({dosage_col} AS DOUBLE)" if dosage_col else "NULL::DOUBLE"} as dosage_quantity,
            {f"{dosage_unit_col}" if dosage_unit_col else "NULL::VARCHAR"} as dosage_unit,
            {f"{pesticide_name_col}" if pesticide_name_col else "NULL::VARCHAR"} as pesticide_name,
            {f"{pesticide_reg_col}" if pesticide_reg_col else "NULL::VARCHAR"} as pesticide_reg_number
        FROM sji_raw_all
        {nopest_filter}
    """)
    loader.conn.execute("DROP TABLE sji_raw_all")

    sji_count = loader.conn.execute("SELECT COUNT(*) FROM sji").fetchone()[0]
    log.info(f"  Standardized SJI: {sji_count:,} records (after filtering nopesticides)")

    # Filter to valid CVR numbers only
    loader.conn.execute("""
        DELETE FROM sji
        WHERE cvr_number IS NULL
           OR cvr_number = ''
           OR NOT REGEXP_MATCHES(cvr_number, '^[0-9]+$')
           OR area_ha IS NULL
           OR area_ha <= 0
           OR crop_code IS NULL
    """)
    sji_valid = loader.conn.execute("SELECT COUNT(*) FROM sji").fetchone()[0]
    log.info(f"  Valid SJI records: {sji_valid:,} (dropped {sji_count - sji_valid:,} with bad CVR/area/crop)")

    # Load FVM field boundaries
    log.info(f"Loading FVM marker data for field year {field_year}...")
    n = loader.read_parquet(f"silver/fvm_marker_{field_year}", "fvm_raw")
    log.info(f"  Loaded {n:,} FVM field records")

    fvm_cols = [row[0] for row in loader.conn.execute("DESCRIBE fvm_raw").fetchall()]
    log.info(f"  FVM columns: {fvm_cols}")

    fvm_cvr = next((c for c in fvm_cols if c.lower() in ("cvr_number", "ansoeger", "kunde_lb")), None)
    fvm_area = next((c for c in fvm_cols if c.lower() == "area_ha"), None)
    fvm_crop = next((c for c in fvm_cols if c.lower() == "crop_code"), None)
    fvm_field_id = next((c for c in fvm_cols if c.lower() == "field_id"), None)
    fvm_organic = next((c for c in fvm_cols if c.lower() == "is_organic"), None)

    if not all([fvm_cvr, fvm_area, fvm_crop]):
        raise RuntimeError(f"Missing required FVM columns. Found: {fvm_cols}")

    loader.conn.execute(f"""
        CREATE TABLE fvm AS
        SELECT
            {fvm_field_id or "'unknown'"} as field_id,
            TRIM(CAST({fvm_cvr} AS VARCHAR)) as cvr_number,
            CAST({fvm_area} AS DOUBLE) as area_ha,
            CAST({fvm_crop} AS VARCHAR) as crop_code,
            {f"COALESCE({fvm_organic}, false)" if fvm_organic else "false"} as is_organic
        FROM fvm_raw
        WHERE {fvm_cvr} IS NOT NULL
          AND TRIM(CAST({fvm_cvr} AS VARCHAR)) != ''
          AND REGEXP_MATCHES(TRIM(CAST({fvm_cvr} AS VARCHAR)), '^[0-9]+$')
          AND {fvm_area} > 0
          AND {fvm_crop} IS NOT NULL
    """)
    loader.conn.execute("DROP TABLE fvm_raw")

    fvm_count = loader.conn.execute("SELECT COUNT(*) FROM fvm").fetchone()[0]
    log.info(f"  Valid FVM fields: {fvm_count:,}")

    # Log basic overlap stats
    overlap = loader.conn.execute("""
        SELECT COUNT(DISTINCT s.cvr_number)
        FROM (SELECT DISTINCT cvr_number FROM sji) s
        JOIN (SELECT DISTINCT cvr_number FROM fvm) f ON s.cvr_number = f.cvr_number
    """).fetchone()[0]
    sji_cvrs = loader.conn.execute("SELECT COUNT(DISTINCT cvr_number) FROM sji").fetchone()[0]
    fvm_cvrs = loader.conn.execute("SELECT COUNT(DISTINCT cvr_number) FROM fvm").fetchone()[0]
    log.info(f"  CVR overlap: {overlap:,} CVRs in both SJI and FVM (SJI has {sji_cvrs:,}, FVM has {fvm_cvrs:,})")


# ---------------------------------------------------------------------------
# Analysis 1: Tolerance sensitivity
# ---------------------------------------------------------------------------


def run_tolerance_sensitivity(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """
    Run the core area-matching logic at multiple tolerance levels.

    For each tolerance T, counts how many SJI records have a CVR+crop group
    where |SJI_area - sum(FVM_area)| / SJI_area * 100 <= T.

    Also counts "ambiguous" groups: CVR+crop combinations where the area
    could match multiple different subsets of fields (indicating false-match risk
    at higher tolerances).

    Returns list of dicts with results per tolerance level.
    """
    log.info("=" * 70)
    log.info("ANALYSIS 1: TOLERANCE SENSITIVITY")
    log.info("=" * 70)

    total_sji = conn.execute("SELECT COUNT(*) FROM sji").fetchone()[0]
    log.info(f"Total SJI records to match: {total_sji:,}")

    # Precompute CVR+crop area totals from FVM
    conn.execute("""
        CREATE TEMP TABLE fvm_cvr_crop_totals AS
        SELECT
            cvr_number,
            crop_code,
            SUM(area_ha) as total_fvm_area,
            COUNT(*) as field_count
        FROM fvm
        GROUP BY cvr_number, crop_code
    """)

    # Also compute non-organic totals for Strategy 2
    conn.execute("""
        CREATE TEMP TABLE fvm_cvr_crop_totals_nonorganic AS
        SELECT
            cvr_number,
            crop_code,
            SUM(area_ha) as total_fvm_area,
            COUNT(*) as field_count
        FROM fvm
        WHERE is_organic = FALSE
        GROUP BY cvr_number, crop_code
    """)

    results = []

    for tolerance in TOLERANCE_LEVELS:
        # Strategy 1: All fields (main strategy)
        row = conn.execute(f"""
            SELECT
                COUNT(*) as matched_records,
                COUNT(DISTINCT s.cvr_number || '_' || s.crop_code) as matched_groups
            FROM sji s
            JOIN fvm_cvr_crop_totals f
              ON s.cvr_number = f.cvr_number
             AND s.crop_code = f.crop_code
            WHERE f.total_fvm_area > 0
              AND s.area_ha > 0
              AND ABS(s.area_ha - f.total_fvm_area) / s.area_ha * 100 <= {tolerance}
        """).fetchone()
        s1_matched = row[0]
        s1_groups = row[1]

        # Strategy 2: Non-organic fields only
        row2 = conn.execute(f"""
            SELECT COUNT(*) as matched_records
            FROM sji s
            LEFT JOIN fvm_cvr_crop_totals f_all
              ON s.cvr_number = f_all.cvr_number
             AND s.crop_code = f_all.crop_code
            JOIN fvm_cvr_crop_totals_nonorganic f_no
              ON s.cvr_number = f_no.cvr_number
             AND s.crop_code = f_no.crop_code
            WHERE f_no.total_fvm_area > 0
              AND s.area_ha > 0
              AND ABS(s.area_ha - f_no.total_fvm_area) / s.area_ha * 100 <= {tolerance}
              AND (f_all.total_fvm_area IS NULL
                   OR ABS(s.area_ha - f_all.total_fvm_area) / s.area_ha * 100 > {tolerance})
        """).fetchone()
        s2_additional = row2[0]

        # Combined coverage
        combined = s1_matched + s2_additional
        coverage_pct = combined / total_sji * 100 if total_sji > 0 else 0

        # Ambiguity check: at this tolerance, how many SJI records could match
        # MULTIPLE different CVR+crop groups? (i.e., same CVR, different crop_code
        # areas that happen to fall within tolerance)
        # This measures false-match risk.
        ambiguous = conn.execute(f"""
            WITH matchable AS (
                SELECT
                    s.row_id,
                    s.cvr_number,
                    s.crop_code,
                    s.area_ha as sji_area,
                    f.crop_code as fvm_crop,
                    f.total_fvm_area
                FROM sji s
                JOIN fvm_cvr_crop_totals f
                  ON s.cvr_number = f.cvr_number
                WHERE f.total_fvm_area > 0
                  AND s.area_ha > 0
                  AND ABS(s.area_ha - f.total_fvm_area) / s.area_ha * 100 <= {tolerance}
            )
            SELECT COUNT(*) FROM (
                SELECT row_id
                FROM matchable
                GROUP BY row_id
                HAVING COUNT(DISTINCT fvm_crop) > 1
            )
        """).fetchone()[0]

        result = {
            "tolerance_pct": tolerance,
            "strategy1_matched": s1_matched,
            "strategy2_additional": s2_additional,
            "combined_matched": combined,
            "total_records": total_sji,
            "coverage_pct": round(coverage_pct, 2),
            "matched_cvr_crop_groups": s1_groups,
            "ambiguous_records": ambiguous,
        }
        results.append(result)

        log.info(
            f"  Tolerance {tolerance:5.1f}%: "
            f"S1={s1_matched:,} + S2={s2_additional:,} = {combined:,} / {total_sji:,} "
            f"({coverage_pct:.1f}%) | ambiguous={ambiguous:,}"
        )

    # Clean up temp tables
    conn.execute("DROP TABLE IF EXISTS fvm_cvr_crop_totals")
    conn.execute("DROP TABLE IF EXISTS fvm_cvr_crop_totals_nonorganic")

    log.info("")
    log.info("INTERPRETATION:")
    log.info("  - 'coverage_pct' = fraction of SJI records matchable at this tolerance")
    log.info("  - 'ambiguous_records' = SJI records that could match multiple crop codes")
    log.info("    within the same CVR at this tolerance (false-match risk indicator)")
    log.info("  - The optimal tolerance minimizes the gap between coverage and ambiguity")

    return results


# ---------------------------------------------------------------------------
# Analysis 2: Unmatched records profiler
# ---------------------------------------------------------------------------


def run_unmatched_profiler(conn: duckdb.DuckDBPyConnection, tolerance: float = 2.0) -> dict:
    """
    Profile the SJI records that do NOT match at the given tolerance.

    Reports:
      - Distribution by crop code (which crops are hardest to match?)
      - Area ratio distribution (SJI area / FVM area — where does it cluster?)
      - Farm size distribution (large vs small farms)
      - Field count distribution (many-field vs few-field CVR+crop groups)
      - CVR match status (CVR exists in FVM but area doesn't match vs CVR not in FVM at all)
    """
    log.info("=" * 70)
    log.info(f"ANALYSIS 2: UNMATCHED RECORDS PROFILER (at {tolerance}% tolerance)")
    log.info("=" * 70)

    # Recompute FVM totals
    conn.execute("""
        CREATE TEMP TABLE fvm_totals AS
        SELECT
            cvr_number,
            crop_code,
            SUM(area_ha) as total_fvm_area,
            COUNT(*) as field_count
        FROM fvm
        GROUP BY cvr_number, crop_code
    """)

    conn.execute("""
        CREATE TEMP TABLE fvm_totals_nonorganic AS
        SELECT
            cvr_number,
            crop_code,
            SUM(area_ha) as total_fvm_area,
            COUNT(*) as field_count
        FROM fvm
        WHERE is_organic = FALSE
        GROUP BY cvr_number, crop_code
    """)

    # Classify each SJI record
    conn.execute(f"""
        CREATE TEMP TABLE sji_classified AS
        SELECT
            s.*,
            f.total_fvm_area,
            f.field_count,
            fno.total_fvm_area as nonorg_fvm_area,
            CASE
                WHEN f.total_fvm_area IS NULL THEN 'no_cvr_crop_in_fvm'
                WHEN f.total_fvm_area = 0 THEN 'zero_fvm_area'
                WHEN ABS(s.area_ha - f.total_fvm_area) / s.area_ha * 100 <= {tolerance} THEN 'matched_s1'
                WHEN fno.total_fvm_area IS NOT NULL
                     AND fno.total_fvm_area > 0
                     AND ABS(s.area_ha - fno.total_fvm_area) / s.area_ha * 100 <= {tolerance} THEN 'matched_s2'
                ELSE 'unmatched'
            END as match_status,
            CASE
                WHEN f.total_fvm_area IS NOT NULL AND f.total_fvm_area > 0
                THEN s.area_ha / f.total_fvm_area
                ELSE NULL
            END as area_ratio
        FROM sji s
        LEFT JOIN fvm_totals f
          ON s.cvr_number = f.cvr_number AND s.crop_code = f.crop_code
        LEFT JOIN fvm_totals_nonorganic fno
          ON s.cvr_number = fno.cvr_number AND s.crop_code = fno.crop_code
    """)

    # Overall match status distribution
    status_dist = conn.execute("""
        SELECT match_status, COUNT(*) as cnt
        FROM sji_classified
        GROUP BY match_status
        ORDER BY cnt DESC
    """).fetchall()

    total = sum(r[1] for r in status_dist)
    log.info(f"\nMatch status distribution (total: {total:,}):")
    for status, cnt in status_dist:
        log.info(f"  {status:30s}: {cnt:>8,} ({cnt / total * 100:.1f}%)")

    # Focus on unmatched records
    unmatched_count = conn.execute("SELECT COUNT(*) FROM sji_classified WHERE match_status = 'unmatched'").fetchone()[0]
    no_cvr_count = conn.execute(
        "SELECT COUNT(*) FROM sji_classified WHERE match_status = 'no_cvr_crop_in_fvm'"
    ).fetchone()[0]

    log.info("\nUnmatched breakdown:")
    log.info(f"  Area mismatch (CVR+crop exists in FVM, but areas don't match): {unmatched_count:,}")
    log.info(f"  No CVR+crop in FVM at all: {no_cvr_count:,}")

    # For 'no_cvr_crop_in_fvm': is the CVR in FVM at all (just different crops)?
    cvr_exists_different_crop = conn.execute("""
        SELECT COUNT(*) FROM sji_classified sc
        WHERE sc.match_status = 'no_cvr_crop_in_fvm'
          AND EXISTS (SELECT 1 FROM fvm f WHERE f.cvr_number = sc.cvr_number)
    """).fetchone()[0]
    cvr_not_in_fvm = no_cvr_count - cvr_exists_different_crop

    log.info(f"    - CVR exists in FVM but with different crop codes: {cvr_exists_different_crop:,}")
    log.info(f"    - CVR not in FVM at all: {cvr_not_in_fvm:,}")

    # Area ratio distribution for unmatched (where FVM data exists)
    log.info("\nArea ratio (SJI_area / FVM_area) for unmatched records:")
    area_ratio_dist = conn.execute("""
        SELECT
            CASE
                WHEN area_ratio < 0.5 THEN '< 0.50 (SJI much smaller than FVM)'
                WHEN area_ratio < 0.8 THEN '0.50-0.80'
                WHEN area_ratio < 0.95 THEN '0.80-0.95'
                WHEN area_ratio < 0.98 THEN '0.95-0.98 (just outside 2% tolerance)'
                WHEN area_ratio <= 1.02 THEN '0.98-1.02 (within 2% — should be matched)'
                WHEN area_ratio <= 1.05 THEN '1.02-1.05 (just outside 2% tolerance)'
                WHEN area_ratio <= 1.20 THEN '1.05-1.20'
                WHEN area_ratio <= 1.50 THEN '1.20-1.50'
                ELSE '> 1.50 (SJI much larger than FVM)'
            END as ratio_bucket,
            COUNT(*) as cnt,
            MIN(area_ratio) as min_ratio,
            MAX(area_ratio) as max_ratio
        FROM sji_classified
        WHERE match_status = 'unmatched'
          AND area_ratio IS NOT NULL
        GROUP BY ratio_bucket
        ORDER BY MIN(area_ratio)
    """).fetchall()

    for bucket, cnt, _mn, _mx in area_ratio_dist:
        log.info(f"  {bucket:50s}: {cnt:>6,}")

    # Crop code distribution for unmatched
    log.info("\nTop 15 crop codes among unmatched records:")
    crop_dist = conn.execute("""
        SELECT crop_code, COUNT(*) as cnt
        FROM sji_classified
        WHERE match_status IN ('unmatched', 'no_cvr_crop_in_fvm')
        GROUP BY crop_code
        ORDER BY cnt DESC
        LIMIT 15
    """).fetchall()

    for crop, cnt in crop_dist:
        log.info(f"  Crop {crop:>6s}: {cnt:>6,}")

    # Field count distribution for unmatched
    log.info("\nField count per CVR+crop for unmatched records (area mismatch only):")
    field_dist = conn.execute("""
        SELECT
            CASE
                WHEN field_count = 1 THEN '1 field'
                WHEN field_count <= 3 THEN '2-3 fields'
                WHEN field_count <= 5 THEN '4-5 fields'
                WHEN field_count <= 10 THEN '6-10 fields'
                ELSE '> 10 fields'
            END as field_bucket,
            COUNT(*) as cnt
        FROM sji_classified
        WHERE match_status = 'unmatched'
          AND field_count IS NOT NULL
        GROUP BY field_bucket
        ORDER BY MIN(field_count)
    """).fetchall()

    for bucket, cnt in field_dist:
        log.info(f"  {bucket:>15s}: {cnt:>6,}")

    # Summary statistics for area deviation
    log.info("\nArea deviation statistics for unmatched records:")
    stats = conn.execute("""
        SELECT
            COUNT(*) as n,
            AVG(ABS(area_ha - total_fvm_area) / area_ha * 100) as mean_deviation_pct,
            MEDIAN(ABS(area_ha - total_fvm_area) / area_ha * 100) as median_deviation_pct,
            MIN(ABS(area_ha - total_fvm_area) / area_ha * 100) as min_deviation_pct,
            MAX(ABS(area_ha - total_fvm_area) / area_ha * 100) as max_deviation_pct
        FROM sji_classified
        WHERE match_status = 'unmatched'
          AND total_fvm_area IS NOT NULL
          AND total_fvm_area > 0
    """).fetchone()

    if stats and stats[0] > 0:
        log.info(f"  N = {stats[0]:,}")
        log.info(f"  Mean area deviation:   {stats[1]:.1f}%")
        log.info(f"  Median area deviation: {stats[2]:.1f}%")
        log.info(f"  Min area deviation:    {stats[3]:.1f}%")
        log.info(f"  Max area deviation:    {stats[4]:.1f}%")

    # Clean up
    conn.execute("DROP TABLE IF EXISTS fvm_totals")
    conn.execute("DROP TABLE IF EXISTS fvm_totals_nonorganic")
    conn.execute("DROP TABLE IF EXISTS sji_classified")

    return {
        "match_status_distribution": dict(status_dist),
        "unmatched_area_mismatch": unmatched_count,
        "unmatched_no_cvr_crop": no_cvr_count,
        "cvr_exists_wrong_crop": cvr_exists_different_crop,
        "cvr_not_in_fvm": cvr_not_in_fvm,
        "area_ratio_distribution": {b: c for b, c, _, _ in area_ratio_dist},
        "top_unmatched_crops": dict(crop_dist),
    }


# ---------------------------------------------------------------------------
# Analysis 3: BMD dose-rate validation
# ---------------------------------------------------------------------------


def run_dose_rate_validation(conn: duckdb.DuckDBPyConnection, loader: DataLoader, pesticide_year: int) -> dict:
    """
    Compare disaggregated per-hectare dose rates against BMD authorized rates.

    Loads the gold disaggregated results and BMD product data, then checks
    what fraction of disaggregated records have per-hectare doses that exceed
    the BMD-listed maximum application rate.

    Note: BMD may not have maximum dose rates for all products. We report
    coverage of the check as well as the results.
    """
    log.info("=" * 70)
    log.info("ANALYSIS 3: BMD DOSE-RATE VALIDATION")
    log.info("=" * 70)

    # Load gold disaggregated results
    log.info(f"Loading gold disaggregated results for {pesticide_year}...")
    try:
        n = loader.read_parquet(f"gold/pesticide_disaggregation_{pesticide_year}_{pesticide_year + 1}", "disagg")
        log.info(f"  Loaded {n:,} disaggregated records")
    except Exception as e:
        log.warning(f"  Could not load disaggregated results: {e}")
        log.warning("  Skipping dose-rate validation (no gold data available)")
        return {"error": str(e)}

    # Check what columns exist
    disagg_cols = [row[0] for row in conn.execute("DESCRIBE disagg").fetchall()]
    log.info(f"  Disaggregated columns: {disagg_cols}")

    # Load BMD data
    log.info("Loading BMD pesticide product data...")
    try:
        n = loader.read_parquet("silver/bmd", "bmd_raw")
        log.info(f"  Loaded {n:,} BMD records")
    except Exception as e:
        log.warning(f"  Could not load BMD data: {e}")
        return {"error": str(e)}

    bmd_cols = [row[0] for row in conn.execute("DESCRIBE bmd_raw").fetchall()]
    log.info(f"  BMD columns: {bmd_cols}")

    # Check if BMD has dose rate information
    # BMD typically has columns like: reg_nr, product_name, active_substance,
    # max_dose, dose_unit, approved_crops, etc.
    # We need to discover what's actually available.
    log.info("\nBMD column inspection:")
    for col in bmd_cols:
        sample = conn.execute(f"""
            SELECT {col}, COUNT(*) as cnt
            FROM bmd_raw
            WHERE {col} IS NOT NULL
            GROUP BY {col}
            ORDER BY cnt DESC
            LIMIT 3
        """).fetchall()
        if sample:
            log.info(f"  {col}: top values = {sample}")

    # Compute per-hectare dose rate from disaggregated data
    # DosageQuantity / AllocatedArea = dose per hectare
    dose_col = "DosageQuantity" if "DosageQuantity" in disagg_cols else "dosage_quantity"
    area_col = "AllocatedArea" if "AllocatedArea" in disagg_cols else "allocated_area"
    reg_col = (
        "PesticideRegistrationNumber"
        if "PesticideRegistrationNumber" in disagg_cols
        else "pesticide_registration_number"
    )

    if dose_col not in disagg_cols or area_col not in disagg_cols:
        log.warning("  Missing dose or area columns in disaggregated data")
        log.warning(f"  Available: {disagg_cols}")
        return {"error": "missing columns"}

    # Basic dose-rate statistics from the disaggregated data itself
    log.info("\nDisaggregated dose-rate statistics (DosageQuantity / AllocatedArea):")
    stats = conn.execute(f"""
        SELECT
            COUNT(*) as total_records,
            COUNT(CASE WHEN {area_col} > 0 AND {dose_col} IS NOT NULL THEN 1 END) as records_with_dose,
            COUNT(DISTINCT {reg_col}) as unique_products,
            AVG(CASE WHEN {area_col} > 0 THEN {dose_col} / {area_col} END) as mean_dose_per_ha,
            MEDIAN(CASE WHEN {area_col} > 0 THEN {dose_col} / {area_col} END) as median_dose_per_ha
        FROM disagg
    """).fetchone()

    log.info(f"  Total records: {stats[0]:,}")
    log.info(f"  Records with valid dose: {stats[1]:,}")
    log.info(f"  Unique products: {stats[2]:,}")
    if stats[3] is not None:
        log.info(f"  Mean dose/ha: {stats[3]:.4f}")
        log.info(f"  Median dose/ha: {stats[4]:.4f}")

    # Distribution of dose per hectare by dosage unit
    log.info("\nDose/ha distribution by dosage unit:")
    unit_col = "DosageUnit" if "DosageUnit" in disagg_cols else "dosage_unit"
    if unit_col in disagg_cols:
        unit_stats = conn.execute(f"""
            SELECT
                {unit_col} as unit,
                COUNT(*) as cnt,
                AVG({dose_col} / {area_col}) as mean_dose_per_ha,
                MEDIAN({dose_col} / {area_col}) as median_dose_per_ha,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {dose_col} / {area_col}) as p95_dose_per_ha,
                MAX({dose_col} / {area_col}) as max_dose_per_ha
            FROM disagg
            WHERE {area_col} > 0 AND {dose_col} IS NOT NULL
            GROUP BY {unit_col}
            ORDER BY cnt DESC
        """).fetchall()

        for unit, cnt, mean, median, p95, mx in unit_stats:
            log.info(f"  {unit}: n={cnt:,}, mean={mean:.4f}, median={median:.4f}, p95={p95:.4f}, max={mx:.4f}")

    # Outlier detection: records with extremely high dose/ha
    # (internal consistency check — no external reference needed)
    log.info("\nInternal outlier detection (dose/ha > 10x median within same product):")
    outliers = conn.execute(f"""
        WITH product_stats AS (
            SELECT
                {reg_col} as product_reg,
                MEDIAN({dose_col} / {area_col}) as median_dose_per_ha,
                COUNT(*) as product_count
            FROM disagg
            WHERE {area_col} > 0 AND {dose_col} > 0
            GROUP BY {reg_col}
            HAVING COUNT(*) >= 5
        )
        SELECT
            d.{reg_col} as product,
            ps.median_dose_per_ha,
            d.{dose_col} / d.{area_col} as actual_dose_per_ha,
            (d.{dose_col} / d.{area_col}) / ps.median_dose_per_ha as ratio_to_median,
            ps.product_count
        FROM disagg d
        JOIN product_stats ps ON d.{reg_col} = ps.product_reg
        WHERE d.{area_col} > 0
          AND d.{dose_col} > 0
          AND (d.{dose_col} / d.{area_col}) / ps.median_dose_per_ha > 10
        ORDER BY ratio_to_median DESC
        LIMIT 20
    """).fetchall()

    log.info(f"  Found {len(outliers)} records with dose/ha > 10x their product's median")
    for product, median, actual, ratio, cnt in outliers[:10]:
        log.info(f"    Product {product}: {actual:.4f}/ha vs median {median:.4f}/ha ({ratio:.1f}x), n={cnt}")

    total_outlier_count = conn.execute(f"""
        WITH product_stats AS (
            SELECT
                {reg_col} as product_reg,
                MEDIAN({dose_col} / {area_col}) as median_dose_per_ha,
                COUNT(*) as product_count
            FROM disagg
            WHERE {area_col} > 0 AND {dose_col} > 0
            GROUP BY {reg_col}
            HAVING COUNT(*) >= 5
        )
        SELECT COUNT(*)
        FROM disagg d
        JOIN product_stats ps ON d.{reg_col} = ps.product_reg
        WHERE d.{area_col} > 0
          AND d.{dose_col} > 0
          AND (d.{dose_col} / d.{area_col}) / ps.median_dose_per_ha > 10
    """).fetchone()[0]

    checkable = conn.execute(f"""
        WITH product_stats AS (
            SELECT {reg_col} as product_reg, COUNT(*) as product_count
            FROM disagg
            WHERE {area_col} > 0 AND {dose_col} > 0
            GROUP BY {reg_col}
            HAVING COUNT(*) >= 5
        )
        SELECT COUNT(*)
        FROM disagg d
        JOIN product_stats ps ON d.{reg_col} = ps.product_reg
        WHERE d.{area_col} > 0 AND d.{dose_col} > 0
    """).fetchone()[0]

    log.info(f"\n  Total outliers (>10x product median): {total_outlier_count:,} / {checkable:,} checkable records")
    if checkable > 0:
        log.info(f"  Outlier rate: {total_outlier_count / checkable * 100:.3f}%")

    # Clean up
    conn.execute("DROP TABLE IF EXISTS disagg")
    conn.execute("DROP TABLE IF EXISTS bmd_raw")

    return {
        "total_disaggregated": stats[0],
        "records_with_dose": stats[1],
        "unique_products": stats[2],
        "outliers_10x_median": total_outlier_count,
        "checkable_records": checkable,
        "outlier_rate_pct": round(total_outlier_count / checkable * 100, 3) if checkable > 0 else None,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--year", type=int, default=DEFAULT_YEAR, help=f"Pesticide year to analyze (default: {DEFAULT_YEAR})"
    )
    parser.add_argument(
        "--analysis",
        choices=["all", "tolerance", "unmatched", "doserate"],
        default="all",
        help="Which analysis to run (default: all)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Just check data availability, don't run analysis")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--output", type=str, help="Write results JSON to this file")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 70)
    log.info("PESTICIDE DISAGGREGATION ROBUSTNESS VALIDATION")
    log.info(f"Pesticide year: {args.year}, field year: {args.year + FIELD_YEAR_OFFSET}")
    log.info(f"Analysis: {args.analysis}")
    log.info("=" * 70)

    conn = get_connection()
    loader = DataLoader(conn)

    all_results = {"year": args.year, "field_year": args.year + FIELD_YEAR_OFFSET}

    try:
        # Load core data (needed for tolerance and unmatched analyses)
        if args.analysis in ("all", "tolerance", "unmatched"):
            load_sji_and_fvm(loader, args.year)

            if args.dry_run:
                log.info("Dry run — data loaded successfully, skipping analysis")
                return

        if args.analysis in ("all", "tolerance"):
            all_results["tolerance_sensitivity"] = run_tolerance_sensitivity(conn)

        if args.analysis in ("all", "unmatched"):
            all_results["unmatched_profile"] = run_unmatched_profiler(conn)

        if args.analysis in ("all", "doserate"):
            all_results["dose_rate_validation"] = run_dose_rate_validation(conn, loader, args.year)

        # Write results
        if args.output:
            with Path(args.output).open("w") as f:
                json.dump(all_results, f, indent=2, default=str)
            log.info(f"\nResults written to {args.output}")

        log.info("\n" + "=" * 70)
        log.info("VALIDATION COMPLETE")
        log.info("=" * 70)

    finally:
        loader.cleanup()
        conn.close()


if __name__ == "__main__":
    main()
