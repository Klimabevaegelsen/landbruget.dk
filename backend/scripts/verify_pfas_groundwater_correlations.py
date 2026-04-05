#!/usr/bin/env python3
"""
Verification script: analyze PFAS groundwater correlations with agricultural
pesticide application intensity in Danish GRUKO catchment areas.

Four analysis tiers:
  - Tier 1: TFA (trifluoroacetic acid) vs fluorinated pesticide intensity
  - Tier 2: Traditional PFAS (PFOS/PFOA/PFHxS/PFNA) vs total agricultural intensity
  - Tier 3: Exploratory screen — all 26 PFAS substances vs both intensity measures
  - Tier 4: Negative controls — substances expected to show no agricultural correlation

Data sources (all from R2 / landbruget-data bucket):
  - silver/geus_clean_all (parameter_group='pfas') — PFAS groundwater detections
  - silver/geus_dataverse_pesticides_pfas — fallback PFAS data
  - gold/pesticide_disaggregation_{year}_{year+1}/ — field-level kg/ha
  - silver/grukos/ — GRUKO catchment polygons
  - silver/bmd — BMD product->active ingredient mapping
  - silver/soil_types — soil type polygons

Usage:
    cd backend && source venv/bin/activate
    python scripts/verify_pfas_groundwater_correlations.py [--dry-run] [--verbose]

Auth: Uses wrangler CLI (must be logged in) or R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY/R2_ACCOUNT_ID env vars.
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
import numpy as np
import statsmodels.api as sm
from scipy import stats as scipy_stats
from sklearn.metrics import roc_auc_score
from statsmodels.stats.multitest import multipletests
from statsmodels.stats.outliers_influence import variance_inflation_factor

# Add backend to path for common imports
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("verify_pfas_correlations")

BUCKET = "landbruget-data"

# ---------------------------------------------------------------------------
# PFAS Detection thresholds (per-substance half-LOQ values)
# ---------------------------------------------------------------------------
# Low LOQ group: PFOS, PFOA, PFHxS, PFNA — LOQ = 0.003 µg/L → half-LOQ = 0.0015
# All others: LOQ = 0.15 µg/L → half-LOQ = 0.075
PFAS_DETECTION_THRESHOLDS = {
    "PFOS": 0.0015,
    "PFOA": 0.0015,
    "PFOA_sum": 0.0015,
    "PFHxS": 0.0015,
    "PFHxS_sum": 0.0015,
    "PFNA": 0.0015,
    "PFNA_sum": 0.0015,
}
DEFAULT_PFAS_THRESHOLD = 0.075

# Minimum sample size per substance (lower than pesticide script — PFAS data is sparser)
MIN_DETECTIONS = 20

# Application years — three consecutive years temporally aligned
APPLICATION_YEARS = [2015, 2016, 2017]

# Detection windows for PFAS
# TFA monitoring started in 2020; traditional PFAS monitoring started earlier
TFA_DETECTION_YEAR_START = 2020
TRADITIONAL_PFAS_DETECTION_YEAR_START = 2018

# Soil-dependent transit times (years from surface application to groundwater detection)
SOIL_TRANSIT_TIMES = {
    1: 3,  # Grovsandet jord (coarse sand) — fast transit
    2: 3,  # Finsandet jord (fine sand) — fast transit
    3: 5,  # Lerblandet sandjord (clay-mixed sand) — intermediate
    4: 7,  # Sandblandet lerjord (sand-mixed clay) — slow transit
    5: 7,  # Lerjord (clay) — slow transit
    6: 7,  # Svær lerjord (heavy clay) — slow transit
}
DEFAULT_TRANSIT_YEARS = 5  # fallback (Humusjord, unknown)

# ---------------------------------------------------------------------------
# TFA-Forming Parent Mapping
# ---------------------------------------------------------------------------
# TFA (trifluoroacetic acid) is a degradation product of fluorinated pesticides
# containing -CF3 groups.
# Sources: TriFluPest study (Ohlin et al. 2025), PPDB database, PAN Europe 2023.

TFA_FORMING_PARENTS = {
    # Experimentally confirmed with formation rates (TriFluPest study)
    "fluopyram",
    "fluazinam",
    "fluazifop-p-butyl",
    "fluazifop-butyl",
    "diflufenican",
    "mefentrifluconazol",
    "trifluralin",
    "tau-fluvalinat",
    # CF3-containing active ingredients (structural basis from PPDB)
    "flonicamid",
    "gamma-cyhalothrin",
    "lambda-cyhalothrin",
    "oxathiapiprolin",
    "picolinafen",
    "pyroxsulam",
    "triflusulfuron-methyl",
    "tefluthrin",
    "fipronil",
    "flupyrsulfuron-methyl",
    "flurprimidol",
    "fluvalinate",
    "haloxyfop-ethoxyethyl",
    "indoxacarb",
    "mefluidid",
    "picoxystrobin",
    "flurtamone",
    "norflurazon",
    "benfluorex",
    "cyflufenamid",
    "flubendiamide",
    "fluxapyroxad",
    "isopyrazam",
    "penthiopyrad",
    "sedaxane",
    "thifluzamide",
}

# GEUS PFAS substance names (both short and Danish full names)
# The 26 substances in GEUS PFAS data
PFAS_SHORT_NAMES = {
    "PFOS",
    "PFOA",
    "PFHxS",
    "PFNA",
    "PFBS",
    "PFBA",
    "PFHxA",
    "PFPeA",
    "PFHpA",
    "PFHpS",
    "PFPeS",
    "6:2 FTS",
    "PFOSA",
    "PFDoDA",
    "PFUnDA",
    "PFDS",
    "PFTrDA",
    "PFTrDS",
    "PFTeDA",
    "TFA",
    "SUM PFAS-4",
    "SUM PFAS-12",
    "SUM PFAS-22",
}

# Mapping from Danish GEUS names to short names
# Actual GEUS stof_tekst values from geus_clean_all (parameter_group='pfas'):
#   - Some use "(lineær)" suffix, e.g. "Perfluorheptansyre (lineær)"
#   - Some use "(sum forgrenet og lineær)", e.g. "Perfluoroctansulfonsyre (sum forgrenet og lineær)"
#   - TFA has no parenthetical: "Trifluoreddikesyre"
#   - SUM columns: "SUM PFAS-4", "SUM PFAS-12", "SUM PFAS-22"
PFAS_DANISH_TO_SHORT = {
    # TFA
    "Trifluoreddikesyre": "TFA",
    "Trifluoreddikesyre (TFA)": "TFA",
    # PFOS variants
    "Perfluoroctansulfonsyre (sum forgrenet og lineær)": "PFOS",
    "Perfluoroctansulfonsyre (PFOS)": "PFOS",
    "Perfluoroctansulfonsyre": "PFOS",
    # PFOA variants
    "Perfluoroctansyre": "PFOA",
    "Perfluoroctansyre (sum forgrenet og lineær)": "PFOA_sum",
    "Perfluoroctansyre (PFOA)": "PFOA",
    # PFHxS variants
    "Perfluorhexansulfonsyre": "PFHxS",
    "Perfluorhexansulfonsyre (sum forgrenet og lineær)": "PFHxS_sum",
    "Perfluorhexansulfonsyre (PFHxS)": "PFHxS",
    # PFNA variants
    "Perfluornonansyre": "PFNA",
    "Perfluornonansyre (sum forgrenet og lineær)": "PFNA_sum",
    "Perfluornonansyre (PFNA)": "PFNA",
    # Other PFAS — lineær forms
    "Perfluorbutansulfonsyre (lineær)": "PFBS",
    "Perfluorbutansyre (lineær)": "PFBA",
    "Perfluorhexansyre (lineær)": "PFHxA",
    "Perfluorpentansyre (lineær)": "PFPeA",
    "Perfluorpentansyre (sum forgrenet og lineær)": "PFPeA_sum",
    "Perfluorheptansyre (lineær)": "PFHpA",
    "Perfluorheptansulfonsyre (lineær)": "PFHpS",
    "Perfluorpentansulfonsyre (lineær)": "PFPeS",
    "6:2 Fluortelomersulfonsyre (lineær)": "6:2 FTS",
    "6:2 Fluortelomersulfonsyre (6:2 FTS)": "6:2 FTS",
    "6:2 FTS": "6:2 FTS",
    "Perfluoroctansulfonamid": "PFOSA",
    "Perfluoroctansulfonamid (PFOSA)": "PFOSA",
    # Long-chain PFAS — lineær forms
    "Perfluordodecansyre (lineær)": "PFDoDA",
    "Perfluorundecansyre (lineær)": "PFUnDA",
    "Perfluordecansyre (lineær)": "PFDS_acid",
    "Perfluordecansulfonsyre (lineær)": "PFDS",
    "Perfluortridecansyre (lineær)": "PFTrDA",
    "Perfluortridecansulfonsyre (lineær)": "PFTrDS",
    "Perfluorundecansulfonsyre (lineær)": "PFUnDS",
    "Perfluordodecansulfonsyre (lineær)": "PFDoDS",
    "Perfluornonansulfonsyre (lineær)": "PFNS",
    "Perfluortetradecansyre (PFTeDA)": "PFTeDA",
    # SUM columns
    "SUM PFAS-4": "SUM PFAS-4",
    "SUM PFAS-12": "SUM PFAS-12",
    "SUM PFAS-22": "SUM PFAS-22",
    "Sum af 4 PFAS": "SUM PFAS-4",
    "Sum af 12 PFAS": "SUM PFAS-12",
    "Sum af 22 PFAS": "SUM PFAS-22",
}

# Traditional PFAS (Tier 2) — low-LOQ group from industrial/biosolids sources
TRADITIONAL_PFAS = {"PFOS", "PFOA", "PFHxS", "PFNA"}

# Non-agricultural PFAS (Tier 4 negative controls)
# These are industrial-source PFAS not expected to correlate with agricultural intensity
NON_AGRICULTURAL_PFAS = {"PFDS", "PFUnDA", "PFDoDA"}

# Atmospheric PFAS (Tier 4 negative controls)
# Short-chain PFAS from diffuse atmospheric deposition, not ag-specific
ATMOSPHERIC_PFAS = {"PFBA", "PFPeA"}

# High-Koc fluorinated pesticides (Tier 4 negative controls for TFA)
# These bind strongly to soil and should NOT leach to produce TFA in groundwater
HIGH_KOC_FLUORINATED = {
    "diflufenican": 3400,  # Koc=3400, strong sorption
    "trifluralin": 8000,  # Koc=8000, very strong sorption
    "tau-fluvalinat": 100000,  # Koc extremely high
}

# PFAS substance classification for output
PFAS_SUBSTANCE_TYPE = {
    "TFA": "degradation_product",
    "PFOS": "traditional",
    "PFOA": "traditional",
    "PFHxS": "traditional",
    "PFNA": "traditional",
    "PFBS": "short_chain",
    "PFBA": "short_chain_atmospheric",
    "PFHxA": "short_chain",
    "PFPeA": "short_chain_atmospheric",
    "PFHpA": "medium_chain",
    "PFHpS": "medium_chain",
    "PFPeS": "short_chain",
    "6:2 FTS": "fluorotelomer",
    "PFOSA": "precursor",
    "PFDoDA": "long_chain_industrial",
    "PFUnDA": "long_chain_industrial",
    "PFDS": "long_chain_industrial",
    "PFTrDA": "long_chain_industrial",
    "PFTrDS": "long_chain_industrial",
    "PFTeDA": "long_chain_industrial",
    "SUM PFAS-4": "sum",
    "SUM PFAS-12": "sum",
    "SUM PFAS-22": "sum",
}


# ---------------------------------------------------------------------------
# Statistical helper functions
# ---------------------------------------------------------------------------


def _bootstrap_ci_pointbiserial(
    detected: np.ndarray,
    intensity: np.ndarray,
    n_boot: int = 2000,
    alpha: float = 0.05,
    seed: int = 42,
) -> tuple[float, float]:
    """Bootstrap confidence interval for point-biserial correlation.

    Returns (ci_low, ci_high) at the (1-alpha) level.
    More appropriate than Fisher z-transform for binary data.
    """
    rng = np.random.RandomState(seed)
    n = len(detected)
    boot_rs = np.empty(n_boot)
    for b in range(n_boot):
        idx = rng.randint(0, n, size=n)
        d_b = detected[idx]
        i_b = intensity[idx]
        if d_b.std() == 0 or i_b.std() == 0:
            boot_rs[b] = 0.0
        else:
            boot_rs[b], _ = scipy_stats.pointbiserialr(d_b, i_b)
    lo = np.percentile(boot_rs, 100 * alpha / 2)
    hi = np.percentile(boot_rs, 100 * (1 - alpha / 2))
    return float(lo), float(hi)


def _compute_morans_i(
    values: np.ndarray,
    centroids: np.ndarray,
    k: int = 8,
) -> dict:
    """Compute Moran's I spatial autocorrelation using k-nearest neighbours.

    Parameters
    ----------
    values : 1-D array of the variable of interest
    centroids : (n, 2) array of x, y coordinates
    k : number of nearest neighbours for spatial weights

    Returns dict with I, E_I, z_score, p_value.
    """
    from scipy.spatial import cKDTree

    n = len(values)
    if n < 10:
        return {"I": None, "E_I": None, "z_score": None, "p_value": None, "n": n}

    tree = cKDTree(centroids)
    mean_val = values.mean()
    dev = values - mean_val
    ss = (dev**2).sum()

    if ss == 0:
        return {"I": 0.0, "E_I": -1 / (n - 1), "z_score": 0.0, "p_value": 1.0, "n": n}

    # Build k-NN weights (binary, row-standardised implicitly via sum)
    actual_k = min(k, n - 1)
    _, indices = tree.query(centroids, k=actual_k + 1)  # includes self
    indices = indices[:, 1:]  # drop self

    W_sum = 0.0
    cross_sum = 0.0
    for i in range(n):
        for j_idx in range(actual_k):
            j = indices[i, j_idx]
            cross_sum += dev[i] * dev[j]
            W_sum += 1.0

    I = (n / W_sum) * (cross_sum / ss)  # noqa: E741
    E_I = -1.0 / (n - 1)

    # Variance under normality assumption
    S1 = 2 * W_sum  # each (i,j) counted once, but W is directed → S1 = 2*sum(wij^2)
    # For binary weights wij=1: S1 = 2*W_sum (each pair i→j has wij²=1)
    # S2 = sum_i (sum_j wij + sum_j wji)^2
    row_sums = np.full(n, actual_k, dtype=float)
    col_sums = np.zeros(n)
    for i in range(n):
        for j_idx in range(actual_k):
            col_sums[indices[i, j_idx]] += 1.0
    S2 = float(((row_sums + col_sums) ** 2).sum())

    k2 = float((dev**4).sum() / n) / (ss / n) ** 2  # kurtosis
    A = n * ((n**2 - 3 * n + 3) * S1 - n * S2 + 3 * W_sum**2)
    B = k2 * ((n**2 - n) * S1 - 2 * n * S2 + 6 * W_sum**2)
    C = (n - 1) * (n - 2) * (n - 3) * W_sum**2

    if C == 0:
        return {"I": float(I), "E_I": float(E_I), "z_score": None, "p_value": None, "n": n}

    var_I = (A - B) / C - E_I**2
    if var_I <= 0:
        var_I = 1e-10

    z = (I - E_I) / np.sqrt(var_I)
    p = 2 * (1 - scipy_stats.norm.cdf(abs(z)))

    return {
        "I": round(float(I), 4),
        "E_I": round(float(E_I), 6),
        "z_score": round(float(z), 3),
        "p_value": float(p),
        "n": n,
    }


def _firth_logistic_regression(
    y: np.ndarray,
    X: np.ndarray,
    max_iter: int = 100,
    tol: float = 1e-6,
) -> dict | None:
    """Firth penalized logistic regression (Firth, 1993).

    Reduces small-sample bias in maximum likelihood estimation,
    particularly important when events-per-variable (EPV) < 10.

    Returns dict with coefficients, standard errors, p-values, and confidence intervals.
    """
    _n, p = X.shape

    # Initialize with zeros
    beta = np.zeros(p)

    for iteration in range(max_iter):  # noqa: B007
        pi = 1.0 / (1.0 + np.exp(-X @ beta))
        pi = np.clip(pi, 1e-10, 1 - 1e-10)

        W = np.diag(pi * (1 - pi))
        XtWX = X.T @ W @ X

        try:
            XtWX_inv = np.linalg.inv(XtWX)
        except np.linalg.LinAlgError:
            return None

        # Hat matrix diagonal
        H = X @ XtWX_inv @ X.T @ W
        h = np.diag(H)

        # Firth-adjusted score
        U = X.T @ (y - pi + h * (0.5 - pi))

        # Newton step
        delta = XtWX_inv @ U

        beta += delta

        if np.max(np.abs(delta)) < tol:
            break

    # Final estimates
    pi = 1.0 / (1.0 + np.exp(-X @ beta))
    pi = np.clip(pi, 1e-10, 1 - 1e-10)
    W = np.diag(pi * (1 - pi))
    XtWX = X.T @ W @ X

    try:
        cov = np.linalg.inv(XtWX)
    except np.linalg.LinAlgError:
        return None

    se = np.sqrt(np.diag(cov))
    z_vals = beta / se
    p_vals = 2 * (1 - scipy_stats.norm.cdf(np.abs(z_vals)))

    # Profile-likelihood-based CI (Wald approximation for Firth)
    ci_low = beta - 1.96 * se
    ci_high = beta + 1.96 * se

    # Log-likelihood (penalized)  # noqa: ERA001
    ll = float(np.sum(y * np.log(pi) + (1 - y) * np.log(1 - pi)))
    # Add Firth penalty: 0.5 * log(det(I(beta)))
    try:
        _, logdet = np.linalg.slogdet(XtWX)
        ll_penalized = ll + 0.5 * logdet
    except Exception:
        ll_penalized = ll

    return {
        "coefficients": beta.tolist(),
        "std_errors": se.tolist(),
        "z_values": z_vals.tolist(),
        "p_values": p_vals.tolist(),
        "ci_low": ci_low.tolist(),
        "ci_high": ci_high.tolist(),
        "log_likelihood_penalized": ll_penalized,
        "n_iterations": iteration + 1,
        "converged": iteration < max_iter - 1,
    }


def _compute_nagelkerke_r2(model) -> float | None:
    """Nagelkerke R-squared (1991) from a fitted statsmodels Logit model."""
    try:
        n = int(model.nobs)
        ll_full = model.llf
        ll_null = model.llnull
        cox_snell = 1.0 - np.exp(-2.0 / n * (ll_full - ll_null))
        max_r2 = 1.0 - np.exp(2.0 / n * ll_null)
        if max_r2 == 0:
            return None
        return float(cox_snell / max_r2)
    except Exception:
        return None


def _hosmer_lemeshow(y_true: np.ndarray, y_pred_prob: np.ndarray, g: int = 10) -> tuple[float, float]:
    """Hosmer-Lemeshow goodness-of-fit test.

    Returns (chi2_statistic, p_value). Non-significant p (>0.05) = adequate fit.
    """
    try:
        order = np.argsort(y_pred_prob)
        y_sorted = y_true[order]
        p_sorted = y_pred_prob[order]

        groups = np.array_split(np.arange(len(y_sorted)), g)
        chi2 = 0.0
        for grp in groups:
            if len(grp) == 0:
                continue
            obs = y_sorted[grp].sum()
            exp_val = p_sorted[grp].sum()
            n_grp = len(grp)
            exp_neg = n_grp - exp_val
            if exp_val > 0:
                chi2 += (obs - exp_val) ** 2 / exp_val
            if exp_neg > 0:
                chi2 += ((n_grp - obs) - exp_neg) ** 2 / exp_neg

        df = g - 2
        p_value = 1.0 - scipy_stats.chi2.cdf(chi2, df)
        return float(chi2), float(p_value)
    except Exception:
        return np.nan, np.nan


def _compute_auc(y_true: np.ndarray, y_pred_prob: np.ndarray) -> float | None:
    """AUC via sklearn."""
    try:
        if len(np.unique(y_true)) < 2:
            return None
        return float(roc_auc_score(y_true, y_pred_prob))
    except Exception:
        return None


def _compute_vif(X: np.ndarray, col_idx: int) -> float:
    """VIF for a specific column in design matrix X (with constant)."""
    try:
        return float(variance_inflation_factor(X, col_idx))
    except Exception:
        return np.nan


# ---------------------------------------------------------------------------
# R2 data access — tries native DuckDB R2 auth, falls back to wrangler CLI
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
    """List objects under a prefix using Cloudflare API via wrangler's OAuth token.

    Triggers a wrangler CLI call first to ensure the OAuth token is refreshed.
    """
    # Force token refresh by calling wrangler (it refreshes expired tokens automatically)
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
            self._tmpdir = tempfile.mkdtemp(prefix="verify_pfas_")
            log.info(f"  Temp dir: {self._tmpdir}")

    def read_parquet(self, r2_prefix: str, table_name: str, extra_select: str = "*") -> int:
        """Load parquet file(s) from R2 prefix into a DuckDB table. Returns row count."""
        if self.has_native_r2:
            return self._read_native(r2_prefix, table_name, extra_select)
        return self._read_via_wrangler(r2_prefix, table_name, extra_select)

    def _read_native(self, r2_prefix: str, table_name: str, extra_select: str) -> int:
        """Read parquet directly from R2 using DuckDB native auth."""
        for path in [f"r2://{BUCKET}/{r2_prefix}/*.parquet", f"r2://{BUCKET}/{r2_prefix}/data.parquet"]:
            try:
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT {extra_select} FROM read_parquet('{path}')")
                return self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            except Exception:  # noqa: S112
                continue
        raise RuntimeError(f"Could not read {r2_prefix} from R2")

    def _read_via_wrangler(self, r2_prefix: str, table_name: str, extra_select: str) -> int:
        """Download parquet files via wrangler, then read from local disk."""
        local_dir = Path(self._tmpdir) / r2_prefix.replace("/", "_")
        local_dir.mkdir(parents=True, exist_ok=True)

        # List files under prefix (add trailing slash to avoid matching sibling prefixes)
        list_prefix = r2_prefix.rstrip("/") + "/"
        log.info(f"  Listing {list_prefix}...")
        try:
            keys = _wrangler_list_prefix(list_prefix)
        except Exception as e:
            log.warning(f"  List failed ({e}), trying known filenames...")
            keys = []

        # Only keep files that are strictly under this prefix
        parquet_keys = [k for k in keys if k.endswith(".parquet") and k.startswith(list_prefix)]
        if not parquet_keys:
            raise RuntimeError(f"No parquet files found under {r2_prefix}")

        # Pick the latest version: prefer root-level file, else latest timestamped
        root_files = [k for k in parquet_keys if "/" not in k[len(list_prefix) :]]
        parquet_keys = [root_files[-1]] if root_files else [sorted(parquet_keys)[-1]]

        log.info(f"  Using: {parquet_keys[0]}")

        # Download each parquet file
        local_files = []
        for key in parquet_keys:
            fname = key.replace("/", "_")
            local_path = str(local_dir / fname)
            log.info(f"  Downloading {key}...")
            _wrangler_download(key, local_path)
            local_files.append(local_path)

        # Read into DuckDB
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
# Analysis functions
# ---------------------------------------------------------------------------


def get_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial")
    return conn


def discover_data(loader: DataLoader) -> dict:
    """Quick data discovery — check what's available for PFAS analysis."""
    info = {}
    conn = loader.conn

    # PFAS groundwater data: try geus_clean_all first, then fallback
    log.info("Checking PFAS groundwater data (geus_clean_all)...")
    try:
        n = loader.read_parquet("silver/geus_clean_all", "_pfas_check")
        row = conn.execute("""
            SELECT COUNT(*) as n, MIN(year), MAX(year)
            FROM _pfas_check
            WHERE parameter_group = 'pfas'
        """).fetchone()
        info["pfas_rows"] = row[0]
        log.info(f"  PFAS in geus_clean_all: {row[0]:,} rows, years {row[1]}-{row[2]}")
        cols = [
            c[0]
            for c in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='_pfas_check'"
            ).fetchall()
        ]
        log.info(f"  Columns: {cols}")

        # Check substance names in PFAS data
        subs = conn.execute("""
            SELECT stof_tekst, COUNT(*) as n
            FROM _pfas_check
            WHERE parameter_group = 'pfas'
            GROUP BY stof_tekst
            ORDER BY n DESC
            LIMIT 30
        """).fetchall()
        log.info(f"  PFAS substances found ({len(subs)}):")
        for s, cnt in subs:
            log.info(f"    {s}: {cnt:,}")

        info["pfas_source"] = "geus_clean_all"
        conn.execute("DROP TABLE _pfas_check")
    except Exception as e:
        log.warning(f"  geus_clean_all not found or no PFAS data: {e}")
        info["pfas_rows"] = 0

    # Fallback: try dedicated PFAS table
    if info.get("pfas_rows", 0) == 0:
        log.info("Trying fallback: silver/geus_dataverse_pesticides_pfas...")
        try:
            n = loader.read_parquet("silver/geus_dataverse_pesticides_pfas", "_pfas_fb_check")
            row = conn.execute("SELECT COUNT(*) as n, MIN(year), MAX(year) FROM _pfas_fb_check").fetchone()
            info["pfas_rows"] = row[0]
            log.info(f"  PFAS fallback: {row[0]:,} rows, years {row[1]}-{row[2]}")
            cols = [
                c[0]
                for c in conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name='_pfas_fb_check'"
                ).fetchall()
            ]
            log.info(f"  Columns: {cols}")
            info["pfas_source"] = "geus_dataverse_pesticides_pfas"
            conn.execute("DROP TABLE _pfas_fb_check")
        except Exception as e:
            log.error(f"  PFAS fallback not found: {e}")
            info["pfas_rows"] = 0

    # GRUKOS
    log.info("Checking GRUKOS...")
    try:
        n = loader.read_parquet("silver/grukos", "_grukos_check")
        info["grukos_count"] = n
        log.info(f"  GRUKOS: {n:,} features")
        cols = [
            c[0]
            for c in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='_grukos_check'"
            ).fetchall()
        ]
        log.info(f"  Columns: {cols}")
        conn.execute("DROP TABLE _grukos_check")
    except Exception as e:
        log.error(f"  GRUKOS not found: {e}")
        info["grukos_count"] = 0

    # Disaggregation
    for year in APPLICATION_YEARS:
        key = f"disagg_{year}"
        try:
            n = loader.read_parquet(f"gold/pesticide_disaggregation_{year}_{year + 1}", f"_disagg_check_{year}")
            info[key] = n
            log.info(f"  Disaggregation {year}: {n:,} rows")
            cols = [
                c[0]
                for c in conn.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name='_disagg_check_{year}'"
                ).fetchall()
            ]
            log.info(f"  Columns: {cols}")
            conn.execute(f"DROP TABLE _disagg_check_{year}")
        except Exception as e:
            log.error(f"  Disaggregation {year} not found: {e}")
            info[key] = 0

    # BMD
    log.info("Checking BMD...")
    try:
        n = loader.read_parquet("silver/bmd", "_bmd_check")
        info["bmd_rows"] = n
        log.info(f"  BMD: {n:,} rows")
        conn.execute("DROP TABLE _bmd_check")
    except Exception as e:
        log.error(f"  BMD not found: {e}")
        info["bmd_rows"] = 0

    return info


def load_data(loader: DataLoader, pfas_source: str) -> None:
    """Load all required datasets into DuckDB."""
    conn = loader.conn

    # ── Load PFAS groundwater data ────────────────────────────────────────
    if pfas_source == "geus_clean_all":
        log.info("Loading PFAS groundwater data from geus_clean_all (SAMPLE-LEVEL, parameter_group='pfas')...")
        n = loader.read_parquet("silver/geus_clean_all", "_geus_all")
        log.info(f"  Loaded {n:,} total records from geus_clean_all")

        # Filter to PFAS only, exclude contaminated sites
        conn.execute("""
            CREATE TABLE pfas_raw AS
            SELECT * FROM _geus_all
            WHERE parameter_group = 'pfas'
              AND data_type NOT IN ('DEPOT', 'DEPOT (øvrige)')
        """)
        conn.execute("DROP TABLE _geus_all")
    else:
        log.warning(
            "⚠ Loading PFAS data from geus_dataverse_pesticides_pfas (ANNUAL MEANS fallback — not sample-level!)"
        )
        log.warning("  Results based on annual means may undercount detections. Prefer geus_clean_all.")
        n = loader.read_parquet("silver/geus_dataverse_pesticides_pfas", "_pfas_fb")
        log.info(f"  Loaded {n:,} records from fallback")

        # Adapt column names if needed
        cols = [
            c[0]
            for c in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='_pfas_fb'"
            ).fetchall()
        ]
        log.info(f"  Fallback columns: {cols}")

        # Try to normalize column names
        select_parts = []
        if "substance_name" in cols:
            select_parts.append("substance_name AS stof_tekst")
        elif "stof_tekst" in cols:
            select_parts.append("stof_tekst")

        if "mean_concentration" in cols:
            select_parts.append("mean_concentration AS maengde")
        elif "concentration" in cols:
            select_parts.append("concentration AS maengde")
        elif "maengde" in cols:
            select_parts.append("maengde")

        if "year" in cols:
            select_parts.append("year")

        # Include all other columns as-is
        other_cols = [
            c
            for c in cols
            if c not in ("substance_name", "mean_concentration", "concentration", "maengde", "stof_tekst", "year")
        ]
        for c in other_cols:
            select_parts.append(c)
        if "year" not in cols:
            select_parts.append("EXTRACT(YEAR FROM sample_date) AS year")

        conn.execute(f"""
            CREATE TABLE pfas_raw AS
            SELECT {", ".join(select_parts)}
            FROM _pfas_fb
            WHERE data_type IS NULL OR data_type NOT IN ('DEPOT', 'DEPOT (øvrige)')
        """)
        conn.execute("DROP TABLE _pfas_fb")

    n = conn.execute("SELECT COUNT(*) FROM pfas_raw").fetchone()[0]
    log.info(f"  PFAS records after filtering: {n:,}")

    # Normalize substance names: map Danish long names to short standard names
    # First check what name column we have
    pfas_cols = [
        c[0]
        for c in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='pfas_raw'"
        ).fetchall()
    ]
    log.info(f"  PFAS table columns: {pfas_cols}")

    substance_col = "stof_tekst" if "stof_tekst" in pfas_cols else "substance_name"
    concentration_col = "maengde" if "maengde" in pfas_cols else "concentration"

    # Add normalized substance name column
    conn.execute("ALTER TABLE pfas_raw ADD COLUMN IF NOT EXISTS pfas_name VARCHAR")

    # Build CASE expression for name normalization
    case_parts = []
    for danish_name, short_name in PFAS_DANISH_TO_SHORT.items():
        safe_danish = danish_name.replace("'", "''")
        safe_short = short_name.replace("'", "''")
        case_parts.append(f"WHEN {substance_col} = '{safe_danish}' THEN '{safe_short}'")

    # Also handle exact short name matches
    for short_name in PFAS_SHORT_NAMES:
        safe_short = short_name.replace("'", "''")
        case_parts.append(f"WHEN {substance_col} = '{safe_short}' THEN '{safe_short}'")

    case_expr = " ".join(case_parts)
    conn.execute(f"""
        UPDATE pfas_raw
        SET pfas_name = CASE {case_expr} ELSE {substance_col} END
    """)

    # Report substance breakdown
    rows = conn.execute(f"""
        SELECT pfas_name, COUNT(*) as n,
               SUM(CASE WHEN {concentration_col} > 0 THEN 1 ELSE 0 END) as n_positive
        FROM pfas_raw
        GROUP BY pfas_name ORDER BY n DESC
    """).fetchall()
    log.info(f"  PFAS substances ({len(rows)}):")
    for name, cnt, n_pos in rows:
        log.info(f"    {name:<40} {cnt:>8,} samples, {n_pos:>6,} positive")

    # Report year breakdown
    rows = conn.execute("""
        SELECT year, COUNT(*) as n FROM pfas_raw GROUP BY year ORDER BY year
    """).fetchall()
    log.info("  PFAS year distribution:")
    for yr, cnt in rows:
        log.info(f"    {yr}: {cnt:,}")

    # ── Load GRUKOS ───────────────────────────────────────────────────────
    log.info("Loading GRUKOS catchment polygons...")
    n = loader.read_parquet("silver/grukos", "grukos_raw")
    log.info(f"  Loaded {n:,} GRUKOS features")

    conn.execute("ALTER TABLE grukos_raw ADD COLUMN gruko_id VARCHAR")
    conn.execute("UPDATE grukos_raw SET gruko_id = id")

    rows = conn.execute("""
        SELECT layer, COUNT(*) as n FROM grukos_raw GROUP BY layer ORDER BY layer
    """).fetchall()
    for layer, cnt in rows:
        log.info(f"  Layer {layer}: {cnt:,} features")

    # ── Load BMD ──────────────────────────────────────────────────────────
    log.info("Loading BMD product->active ingredient mapping...")
    n = loader.read_parquet("silver/bmd", "bmd_raw")
    log.info(f"  Loaded {n:,} BMD product records")

    conn.execute("""
        CREATE TABLE _bmd_numbered AS
        SELECT
            CAST(registrerings_nr AS VARCHAR) as reg_nr,
            string_split(aktivstofnavn_e, ';') as ingredients,
            string_split(koncentration_er, ';') as concentrations,
            string_split(enhed_er, ';') as units
        FROM bmd_raw
        WHERE aktivstofnavn_e IS NOT NULL AND aktivstofnavn_e != ''
    """)
    conn.execute("""
        CREATE TABLE bmd_ingredients AS
        SELECT
            reg_nr,
            TRIM(REGEXP_REPLACE(LOWER(TRIM(ingredients[i])), '\\s*\\([^)]*\\)\\s*$', '')) as active_ingredient,
            TRY_CAST(REPLACE(TRIM(concentrations[i]), ',', '.') AS DOUBLE) as concentration_g,
            TRIM(units[i]) as conc_unit
        FROM _bmd_numbered,
             generate_series(1, GREATEST(len(ingredients), 1)) t(i)
        WHERE i <= len(ingredients)
    """)
    conn.execute("DROP TABLE _bmd_numbered")

    n = conn.execute("SELECT COUNT(*) FROM bmd_ingredients").fetchone()[0]
    n_ing = conn.execute("SELECT COUNT(DISTINCT active_ingredient) FROM bmd_ingredients").fetchone()[0]
    log.info(f"  BMD ingredients: {n:,} mappings, {n_ing} unique active ingredients")

    # ── Load disaggregation ───────────────────────────────────────────────
    log.info("Loading pesticide disaggregation data...")
    parts = []
    for year in APPLICATION_YEARS:
        tbl = f"_disagg_{year}"
        loader.read_parquet(f"gold/pesticide_disaggregation_{year}_{year + 1}", tbl)
        parts.append(f"SELECT *, {year} as application_year FROM {tbl}")

    conn.execute(f"CREATE TABLE disagg_raw AS {' UNION ALL '.join(parts)}")
    for year in APPLICATION_YEARS:
        conn.execute(f"DROP TABLE _disagg_{year}")
    n = conn.execute("SELECT COUNT(*) FROM disagg_raw").fetchone()[0]
    log.info(f"  Loaded {n:,} disaggregated pesticide records")

    # Map disagg products to active ingredients via BMD registration number
    conn.execute("""
        CREATE TABLE disagg_with_ingredient AS
        SELECT
            d.*,
            bi.active_ingredient,
            bi.concentration_g,
            CASE
                WHEN bi.concentration_g IS NOT NULL AND bi.concentration_g > 0
                THEN d.DosageQuantity * bi.concentration_g / 1000.0
                ELSE d.DosageQuantity / COUNT(*) OVER (PARTITION BY d.DisaggregatedID)
            END as ingredient_dosage_kg
        FROM disagg_raw d
        JOIN bmd_ingredients bi
            ON CAST(d.PesticideRegistrationNumber AS VARCHAR) = bi.reg_nr
    """)
    n_mapped = conn.execute("SELECT COUNT(*) FROM disagg_with_ingredient").fetchone()[0]
    n_total = conn.execute("SELECT COUNT(*) FROM disagg_raw").fetchone()[0]
    log.info(f"  Mapped to active ingredients: {n_mapped:,} / {n_total:,} ({100 * n_mapped / max(n_total, 1):.1f}%)")

    # ── Load field-GRUKO intersections ────────────────────────────────────
    log.info("Loading field-GRUKO intersections...")
    field_gruko_years = sorted({y + 1 for y in APPLICATION_YEARS})

    fg_parts = []
    missing_years = []
    for field_year in field_gruko_years:
        tbl = f"_fg_{field_year}"
        try:
            loader.read_parquet(
                f"gold/field_analysis_field_grukos_intersections_{field_year}",
                tbl,
            )
            n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log.info(f"  field_grukos_intersections_{field_year}: {n:,} rows (pre-computed)")
            fg_parts.append(f"SELECT field_uuid, grukos_id as gruko_id, field_grukos_geometry as geometry FROM {tbl}")
        except Exception:
            missing_years.append(field_year)

    if missing_years:
        log.info(f"  Computing intersections for years without pre-computed data: {missing_years}")
        fld_parts = []
        for field_year in missing_years:
            tbl = f"_fields_{field_year}"
            try:
                loader.read_parquet(
                    f"silver/fvm_marker_{field_year}",
                    tbl,
                    extra_select="field_uuid, geometry",
                )
                n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                log.info(f"  fvm_marker_{field_year}: {n:,} fields")
                fld_parts.append(f"SELECT DISTINCT field_uuid, geometry FROM {tbl}")
            except Exception as e:
                log.warning(f"  fvm_marker_{field_year} not available: {e}")

        if fld_parts:
            conn.execute(f"CREATE TABLE _all_fields AS {' UNION ALL '.join(fld_parts)}")
            conn.execute("""
                CREATE TABLE _unique_fields AS
                SELECT field_uuid, FIRST(geometry) as geometry
                FROM _all_fields GROUP BY field_uuid
            """)
            conn.execute("DROP TABLE _all_fields")

            log.info("  Computing ST_Intersection (field x GRUKO)...")
            conn.execute("""
                CREATE TABLE _computed_fg AS
                SELECT f.field_uuid, g.gruko_id,
                       ST_Intersection(f.geometry, g.geometry_spatial) as geometry
                FROM _unique_fields f
                JOIN grukos_raw g ON ST_Intersects(f.geometry, g.geometry_spatial)
            """)
            n = conn.execute("SELECT COUNT(*) FROM _computed_fg").fetchone()[0]
            log.info(f"  Computed {n:,} field-GRUKO intersections")
            fg_parts.append("SELECT field_uuid, gruko_id, geometry FROM _computed_fg")
            conn.execute("DROP TABLE _unique_fields")

    conn.execute(f"""
        CREATE TABLE field_gruko_intersections AS
        {" UNION ALL ".join(fg_parts)}
    """)
    for field_year in field_gruko_years:
        conn.execute(f"DROP TABLE IF EXISTS _fg_{field_year}")
    for field_year in missing_years:
        conn.execute(f"DROP TABLE IF EXISTS _fields_{field_year}")
    conn.execute("DROP TABLE IF EXISTS _computed_fg")

    n = conn.execute("SELECT COUNT(*) FROM field_gruko_intersections").fetchone()[0]
    n_fields = conn.execute("SELECT COUNT(DISTINCT field_uuid) FROM field_gruko_intersections").fetchone()[0]
    log.info(f"  Total field-GRUKO intersections: {n:,} ({n_fields:,} unique fields)")

    # ── Load soil types ───────────────────────────────────────────────────
    log.info("Loading soil type polygons...")
    try:
        n = loader.read_parquet("silver/soil_types", "soil_types_raw")
        log.info(f"  Loaded {n:,} soil type polygons")
        rows = conn.execute("""
            SELECT soil_description, soil_height, COUNT(*) as n
            FROM soil_types_raw
            GROUP BY soil_description, soil_height
            ORDER BY soil_height
        """).fetchall()
        for desc, height, cnt in rows:
            transit = SOIL_TRANSIT_TIMES.get(height, DEFAULT_TRANSIT_YEARS)
            log.info(f"    {desc} (height={height}): {cnt:,} polygons -> {transit}yr transit")
    except Exception as e:
        log.warning(f"  Could not load soil types: {e}")
        log.warning("  Will use default transit time for all GRUKOs")


def build_fluorinated_ingredient_list(conn: duckdb.DuckDBPyConnection) -> set[str]:
    """Auto-discover fluorinated ingredients from BMD data and merge with known TFA parents.

    Fluorinated ingredients are identified by matching name patterns:
    - Contains 'flu' (fluopyram, fluazinam, etc.)
    - Contains 'tri' + 'flu' (trifluralin, triflusulfuron)
    - Contains 'cyflu' (cyflufenamid)
    - Contains 'flur' (flurtamone, norflurazon)
    - Contains 'haloxy' (haloxyfop)
    - Contains 'fipronil'
    - Contains 'lambda-cyhalothrin', 'gamma-cyhalothrin'
    """
    log.info("Building fluorinated ingredient list from BMD data...")

    # Auto-discover fluorinated ingredients from BMD
    rows = conn.execute("""
        SELECT DISTINCT active_ingredient
        FROM bmd_ingredients
        WHERE active_ingredient IS NOT NULL
          AND (
              active_ingredient LIKE '%flu%'
              OR active_ingredient LIKE '%fipronil%'
              OR active_ingredient LIKE '%cyhalothrin%'
              OR active_ingredient LIKE '%tefluthrin%'
              OR active_ingredient LIKE '%indoxacarb%'
              OR active_ingredient LIKE '%picoxystrobin%'
          )
        ORDER BY active_ingredient
    """).fetchall()

    bmd_fluorinated = {r[0] for r in rows}
    log.info(f"  Auto-discovered {len(bmd_fluorinated)} fluorinated ingredients from BMD:")
    for ing in sorted(bmd_fluorinated):
        in_tfa = "TFA-parent" if ing in TFA_FORMING_PARENTS else ""
        log.info(f"    {ing:<45} {in_tfa}")

    # Merge with known TFA-forming parents
    all_fluorinated = bmd_fluorinated | TFA_FORMING_PARENTS
    log.info(
        f"  Total fluorinated ingredients: {len(all_fluorinated)} (BMD: {len(bmd_fluorinated)}, known TFA parents: {len(TFA_FORMING_PARENTS)})"
    )

    # Store the fluorinated list in a DuckDB table for SQL queries
    if all_fluorinated:
        values = ", ".join(f"('{ing}')" for ing in all_fluorinated)
        conn.execute(f"""
            CREATE TABLE fluorinated_ingredients AS
            SELECT col0 as ingredient FROM (VALUES {values})
        """)
    else:
        conn.execute("CREATE TABLE fluorinated_ingredients (ingredient VARCHAR)")

    return all_fluorinated


def build_gruko_application_intensity(conn: duckdb.DuckDBPyConnection) -> None:
    """Aggregate pesticide application intensity per substance per GRUKO.

    Creates TWO intensity tables:
    1. gruko_intensity — all substances (for Tier 2: total agricultural intensity)
    2. gruko_intensity_fluorinated — fluorinated-only (for Tier 1: TFA correlation)
    """
    log.info("Building GRUKO-level application intensity (area-weighted)...")

    # Join disagg (with active ingredient) to field-GRUKO intersections
    conn.execute("""
        CREATE TABLE field_gruko_join AS
        SELECT
            d.*,
            fg.gruko_id,
            ST_Area(fg.geometry) / 10000.0 as intersection_area_ha,
            CASE
                WHEN d.AllocatedArea > 0
                THEN d.ingredient_dosage_kg * (ST_Area(fg.geometry) / 10000.0) / d.AllocatedArea
                ELSE 0
            END as kg_in_gruko
        FROM disagg_with_ingredient d
        JOIN field_gruko_intersections fg ON d.field_uuid = fg.field_uuid
    """)

    n = conn.execute("SELECT COUNT(*) FROM field_gruko_join").fetchone()[0]
    n_fields = conn.execute("SELECT COUNT(DISTINCT field_uuid) FROM field_gruko_join").fetchone()[0]
    log.info(f"  Field-GRUKO join: {n:,} rows ({n_fields:,} unique fields)")

    # Sanity check: compare total kg before/after area weighting
    total_kg_raw = conn.execute("SELECT SUM(ingredient_dosage_kg) FROM disagg_with_ingredient").fetchone()[0]
    total_kg_weighted = conn.execute("SELECT SUM(kg_in_gruko) FROM field_gruko_join").fetchone()[0]
    if total_kg_raw and total_kg_raw > 0:
        pct = 100 * (total_kg_weighted or 0) / total_kg_raw
        log.info(f"  Area-weighted kg: {total_kg_weighted:,.0f} / {total_kg_raw:,.0f} = {pct:.1f}%")

    # Aggregate by active ingredient per GRUKO (ALL substances)
    conn.execute("""
        CREATE TABLE gruko_intensity AS
        SELECT
            gruko_id,
            active_ingredient as substance,
            SUM(kg_in_gruko) / NULLIF(SUM(intersection_area_ha), 0) as kg_per_ha,
            SUM(kg_in_gruko) as total_kg,
            SUM(intersection_area_ha) as total_area_ha,
            COUNT(*) as n_applications
        FROM field_gruko_join
        WHERE intersection_area_ha > 0 AND kg_in_gruko > 0
        GROUP BY gruko_id, active_ingredient
    """)

    n_combos = conn.execute("SELECT COUNT(*) FROM gruko_intensity").fetchone()[0]
    n_subs = conn.execute("SELECT COUNT(DISTINCT substance) FROM gruko_intensity").fetchone()[0]
    n_grukos = conn.execute("SELECT COUNT(DISTINCT gruko_id) FROM gruko_intensity").fetchone()[0]
    log.info(f"  Total intensity: {n_combos:,} combos, {n_subs} substances, {n_grukos} GRUKOs")

    # Create total intensity per GRUKO (all substances summed)
    conn.execute("""
        CREATE TABLE gruko_total_intensity AS
        SELECT
            gruko_id,
            SUM(total_kg) as total_kg,
            SUM(total_kg) / NULLIF(SUM(total_area_ha), 0) as kg_per_ha,
            SUM(n_applications) as n_applications
        FROM gruko_intensity
        GROUP BY gruko_id
    """)

    n_total = conn.execute("SELECT COUNT(*) FROM gruko_total_intensity").fetchone()[0]
    log.info(f"  Total intensity per GRUKO: {n_total:,} GRUKOs")

    # Create fluorinated-only intensity per GRUKO
    conn.execute("""
        CREATE TABLE gruko_intensity_fluorinated AS
        SELECT
            gi.gruko_id,
            SUM(gi.total_kg) as total_kg,
            SUM(gi.total_kg) / NULLIF(SUM(gi.total_area_ha), 0) as kg_per_ha,
            SUM(gi.n_applications) as n_applications
        FROM gruko_intensity gi
        JOIN fluorinated_ingredients fi ON gi.substance = fi.ingredient
        GROUP BY gi.gruko_id
    """)

    n_fluor = conn.execute("SELECT COUNT(*) FROM gruko_intensity_fluorinated").fetchone()[0]
    if n_fluor > 0:
        stats = conn.execute("""
            SELECT AVG(total_kg), MEDIAN(total_kg), AVG(kg_per_ha)
            FROM gruko_intensity_fluorinated
        """).fetchone()
        log.info(
            f"  Fluorinated intensity: {n_fluor:,} GRUKOs, mean={stats[0]:.1f} kg, median={stats[1]:.1f} kg, mean kg/ha={stats[2]:.3f}"
        )
    else:
        log.warning("  No fluorinated pesticide intensity found!")

    # Show top fluorinated ingredients by total kg
    rows = conn.execute("""
        SELECT gi.substance, SUM(gi.total_kg) as total_kg, COUNT(DISTINCT gi.gruko_id) as n_grukos
        FROM gruko_intensity gi
        JOIN fluorinated_ingredients fi ON gi.substance = fi.ingredient
        GROUP BY gi.substance
        ORDER BY total_kg DESC
        LIMIT 20
    """).fetchall()
    log.info("  Top fluorinated ingredients by kg a.i.:")
    for s, kg, ng in rows:
        in_tfa = "TFA" if s in TFA_FORMING_PARENTS else ""
        log.info(f"    {s:<40} {kg:>12,.0f} kg  {ng:>5} GRUKOs  {in_tfa}")


def build_gruko_soil_transit(conn: duckdb.DuckDBPyConnection) -> None:
    """Assign soil-dependent transit times to each GRUKO polygon.

    Spatial join: for each GRUKO, find the dominant soil type (largest overlap area)
    and map it to a transit time in years. Sandy soils ~3yr, clay ~7yr.
    """
    has_soil = conn.execute("""
        SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'soil_types_raw'
    """).fetchone()[0]

    if not has_soil:
        log.info("No soil type data — using default transit time for all GRUKOs")
        conn.execute(f"""
            CREATE TABLE gruko_transit AS
            SELECT DISTINCT gruko_id, {DEFAULT_TRANSIT_YEARS} as transit_years,
                   'unknown' as dominant_soil, 0 as soil_height
            FROM grukos_raw
        """)
        return

    log.info("Assigning soil-dependent transit times to GRUKOs...")

    has_geom_col = conn.execute("""
        SELECT data_type FROM information_schema.columns
        WHERE table_name = 'soil_types_raw' AND column_name = 'geometry'
    """).fetchone()
    soil_geom_type = has_geom_col[0] if has_geom_col else "unknown"
    log.info(f"  Soil geometry column type: {soil_geom_type}")

    soil_geom_expr = "ST_GeomFromText(s.geometry)" if soil_geom_type.upper() in ("VARCHAR", "TEXT") else "s.geometry"

    conn.execute(f"""
        CREATE TABLE _gruko_soil_overlap AS
        SELECT
            g.gruko_id,
            s.soil_description,
            s.soil_height,
            ST_Area(ST_Intersection(g.geometry_spatial, {soil_geom_expr})) as overlap_area
        FROM grukos_raw g
        JOIN soil_types_raw s ON ST_Intersects(g.geometry_spatial, {soil_geom_expr})
        WHERE ST_Area(ST_Intersection(g.geometry_spatial, {soil_geom_expr})) > 0
    """)

    n_overlaps = conn.execute("SELECT COUNT(*) FROM _gruko_soil_overlap").fetchone()[0]
    log.info(f"  GRUKO-soil overlaps: {n_overlaps:,}")

    conn.execute("""
        CREATE TABLE _gruko_dominant_soil AS
        SELECT gruko_id, soil_description, soil_height, overlap_area
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY gruko_id ORDER BY overlap_area DESC) as rn
            FROM _gruko_soil_overlap
        )
        WHERE rn = 1
    """)

    n_assigned = conn.execute("SELECT COUNT(*) FROM _gruko_dominant_soil").fetchone()[0]
    n_total = conn.execute("SELECT COUNT(DISTINCT gruko_id) FROM grukos_raw").fetchone()[0]
    log.info(f"  GRUKOs with soil assignment: {n_assigned:,} / {n_total:,}")

    case_parts = " ".join(f"WHEN soil_height = {h} THEN {t}" for h, t in SOIL_TRANSIT_TIMES.items())
    conn.execute(f"""
        CREATE TABLE gruko_transit AS
        SELECT
            g.gruko_id,
            CASE {case_parts} ELSE {DEFAULT_TRANSIT_YEARS} END as transit_years,
            COALESCE(ds.soil_description, 'unknown') as dominant_soil,
            COALESCE(ds.soil_height, 0) as soil_height
        FROM (SELECT DISTINCT gruko_id FROM grukos_raw) g
        LEFT JOIN _gruko_dominant_soil ds ON g.gruko_id = ds.gruko_id
    """)

    conn.execute("DROP TABLE _gruko_soil_overlap")
    conn.execute("DROP TABLE _gruko_dominant_soil")

    rows = conn.execute("""
        SELECT transit_years, dominant_soil, COUNT(*) as n
        FROM gruko_transit
        GROUP BY transit_years, dominant_soil
        ORDER BY transit_years, n DESC
    """).fetchall()
    for transit, soil, cnt in rows:
        log.info(f"    {transit}yr transit ({soil}): {cnt:,} GRUKOs")


def build_gruko_detections(conn: duckdb.DuckDBPyConnection) -> None:
    """Spatially assign PFAS borehole samples to GRUKOs, compute binary detection.

    Uses per-substance detection thresholds for PFAS.
    TFA uses 2020+ detection window; traditional PFAS uses 2018+.
    """
    log.info("Building GRUKO-level PFAS groundwater detections...")

    # Check available columns in pfas_raw
    pfas_cols = [
        c[0]
        for c in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='pfas_raw'"
        ).fetchall()
    ]

    has_geom = "geometry" in pfas_cols
    has_xy = "x" in pfas_cols and "y" in pfas_cols
    concentration_col = "maengde" if "maengde" in pfas_cols else "concentration"

    if has_geom:
        geom_expr = "pfas.geometry"
    elif has_xy:
        geom_expr = "ST_Point(pfas.x, pfas.y)"
    else:
        raise RuntimeError("PFAS data has neither geometry nor x/y columns")

    # Spatial join: assign PFAS samples to GRUKOs
    conn.execute(f"""
        CREATE TABLE pfas_gruko AS
        SELECT pfas.*, g.gruko_id
        FROM pfas_raw pfas
        JOIN grukos_raw g ON ST_Within({geom_expr}, g.geometry_spatial)
    """)

    n_matched = conn.execute("SELECT COUNT(*) FROM pfas_gruko").fetchone()[0]
    n_total = conn.execute("SELECT COUNT(*) FROM pfas_raw").fetchone()[0]
    log.info(
        f"  PFAS samples matched to GRUKOs: {n_matched:,} / {n_total:,} ({100 * n_matched / max(n_total, 1):.1f}%)"
    )

    # Check if sample_id column exists
    has_sample_id = "sample_id" in pfas_cols
    sample_count_expr = "COUNT(DISTINCT pg.sample_id)" if has_sample_id else "COUNT(*)"

    # Build per-substance threshold CASE expression
    threshold_cases = []
    for substance, threshold in PFAS_DETECTION_THRESHOLDS.items():
        threshold_cases.append(f"WHEN pg.pfas_name = '{substance}' THEN {threshold}")
    threshold_case = " ".join(threshold_cases)
    threshold_expr = f"CASE {threshold_case} ELSE {DEFAULT_PFAS_THRESHOLD} END"

    # Build detection year filter CASE expression
    # TFA: 2020+, Traditional PFAS: 2018+, Others: 2018+
    year_filter_cases = f"""
        CASE
            WHEN pg.pfas_name = 'TFA' THEN pg.year >= {TFA_DETECTION_YEAR_START}
            ELSE pg.year >= {TRADITIONAL_PFAS_DETECTION_YEAR_START}
        END
    """

    conn.execute(f"""
        CREATE TABLE gruko_detections AS
        SELECT
            pg.gruko_id,
            pg.pfas_name as substance,
            MAX(CASE WHEN pg.{concentration_col} > {threshold_expr} THEN 1 ELSE 0 END) as detected,
            {sample_count_expr} as n_samples,
            MAX(pg.{concentration_col}) as max_concentration,
            MIN(pg.year) as min_year,
            MAX(pg.year) as max_year
        FROM pfas_gruko pg
        WHERE {year_filter_cases}
        GROUP BY pg.gruko_id, pg.pfas_name
    """)

    n_combos = conn.execute("SELECT COUNT(*) FROM gruko_detections").fetchone()[0]
    n_subs = conn.execute("SELECT COUNT(DISTINCT substance) FROM gruko_detections").fetchone()[0]
    n_grukos = conn.execute("SELECT COUNT(DISTINCT gruko_id) FROM gruko_detections").fetchone()[0]
    log.info(f"  PFAS detections: {n_combos:,} combos, {n_subs} substances, {n_grukos} GRUKOs")

    # Report detection rates per substance
    rows = conn.execute("""
        SELECT substance,
               SUM(detected) as n_detected,
               COUNT(*) as n_grukos,
               ROUND(100.0 * SUM(detected) / COUNT(*), 1) as detection_rate
        FROM gruko_detections
        GROUP BY substance
        ORDER BY n_detected DESC
    """).fetchall()
    log.info("  PFAS detection rates per substance:")
    for name, n_det, n_grk, rate in rows:
        threshold = PFAS_DETECTION_THRESHOLDS.get(name, DEFAULT_PFAS_THRESHOLD)
        log.info(f"    {name:<30} {n_det:>5} / {n_grk:>5} ({rate:.1f}%)  [threshold={threshold} ug/L]")


def build_gruko_covariates(conn: duckdb.DuckDBPyConnection) -> None:
    """Build per-GRUKO hydrogeological covariates for multivariate regression.

    Creates gruko_covariates table with:
    - n_wells: COUNT(DISTINCT dgu_nr) — monitoring density
    - median_intake_depth_m: MEDIAN((intake_top_m + intake_bottom_m)/2) — well depth
    - n_analyses: COUNT(*) total analyses
    - soil_height: from gruko_transit
    """
    log.info("Building per-GRUKO hydrogeological covariates...")

    # Check available columns
    pfas_cols = [
        c[0]
        for c in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='pfas_gruko'"
        ).fetchall()
    ]

    has_dgu = "dgu_nr" in pfas_cols
    has_intake = "intake_top_m" in pfas_cols and "intake_bottom_m" in pfas_cols

    wells_expr = "COUNT(DISTINCT pg.dgu_nr)" if has_dgu else "COUNT(DISTINCT pg.gruko_id)"
    depth_expr = "MEDIAN((pg.intake_top_m + pg.intake_bottom_m) / 2.0)" if has_intake else "NULL"

    conn.execute(f"""
        CREATE TABLE gruko_covariates AS
        SELECT
            pg.gruko_id,
            {wells_expr} as n_wells,
            {depth_expr} as median_intake_depth_m,
            COUNT(*) as n_analyses,
            COALESCE(gt.soil_height, 0) as soil_height
        FROM pfas_gruko pg
        LEFT JOIN gruko_transit gt ON pg.gruko_id = gt.gruko_id
        GROUP BY pg.gruko_id, gt.soil_height
    """)

    n = conn.execute("SELECT COUNT(*) FROM gruko_covariates").fetchone()[0]
    stats = conn.execute("""
        SELECT
            AVG(n_wells), MEDIAN(n_wells),
            AVG(median_intake_depth_m), MEDIAN(median_intake_depth_m),
            AVG(n_analyses), MEDIAN(n_analyses)
        FROM gruko_covariates
    """).fetchone()
    log.info(f"  Covariates for {n:,} GRUKOs")
    log.info(f"    Wells: mean={stats[0]:.1f}, median={stats[1]:.0f}")
    if stats[2] is not None:
        log.info(f"    Intake depth: mean={stats[2]:.1f}m, median={stats[3]:.1f}m")
    log.info(f"    Analyses: mean={stats[4]:.0f}, median={stats[5]:.0f}")

    # Monitoring density diagnostic
    rows = conn.execute("""
        SELECT gc.gruko_id, gc.n_wells, COALESCE(SUM(gi.total_kg), 0) as total_intensity
        FROM gruko_covariates gc
        LEFT JOIN gruko_total_intensity gi ON gc.gruko_id = gi.gruko_id
        GROUP BY gc.gruko_id, gc.n_wells
    """).fetchall()

    if len(rows) >= 10:
        wells = np.array([r[1] for r in rows])
        intensity = np.array([r[2] for r in rows])
        rho, p = scipy_stats.spearmanr(wells, intensity)
        log.info(f"  Monitoring density diagnostic: Spearman(n_wells, total_intensity) = {rho:.3f}, p={p:.4f}")
        if p < 0.05 and abs(rho) > 0.3:
            log.warning("  *** Monitoring density correlated with application intensity — potential confound")
    else:
        log.warning("  Too few GRUKOs for monitoring density diagnostic")


def _get_intensity_sql(intensity_type: str, substance_list: list[str] | None = None) -> str:
    """Return SQL subquery for application intensity per GRUKO.

    intensity_type:
      'fluorinated' — sum of fluorinated-only ingredients (for TFA)
      'total' — total agricultural intensity (all substances)
      'specific' — sum of specific substances from substance_list
    """
    if intensity_type == "fluorinated":
        return """
            SELECT gruko_id, total_kg as intensity
            FROM gruko_intensity_fluorinated
        """
    if intensity_type == "total":
        return """
            SELECT gruko_id, total_kg as intensity
            FROM gruko_total_intensity
        """
    if intensity_type == "specific" and substance_list:
        parent_filter = ", ".join(f"'{p}'" for p in substance_list)
        return f"""
            SELECT gruko_id, SUM(total_kg) as intensity
            FROM gruko_intensity
            WHERE substance IN ({parent_filter})
            GROUP BY gruko_id
        """
    return """
            SELECT gruko_id, total_kg as intensity
            FROM gruko_total_intensity
        """


def _run_single_correlation(
    conn: duckdb.DuckDBPyConnection,
    pfas_substance: str,
    intensity_sql: str,
    intensity_label: str,
) -> dict | None:
    """Run point-biserial correlation for a single PFAS substance vs intensity measure.

    Returns a dict with results or None if insufficient data.
    """
    safe_name = pfas_substance.replace("'", "''")

    try:
        rows = conn.execute(f"""
            SELECT d.gruko_id, d.detected, COALESCE(i.intensity, 0) as intensity
            FROM gruko_detections d
            LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
            WHERE d.substance = '{safe_name}'
        """).fetchall()
    except Exception as e:
        log.warning(f"  Skipping {pfas_substance}: {e}")
        return None

    if len(rows) < MIN_DETECTIONS:
        return None

    detected = np.array([r[1] for r in rows])
    intensity = np.array([r[2] for r in rows])
    n = len(rows)

    nonzero_intensity = int((intensity > 0).sum())
    if detected.std() == 0 or intensity.std() == 0:
        log.info(
            f"  DROPPED {pfas_substance} ({intensity_label}): det_std={detected.std():.4f}, "
            f"int_std={intensity.std():.4f}, nonzero_int={nonzero_intensity}/{n}, "
            f"n_det={int(detected.sum())}"
        )
        return None

    r, p_value = scipy_stats.pointbiserialr(detected, intensity)
    detection_rate = 100.0 * detected.sum() / n

    # Bootstrap 95% CI for r (more appropriate than Fisher z for binary data)
    r_ci_low, r_ci_high = _bootstrap_ci_pointbiserial(detected, intensity, n_boot=2000)
    # Also compute Fisher z for comparison (reported in supplementary)
    z = np.arctanh(r)
    se = 1.0 / np.sqrt(n - 3)
    r_ci_low_fisher = np.tanh(z - 1.96 * se)
    r_ci_high_fisher = np.tanh(z + 1.96 * se)

    # Logistic regression: P(detected) ~ intensity
    logit_p = None
    logit_or = None
    logit_or_ci_low = None
    logit_or_ci_high = None
    logit_auc = None
    logit_nagelkerke = None
    logit_hl_p = None
    try:
        X = sm.add_constant(intensity)
        logit_model = sm.Logit(detected, X).fit(disp=0, maxiter=100)
        logit_p = logit_model.pvalues[1]
        logit_or = round(np.exp(logit_model.params[1]), 4)
        ci = logit_model.conf_int()
        _or_low = np.exp(ci[1, 0])
        _or_high = np.exp(ci[1, 1])
        logit_or_ci_low = round(_or_low, 4) if np.isfinite(_or_low) else None
        logit_or_ci_high = round(_or_high, 4) if np.isfinite(_or_high) else None
        y_pred = logit_model.predict(X)
        logit_auc = _compute_auc(detected, y_pred)
        logit_nagelkerke = _compute_nagelkerke_r2(logit_model)
        _, logit_hl_p = _hosmer_lemeshow(detected, y_pred)
    except Exception:  # noqa: S110
        pass

    # Quartile dose-response
    q_rates = {}
    q4_q1 = None
    if intensity.max() > 0 and (intensity > 0).sum() >= 4:
        quartiles = np.percentile(intensity[intensity > 0], [25, 50, 75])
        q1_mask = intensity <= quartiles[0]
        q2_mask = (intensity > quartiles[0]) & (intensity <= quartiles[1])
        q3_mask = (intensity > quartiles[1]) & (intensity <= quartiles[2])
        q4_mask = intensity > quartiles[2]
        q_rates = {
            "q1_rate": round(100 * detected[q1_mask].mean(), 1) if q1_mask.sum() > 0 else None,
            "q2_rate": round(100 * detected[q2_mask].mean(), 1) if q2_mask.sum() > 0 else None,
            "q3_rate": round(100 * detected[q3_mask].mean(), 1) if q3_mask.sum() > 0 else None,
            "q4_rate": round(100 * detected[q4_mask].mean(), 1) if q4_mask.sum() > 0 else None,
            "q1_n": int(q1_mask.sum()),
            "q2_n": int(q2_mask.sum()),
            "q3_n": int(q3_mask.sum()),
            "q4_n": int(q4_mask.sum()),
        }
        if q1_mask.sum() > 0 and q4_mask.sum() > 0:
            q1_rate = detected[q1_mask].mean()
            q4_rate = detected[q4_mask].mean()
            if q1_rate > 0:
                q4_q1 = q4_rate / q1_rate

    # Descriptive statistics for intensity
    intensity_nonzero = intensity[intensity > 0]

    stype = PFAS_SUBSTANCE_TYPE.get(pfas_substance, "unknown")
    threshold = PFAS_DETECTION_THRESHOLDS.get(pfas_substance, DEFAULT_PFAS_THRESHOLD)

    return {
        "substance": pfas_substance,
        "type": stype,
        "intensity_measure": intensity_label,
        "threshold_ugl": threshold,
        "r": round(r, 3),
        "r_ci_low": round(r_ci_low, 3),
        "r_ci_high": round(r_ci_high, 3),
        "r_ci_low_fisher": round(r_ci_low_fisher, 3),
        "r_ci_high_fisher": round(r_ci_high_fisher, 3),
        "p_value": p_value,
        "logit_p": logit_p,
        "logit_or": logit_or,
        "logit_or_ci_low": logit_or_ci_low,
        "logit_or_ci_high": logit_or_ci_high,
        "logit_auc": round(logit_auc, 2) if logit_auc else None,
        "logit_nagelkerke_r2": round(logit_nagelkerke, 3) if logit_nagelkerke else None,
        "logit_hl_p": round(logit_hl_p, 3) if logit_hl_p and np.isfinite(logit_hl_p) else None,
        "q4_q1": round(q4_q1, 1) if q4_q1 else None,
        "q_rates": q_rates,
        "detection_rate": round(detection_rate, 1),
        "n_grukos": n,
        "n_detected": int(detected.sum()),
        "intensity_mean": round(float(intensity.mean()), 2),
        "intensity_sd": round(float(intensity.std()), 2),
        "intensity_median": round(float(np.median(intensity)), 2),
        "intensity_nonzero_n": len(intensity_nonzero),
    }


def _apply_fdr(results: list[dict]) -> None:
    """Apply Benjamini-Hochberg FDR correction to a list of results."""
    if not results:
        return

    p_vals = [r["p_value"] for r in results]
    reject, p_adj, _, _ = multipletests(p_vals, alpha=0.05, method="fdr_bh")
    for r, pa, sig in zip(results, p_adj, reject, strict=False):
        r["p_fdr"] = pa
        r["sig_fdr"] = bool(sig)

    # Also FDR-correct logistic p-values
    logit_ps = [r["logit_p"] if r["logit_p"] is not None else 1.0 for r in results]
    reject_l, p_adj_l, _, _ = multipletests(logit_ps, alpha=0.05, method="fdr_bh")
    for r, pa, sig in zip(results, p_adj_l, reject_l, strict=False):
        r["logit_p_fdr"] = pa
        r["logit_sig_fdr"] = bool(sig)


def run_tier1_tfa_correlations(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Tier 1: TFA vs fluorinated pesticide intensity.

    Tests correlation between TFA (trifluoroacetic acid) groundwater detection
    and the summed application intensity of TFA-forming parent pesticides.
    """
    log.info("=" * 60)
    log.info("TIER 1: TFA vs fluorinated pesticide intensity")
    log.info("=" * 60)

    intensity_sql = _get_intensity_sql("fluorinated")
    result = _run_single_correlation(conn, "TFA", intensity_sql, "fluorinated_kg")

    results = []
    if result:
        results.append(result)
        log.info(
            f"  TFA vs fluorinated: r={result['r']:.3f}, p={result['p_value']:.4f}, "
            f"n={result['n_grukos']}, det_rate={result['detection_rate']:.1f}%"
        )
    else:
        log.warning("  TFA: insufficient data for correlation")

    # Also test TFA vs individual TFA-forming parents
    log.info("  Testing TFA vs individual TFA-forming parent substances...")
    for parent in sorted(TFA_FORMING_PARENTS):
        parent_sql = _get_intensity_sql("specific", [parent])
        res = _run_single_correlation(conn, "TFA", parent_sql, f"fluorinated_{parent}")
        if res:
            res["substance"] = f"TFA_vs_{parent}"
            res["type"] = "tfa_parent_specific"
            results.append(res)

    if results:
        _apply_fdr(results)
        n_sig = sum(1 for r in results if r.get("sig_fdr") and r["r"] > 0)
        log.info(f"  Tier 1 results: {len(results)} tests, {n_sig} FDR-significant")

    return results


def run_tier2_traditional_pfas(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Tier 2: Traditional PFAS (PFOS/PFOA/PFHxS/PFNA) vs total agricultural intensity.

    Tests biosolids/formulation/container contamination pathways.
    """
    log.info("=" * 60)
    log.info("TIER 2: Traditional PFAS vs total agricultural intensity")
    log.info("=" * 60)

    intensity_sql = _get_intensity_sql("total")
    results = []

    for pfas_name in sorted(TRADITIONAL_PFAS):
        result = _run_single_correlation(conn, pfas_name, intensity_sql, "total_ag_kg")
        if result:
            results.append(result)
            log.info(
                f"  {pfas_name}: r={result['r']:.3f}, p={result['p_value']:.4f}, "
                f"n={result['n_grukos']}, det_rate={result['detection_rate']:.1f}%"
            )
        else:
            log.info(f"  {pfas_name}: insufficient data")

    # Also test against fluorinated intensity (alternative pathway)
    log.info("  Also testing traditional PFAS vs fluorinated pesticide intensity...")
    fluor_sql = _get_intensity_sql("fluorinated")
    for pfas_name in sorted(TRADITIONAL_PFAS):
        result = _run_single_correlation(conn, pfas_name, fluor_sql, "fluorinated_kg")
        if result:
            result["substance"] = f"{pfas_name}_vs_fluorinated"
            result["type"] = "traditional_vs_fluorinated"
            results.append(result)

    if results:
        _apply_fdr(results)
        n_sig = sum(1 for r in results if r.get("sig_fdr") and r["r"] > 0)
        log.info(f"  Tier 2 results: {len(results)} tests, {n_sig} FDR-significant")

    return results


def run_tier3_exploratory_screen(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Tier 3: Exploratory screen — all PFAS substances vs both intensity measures.

    Tests each of the 26 PFAS substances against:
    1. Total fluorinated pesticide intensity
    2. Total overall pesticide intensity
    FDR correction applied across ALL tests.
    """
    log.info("=" * 60)
    log.info("TIER 3: Exploratory screen — all PFAS vs all intensities")
    log.info("=" * 60)

    # Get all PFAS substances that have detection data
    substances = conn.execute(f"""
        SELECT substance, SUM(detected) as n_detected, COUNT(*) as n_grukos
        FROM gruko_detections
        GROUP BY substance
        HAVING SUM(detected) >= {MIN_DETECTIONS}
        ORDER BY SUM(detected) DESC
    """).fetchall()

    log.info(f"  {len(substances)} PFAS substances with >= {MIN_DETECTIONS} detections")

    results = []

    # Test each substance against fluorinated intensity
    fluor_sql = _get_intensity_sql("fluorinated")
    for substance_name, _n_detected, _n_grukos in substances:
        result = _run_single_correlation(conn, substance_name, fluor_sql, "fluorinated_kg")
        if result:
            results.append(result)

    # Test each substance against total intensity
    total_sql = _get_intensity_sql("total")
    for substance_name, _n_detected, _n_grukos in substances:
        result = _run_single_correlation(conn, substance_name, total_sql, "total_ag_kg")
        if result:
            result["substance"] = f"{substance_name}_vs_total"
            results.append(result)

    if results:
        # Apply FDR correction across ALL Tier 3 tests
        _apply_fdr(results)
        n_sig_fluor = sum(
            1 for r in results if r.get("sig_fdr") and r["r"] > 0 and r["intensity_measure"] == "fluorinated_kg"
        )
        n_sig_total = sum(
            1 for r in results if r.get("sig_fdr") and r["r"] > 0 and r["intensity_measure"] == "total_ag_kg"
        )
        log.info(f"  Tier 3 results: {len(results)} tests total")
        log.info(f"    vs fluorinated: {n_sig_fluor} FDR-significant")
        log.info(f"    vs total ag: {n_sig_total} FDR-significant")

    return results


def run_tier4_negative_controls(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Tier 4: Negative control validation.

    Three control groups:
    1. High-Koc fluorinated pesticides — should NOT show TFA correlation (they don't leach)
    2. Non-agricultural PFAS (PFDS, PFUnDA, PFDoDA) — no agricultural correlation expected
    3. Atmospheric PFAS (PFBA, PFPeA) — diffuse source, no ag-specific spatial correlation
    """
    log.info("=" * 60)
    log.info("TIER 4: Negative control validation")
    log.info("=" * 60)

    neg_results = []

    # ── Control Group 1: High-Koc fluorinated pesticides vs TFA ──────────
    log.info("  Control Group 1: High-Koc fluorinated pesticides (expect no TFA correlation)...")
    for parent, koc in HIGH_KOC_FLUORINATED.items():
        parent_sql = _get_intensity_sql("specific", [parent])
        result = _run_single_correlation(conn, "TFA", parent_sql, f"high_koc_{parent}")
        if result:
            result["control_group"] = "high_koc_fluorinated"
            result["substance"] = f"TFA_vs_{parent}"
            result["koc"] = koc
            result["expected"] = "non-significant"
            is_sig = result["p_value"] < 0.05 and result["r"] > 0
            result["note"] = "UNEXPECTED" if is_sig else "as expected (non-significant)"
            neg_results.append(result)
            log.info(
                f"    TFA vs {parent} (Koc={koc}): r={result['r']:.3f}, p={result['p_value']:.4f} — {result['note']}"
            )
        else:
            neg_results.append(
                {
                    "substance": f"TFA_vs_{parent}",
                    "control_group": "high_koc_fluorinated",
                    "koc": koc,
                    "r": None,
                    "p_value": None,
                    "expected": "non-significant",
                    "note": "insufficient data",
                    "n_grukos": 0,
                    "detection_rate": 0,
                    "type": "negative_control",
                    "intensity_measure": f"high_koc_{parent}",
                }
            )

    # ── Control Group 2: Non-agricultural PFAS vs all intensities ────────
    log.info("  Control Group 2: Non-agricultural PFAS (expect no ag correlation)...")
    for pfas_name in sorted(NON_AGRICULTURAL_PFAS):
        for intensity_type, label in [("fluorinated", "fluorinated_kg"), ("total", "total_ag_kg")]:
            intensity_sql = _get_intensity_sql(intensity_type)
            result = _run_single_correlation(conn, pfas_name, intensity_sql, label)
            if result:
                result["control_group"] = "non_agricultural"
                result["expected"] = "non-significant"
                is_sig = result["p_value"] < 0.05 and result["r"] > 0
                result["note"] = "UNEXPECTED" if is_sig else "as expected (non-significant)"
                neg_results.append(result)
                log.info(
                    f"    {pfas_name} vs {label}: r={result['r']:.3f}, p={result['p_value']:.4f} — {result['note']}"
                )
            else:
                neg_results.append(
                    {
                        "substance": pfas_name,
                        "control_group": "non_agricultural",
                        "r": None,
                        "p_value": None,
                        "expected": "non-significant",
                        "note": f"insufficient data ({label})",
                        "n_grukos": 0,
                        "detection_rate": 0,
                        "type": "negative_control",
                        "intensity_measure": label,
                    }
                )

    # ── Control Group 3: Atmospheric PFAS vs all intensities ─────────────
    log.info("  Control Group 3: Atmospheric PFAS (expect no ag-specific correlation)...")
    for pfas_name in sorted(ATMOSPHERIC_PFAS):
        for intensity_type, label in [("fluorinated", "fluorinated_kg"), ("total", "total_ag_kg")]:
            intensity_sql = _get_intensity_sql(intensity_type)
            result = _run_single_correlation(conn, pfas_name, intensity_sql, label)
            if result:
                result["control_group"] = "atmospheric"
                result["expected"] = "non-significant"
                is_sig = result["p_value"] < 0.05 and result["r"] > 0
                result["note"] = "UNEXPECTED" if is_sig else "as expected (non-significant)"
                neg_results.append(result)
                log.info(
                    f"    {pfas_name} vs {label}: r={result['r']:.3f}, p={result['p_value']:.4f} — {result['note']}"
                )
            else:
                neg_results.append(
                    {
                        "substance": pfas_name,
                        "control_group": "atmospheric",
                        "r": None,
                        "p_value": None,
                        "expected": "non-significant",
                        "note": f"insufficient data ({label})",
                        "n_grukos": 0,
                        "detection_rate": 0,
                        "type": "negative_control",
                        "intensity_measure": label,
                    }
                )

    # Apply BH-FDR correction across all negative control p-values
    nc_pvals = [r["p_value"] if r.get("p_value") is not None else 1.0 for r in neg_results]
    if any(p < 1.0 for p in nc_pvals):
        _, p_adj_nc, _, _ = multipletests(nc_pvals, alpha=0.05, method="fdr_bh")
        for r, qa in zip(neg_results, p_adj_nc, strict=False):
            r["q_fdr"] = round(float(qa), 3)
    else:
        for r in neg_results:
            r["q_fdr"] = 1.0

    n_nonsig = sum(1 for r in neg_results if r.get("note", "").startswith("as expected"))
    n_unexpected = sum(1 for r in neg_results if r.get("note") == "UNEXPECTED")
    log.info(f"  Negative controls: {len(neg_results)} tested, {n_nonsig} as expected, {n_unexpected} unexpected")

    return neg_results


def run_multivariate_logistic(
    conn: duckdb.DuckDBPyConnection,
    results: list[dict],
    intensity_type: str = "fluorinated",
) -> list[dict]:
    """Multivariate logistic regression controlling for hydrogeological covariates.

    For each FDR-significant result, fit:
        P(detected=1) = logit^{-1}(b0 + b1*intensity + b2*soil_height + b3*median_intake_depth + b4*n_wells)
    """
    log.info(f"Running multivariate logistic regression (intensity={intensity_type})...")

    sig = [r for r in results if r.get("sig_fdr") and r["r"] > 0]
    if not sig:
        log.warning("  No FDR-significant substances for multivariate analysis")
        return []

    mv_results = []

    for sub in sig:
        substance_name = sub["substance"]
        # Determine intensity SQL based on the original intensity measure
        if sub.get("intensity_measure") == "fluorinated_kg":
            intensity_sql = _get_intensity_sql("fluorinated")
        elif sub.get("intensity_measure") == "total_ag_kg":
            intensity_sql = _get_intensity_sql("total")
        else:
            intensity_sql = _get_intensity_sql(intensity_type)

        # Use the base substance name (strip prefixes like "TFA_vs_" for DB lookup)
        base_substance = substance_name
        if "_vs_" in substance_name:
            base_substance = substance_name.split("_vs_")[0]
        safe_base = base_substance.replace("'", "''")

        try:
            rows = conn.execute(f"""
                SELECT
                    d.gruko_id,
                    d.detected,
                    COALESCE(i.intensity, 0) as intensity,
                    COALESCE(gc.soil_height, 0) as soil_height,
                    COALESCE(gc.median_intake_depth_m, 0) as median_intake_depth,
                    COALESCE(gc.n_wells, 0) as n_wells
                FROM gruko_detections d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                LEFT JOIN gruko_covariates gc ON d.gruko_id = gc.gruko_id
                WHERE d.substance = '{safe_base}'
            """).fetchall()
        except Exception as e:
            log.warning(f"  Skipping {substance_name}: {e}")
            continue

        if len(rows) < 50:
            continue

        detected = np.array([r[1] for r in rows])
        intensity = np.array([r[2] for r in rows])
        soil_h = np.array([r[3] for r in rows])
        depth = np.array([r[4] for r in rows])
        wells = np.array([r[5] for r in rows])

        if detected.std() == 0 or intensity.std() == 0:
            continue

        # Fit multivariate logistic regression
        X = np.column_stack([intensity, soil_h, depth, wells])
        X = sm.add_constant(X)

        try:
            model = sm.Logit(detected, X).fit(disp=0, maxiter=200)
        except Exception:
            try:
                model = sm.Logit(detected, X).fit_regularized(disp=0, maxiter=200, alpha=0.1)
            except Exception as e:
                log.warning(f"  Convergence failure for {substance_name}: {e}")
                continue

        # Extract intensity coefficient
        intensity_coef = model.params[1]
        intensity_or = np.exp(intensity_coef)

        try:
            ci = model.conf_int()
            or_ci_low = np.exp(ci[1, 0])
            or_ci_high = np.exp(ci[1, 1])
        except Exception:
            or_ci_low = or_ci_high = None

        try:
            pvals = model.pvalues
            p_intensity = pvals[1]
            p_soil = pvals[2]
            p_depth = pvals[3]
            p_wells = pvals[4]
        except Exception:
            p_intensity = p_soil = p_depth = p_wells = None

        try:
            pseudo_r2 = model.prsquared
            aic = model.aic
        except Exception:
            pseudo_r2 = aic = None

        try:
            y_pred = model.predict(X)
            mv_auc = _compute_auc(detected, y_pred)
        except Exception:
            mv_auc = None
        mv_nagelkerke = _compute_nagelkerke_r2(model)
        try:
            y_pred_hl = model.predict(X)
            _, mv_hl_p = _hosmer_lemeshow(detected, y_pred_hl)
        except Exception:
            mv_hl_p = None

        n_events = int(detected.sum())
        n_predictors = X.shape[1] - 1
        epv = n_events / n_predictors if n_predictors > 0 else None

        vif_intensity = _compute_vif(X, 1)

        mv_results.append(
            {
                "substance": substance_name,
                "type": sub["type"],
                "intensity_measure": sub.get("intensity_measure", intensity_type),
                "bivariate_r": sub["r"],
                "bivariate_or": sub.get("logit_or"),
                "mv_intensity_or": round(intensity_or, 4),
                "mv_or_ci_low": round(or_ci_low, 4) if or_ci_low else None,
                "mv_or_ci_high": round(or_ci_high, 4) if or_ci_high else None,
                "p_intensity": p_intensity,
                "p_soil": p_soil,
                "p_depth": p_depth,
                "p_wells": p_wells,
                "pseudo_r2": round(pseudo_r2, 4) if pseudo_r2 else None,
                "nagelkerke_r2": round(mv_nagelkerke, 3) if mv_nagelkerke else None,
                "auc": round(mv_auc, 2) if mv_auc else None,
                "hl_p": round(mv_hl_p, 3) if mv_hl_p and np.isfinite(mv_hl_p) else None,
                "epv": round(epv, 1) if epv else None,
                "vif_intensity": round(vif_intensity, 1) if np.isfinite(vif_intensity) else None,
                "aic": round(aic, 1) if aic else None,
                "n": len(rows),
                "n_events": n_events,
            }
        )

    n_still_sig = sum(1 for r in mv_results if r.get("p_intensity") is not None and r["p_intensity"] < 0.05)
    log.info(
        f"  Multivariate results: {len(mv_results)} substances, {n_still_sig} with intensity p<0.05 after covariate adjustment"
    )

    return mv_results


def run_monitoring_density_stratified(conn: duckdb.DuckDBPyConnection, results: list[dict]) -> list[dict]:
    """Monitoring density stratified analysis.

    Splits GRUKOs into tertiles by n_wells, computes point-biserial correlations
    within each tertile for FDR-significant substances.
    """
    log.info("Running monitoring density stratified analysis...")

    well_rows = conn.execute("""
        SELECT gruko_id, n_wells FROM gruko_covariates ORDER BY n_wells
    """).fetchall()

    if not well_rows:
        log.warning("  No covariate data for stratified analysis")
        return []

    wells_arr = np.array([r[1] for r in well_rows])

    t33 = np.percentile(wells_arr, 33.3)
    t67 = np.percentile(wells_arr, 66.7)
    log.info(f"  Tertile cutpoints: low <= {t33:.0f}, medium <= {t67:.0f}, high > {t67:.0f}")

    gruko_tertile = {}
    for gid, nw in well_rows:
        if nw <= t33:
            gruko_tertile[gid] = "low"
        elif nw <= t67:
            gruko_tertile[gid] = "medium"
        else:
            gruko_tertile[gid] = "high"

    tertile_counts = {t: sum(1 for v in gruko_tertile.values() if v == t) for t in ["low", "medium", "high"]}
    log.info(
        f"  Tertile sizes: low={tertile_counts['low']}, medium={tertile_counts['medium']}, high={tertile_counts['high']}"
    )

    sig = [r for r in results if r.get("sig_fdr") and r["r"] > 0]
    strat_results = []

    for sub in sig:
        substance_name = sub["substance"]
        base_substance = substance_name
        if "_vs_" in substance_name:
            base_substance = substance_name.split("_vs_")[0]
        safe_name = base_substance.replace("'", "''")

        if sub.get("intensity_measure") == "fluorinated_kg":
            intensity_sql = _get_intensity_sql("fluorinated")
        elif sub.get("intensity_measure") == "total_ag_kg":
            intensity_sql = _get_intensity_sql("total")
        else:
            intensity_sql = _get_intensity_sql("fluorinated")

        try:
            rows = conn.execute(f"""
                SELECT d.gruko_id, d.detected, COALESCE(i.intensity, 0) as intensity
                FROM gruko_detections d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                WHERE d.substance = '{safe_name}'
            """).fetchall()
        except Exception:  # noqa: S112
            continue

        for tertile in ["low", "medium", "high"]:
            tertile_grukos = {gid for gid, t in gruko_tertile.items() if t == tertile}
            subset = [(r[1], r[2]) for r in rows if r[0] in tertile_grukos]

            if len(subset) < 20:
                strat_results.append(
                    {
                        "substance": substance_name,
                        "tertile": tertile,
                        "r": None,
                        "p": None,
                        "n": len(subset),
                    }
                )
                continue

            det = np.array([s[0] for s in subset])
            intens = np.array([s[1] for s in subset])

            if det.std() == 0 or intens.std() == 0:
                strat_results.append(
                    {
                        "substance": substance_name,
                        "tertile": tertile,
                        "r": 0.0,
                        "p": 1.0,
                        "n": len(subset),
                    }
                )
                continue

            r_val, p_val = scipy_stats.pointbiserialr(det, intens)
            strat_results.append(
                {
                    "substance": substance_name,
                    "tertile": tertile,
                    "r": round(r_val, 3),
                    "p": p_val,
                    "n": len(subset),
                }
            )

    log.info(f"  Stratified results: {len(strat_results)} substance-tertile combinations")
    return strat_results


def run_power_analysis(results: list[dict]) -> dict:
    """Compute statistical power analysis for the correlation tests.

    Determines minimum detectable r at 80% power given median sample size,
    and classifies observed effects into practical significance tiers.
    """
    log.info("Running power analysis...")

    ns = [r["n_grukos"] for r in results if r.get("n_grukos")]
    if not ns:
        return {}

    median_n = int(np.median(ns))
    mean_n = int(np.mean(ns))
    min_n = min(ns)
    max_n = max(ns)

    z_alpha = 1.96
    z_beta = 0.842
    r_min = np.tanh((z_alpha + z_beta) / np.sqrt(max(median_n - 3, 1)))

    sig = [r for r in results if r.get("sig_fdr") and r["r"] > 0]
    r_values = [r["r"] for r in sig]

    n_strong = sum(1 for rv in r_values if rv >= 0.20)
    n_moderate = sum(1 for rv in r_values if 0.10 <= rv < 0.20)
    n_weak = sum(1 for rv in r_values if rv < 0.10)
    n_above_min = sum(1 for rv in r_values if rv > r_min)

    power_info = {
        "median_n": median_n,
        "mean_n": mean_n,
        "min_n": min_n,
        "max_n": max_n,
        "r_min_80pct": round(r_min, 4),
        "n_sig": len(sig),
        "n_strong": n_strong,
        "n_moderate": n_moderate,
        "n_weak": n_weak,
        "n_above_min": n_above_min,
        "r_values": r_values,
    }

    log.info(f"  Median n={median_n}, min detectable r (80% power) = {r_min:.4f}")
    log.info(
        f"  Effect tiers: strong (r>=0.20): {n_strong}, moderate (0.10-0.20): {n_moderate}, weak (<0.10): {n_weak}"
    )
    log.info(f"  {n_above_min}/{len(sig)} exceed minimum detectable effect")

    return power_info


# ---------------------------------------------------------------------------
# R1: Spatial autocorrelation (Moran's I)
# ---------------------------------------------------------------------------


def run_spatial_autocorrelation(
    conn: duckdb.DuckDBPyConnection,
    results: list[dict],
) -> dict:
    """Compute Moran's I spatial autocorrelation for intensity and detection surfaces.

    Tests whether pesticide intensity and PFAS detection are spatially clustered,
    which would violate the independence assumption of point-biserial correlation
    and inflate significance levels.

    Uses k=8 nearest-neighbour spatial weights based on catchment centroids.
    """
    log.info("Running spatial autocorrelation analysis (Moran's I)...")

    output = {"intensity_surfaces": {}, "detection_surfaces": {}}

    # Get catchment centroids
    try:
        centroid_rows = conn.execute("""
            SELECT gruko_id,
                   ST_X(ST_Centroid(geometry_spatial)) as cx,
                   ST_Y(ST_Centroid(geometry_spatial)) as cy
            FROM grukos_raw
            WHERE geometry_spatial IS NOT NULL
        """).fetchall()
    except Exception as e:
        log.warning(f"  Cannot compute centroids: {e}")
        return output

    if len(centroid_rows) < 20:
        log.warning(f"  Only {len(centroid_rows)} catchments with centroids — skipping Moran's I")
        return output

    # Build centroid lookup: gruko_id -> (cx, cy)
    centroid_map = {r[0]: (r[1], r[2]) for r in centroid_rows if r[1] is not None and r[2] is not None}
    log.info(f"  {len(centroid_map)} catchments with valid centroids")

    # --- Intensity surface: fluorinated pesticide intensity ---
    try:
        int_rows = conn.execute("""
            SELECT gruko_id, SUM(kg_per_ha) as intensity
            FROM gruko_intensity_fluorinated
            GROUP BY gruko_id
        """).fetchall()
        int_map = {r[0]: r[1] for r in int_rows}
    except Exception:
        int_map = {}

    # Build aligned arrays for fluorinated intensity
    common_ids = sorted(set(centroid_map.keys()) & set(int_map.keys()))
    if len(common_ids) >= 20:
        centroids_arr = np.array([centroid_map[gid] for gid in common_ids])
        values_arr = np.array([int_map[gid] for gid in common_ids])
        mi_fluor = _compute_morans_i(values_arr, centroids_arr, k=8)
        mi_fluor["variable"] = "fluorinated_intensity"
        output["intensity_surfaces"]["fluorinated"] = mi_fluor
        log.info(
            f"  Fluorinated intensity: Moran's I = {mi_fluor['I']}, z = {mi_fluor['z_score']}, "
            f"p = {mi_fluor['p_value']:.6f}, n = {mi_fluor['n']}"
        )

    # --- Intensity surface: total agricultural intensity ---
    try:
        tot_rows = conn.execute("""
            SELECT gruko_id, total_kg_per_ha as intensity
            FROM gruko_total_intensity
        """).fetchall()
        tot_map = {r[0]: r[1] for r in tot_rows}
    except Exception:
        tot_map = {}

    common_ids_tot = sorted(set(centroid_map.keys()) & set(tot_map.keys()))
    if len(common_ids_tot) >= 20:
        centroids_arr = np.array([centroid_map[gid] for gid in common_ids_tot])
        values_arr = np.array([tot_map[gid] for gid in common_ids_tot])
        mi_total = _compute_morans_i(values_arr, centroids_arr, k=8)
        mi_total["variable"] = "total_ag_intensity"
        output["intensity_surfaces"]["total"] = mi_total
        log.info(
            f"  Total ag intensity: Moran's I = {mi_total['I']}, z = {mi_total['z_score']}, "
            f"p = {mi_total['p_value']:.6f}, n = {mi_total['n']}"
        )

    # --- Detection surfaces for FDR-significant substances ---
    sig = [r for r in results if r.get("sig_fdr") and r["r"] > 0]
    seen_substances = set()
    for sub in sig:
        substance_name = sub["substance"]
        base_substance = substance_name.split("_vs_")[0] if "_vs_" in substance_name else substance_name
        if base_substance in seen_substances:
            continue
        seen_substances.add(base_substance)

        safe = base_substance.replace("'", "''")
        try:
            det_rows = conn.execute(f"""
                SELECT gruko_id, detected
                FROM gruko_detections
                WHERE substance = '{safe}'
            """).fetchall()
        except Exception:  # noqa: S112
            continue

        det_map = {r[0]: r[1] for r in det_rows}
        common = sorted(set(centroid_map.keys()) & set(det_map.keys()))
        if len(common) < 20:
            continue

        centroids_arr = np.array([centroid_map[gid] for gid in common])
        det_arr = np.array([det_map[gid] for gid in common], dtype=float)

        if det_arr.std() == 0:
            continue

        mi_det = _compute_morans_i(det_arr, centroids_arr, k=8)
        mi_det["variable"] = f"detection_{base_substance}"
        output["detection_surfaces"][base_substance] = mi_det
        log.info(
            f"  {base_substance} detection: Moran's I = {mi_det['I']}, z = {mi_det['z_score']}, "
            f"p = {mi_det['p_value']:.6f}"
        )

    # --- Compute effective sample size correction ---
    # If Moran's I is significant, the effective n is reduced
    for _key, mi in {**output.get("intensity_surfaces", {}), **output.get("detection_surfaces", {})}.items():
        if mi.get("I") is not None and mi.get("n"):
            I_val = mi["I"]
            n_val = mi["n"]
            # Effective sample size under spatial autocorrelation (Clifford & Richardson 1991)
            # n_eff ≈ n / (1 + (n-1) * rho_bar) where rho_bar ≈ I for global average
            if I_val > 0:
                n_eff = n_val / (1 + (n_val - 1) * abs(I_val))
                mi["n_effective"] = round(n_eff, 0)
                mi["inflation_factor"] = round(n_val / n_eff, 2)
            else:
                mi["n_effective"] = n_val
                mi["inflation_factor"] = 1.0

    return output


# ---------------------------------------------------------------------------
# R3: TFA threshold sensitivity analysis
# ---------------------------------------------------------------------------


def run_tfa_threshold_sensitivity(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Re-analyse TFA detection at progressively higher concentration thresholds.

    Tests whether spatial gradients in TFA emerge at higher cut-points
    (0.5, 1.0, 2.0, 5.0 µg/L), which would enable TFA-agriculture correlation.
    Also reports the TFA concentration distribution for context.
    """
    log.info("Running TFA threshold sensitivity analysis...")

    results = []
    thresholds = [0.075, 0.5, 1.0, 2.0, 5.0, 10.0]

    # Detect concentration column name
    pfas_cols = [
        c[0]
        for c in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='pfas_raw'"
        ).fetchall()
    ]
    conc_col = "maengde" if "maengde" in pfas_cols else "concentration"

    # First, get TFA concentration distribution
    try:
        dist_rows = conn.execute(f"""
            SELECT
                COUNT(*) as n_samples,
                COUNT(CASE WHEN {conc_col} > 0.075 THEN 1 END) as n_above_075,
                ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {conc_col}), 3) as p25,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {conc_col}), 3) as p50,
                ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {conc_col}), 3) as p75,
                ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY {conc_col}), 3) as p90,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {conc_col}), 3) as p95,
                ROUND(MAX({conc_col}), 3) as max_conc
            FROM pfas_raw
            WHERE pfas_name = 'TFA'
              AND year >= 2020
        """).fetchone()
    except Exception as e:
        log.warning(f"  Cannot get TFA distribution: {e}")
        dist_rows = None

    if dist_rows:
        log.info(
            f"  TFA distribution: n={dist_rows[0]}, median={dist_rows[3]} µg/L, "
            f"P75={dist_rows[4]}, P90={dist_rows[5]}, P95={dist_rows[6]}, max={dist_rows[7]} µg/L"
        )
        results.append(
            {
                "analysis": "tfa_distribution",
                "n_samples": dist_rows[0],
                "n_above_075": dist_rows[1],
                "p25": dist_rows[3],
                "p50": dist_rows[3],
                "p75": dist_rows[4],
                "p90": dist_rows[5],
                "p95": dist_rows[6],
                "max": dist_rows[7],
            }
        )

    # Get fluorinated intensity SQL
    intensity_sql = _get_intensity_sql("fluorinated")

    for threshold in thresholds:
        try:
            rows = conn.execute(f"""
                WITH tfa_det AS (
                    SELECT
                        pg.gruko_id,
                        MAX(CASE WHEN pg.{conc_col} > {threshold} THEN 1 ELSE 0 END) as detected
                    FROM pfas_gruko pg
                    WHERE pg.pfas_name = 'TFA'
                      AND pg.year >= 2020
                    GROUP BY pg.gruko_id
                )
                SELECT d.gruko_id, d.detected, COALESCE(i.intensity, 0) as intensity
                FROM tfa_det d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
            """).fetchall()
        except Exception:
            # Fallback: use gruko_detections with modified threshold
            try:
                rows = conn.execute(f"""
                    WITH tfa_det AS (
                        SELECT gruko_id,
                               MAX(CASE WHEN max_concentration > {threshold} THEN 1 ELSE 0 END) as detected
                        FROM gruko_detections
                        WHERE substance = 'TFA'
                        GROUP BY gruko_id
                    )
                    SELECT d.gruko_id, d.detected, COALESCE(i.intensity, 0) as intensity
                    FROM tfa_det d
                    LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                """).fetchall()
            except Exception as e:
                log.warning(f"  TFA threshold {threshold}: {e}")
                continue

        if not rows:
            continue

        detected = np.array([r[1] for r in rows])
        intensity = np.array([r[2] for r in rows])
        n = len(rows)
        n_det = int(detected.sum())
        det_rate = 100 * n_det / n if n > 0 else 0

        r_val = None
        p_val = None
        r_ci_low = r_ci_high = None

        if detected.std() > 0 and intensity.std() > 0 and n_det >= 10:
            r_val, p_val = scipy_stats.pointbiserialr(detected, intensity)
            # Bootstrap CI
            r_ci_low, r_ci_high = _bootstrap_ci_pointbiserial(detected, intensity)

        result = {
            "analysis": "tfa_threshold_sensitivity",
            "threshold_ugl": threshold,
            "n_catchments": n,
            "n_detected": n_det,
            "detection_rate_pct": round(det_rate, 1),
            "r": round(r_val, 3) if r_val is not None else None,
            "p_value": p_val,
            "r_ci_low": round(r_ci_low, 3) if r_ci_low is not None else None,
            "r_ci_high": round(r_ci_high, 3) if r_ci_high is not None else None,
            "testable": detected.std() > 0 and n_det >= 10,
        }
        results.append(result)
        log.info(
            f"  TFA @ {threshold} µg/L: {n_det}/{n} detected ({det_rate:.1f}%), "
            f"r = {f'{r_val:.3f}' if r_val is not None else 'N/A'}, p = {f'{p_val:.4f}' if p_val is not None else 'N/A'}"
        )

    return results


# ---------------------------------------------------------------------------
# R4: Detection threshold sensitivity for key PFAS
# ---------------------------------------------------------------------------


def run_detection_threshold_sensitivity(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Test robustness of key findings at alternative detection thresholds.

    For PFOA, PFOS, SUM PFAS-22: test at LOQ, 1.5*LOQ, 2*LOQ.
    """
    log.info("Running detection threshold sensitivity analysis...")

    results = []

    # Define substance-specific thresholds to test
    test_cases = [
        # (substance, base_loq, [multipliers])  # noqa: ERA001
        ("PFOA", 0.003, [0.5, 1.0, 1.5, 2.0, 3.0]),
        ("PFOS", 0.003, [0.5, 1.0, 1.5, 2.0, 3.0]),
        ("SUM PFAS-22", 0.15, [0.5, 1.0, 1.5, 2.0, 3.0]),
        ("PFHxS", 0.003, [0.5, 1.0, 1.5, 2.0, 3.0]),
    ]

    intensity_sql_fluor = _get_intensity_sql("fluorinated")

    for substance, base_loq, multipliers in test_cases:
        safe = substance.replace("'", "''")
        for mult in multipliers:
            threshold = base_loq * mult

            try:
                rows = conn.execute(f"""
                    WITH det_custom AS (
                        SELECT gruko_id,
                               MAX(CASE WHEN max_concentration > {threshold} THEN 1 ELSE 0 END) as detected
                        FROM gruko_detections
                        WHERE substance = '{safe}'
                        GROUP BY gruko_id
                    )
                    SELECT d.gruko_id, d.detected, COALESCE(i.intensity, 0) as intensity
                    FROM det_custom d
                    LEFT JOIN ({intensity_sql_fluor}) i ON d.gruko_id = i.gruko_id
                """).fetchall()
            except Exception as e:
                log.warning(f"  {substance} @ {threshold}: {e}")
                continue

            if not rows:
                continue

            detected = np.array([r[1] for r in rows])
            intensity = np.array([r[2] for r in rows])
            n = len(rows)
            n_det = int(detected.sum())
            det_rate = 100 * n_det / n if n > 0 else 0

            r_val = p_val = None
            if detected.std() > 0 and intensity.std() > 0 and n_det >= MIN_DETECTIONS:
                r_val, p_val = scipy_stats.pointbiserialr(detected, intensity)

            results.append(
                {
                    "substance": substance,
                    "threshold_multiplier": mult,
                    "threshold_ugl": round(threshold, 4),
                    "n_catchments": n,
                    "n_detected": n_det,
                    "detection_rate_pct": round(det_rate, 1),
                    "r": round(r_val, 3) if r_val is not None else None,
                    "p_value": p_val,
                    "significant_005": p_val < 0.05 if p_val is not None else None,
                }
            )
            log.info(
                f"  {substance} @ {mult}×LOQ ({threshold:.4f}): {n_det}/{n} ({det_rate:.1f}%), "  # noqa: RUF001
                f"r={f'{r_val:.3f}' if r_val is not None else 'N/A'}"
            )

    return results


# ---------------------------------------------------------------------------
# R5: Firth penalized logistic regression
# ---------------------------------------------------------------------------


def run_firth_regression(
    conn: duckdb.DuckDBPyConnection,
    mv_results: list[dict],
) -> list[dict]:
    """Apply Firth penalized logistic regression for low-EPV associations.

    Targets associations where EPV < 10 (PFHxS_sum, SUM PFAS-22)
    and compares with standard MLE results.
    """
    log.info("Running Firth penalized logistic regression for low-EPV associations...")

    firth_results = []

    # Select associations with EPV < 12 (include marginal cases)
    low_epv = [mv for mv in mv_results if mv.get("epv") is not None and mv["epv"] < 12]
    if not low_epv:
        log.info("  No low-EPV associations to test")
        return firth_results

    for mv in low_epv:
        substance_name = mv["substance"]
        base_substance = substance_name.split("_vs_")[0] if "_vs_" in substance_name else substance_name
        safe_base = base_substance.replace("'", "''")

        # Determine intensity SQL
        if mv.get("intensity_measure") == "fluorinated_kg":
            intensity_sql = _get_intensity_sql("fluorinated")
        elif mv.get("intensity_measure") == "total_ag_kg":
            intensity_sql = _get_intensity_sql("total")
        else:
            intensity_sql = _get_intensity_sql("fluorinated")

        try:
            rows = conn.execute(f"""
                SELECT
                    d.detected,
                    COALESCE(i.intensity, 0) as intensity,
                    COALESCE(gc.soil_height, 0) as soil_height,
                    COALESCE(gc.median_intake_depth_m, 0) as median_intake_depth,
                    COALESCE(gc.n_wells, 0) as n_wells
                FROM gruko_detections d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                LEFT JOIN gruko_covariates gc ON d.gruko_id = gc.gruko_id
                WHERE d.substance = '{safe_base}'
            """).fetchall()
        except Exception as e:
            log.warning(f"  Firth {substance_name}: {e}")
            continue

        if len(rows) < 50:
            continue

        y = np.array([r[0] for r in rows], dtype=float)
        X_raw = np.array([[r[1], r[2], r[3], r[4]] for r in rows], dtype=float)
        X = sm.add_constant(X_raw)

        if y.std() == 0:
            continue

        firth_result = _firth_logistic_regression(y, X)
        if firth_result is None:
            log.warning(f"  Firth regression failed for {substance_name}")
            continue

        # Extract intensity coefficient (index 1, after constant)
        firth_or = np.exp(firth_result["coefficients"][1])
        firth_p = firth_result["p_values"][1]
        firth_ci_low = np.exp(firth_result["ci_low"][1])
        firth_ci_high = np.exp(firth_result["ci_high"][1])

        result = {
            "substance": substance_name,
            "intensity_measure": mv.get("intensity_measure"),
            "mle_or": mv.get("mv_intensity_or"),
            "mle_p": mv.get("p_intensity"),
            "mle_epv": mv.get("epv"),
            "firth_or": round(float(firth_or), 6),
            "firth_p": float(firth_p),
            "firth_ci_low": round(float(firth_ci_low), 6),
            "firth_ci_high": round(float(firth_ci_high), 6),
            "firth_converged": firth_result["converged"],
            "firth_iterations": firth_result["n_iterations"],
            "n": len(rows),
            "n_events": int(y.sum()),
        }
        firth_results.append(result)
        mle_p = mv.get("p_intensity")
        mle_epv = mv.get("epv")
        log.info(
            f"  {substance_name}: MLE OR={mv.get('mv_intensity_or')}, p={f'{mle_p:.4f}' if mle_p is not None else 'N/A'} → "
            f"Firth OR={firth_or:.6f}, p={firth_p:.4f} (EPV={f'{mle_epv:.0f}' if mle_epv is not None else 'N/A'})"
        )

    return firth_results


# ---------------------------------------------------------------------------
# S6: Biosolids proxy test (soil type × PFAS detection)  # noqa: RUF003
# ---------------------------------------------------------------------------


def run_biosolids_proxy_test(
    conn: duckdb.DuckDBPyConnection,
    results: list[dict],
) -> list[dict]:
    """Cross-tabulate soil type with PFAS detection as a biosolids proxy.

    Clay-rich soils (soil_height 4-6) are more likely to receive biosolids.
    If PFAS detection correlates with clay soils independently of pesticide intensity,
    this supports the biosolids pathway hypothesis.
    """
    log.info("Running biosolids proxy test (soil type × PFAS detection)...")  # noqa: RUF001

    proxy_results = []

    sig = [r for r in results if r.get("sig_fdr") and r["r"] > 0]
    seen = set()

    for sub in sig:
        substance_name = sub["substance"]
        base = substance_name.split("_vs_")[0] if "_vs_" in substance_name else substance_name
        if base in seen:
            continue
        seen.add(base)
        safe = base.replace("'", "''")

        try:
            rows = conn.execute(f"""
                SELECT
                    d.detected,
                    COALESCE(gt.soil_height, 0) as soil_height
                FROM gruko_detections d
                LEFT JOIN gruko_transit gt ON d.gruko_id = gt.gruko_id
                WHERE d.substance = '{safe}'
                  AND gt.soil_height IS NOT NULL
                  AND gt.soil_height > 0
            """).fetchall()
        except Exception:  # noqa: S112
            continue

        if len(rows) < 20:
            continue

        detected = np.array([r[0] for r in rows])
        soil = np.array([r[1] for r in rows])

        # Binary: clay-rich (4,5,6) vs sandy (1,2,3)
        clay = (soil >= 4).astype(float)

        if clay.std() == 0 or detected.std() == 0:
            continue

        # Chi-square test
        contingency = np.array(
            [
                [((clay == 0) & (detected == 0)).sum(), ((clay == 0) & (detected == 1)).sum()],
                [((clay == 1) & (detected == 0)).sum(), ((clay == 1) & (detected == 1)).sum()],
            ]
        )
        chi2, chi_p, _dof, _expected = scipy_stats.chi2_contingency(contingency)

        # Detection rates by soil group
        sandy_det = detected[clay == 0].mean() * 100 if (clay == 0).sum() > 0 else None
        clay_det = detected[clay == 1].mean() * 100 if (clay == 1).sum() > 0 else None

        proxy_results.append(
            {
                "substance": base,
                "n_total": len(rows),
                "n_sandy": int((clay == 0).sum()),
                "n_clay": int((clay == 1).sum()),
                "det_rate_sandy_pct": round(sandy_det, 1) if sandy_det is not None else None,
                "det_rate_clay_pct": round(clay_det, 1) if clay_det is not None else None,
                "chi2": round(chi2, 3),
                "chi_p": chi_p,
                "significant": chi_p < 0.05,
                "clay_higher": clay_det > sandy_det if clay_det and sandy_det else None,
            }
        )
        log.info(f"  {base}: sandy={sandy_det:.1f}% vs clay={clay_det:.1f}%, χ²={chi2:.3f}, p={chi_p:.4f}")

    return proxy_results


# ---------------------------------------------------------------------------
# N1: Non-monotonic dose-response test
# ---------------------------------------------------------------------------


def run_nonmonotonic_test(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Test for non-monotonic (U-shaped) dose-response by adding quadratic term.

    Specifically targets PFOA vs fluorinated intensity where the quartile
    analysis showed a U-shape (11.0% → 6.3% → 6.0% → 14.1%).
    """
    log.info("Running non-monotonic dose-response test...")

    results = []
    test_substances = ["PFOA", "PFOS", "PFNA"]  # substances with suspected non-monotonicity
    intensity_sql = _get_intensity_sql("fluorinated")

    for substance in test_substances:
        safe = substance.replace("'", "''")
        try:
            rows = conn.execute(f"""
                SELECT d.detected, COALESCE(i.intensity, 0) as intensity
                FROM gruko_detections d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                WHERE d.substance = '{safe}'
            """).fetchall()
        except Exception:  # noqa: S112
            continue

        if len(rows) < 50:
            continue

        detected = np.array([r[0] for r in rows], dtype=float)
        intensity = np.array([r[1] for r in rows], dtype=float)

        if detected.std() == 0 or intensity.std() == 0:
            continue

        # Center intensity for numerical stability
        int_centered = intensity - intensity.mean()
        int_squared = int_centered**2

        # Linear model
        X_lin = sm.add_constant(int_centered)
        try:
            model_lin = sm.Logit(detected, X_lin).fit(disp=0, maxiter=100)
            aic_lin = model_lin.aic
            ll_lin = model_lin.llf
        except Exception:  # noqa: S112
            continue

        # Quadratic model
        X_quad = sm.add_constant(np.column_stack([int_centered, int_squared]))
        try:
            model_quad = sm.Logit(detected, X_quad).fit(disp=0, maxiter=100)
            aic_quad = model_quad.aic
            ll_quad = model_quad.llf
            p_quad_term = model_quad.pvalues[2]
        except Exception:  # noqa: S112
            continue

        # Likelihood ratio test (1 df)
        lr_stat = 2 * (ll_quad - ll_lin)
        lr_p = 1 - scipy_stats.chi2.cdf(lr_stat, df=1)

        results.append(
            {
                "substance": substance,
                "aic_linear": round(aic_lin, 1),
                "aic_quadratic": round(aic_quad, 1),
                "aic_improvement": round(aic_lin - aic_quad, 1),
                "p_quadratic_term": float(p_quad_term),
                "lr_statistic": round(lr_stat, 3),
                "lr_p_value": float(lr_p),
                "quadratic_significant": lr_p < 0.05,
                "quadratic_coef": round(model_quad.params[2], 6),
                "n": len(rows),
            }
        )
        log.info(
            f"  {substance}: AIC linear={aic_lin:.1f} vs quad={aic_quad:.1f}, "
            f"LR p={lr_p:.4f}, quad coef={model_quad.params[2]:.6f}"
        )

    return results


# ---------------------------------------------------------------------------
# N4: Monitoring well target calculation
# ---------------------------------------------------------------------------


def compute_monitoring_well_target(power_info: dict, results: list[dict]) -> dict:
    """Estimate wells per catchment needed for 80% power at observed effect sizes.

    Uses the observation that correlations only appear in well-monitored catchments
    to compute minimum monitoring density needed for detection.
    """
    log.info("Computing monitoring well targets...")

    if not power_info or not results:
        return {}

    # Get observed effect sizes for key substances
    sig = [r for r in results if r.get("sig_fdr") and r["r"] > 0]
    if not sig:
        return {}

    # For 80% power with alpha=0.05, need n >= ((z_alpha + z_beta) / arctanh(r))^2 + 3
    z_alpha = 1.96
    z_beta = 0.842

    targets = {}
    for sub in sig:
        r_val = sub["r"]
        if r_val <= 0:
            continue
        z_r = np.arctanh(r_val)
        n_needed = int(np.ceil(((z_alpha + z_beta) / z_r) ** 2 + 3))

        # Estimate wells per catchment (from observation that >4 wells needed)
        # Average detection rate determines how many wells needed per catchment
        det_rate = sub["detection_rate"] / 100
        # Expected detections per well = det_rate
        # Need at least 1 expected detection per catchment for binary detection
        wells_for_detection = max(1, int(np.ceil(1 / det_rate))) if det_rate > 0 else None

        targets[sub["substance"]] = {
            "r": sub["r"],
            "detection_rate": sub["detection_rate"],
            "n_catchments_needed": n_needed,
            "current_n": sub["n_grukos"],
            "wells_per_catchment_for_detection": wells_for_detection,
        }

    log.info(f"  Computed targets for {len(targets)} substances")
    return targets


def print_results(
    tier1_results: list[dict],
    tier2_results: list[dict],
    tier3_results: list[dict],
    tier4_results: list[dict],
    mv_results: list[dict] | None = None,
    power_info: dict | None = None,
    strat_results: list[dict] | None = None,
    spatial_autocorr: dict | None = None,
    tfa_sensitivity: list[dict] | None = None,
    threshold_sensitivity: list[dict] | None = None,
    firth_results: list[dict] | None = None,
    biosolids_proxy: list[dict] | None = None,
    nonmonotonic: list[dict] | None = None,
    well_targets: dict | None = None,
) -> None:
    """Print PFAS-specific results tables."""

    # ─── TIER 1: TFA vs Fluorinated Pesticide Intensity ──────────────────
    print("\n" + "=" * 160)  # noqa: T201
    print("TIER 1: TFA vs FLUORINATED PESTICIDE INTENSITY")  # noqa: T201
    print("=" * 160)  # noqa: T201
    print(  # noqa: T201
        f"{'#':<3} {'Test':<45} {'Type':<20} {'r':>6} {'95% CI(r)':>16} "
        f"{'OR':>8} {'95% CI(OR)':>18} {'AUC':>5} {'q_FDR':>9} {'FDR':>3} {'Det%':>6} {'n':>5}"
    )
    print("-" * 160)  # noqa: T201

    for i, r in enumerate(tier1_results, 1):
        ci_r = f"[{r.get('r_ci_low', 0):.3f}, {r.get('r_ci_high', 0):.3f}]"
        or_val = r.get("logit_or", "")
        or_str = f"{or_val:.4f}" if or_val else "-"
        ci_or = ""
        if r.get("logit_or_ci_low") and r.get("logit_or_ci_high"):
            ci_or = f"[{r['logit_or_ci_low']:.4f}, {r['logit_or_ci_high']:.4f}]"
        else:
            ci_or = "-"
        fdr_str = f"{r.get('p_fdr', 1):.4f}" if r.get("p_fdr", 1) >= 0.001 else "<0.001"
        fdr_mark = "*" if r.get("sig_fdr") else " "
        auc_str = f"{r['logit_auc']:.2f}" if r.get("logit_auc") else "-"
        print(  # noqa: T201
            f"{i:<3} {r['substance']:<45} {r.get('type', ''):<20} {r['r']:>6.3f} {ci_r:>16} "
            f"{or_str:>8} {ci_or:>18} {auc_str:>5} {fdr_str:>9} {fdr_mark:>3} {r['detection_rate']:>5.1f}% {r['n_grukos']:>5}"
        )

    n_sig_t1 = sum(1 for r in tier1_results if r.get("sig_fdr") and r["r"] > 0)
    print(f"\n  Tier 1 FDR-significant: {n_sig_t1}/{len(tier1_results)}")  # noqa: T201

    # ─── TIER 2: Traditional PFAS vs Total Ag Intensity ──────────────────
    print("\n" + "=" * 160)  # noqa: T201
    print("TIER 2: TRADITIONAL PFAS (PFOS/PFOA/PFHxS/PFNA) vs TOTAL AGRICULTURAL INTENSITY")  # noqa: T201
    print("=" * 160)  # noqa: T201
    print(  # noqa: T201
        f"{'#':<3} {'Test':<45} {'Type':<20} {'r':>6} {'95% CI(r)':>16} "
        f"{'OR':>8} {'95% CI(OR)':>18} {'AUC':>5} {'q_FDR':>9} {'FDR':>3} {'Det%':>6} {'n':>5}"
    )
    print("-" * 160)  # noqa: T201

    for i, r in enumerate(tier2_results, 1):
        ci_r = f"[{r.get('r_ci_low', 0):.3f}, {r.get('r_ci_high', 0):.3f}]"
        or_val = r.get("logit_or", "")
        or_str = f"{or_val:.4f}" if or_val else "-"
        ci_or = ""
        if r.get("logit_or_ci_low") and r.get("logit_or_ci_high"):
            ci_or = f"[{r['logit_or_ci_low']:.4f}, {r['logit_or_ci_high']:.4f}]"
        else:
            ci_or = "-"
        fdr_str = f"{r.get('p_fdr', 1):.4f}" if r.get("p_fdr", 1) >= 0.001 else "<0.001"
        fdr_mark = "*" if r.get("sig_fdr") else " "
        auc_str = f"{r['logit_auc']:.2f}" if r.get("logit_auc") else "-"
        print(  # noqa: T201
            f"{i:<3} {r['substance']:<45} {r.get('type', ''):<20} {r['r']:>6.3f} {ci_r:>16} "
            f"{or_str:>8} {ci_or:>18} {auc_str:>5} {fdr_str:>9} {fdr_mark:>3} {r['detection_rate']:>5.1f}% {r['n_grukos']:>5}"
        )

    n_sig_t2 = sum(1 for r in tier2_results if r.get("sig_fdr") and r["r"] > 0)
    print(f"\n  Tier 2 FDR-significant: {n_sig_t2}/{len(tier2_results)}")  # noqa: T201

    # ─── TIER 3: Exploratory Screen ──────────────────────────────────────
    print("\n" + "=" * 160)  # noqa: T201
    print("TIER 3: EXPLORATORY SCREEN — ALL PFAS vs ALL INTENSITY MEASURES")  # noqa: T201
    print("=" * 160)  # noqa: T201

    # Split by intensity measure
    fluor_results = [r for r in tier3_results if r.get("intensity_measure") == "fluorinated_kg"]
    total_results = [r for r in tier3_results if r.get("intensity_measure") == "total_ag_kg"]

    for label, subset in [("vs Fluorinated Intensity", fluor_results), ("vs Total Ag Intensity", total_results)]:
        if not subset:
            continue
        print(f"\n  --- {label} ---")  # noqa: T201
        print(  # noqa: T201
            f"  {'#':<3} {'Substance':<30} {'Type':<22} {'r':>6} {'p_raw':>9} {'q_FDR':>9} {'FDR':>3} {'Det%':>6} {'n':>5}"
        )
        print(f"  {'-' * 130}")  # noqa: T201

        # Sort by |r| descending
        subset_sorted = sorted(subset, key=lambda x: abs(x["r"]), reverse=True)
        for i, r in enumerate(subset_sorted, 1):
            p_str = f"{r['p_value']:.4f}" if r["p_value"] >= 0.001 else "<0.001"
            fdr_str = f"{r.get('p_fdr', 1):.4f}" if r.get("p_fdr", 1) >= 0.001 else "<0.001"
            fdr_mark = "*" if r.get("sig_fdr") else " "
            sub_name = r["substance"].replace("_vs_total", "")
            print(  # noqa: T201
                f"  {i:<3} {sub_name:<30} {r.get('type', ''):<22} {r['r']:>6.3f} {p_str:>9} {fdr_str:>9} {fdr_mark:>3} "
                f"{r['detection_rate']:>5.1f}% {r['n_grukos']:>5}"
            )

    n_sig_t3 = sum(1 for r in tier3_results if r.get("sig_fdr") and r["r"] > 0)
    print(f"\n  Tier 3 FDR-significant: {n_sig_t3}/{len(tier3_results)}")  # noqa: T201

    # ─── TIER 4: Negative Controls ───────────────────────────────────────
    print("\n" + "=" * 130)  # noqa: T201
    print("TIER 4: NEGATIVE CONTROLS")  # noqa: T201
    print("=" * 130)  # noqa: T201

    for group_name, group_label in [
        ("high_koc_fluorinated", "HIGH-Koc FLUORINATED (expect no TFA correlation)"),
        ("non_agricultural", "NON-AGRICULTURAL PFAS (expect no ag correlation)"),
        ("atmospheric", "ATMOSPHERIC PFAS (expect no ag-specific correlation)"),
    ]:
        group_results = [r for r in tier4_results if r.get("control_group") == group_name]
        if not group_results:
            continue

        print(f"\n  --- {group_label} ---")  # noqa: T201
        print(f"  {'Test':<40} {'r':>7} {'p':>9} {'q_FDR':>9} {'Det%':>6} {'n':>5}  {'Result'}")  # noqa: T201
        print(f"  {'-' * 100}")  # noqa: T201
        for nc in group_results:
            r_str = f"{nc['r']:.3f}" if nc.get("r") is not None else "-"
            p_str = f"{nc['p_value']:.4f}" if nc.get("p_value") is not None else "-"
            q_str = f"{nc['q_fdr']:.3f}" if nc.get("q_fdr") is not None else "-"
            det_str = f"{nc.get('detection_rate', 0):.1f}%" if nc.get("detection_rate") else "-"
            n_str = f"{nc.get('n_grukos', 0)}" if nc.get("n_grukos") else "-"
            print(  # noqa: T201
                f"  {nc['substance']:<40} {r_str:>7} {p_str:>9} {q_str:>9} {det_str:>6} {n_str:>5}  {nc.get('note', '')}"
            )

    n_nonsig = sum(1 for r in tier4_results if r.get("note", "").startswith("as expected"))
    n_unexpected = sum(1 for r in tier4_results if r.get("note") == "UNEXPECTED")
    print(f"\n  Negative controls: {len(tier4_results)} tested, {n_nonsig} as expected, {n_unexpected} unexpected")  # noqa: T201

    # ─── DOSE-RESPONSE QUARTILE TABLE ────────────────────────────────────
    all_sig = [r for r in (tier1_results + tier2_results) if r.get("sig_fdr") and r["r"] > 0]
    if all_sig:
        print("\n" + "=" * 120)  # noqa: T201
        print("DOSE-RESPONSE QUARTILE TABLE (FDR-significant from Tier 1 & 2)")  # noqa: T201
        print("=" * 120)  # noqa: T201
        print(  # noqa: T201
            f"{'Substance':<45} {'Q1%':>6} {'Q2%':>6} {'Q3%':>6} {'Q4%':>6} {'Q4/Q1':>7} "
            f"{'Q1_n':>5} {'Q2_n':>5} {'Q3_n':>5} {'Q4_n':>5}"
        )
        print("-" * 120)  # noqa: T201
        for r in all_sig:
            qr = r.get("q_rates", {})
            if not qr:
                continue
            q4q1_str = f"{r['q4_q1']:.1f}x" if r.get("q4_q1") else "-"
            print(  # noqa: T201
                f"{r['substance']:<45} "
                f"{qr.get('q1_rate', '-'):>6} {qr.get('q2_rate', '-'):>6} "
                f"{qr.get('q3_rate', '-'):>6} {qr.get('q4_rate', '-'):>6} "
                f"{q4q1_str:>7} "
                f"{qr.get('q1_n', '-'):>5} {qr.get('q2_n', '-'):>5} "
                f"{qr.get('q3_n', '-'):>5} {qr.get('q4_n', '-'):>5}"
            )

    # ─── MULTIVARIATE REGRESSION TABLE ───────────────────────────────────
    if mv_results:
        print("\n" + "=" * 200)  # noqa: T201
        print("MULTIVARIATE LOGISTIC REGRESSION (controlling for soil_height, intake_depth, n_wells)")  # noqa: T201
        print("=" * 200)  # noqa: T201
        print(  # noqa: T201
            f"{'Substance':<45} {'Biv_r':>6} {'Biv_OR':>8} {'MV_OR':>8} {'MV_95%CI':>20} "
            f"{'p_int':>8} {'p_soil':>8} {'p_depth':>8} {'p_wells':>8} "
            f"{'AUC':>5} {'Nag.R2':>7} {'H-L p':>7} {'VIF':>5} {'EPV':>6} {'n':>5}"
        )
        print("-" * 200)  # noqa: T201
        for mv in mv_results:
            ci_str = ""
            if mv.get("mv_or_ci_low") and mv.get("mv_or_ci_high"):
                ci_str = f"[{mv['mv_or_ci_low']:.4f},{mv['mv_or_ci_high']:.4f}]"
            biv_or_str = f"{mv['bivariate_or']:.4f}" if mv.get("bivariate_or") else "-"

            def _fmt_p(val):
                if val is None:
                    return "-"
                return f"{val:.4f}" if val >= 0.001 else "<.001"

            auc_str = f"{mv['auc']:.2f}" if mv.get("auc") else "-"
            nag_str = f"{mv['nagelkerke_r2']:.3f}" if mv.get("nagelkerke_r2") else "-"
            hl_str = f"{mv['hl_p']:.3f}" if mv.get("hl_p") else "-"
            vif_str = f"{mv['vif_intensity']:.1f}" if mv.get("vif_intensity") else "-"
            epv_str = f"{mv['epv']:.0f}" if mv.get("epv") else "-"
            print(  # noqa: T201
                f"{mv['substance']:<45} {mv['bivariate_r']:>6.3f} {biv_or_str:>8} "
                f"{mv['mv_intensity_or']:>8.4f} {ci_str:>20} "
                f"{_fmt_p(mv['p_intensity']):>8} {_fmt_p(mv['p_soil']):>8} "
                f"{_fmt_p(mv['p_depth']):>8} {_fmt_p(mv['p_wells']):>8} "
                f"{auc_str:>5} {nag_str:>7} {hl_str:>7} {vif_str:>5} {epv_str:>6} {mv['n']:>5}"
            )

        n_still_sig = sum(1 for mv in mv_results if mv.get("p_intensity") is not None and mv["p_intensity"] < 0.05)
        print(f"\n  {n_still_sig}/{len(mv_results)} substances remain significant after covariate adjustment")  # noqa: T201

    # ─── POWER ANALYSIS ──────────────────────────────────────────────────
    if power_info:
        print("\n" + "=" * 80)  # noqa: T201
        print("POWER ANALYSIS")  # noqa: T201
        print("=" * 80)  # noqa: T201
        print(  # noqa: T201
            f"  Sample sizes: median={power_info['median_n']}, mean={power_info['mean_n']}, "
            f"range=[{power_info['min_n']}, {power_info['max_n']}]"
        )
        print(f"  Minimum detectable r (80% power, alpha=0.05): {power_info['r_min_80pct']:.4f}")  # noqa: T201
        print(f"  FDR-significant substances: {power_info['n_sig']}")  # noqa: T201
        print(f"    Strong effect (r >= 0.20): {power_info['n_strong']}")  # noqa: T201
        print(f"    Moderate effect (0.10 <= r < 0.20): {power_info['n_moderate']}")  # noqa: T201
        print(f"    Weak effect (r < 0.10): {power_info['n_weak']}")  # noqa: T201
        print(f"  {power_info['n_above_min']}/{power_info['n_sig']} exceed minimum detectable effect size")  # noqa: T201

    # ─── MONITORING DENSITY STRATIFIED ───────────────────────────────────
    if strat_results:
        print("\n" + "=" * 100)  # noqa: T201
        print("MONITORING DENSITY STRATIFIED CORRELATIONS")  # noqa: T201
        print("=" * 100)  # noqa: T201
        substances_seen = []
        for sr in strat_results:
            if sr["substance"] not in substances_seen:
                substances_seen.append(sr["substance"])

        print(f"  {'Substance':<45} {'Low (r)':>10} {'Medium (r)':>12} {'High (r)':>10}")  # noqa: T201
        print(f"  {'-' * 45} {'-' * 10} {'-' * 12} {'-' * 10}")  # noqa: T201
        for sname in substances_seen:
            sub_strats = {sr["tertile"]: sr for sr in strat_results if sr["substance"] == sname}
            low_r = f"{sub_strats['low']['r']:.3f}" if sub_strats.get("low", {}).get("r") is not None else "-"
            med_r = f"{sub_strats['medium']['r']:.3f}" if sub_strats.get("medium", {}).get("r") is not None else "-"
            high_r = f"{sub_strats['high']['r']:.3f}" if sub_strats.get("high", {}).get("r") is not None else "-"
            low_p = sub_strats.get("low", {}).get("p", 1)
            med_p = sub_strats.get("medium", {}).get("p", 1)
            high_p = sub_strats.get("high", {}).get("p", 1)
            low_sig = (
                "**" if low_p is not None and low_p < 0.01 else ("*" if low_p is not None and low_p < 0.05 else "")
            )
            med_sig = (
                "**" if med_p is not None and med_p < 0.01 else ("*" if med_p is not None and med_p < 0.05 else "")
            )
            high_sig = (
                "**" if high_p is not None and high_p < 0.01 else ("*" if high_p is not None and high_p < 0.05 else "")
            )
            print(f"  {sname:<45} {low_r:>8}{low_sig:<2} {med_r:>10}{med_sig:<2} {high_r:>8}{high_sig:<2}")  # noqa: T201

    # ─── SPATIAL AUTOCORRELATION (R1) ────────────────────────────────────
    if spatial_autocorr:
        print("\n" + "=" * 120)  # noqa: T201
        print("SPATIAL AUTOCORRELATION (Moran's I, k=8 nearest neighbours)")  # noqa: T201
        print("=" * 120)  # noqa: T201
        for label, surfaces in [
            ("Intensity Surfaces", "intensity_surfaces"),
            ("Detection Surfaces", "detection_surfaces"),
        ]:
            data = spatial_autocorr.get(surfaces, {})
            if not data:
                continue
            print(f"\n  --- {label} ---")  # noqa: T201
            print(  # noqa: T201
                f"  {'Variable':<40} {'I':>8} {'E(I)':>10} {'z':>8} {'p':>10} {'n':>6} {'n_eff':>7} {'Inflation':>10}"
            )
            print(f"  {'-' * 105}")  # noqa: T201
            for key, mi in data.items():
                if mi.get("I") is None:
                    continue
                p_str = f"{mi['p_value']:.6f}" if mi.get("p_value") is not None else "-"
                n_eff = f"{mi.get('n_effective', '-'):.0f}" if mi.get("n_effective") else "-"
                infl = f"{mi.get('inflation_factor', '-'):.2f}" if mi.get("inflation_factor") else "-"
                print(  # noqa: T201
                    f"  {mi.get('variable', key):<40} {mi['I']:>8.4f} {mi['E_I']:>10.6f} "
                    f"{mi['z_score']:>8.3f} {p_str:>10} {mi['n']:>6} {n_eff:>7} {infl:>10}"
                )

    # ─── TFA THRESHOLD SENSITIVITY (R3) ───────────────────────────────────
    if tfa_sensitivity:
        print("\n" + "=" * 100)  # noqa: T201
        print("TFA THRESHOLD SENSITIVITY ANALYSIS")  # noqa: T201
        print("=" * 100)  # noqa: T201
        # Distribution info
        dist = [t for t in tfa_sensitivity if t.get("analysis") == "tfa_distribution"]
        if dist:
            d = dist[0]
            print(  # noqa: T201
                f"  TFA concentration distribution: median={d.get('p50')} µg/L, P75={d.get('p75')}, "
                f"P90={d.get('p90')}, P95={d.get('p95')}, max={d.get('max')} µg/L"
            )
        # Threshold tests
        thresh = [t for t in tfa_sensitivity if t.get("analysis") == "tfa_threshold_sensitivity"]
        if thresh:
            print(  # noqa: T201
                f"\n  {'Threshold (µg/L)':<20} {'n':>6} {'Detected':>10} {'Det%':>8} {'r':>8} {'p':>10} {'95% CI':>20}"
            )
            print(f"  {'-' * 90}")  # noqa: T201
            for t in thresh:
                r_str = f"{t['r']:.3f}" if t.get("r") is not None else "N/A"
                p_str = f"{t['p_value']:.4f}" if t.get("p_value") is not None else "N/A"
                ci = f"[{t['r_ci_low']:.3f}, {t['r_ci_high']:.3f}]" if t.get("r_ci_low") else "-"
                print(  # noqa: T201
                    f"  {t['threshold_ugl']:<20.3f} {t['n_catchments']:>6} {t['n_detected']:>10} "
                    f"{t['detection_rate_pct']:>7.1f}% {r_str:>8} {p_str:>10} {ci:>20}"
                )

    # ─── DETECTION THRESHOLD SENSITIVITY (R4) ─────────────────────────────
    if threshold_sensitivity:
        print("\n" + "=" * 100)  # noqa: T201
        print("DETECTION THRESHOLD SENSITIVITY (key PFAS at alternative thresholds)")  # noqa: T201
        print("=" * 100)  # noqa: T201
        # Group by substance
        substances = sorted({t["substance"] for t in threshold_sensitivity})
        for sub in substances:
            sub_data = [t for t in threshold_sensitivity if t["substance"] == sub]
            print(  # noqa: T201
                f"\n  --- {sub} (base LOQ = {sub_data[0]['threshold_ugl'] / sub_data[0]['threshold_multiplier']:.4f}) ---"
            )
            print(  # noqa: T201
                f"  {'Multiplier':<12} {'Threshold':>12} {'n':>6} {'Det':>6} {'Det%':>8} {'r':>8} {'p':>10} {'Sig?':>5}"
            )
            print(f"  {'-' * 80}")  # noqa: T201
            for t in sub_data:
                r_str = f"{t['r']:.3f}" if t.get("r") is not None else "N/A"
                p_str = f"{t['p_value']:.4f}" if t.get("p_value") is not None else "N/A"
                sig_str = "*" if t.get("significant_005") else ""
                print(  # noqa: T201
                    f"  {t['threshold_multiplier']:<12.1f} {t['threshold_ugl']:>12.4f} {t['n_catchments']:>6} "
                    f"{t['n_detected']:>6} {t['detection_rate_pct']:>7.1f}% {r_str:>8} {p_str:>10} {sig_str:>5}"
                )

    # ─── FIRTH PENALIZED REGRESSION (R5) ──────────────────────────────────
    if firth_results:
        print("\n" + "=" * 130)  # noqa: T201
        print("FIRTH PENALIZED LOGISTIC REGRESSION (low-EPV associations)")  # noqa: T201
        print("=" * 130)  # noqa: T201
        print(  # noqa: T201
            f"  {'Substance':<35} {'EPV':>5} {'MLE OR':>10} {'MLE p':>10} {'Firth OR':>12} {'Firth p':>10} "
            f"{'Firth 95% CI':>24} {'Conv':>5} {'n':>6}"
        )
        print(f"  {'-' * 130}")  # noqa: T201
        for fr in firth_results:
            mle_p = f"{fr['mle_p']:.4f}" if fr.get("mle_p") is not None else "-"
            ci = f"[{fr['firth_ci_low']:.6f}, {fr['firth_ci_high']:.6f}]"
            conv = "Y" if fr.get("firth_converged") else "N"
            print(  # noqa: T201
                f"  {fr['substance']:<35} {fr.get('mle_epv', 0):>5.0f} {fr.get('mle_or', 0):>10.4f} {mle_p:>10} "
                f"{fr['firth_or']:>12.6f} {fr['firth_p']:>10.4f} {ci:>24} {conv:>5} {fr['n']:>6}"
            )

    # ─── BIOSOLIDS PROXY TEST (S6) ────────────────────────────────────────
    if biosolids_proxy:
        print("\n" + "=" * 110)  # noqa: T201
        print("BIOSOLIDS PROXY TEST (clay-rich soil × PFAS detection)")  # noqa: T201, RUF001
        print("=" * 110)  # noqa: T201
        print(  # noqa: T201
            f"  {'Substance':<25} {'n_sandy':>8} {'n_clay':>8} {'Det% sandy':>12} {'Det% clay':>12} "
            f"{'χ²':>8} {'p':>10} {'Clay higher?':>13}"
        )
        print(f"  {'-' * 110}")  # noqa: T201
        for bp in biosolids_proxy:
            clay_str = "YES" if bp.get("clay_higher") else "no"
            p_str = f"{bp['chi_p']:.4f}" if bp.get("chi_p") is not None else "-"
            print(  # noqa: T201
                f"  {bp['substance']:<25} {bp['n_sandy']:>8} {bp['n_clay']:>8} "
                f"{bp.get('det_rate_sandy_pct', 0):>11.1f}% {bp.get('det_rate_clay_pct', 0):>11.1f}% "
                f"{bp['chi2']:>8.3f} {p_str:>10} {clay_str:>13}"
            )

    # ─── NON-MONOTONIC DOSE-RESPONSE (N1) ─────────────────────────────────
    if nonmonotonic:
        print("\n" + "=" * 110)  # noqa: T201
        print("NON-MONOTONIC DOSE-RESPONSE TEST (quadratic term in logistic regression)")  # noqa: T201
        print("=" * 110)  # noqa: T201
        print(  # noqa: T201
            f"  {'Substance':<20} {'AIC linear':>12} {'AIC quad':>12} {'ΔAIC':>8} {'LR stat':>10} {'LR p':>10} "
            f"{'Quad sig?':>10} {'Quad coef':>12}"
        )
        print(f"  {'-' * 100}")  # noqa: T201
        for nm in nonmonotonic:
            sig_str = "YES *" if nm.get("quadratic_significant") else "no"
            print(  # noqa: T201
                f"  {nm['substance']:<20} {nm['aic_linear']:>12.1f} {nm['aic_quadratic']:>12.1f} "
                f"{nm['aic_improvement']:>8.1f} {nm['lr_statistic']:>10.3f} {nm['lr_p_value']:>10.4f} "
                f"{sig_str:>10} {nm['quadratic_coef']:>12.6f}"
            )

    # ─── MONITORING WELL TARGETS (N4) ─────────────────────────────────────
    if well_targets:
        print("\n" + "=" * 100)  # noqa: T201
        print("MONITORING WELL TARGETS (for 80% power at observed effect sizes)")  # noqa: T201
        print("=" * 100)  # noqa: T201
        print(f"  {'Substance':<40} {'r':>6} {'Det%':>6} {'n_needed':>10} {'n_current':>10} {'Wells/catch':>12}")  # noqa: T201
        print(f"  {'-' * 90}")  # noqa: T201
        for sub, tgt in well_targets.items():
            wells_str = (
                f"{tgt['wells_per_catchment_for_detection']}" if tgt.get("wells_per_catchment_for_detection") else "-"
            )
            print(  # noqa: T201
                f"  {sub:<40} {tgt['r']:>6.3f} {tgt['detection_rate']:>5.1f}% "
                f"{tgt['n_catchments_needed']:>10} {tgt['current_n']:>10} {wells_str:>12}"
            )

    # ─── SUMMARY ─────────────────────────────────────────────────────────
    print("\n" + "=" * 80)  # noqa: T201
    print("SUMMARY")  # noqa: T201
    print("=" * 80)  # noqa: T201
    n_sig_t1 = sum(1 for r in tier1_results if r.get("sig_fdr") and r["r"] > 0)
    n_sig_t2 = sum(1 for r in tier2_results if r.get("sig_fdr") and r["r"] > 0)
    n_sig_t3 = sum(1 for r in tier3_results if r.get("sig_fdr") and r["r"] > 0)
    n_nonsig_t4 = sum(1 for r in tier4_results if r.get("note", "").startswith("as expected"))
    n_unexpected_t4 = sum(1 for r in tier4_results if r.get("note") == "UNEXPECTED")
    print(f"  Tier 1 (TFA vs fluorinated): {n_sig_t1}/{len(tier1_results)} FDR-significant")  # noqa: T201
    print(f"  Tier 2 (Traditional PFAS vs total ag): {n_sig_t2}/{len(tier2_results)} FDR-significant")  # noqa: T201
    print(f"  Tier 3 (Exploratory screen): {n_sig_t3}/{len(tier3_results)} FDR-significant")  # noqa: T201
    print(f"  Tier 4 (Negative controls): {n_nonsig_t4} as expected, {n_unexpected_t4} unexpected")  # noqa: T201
    if mv_results:
        n_mv_sig = sum(1 for mv in mv_results if mv.get("p_intensity") is not None and mv["p_intensity"] < 0.05)
        print(f"  Multivariate: {n_mv_sig}/{len(mv_results)} retain significance after covariate adjustment")  # noqa: T201
    if spatial_autocorr:
        int_surfs = spatial_autocorr.get("intensity_surfaces", {})
        sig_autocorr = sum(1 for mi in int_surfs.values() if mi.get("p_value") is not None and mi["p_value"] < 0.05)
        print(  # noqa: T201
            f"  Spatial autocorrelation: {sig_autocorr}/{len(int_surfs)} intensity surfaces show significant clustering"
        )
    if firth_results:
        n_firth_sig = sum(1 for fr in firth_results if fr.get("firth_p") is not None and fr["firth_p"] < 0.05)
        print(f"  Firth regression: {n_firth_sig}/{len(firth_results)} retain significance after penalization")  # noqa: T201


def main():
    parser = argparse.ArgumentParser(description="Verify PFAS groundwater correlation analysis")
    parser.add_argument("--dry-run", action="store_true", help="Only discover data, don't run analysis")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument(
        "--export-json",
        type=str,
        default=None,
        help="Export results to JSON file",
    )
    parser.add_argument(
        "--tier",
        type=int,
        choices=[1, 2, 3, 4],
        default=None,
        help="Run only a specific tier (1-4). Default: run all.",
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.info("=" * 60)
    log.info("PFAS Groundwater Correlation Verification Script")
    log.info("=" * 60)

    conn = get_connection()
    loader = DataLoader(conn)

    try:
        info = discover_data(loader)

        if args.dry_run:
            log.info("\n--dry-run: stopping after data discovery.")
            return

        if info.get("pfas_rows", 0) == 0:
            log.error("No PFAS data found. Cannot proceed.")
            sys.exit(1)
        if info.get("grukos_count", 0) == 0:
            log.error("No GRUKOS data found. Cannot proceed.")
            sys.exit(1)

        pfas_source = info.get("pfas_source", "geus_clean_all")
        load_data(loader, pfas_source)

        # Build fluorinated ingredient list (auto-discover from BMD)
        fluorinated_set = build_fluorinated_ingredient_list(conn)

        # Build intensity tables
        build_gruko_application_intensity(conn)

        # Build soil transit and detection tables
        build_gruko_soil_transit(conn)
        build_gruko_detections(conn)

        # Build covariates
        build_gruko_covariates(conn)

        # Run analysis tiers
        tier1_results = []
        tier2_results = []
        tier3_results = []
        tier4_results = []
        mv_results = []
        strat_results = []

        if args.tier is None or args.tier == 1:
            tier1_results = run_tier1_tfa_correlations(conn)

        if args.tier is None or args.tier == 2:
            tier2_results = run_tier2_traditional_pfas(conn)

        if args.tier is None or args.tier == 3:
            tier3_results = run_tier3_exploratory_screen(conn)

        if args.tier is None or args.tier == 4:
            tier4_results = run_tier4_negative_controls(conn)

        # Multivariate logistic regression on significant Tier 1-3 results
        all_bivariate = tier1_results + tier2_results + tier3_results
        if all_bivariate:
            mv_results = run_multivariate_logistic(conn, all_bivariate)

        # Monitoring density stratified analysis
        if all_bivariate:
            strat_results = run_monitoring_density_stratified(conn, all_bivariate)

        # Power analysis across all results
        all_results = tier1_results + tier2_results + tier3_results
        power_info = run_power_analysis(all_results) if all_results else {}

        # --- Revision analyses (R1-R5, S6, N1, N4) ---

        # R1: Spatial autocorrelation
        spatial_autocorr = run_spatial_autocorrelation(conn, all_bivariate) if all_bivariate else {}

        # R3: TFA threshold sensitivity
        tfa_sensitivity = run_tfa_threshold_sensitivity(conn)

        # R4: Detection threshold sensitivity
        threshold_sensitivity = run_detection_threshold_sensitivity(conn)

        # R5: Firth penalized regression for low-EPV associations
        firth_results = run_firth_regression(conn, mv_results) if mv_results else []

        # S6: Biosolids proxy test
        biosolids_proxy = run_biosolids_proxy_test(conn, all_bivariate) if all_bivariate else []

        # N1: Non-monotonic dose-response test
        nonmonotonic = run_nonmonotonic_test(conn)

        # N4: Monitoring well targets
        well_targets = compute_monitoring_well_target(power_info, all_results) if power_info else {}

        # Print results
        print_results(
            tier1_results,
            tier2_results,
            tier3_results,
            tier4_results,
            mv_results,
            power_info,
            strat_results,
            spatial_autocorr=spatial_autocorr,
            tfa_sensitivity=tfa_sensitivity,
            threshold_sensitivity=threshold_sensitivity,
            firth_results=firth_results,
            biosolids_proxy=biosolids_proxy,
            nonmonotonic=nonmonotonic,
            well_targets=well_targets,
        )

        # Export JSON if requested
        if args.export_json:
            from datetime import datetime

            class NumpyEncoder(json.JSONEncoder):
                def default(self, obj):
                    if hasattr(obj, "item"):
                        return obj.item()
                    if hasattr(obj, "tolist"):
                        return obj.tolist()
                    return super().default(obj)

            export_data = {
                "metadata": {
                    "script": "verify_pfas_groundwater_correlations.py",
                    "application_years": APPLICATION_YEARS,
                    "tfa_detection_start": TFA_DETECTION_YEAR_START,
                    "traditional_pfas_detection_start": TRADITIONAL_PFAS_DETECTION_YEAR_START,
                    "min_detections": MIN_DETECTIONS,
                    "n_fluorinated_ingredients": len(fluorinated_set),
                    "timestamp": datetime.now().isoformat(),
                },
                "tier1_results": tier1_results,
                "tier2_results": tier2_results,
                "tier3_results": tier3_results,
                "tier4_results": tier4_results,
                "mv_results": mv_results,
                "power_info": power_info,
                "strat_results": strat_results,
                "spatial_autocorrelation": spatial_autocorr,
                "tfa_threshold_sensitivity": tfa_sensitivity,
                "detection_threshold_sensitivity": threshold_sensitivity,
                "firth_regression": firth_results,
                "biosolids_proxy": biosolids_proxy,
                "nonmonotonic_test": nonmonotonic,
                "monitoring_well_targets": well_targets,
            }
            with Path(args.export_json).open("w") as f:
                json.dump(export_data, f, cls=NumpyEncoder, indent=2)
            log.info(f"Exported results to {args.export_json}")

    finally:
        loader.cleanup()
        conn.close()

    log.info("\nDone.")


if __name__ == "__main__":
    main()
