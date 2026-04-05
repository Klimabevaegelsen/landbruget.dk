#!/usr/bin/env python3
"""Generate publication-quality figures for the groundwater correlation paper."""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# Backend path for common imports
sys.path.insert(0, str(Path(__file__).parent.parent))

FIGURES_DIR = Path(__file__).parent / "reports" / "figures"
JSON_PATH = Path(__file__).parent / "reports" / "groundwater_results.json"
BUCKET = "landbruget-data"

log = logging.getLogger(__name__)

# Danish GEUS names → English display labels
DISPLAY_NAMES = {
    "(Aminomethyl)phosphonsyre": "AMPA",
    "Glyphosat": "Glyphosate",
    "Bentazon": "Bentazon",
    "2,4-Dichlorphenoxyeddikesyre": "2,4-D",
    "4-Chlor-2-methylphenol": "4-Chloro-2-methylphenol",
    "1,2,4-Triazol": "1,2,4-Triazole",
    "Pendimethalin": "Pendimethalin",
    "2,4-Dichlorphenol": "2,4-Dichlorophenol",
    "MCPA": "MCPA",
    "Ethylenthiourea": "Ethylenethiourea (ETU)",
    "N,N-Dimethylsulfamid": "DMS",
    "CGA 108906": "CGA 108906",
    "TFMP": "TFMP",
    "N-(2,6-dimethylphenyl)-N-(methoxyacetyl)alanin": "Metalaxyl",
    "Diflufenican": "Diflufenican",
    "Prosulfocarb": "Prosulfocarb",
    "Propiconazol": "Propiconazole",
    "Epoxiconazol": "Epoxiconazole",
    "Boscalid": "Boscalid",
}


def dn(name):
    """Get display name for a substance."""
    return DISPLAY_NAMES.get(name, name)


def setup_style():
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.size": 8,
            "axes.titlesize": 9,
            "axes.labelsize": 8,
            "xtick.labelsize": 7,
            "ytick.labelsize": 7,
            "legend.fontsize": 7,
            "figure.dpi": 300,
            "savefig.dpi": 300,
            "savefig.bbox": "tight",
            "savefig.pad_inches": 0.1,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.linewidth": 0.6,
            "xtick.major.width": 0.6,
            "ytick.major.width": 0.6,
            "lines.linewidth": 1.0,
        }
    )


def load_results(json_path):
    with Path(json_path).open() as f:
        return json.load(f)


def save(fig, name):
    for ext in ("png", "pdf"):
        fig.savefig(FIGURES_DIR / f"{name}.{ext}")
    plt.close(fig)
    print(f"  Saved {name}.png/.pdf")  # noqa: T201


# ---------------------------------------------------------------------------
# Figure 1: Study area map (MAIN FIGURE — requires R2 spatial data)
# ---------------------------------------------------------------------------


APPLICATION_YEARS = [2015, 2016, 2017]


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
    import subprocess
    import urllib.request

    # Force token refresh
    subprocess.run(["wrangler", "whoami"], capture_output=True, text=True, timeout=15)

    account_id = os.getenv("R2_ACCOUNT_ID", "a5f130bfd0d34de38f8e77f6a0f40a27")
    token = _get_wrangler_token()

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
        f"/r2/buckets/{BUCKET}/objects?prefix={prefix}&limit=1000"
    )
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})  # noqa: S310
    with urllib.request.urlopen(req) as resp:  # noqa: S310
        resp_data = json.loads(resp.read())

    if not resp_data.get("success"):
        raise RuntimeError(f"Cloudflare API error: {resp_data.get('errors')}")

    result = resp_data.get("result", [])
    if isinstance(result, dict):
        objects = result.get("objects", [])
    elif isinstance(result, list):
        objects = result
    else:
        objects = []

    return [obj["key"] for obj in objects if obj.get("key", "").endswith(".parquet")]


def _wrangler_download(r2_key: str, local_path: str) -> None:
    """Download a single object from R2 via wrangler CLI."""
    import subprocess

    cmd = ["wrangler", "r2", "object", "get", f"{BUCKET}/{r2_key}", "--file", local_path, "--remote"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"wrangler download failed for {r2_key}: {result.stderr}")


def _load_r2_parquet(conn, r2_prefix: str, table_name: str, cache_dir: Path, extra_select: str = "*") -> int:
    """Download parquet from R2 via wrangler and load into DuckDB table. Returns row count."""
    local_dir = cache_dir / r2_prefix.replace("/", "_")
    local_dir.mkdir(parents=True, exist_ok=True)

    list_prefix = r2_prefix.rstrip("/") + "/"
    print(f"    Listing {list_prefix}...")  # noqa: T201
    keys = _wrangler_list_prefix(list_prefix)
    parquet_keys = [k for k in keys if k.endswith(".parquet") and k.startswith(list_prefix)]

    if not parquet_keys:
        raise RuntimeError(f"No parquet files found under {r2_prefix}")

    # Prefer root-level file, else latest timestamped
    root_files = [k for k in parquet_keys if "/" not in k[len(list_prefix) :]]
    parquet_keys = [root_files[-1]] if root_files else [sorted(parquet_keys)[-1]]

    print(f"    Using: {parquet_keys[0]}")  # noqa: T201

    local_files = []
    for key in parquet_keys:
        fname = key.replace("/", "_")
        local_path = str(local_dir / fname)
        if not Path(local_path).exists():
            print(f"    Downloading {key}...")  # noqa: T201
            _wrangler_download(key, local_path)
        else:
            print(f"    Cached: {fname}")  # noqa: T201
        local_files.append(local_path)

    if len(local_files) == 1:
        sql = f"CREATE TABLE {table_name} AS SELECT {extra_select} FROM read_parquet('{local_files[0]}')"
    else:
        paths = ", ".join(f"'{f}'" for f in local_files)
        sql = f"CREATE TABLE {table_name} AS SELECT {extra_select} FROM read_parquet([{paths}])"

    conn.execute(sql)
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def figure1_study_area_map(data):
    """Three-panel study area map: (a) well density, (b) monitoring coverage, (c) glyphosate intensity."""
    import shutil
    import tempfile

    import duckdb

    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial")

    cache_dir = Path(tempfile.mkdtemp(prefix="fig1_"))
    print(f"  Cache dir: {cache_dir}")  # noqa: T201

    # ---- Load GRUKO polygons ----
    print("  Loading GRUKO polygons...")  # noqa: T201
    try:
        n = _load_r2_parquet(conn, "silver/grukos", "grukos_raw", cache_dir)
    except Exception as e:
        print(f"  ERROR: Could not load GRUKO data: {e}")  # noqa: T201
        conn.close()
        return

    conn.execute("ALTER TABLE grukos_raw ADD COLUMN gruko_id VARCHAR")
    conn.execute("UPDATE grukos_raw SET gruko_id = id")
    print(f"  Loaded {n:,} GRUKO polygons")  # noqa: T201

    # ---- Load GEUS borehole data ----
    print("  Loading GEUS borehole data...")  # noqa: T201
    try:
        n_geus = _load_r2_parquet(conn, "silver/geus_dataverse_pesticides", "geus_raw", cache_dir)
        print(f"  Loaded {n_geus:,} GEUS records")  # noqa: T201
    except Exception as e:
        print(f"  WARNING: Could not load GEUS data ({e}). Falling back to empty.")  # noqa: T201
        conn.execute("CREATE TABLE geus_raw (dgu_nr VARCHAR, x DOUBLE, y DOUBLE)")

    # ---- Compute well density per GRUKO (spatial join) ----
    print("  Computing well density per GRUKO...")  # noqa: T201
    try:
        conn.execute("""
            CREATE TABLE gruko_wells AS
            SELECT g.gruko_id,
                   COUNT(DISTINCT gg.dgu_nr) as n_wells
            FROM grukos_raw g
            LEFT JOIN (
                SELECT DISTINCT dgu_nr, x, y
                FROM geus_raw
                WHERE x IS NOT NULL AND y IS NOT NULL
            ) gg ON ST_Within(ST_Point(gg.x, gg.y), g.geometry_spatial)
            GROUP BY g.gruko_id
        """)
    except Exception as e:
        print(f"  WARNING: Spatial join failed ({e}). Setting n_wells=0.")  # noqa: T201
        conn.execute("CREATE TABLE gruko_wells AS SELECT gruko_id, 0 as n_wells FROM grukos_raw")

    # ---- Load disaggregation data for glyphosate intensity ----
    has_intensity = False
    print("  Loading pesticide disaggregation data...")  # noqa: T201
    try:
        parts = []
        for year in APPLICATION_YEARS:
            tbl = f"_disagg_{year}"
            n_d = _load_r2_parquet(conn, f"gold/pesticide_disaggregation_{year}_{year + 1}", tbl, cache_dir)
            parts.append(f"SELECT *, {year} as application_year FROM {tbl}")
            print(f"    Year {year}: {n_d:,} records")  # noqa: T201
        conn.execute(f"CREATE TABLE disagg_raw AS {' UNION ALL '.join(parts)}")
        for year in APPLICATION_YEARS:
            conn.execute(f"DROP TABLE _disagg_{year}")
    except Exception as e:
        print(f"  WARNING: Could not load disaggregation data ({e}). Panel (c) will be skipped.")  # noqa: T201
        disagg_loaded = False
    else:
        disagg_loaded = True

    if disagg_loaded:
        # ---- Load BMD product→active ingredient mapping ----
        print("  Loading BMD product mapping...")  # noqa: T201
        try:
            _load_r2_parquet(conn, "silver/bmd", "bmd_raw", cache_dir)

            conn.execute("""
                CREATE TABLE _bmd_numbered AS
                SELECT
                    CAST(registrerings_nr AS VARCHAR) as reg_nr,
                    string_split(aktivstofnavn_e, ';') as ingredients,
                    string_split(koncentration_er, ';') as concentrations
                FROM bmd_raw
                WHERE aktivstofnavn_e IS NOT NULL AND aktivstofnavn_e != ''
            """)
            conn.execute("""
                CREATE TABLE bmd_ingredients AS
                SELECT
                    reg_nr,
                    LOWER(TRIM(ingredients[i])) as active_ingredient,
                    TRY_CAST(REPLACE(TRIM(concentrations[i]), ',', '.') AS DOUBLE) as concentration_g
                FROM _bmd_numbered,
                     generate_series(1, GREATEST(len(ingredients), 1)) t(i)
                WHERE i <= len(ingredients)
            """)
            conn.execute("DROP TABLE _bmd_numbered")
        except Exception as e:
            print(f"  WARNING: Could not load BMD data ({e}). Panel (c) will be skipped.")  # noqa: T201
            disagg_loaded = False

    if disagg_loaded:
        # ---- Load field-GRUKO intersections (pre-computed or compute) ----
        print("  Loading field-GRUKO intersections...")  # noqa: T201
        field_years = [y + 1 for y in APPLICATION_YEARS]  # Y+1 pattern
        fg_parts = []
        missing_years = []

        for field_year in field_years:
            tbl = f"_fg_{field_year}"
            try:
                _load_r2_parquet(
                    conn,
                    f"gold/field_analysis_field_grukos_intersections_{field_year}",
                    tbl,
                    cache_dir,
                )
                fg_parts.append(
                    f"SELECT field_uuid, grukos_id as gruko_id, field_grukos_geometry as geometry FROM {tbl}"
                )
                print(f"    Pre-computed intersections for {field_year}: loaded")  # noqa: T201
            except Exception:
                missing_years.append(field_year)

        # For missing years, compute intersections from field geometries
        if missing_years:
            print(f"  Computing intersections for years: {missing_years}")  # noqa: T201
            fld_parts = []
            for field_year in missing_years:
                tbl = f"_fields_{field_year}"
                try:
                    _load_r2_parquet(
                        conn,
                        f"silver/fvm_marker_{field_year}",
                        tbl,
                        cache_dir,
                        extra_select="field_uuid, geometry",
                    )
                    n_f = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                    print(f"    fvm_marker_{field_year}: {n_f:,} fields")  # noqa: T201
                    fld_parts.append(f"SELECT DISTINCT field_uuid, geometry FROM {tbl}")
                except Exception as e:
                    print(f"    fvm_marker_{field_year} not available: {e}")  # noqa: T201

            if fld_parts:
                conn.execute(f"CREATE TABLE _all_fields AS {' UNION ALL '.join(fld_parts)}")
                conn.execute("""
                    CREATE TABLE _unique_fields AS
                    SELECT field_uuid, FIRST(geometry) as geometry
                    FROM _all_fields GROUP BY field_uuid
                """)
                conn.execute("DROP TABLE _all_fields")

                print("  Computing ST_Intersection (field x GRUKO)... this may take a few minutes")  # noqa: T201
                conn.execute("""
                    CREATE TABLE _computed_fg AS
                    SELECT f.field_uuid, g.gruko_id,
                           ST_Intersection(f.geometry, g.geometry_spatial) as geometry
                    FROM _unique_fields f
                    JOIN grukos_raw g ON ST_Intersects(f.geometry, g.geometry_spatial)
                """)
                n_fg = conn.execute("SELECT COUNT(*) FROM _computed_fg").fetchone()[0]
                print(f"  Computed {n_fg:,} field-GRUKO intersections")  # noqa: T201
                fg_parts.append("SELECT field_uuid, gruko_id, geometry FROM _computed_fg")
                conn.execute("DROP TABLE _unique_fields")

        if fg_parts:
            conn.execute(f"""
                CREATE TABLE field_gruko_intersections AS
                {" UNION ALL ".join(fg_parts)}
            """)
            # Clean up temp tables
            for field_year in field_years:
                conn.execute(f"DROP TABLE IF EXISTS _fg_{field_year}")
                conn.execute(f"DROP TABLE IF EXISTS _fields_{field_year}")
            conn.execute("DROP TABLE IF EXISTS _computed_fg")

            # ---- Map disagg to active ingredients via BMD ----
            print("  Mapping products to active ingredients...")  # noqa: T201
            conn.execute("""
                CREATE TABLE disagg_with_ingredient AS
                SELECT d.*, b.active_ingredient,
                       d.DosageQuantity * b.concentration_g / 1000.0 as ingredient_dosage_kg
                FROM disagg_raw d
                JOIN bmd_ingredients b ON CAST(d.PesticideRegistrationNumber AS VARCHAR) = b.reg_nr
                WHERE b.concentration_g IS NOT NULL AND b.concentration_g > 0
            """)

            # ---- Build GRUKO-level glyphosate intensity ----
            print("  Aggregating glyphosate intensity per GRUKO...")  # noqa: T201
            conn.execute("""
                CREATE TABLE field_gruko_join AS
                SELECT d.*, fg.gruko_id,
                       ST_Area(fg.geometry) / 10000.0 as intersection_area_ha,
                       CASE
                           WHEN d.AllocatedArea > 0
                           THEN d.ingredient_dosage_kg * (ST_Area(fg.geometry) / 10000.0) / d.AllocatedArea
                           ELSE 0
                       END as kg_in_gruko
                FROM disagg_with_ingredient d
                JOIN field_gruko_intersections fg ON d.field_uuid = fg.field_uuid
            """)

            conn.execute("""
                CREATE TABLE gruko_glyph AS
                SELECT gruko_id,
                       SUM(kg_in_gruko) as total_kg,
                       SUM(kg_in_gruko) / NULLIF(SUM(intersection_area_ha), 0) as kg_per_ha
                FROM field_gruko_join
                WHERE LOWER(active_ingredient) = 'glyphosat'
                  AND intersection_area_ha > 0 AND kg_in_gruko > 0
                GROUP BY gruko_id
            """)
            n_glyph = conn.execute("SELECT COUNT(*) FROM gruko_glyph").fetchone()[0]
            print(f"  Glyphosate intensity: {n_glyph:,} GRUKOs with data")  # noqa: T201
            has_intensity = True
        else:
            print("  WARNING: No field-GRUKO intersections available. Panel (c) skipped.")  # noqa: T201

    # ---- Build GeoDataFrame ----
    print("  Building GeoDataFrame...")  # noqa: T201
    import geopandas as gpd
    from shapely import wkb

    if has_intensity:
        rows = conn.execute("""
            SELECT g.gruko_id,
                   ST_AsWKB(ST_Simplify(g.geometry_spatial, 100)) as geom_wkb,
                   COALESCE(w.n_wells, 0) as n_wells,
                   COALESCE(gl.kg_per_ha, 0) as glyph_kg_per_ha
            FROM grukos_raw g
            LEFT JOIN gruko_wells w ON g.gruko_id = w.gruko_id
            LEFT JOIN gruko_glyph gl ON g.gruko_id = gl.gruko_id
            WHERE g.geometry_spatial IS NOT NULL
        """).fetchall()
    else:
        rows = conn.execute("""
            SELECT g.gruko_id,
                   ST_AsWKB(ST_Simplify(g.geometry_spatial, 100)) as geom_wkb,
                   COALESCE(w.n_wells, 0) as n_wells,
                   0 as glyph_kg_per_ha
            FROM grukos_raw g
            LEFT JOIN gruko_wells w ON g.gruko_id = w.gruko_id
            WHERE g.geometry_spatial IS NOT NULL
        """).fetchall()

    conn.close()

    gruko_ids = [r[0] for r in rows]
    geometries = [wkb.loads(bytes(r[1])) for r in rows]
    n_wells_list = [r[2] for r in rows]
    glyph_list = [r[3] for r in rows]
    gdf = gpd.GeoDataFrame(
        {"gruko_id": gruko_ids, "n_wells": n_wells_list, "glyph_kg_per_ha": glyph_list},
        geometry=geometries,
        crs="EPSG:25832",
    )

    gdf["sampled"] = gdf["n_wells"] > 0
    n_sampled = int(gdf["sampled"].sum())
    pct_sampled = 100 * n_sampled / len(gdf)
    print(f"  GRUKOs with wells: {n_sampled:,} / {len(gdf):,} ({pct_sampled:.1f}%)")  # noqa: T201

    # ---- Render 3-panel (or 2-panel) figure ----
    import matplotlib.patches as mpatches
    from matplotlib.colors import Normalize

    n_panels = 3 if has_intensity else 2
    fig, axes = plt.subplots(1, n_panels, figsize=(3.5 * n_panels, 5))
    if n_panels == 2:
        axes = list(axes)
    print(f"  Rendering {n_panels}-panel map...")  # noqa: T201

    # Panel (a): Well density
    ax = axes[0]
    gdf["n_wells_cap"] = gdf["n_wells"].clip(upper=20)
    gdf.plot(
        column="n_wells_cap",
        cmap="YlOrRd",
        linewidth=0.1,
        edgecolor="gray",
        ax=ax,
        legend=False,
        missing_kwds={"color": "lightgray"},
    )
    sm_obj = plt.cm.ScalarMappable(cmap="YlOrRd", norm=Normalize(vmin=0, vmax=20))
    sm_obj.set_array([])
    cbar = fig.colorbar(sm_obj, ax=ax, shrink=0.6, pad=0.02)
    cbar.set_label("Wells per catchment", fontsize=7)
    cbar.ax.tick_params(labelsize=6)
    ax.set_title(f"(a) Monitoring well density\n(n = {len(gdf):,} catchments)", fontsize=8)
    ax.set_axis_off()

    # Panel (b): Sampled vs unsampled
    ax = axes[1]
    gdf_unsampled = gdf[~gdf["sampled"]]
    gdf_sampled = gdf[gdf["sampled"]]
    if len(gdf_unsampled) > 0:
        gdf_unsampled.plot(ax=ax, color="#d5dbdb", linewidth=0.1, edgecolor="gray")
    if len(gdf_sampled) > 0:
        gdf_sampled.plot(ax=ax, color="#2980b9", linewidth=0.1, edgecolor="gray")
    legend_elements = [
        mpatches.Patch(facecolor="#2980b9", edgecolor="gray", label=f"Sampled ({n_sampled:,}, {pct_sampled:.0f}%)"),
        mpatches.Patch(facecolor="#d5dbdb", edgecolor="gray", label=f"Unsampled ({len(gdf) - n_sampled:,})"),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=6, framealpha=0.9)
    ax.set_title("(b) GEUS monitoring coverage", fontsize=8)
    ax.set_axis_off()

    # Panel (c): Glyphosate intensity (kg a.i. per ha)
    if has_intensity:
        ax = axes[2]
        gdf_zero = gdf[gdf["glyph_kg_per_ha"] <= 0]
        gdf_pos = gdf[gdf["glyph_kg_per_ha"] > 0]
        gdf_pos = gdf_pos.copy()
        gdf_pos["glyph_log"] = np.log10(gdf_pos["glyph_kg_per_ha"].clip(lower=1e-4))

        if len(gdf_zero) > 0:
            gdf_zero.plot(ax=ax, color="#f0f0f0", linewidth=0.1, edgecolor="gray")
        if len(gdf_pos) > 0:
            vmin = gdf_pos["glyph_log"].quantile(0.05)
            vmax = gdf_pos["glyph_log"].quantile(0.95)
            gdf_pos.plot(
                column="glyph_log",
                cmap="YlGn",
                vmin=vmin,
                vmax=vmax,
                linewidth=0.1,
                edgecolor="gray",
                ax=ax,
                legend=False,
            )
            sm_obj = plt.cm.ScalarMappable(cmap="YlGn", norm=Normalize(vmin=vmin, vmax=vmax))
            sm_obj.set_array([])
            cbar = fig.colorbar(sm_obj, ax=ax, shrink=0.6, pad=0.02)
            cbar.set_label("log₁₀(kg a.i. / ha)", fontsize=7)
            cbar.ax.tick_params(labelsize=6)
        n_with = len(gdf_pos)
        ax.set_title(f"(c) Glyphosate application intensity\n(2015-2017, {n_with:,} catchments)", fontsize=8)
        ax.set_axis_off()

    # Scale bar on last panel — 50 km in EPSG:25832 (meters)
    ax_last = axes[-1]
    xlim = ax_last.get_xlim()
    ylim = ax_last.get_ylim()
    bar_x = xlim[0] + 0.05 * (xlim[1] - xlim[0])
    bar_y = ylim[0] + 0.05 * (ylim[1] - ylim[0])
    ax_last.plot([bar_x, bar_x + 50000], [bar_y, bar_y], "k-", linewidth=1.5)
    ax_last.text(bar_x + 25000, bar_y + 5000, "50 km", ha="center", fontsize=6)

    fig.suptitle("Figure 1: Study area — Danish groundwater catchments", fontsize=9, y=0.98)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    save(fig, "figure_1_study_area_map")

    # Cleanup temp dir
    shutil.rmtree(cache_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Figure 2: Forest plot (MAIN FIGURE)
# ---------------------------------------------------------------------------
def figure2_forest_plot(data):
    """Forest plot of r with 95% CIs for FDR-significant substances."""
    sig = [r for r in data["results"] if r.get("sig_fdr")]
    sig.sort(key=lambda r: r["r"])  # ascending so largest at top

    # Determine which survive multivariate adjustment
    mv_sig = {m["substance"] for m in data["mv_results"] if m.get("p_intensity", 1) < 0.05}

    fig, ax = plt.subplots(figsize=(4.5, 0.35 * len(sig) + 0.8))

    y_positions = range(len(sig))
    for i, r in enumerate(sig):
        survives = r["substance"] in mv_sig
        color = "#c0392b" if survives else "#2c3e50"
        marker = "D" if survives else "o"
        msize = 5 if survives else 4

        ax.plot(
            r["r"],
            i,
            marker=marker,
            color=color,
            markersize=msize,
            markeredgecolor=color,
            markerfacecolor=color if survives else "white",
            zorder=3,
        )
        ci_low = r.get("r_ci_low", r["r"] - 0.03)
        ci_high = r.get("r_ci_high", r["r"] + 0.03)
        ax.hlines(i, ci_low, ci_high, color=color, linewidth=1.0, zorder=2)

    ax.axvline(0, color="gray", linestyle="--", linewidth=0.5, zorder=1)
    ax.axvline(0.10, color="gray", linestyle=":", linewidth=0.5, zorder=1)

    ax.set_yticks(list(y_positions))
    ax.set_yticklabels([dn(r["substance"]) for r in sig])
    ax.set_xlabel("Point-biserial correlation (r)")
    ax.set_title("Figure 2: Correlation coefficients with 95% CIs")

    # Legend
    from matplotlib.lines import Line2D

    legend_elements = [
        Line2D(
            [0],
            [0],
            marker="D",
            color="#c0392b",
            markersize=5,
            markerfacecolor="#c0392b",
            linestyle="None",
            label="Survives multivariate adj.",
        ),
        Line2D(
            [0],
            [0],
            marker="o",
            color="#2c3e50",
            markersize=4,
            markerfacecolor="white",
            linestyle="None",
            label="Bivariate only",
        ),
    ]
    ax.legend(handles=legend_elements, loc="lower right", framealpha=0.9)

    ax.set_xlim(-0.02, max(r["r"] for r in sig) + 0.06)
    fig.tight_layout()
    save(fig, "figure_2_forest_plot")


# ---------------------------------------------------------------------------
# Figure 3: Dose-response curves (MAIN FIGURE)
# ---------------------------------------------------------------------------
def figure3_dose_response(data):
    """Dose-response curves for top 6 substances."""
    sig = [r for r in data["results"] if r.get("sig_fdr")]
    sig.sort(key=lambda r: r["r"], reverse=True)
    top6 = [r for r in sig[:6] if r.get("q_rates")]

    fig, axes = plt.subplots(2, 3, figsize=(7, 4.5), sharey=False)
    axes = axes.flatten()

    quartile_labels = ["Q1\n(lowest)", "Q2", "Q3", "Q4\n(highest)"]
    colors = ["#3498db", "#2980b9", "#2471a3", "#1a5276"]

    for idx, r in enumerate(top6):
        ax = axes[idx]
        q_rates = r["q_rates"]
        rates = [q_rates.get(f"q{i + 1}_rate", 0) for i in range(4)]

        ax.bar(range(4), rates, color=colors, width=0.65, edgecolor="white", linewidth=0.5)
        ax.plot(range(4), rates, color="#e74c3c", marker="o", markersize=3, linewidth=0.8, zorder=3)

        ax.set_xticks(range(4))
        ax.set_xticklabels(quartile_labels, fontsize=6)
        ax.set_ylabel("Detection rate (%)", fontsize=7)

        # Q4/Q1 annotation
        q4q1 = r.get("q4_q1")
        title = f"{dn(r['substance'])}"
        if q4q1 and q4q1 > 0:
            title += f" (Q4/Q1={q4q1:.1f}x)"
        ax.set_title(title, fontsize=7, fontweight="bold")

        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f"))

    # Hide unused axes
    for idx in range(len(top6), 6):
        axes[idx].set_visible(False)

    fig.suptitle("Figure 3: Detection rate by application intensity quartile", fontsize=9, y=1.02)
    fig.tight_layout()
    save(fig, "figure_3_dose_response")


# ---------------------------------------------------------------------------
# Figure 4: Bivariate vs Multivariate OR comparison (MAIN FIGURE)
# ---------------------------------------------------------------------------
def figure4_or_comparison(data):
    """Dumbbell chart comparing bivariate vs multivariate ORs."""
    mv = data["mv_results"]
    # Filter out substances with extreme/non-finite ORs
    mv_clean = [
        m
        for m in mv
        if m.get("bivariate_or") and m["bivariate_or"] < 10 and m.get("mv_intensity_or") and m["mv_intensity_or"] < 10
    ]
    mv_clean.sort(key=lambda m: m["bivariate_or"])

    fig, ax = plt.subplots(figsize=(4.5, 0.4 * len(mv_clean) + 0.8))

    for i, m in enumerate(mv_clean):
        biv_or = m["bivariate_or"]
        mv_or = m["mv_intensity_or"]
        sig = m.get("p_intensity", 1) < 0.05

        # Connection line
        ax.hlines(i, min(biv_or, mv_or), max(biv_or, mv_or), color="#bdc3c7", linewidth=1.5, zorder=1)
        # Bivariate OR
        ax.plot(biv_or, i, "o", color="#95a5a6", markersize=5, zorder=2)
        # Multivariate OR
        color = "#c0392b" if sig else "#2c3e50"
        ax.plot(
            mv_or,
            i,
            "D" if sig else "s",
            color=color,
            markersize=5,
            markerfacecolor=color if sig else "white",
            zorder=3,
        )

    ax.axvline(1.0, color="gray", linestyle="--", linewidth=0.5, zorder=0)

    ax.set_yticks(range(len(mv_clean)))
    ax.set_yticklabels([dn(m["substance"]) for m in mv_clean])
    ax.set_xlabel("Odds Ratio (OR)")
    ax.set_title("Figure 4: Bivariate vs. adjusted OR")

    from matplotlib.lines import Line2D

    legend_elements = [
        Line2D([0], [0], marker="o", color="#95a5a6", markersize=5, linestyle="None", label="Bivariate OR"),
        Line2D(
            [0], [0], marker="D", color="#c0392b", markersize=5, linestyle="None", label="Adjusted OR (significant)"
        ),
        Line2D(
            [0],
            [0],
            marker="s",
            color="#2c3e50",
            markersize=5,
            markerfacecolor="white",
            linestyle="None",
            label="Adjusted OR (n.s.)",
        ),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=6, framealpha=0.9)
    fig.tight_layout()
    save(fig, "figure_4_or_comparison")


# ---------------------------------------------------------------------------
# Figure S1: Negative controls bar chart (APPENDIX)
# ---------------------------------------------------------------------------
def figure_s1_negative_controls(data):
    """Bar chart of negative control results."""
    nc = data["neg_controls"]
    nc.sort(key=lambda x: x["koc"], reverse=True)

    fig, ax = plt.subplots(figsize=(4.5, 2.5))

    names = [dn(c["substance"]) for c in nc]
    r_vals = [c["r"] for c in nc]
    p_vals = [c.get("p_value", 1) for c in nc]
    colors = ["#e74c3c" if p < 0.05 else "#27ae60" for p in p_vals]

    ax.barh(range(len(nc)), r_vals, color=colors, height=0.5, edgecolor="white")

    ax.axvline(0, color="gray", linestyle="-", linewidth=0.5)
    ax.set_yticks(range(len(nc)))
    ax.set_yticklabels([f"{n}\n(Koc={c['koc']})" for n, c in zip(names, nc, strict=False)], fontsize=6)
    ax.set_xlabel("Point-biserial correlation (r)")
    ax.set_title("Figure S1: Negative controls (high-Koc substances)", fontsize=8)

    for i, (r, p) in enumerate(zip(r_vals, p_vals, strict=False)):
        label = f"r={r:.3f}, p={p:.3f}" if p < 1 else f"r={r:.3f}"
        ax.text(max(r, 0) + 0.005, i, label, va="center", fontsize=5.5)

    from matplotlib.patches import Patch

    legend_elements = [
        Patch(facecolor="#27ae60", label="Non-significant (as expected)"),
        Patch(facecolor="#e74c3c", label="Significant (unexpected)"),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=6)
    fig.tight_layout()
    save(fig, "figure_s1_negative_controls")


# ---------------------------------------------------------------------------
# Figure S2: Temporal lag lollipop chart (APPENDIX)
# ---------------------------------------------------------------------------
def figure_s2_temporal_lag(data):
    """Lollipop chart showing optimal temporal lag per substance."""
    lags = data["lag_results"]
    lags.sort(key=lambda x: x.get("lag_years", 0) if isinstance(x.get("lag_years"), (int, float)) else 0)

    fig, ax = plt.subplots(figsize=(5, 0.35 * len(lags) + 0.8))

    for i, lag in enumerate(lags):
        lag_y = lag.get("lag_years", 0)
        if isinstance(lag_y, str):
            lag_y = float(lag_y.replace("y", ""))
        r_val = lag.get("r", 0)
        sig = lag.get("sig_fdr_global", False)

        color = "#2c3e50" if sig else "#95a5a6"
        ax.barh(i, lag_y, height=0.4, color=color, alpha=0.7)
        ax.text(lag_y + 0.15, i, f"r={r_val:.3f}", va="center", fontsize=6)

    ax.set_yticks(range(len(lags)))
    ax.set_yticklabels([dn(lag["substance"]) for lag in lags])
    ax.set_xlabel("Optimal temporal lag (years)")
    ax.set_title("Figure S2: Substance-specific temporal lags", fontsize=8)
    fig.tight_layout()
    save(fig, "figure_s2_temporal_lag")


# ---------------------------------------------------------------------------
# Figure S3: Power analysis visualization (APPENDIX)
# ---------------------------------------------------------------------------
def figure_s3_power_analysis(data):
    """Effect size distribution with power threshold."""
    sig = [r for r in data["results"] if r.get("sig_fdr")]
    sig.sort(key=lambda r: r["r"], reverse=True)
    power = data["power_info"]
    r_min = power.get("r_min_80pct", 0.048)

    fig, ax = plt.subplots(figsize=(5, 3))

    r_vals = [r["r"] for r in sig]
    names = [dn(r["substance"]) for r in sig]
    colors = []
    for r in r_vals:
        if r >= 0.20:
            colors.append("#1a5276")  # strong
        elif r >= 0.10:
            colors.append("#2980b9")  # moderate
        else:
            colors.append("#85c1e9")  # weak

    ax.bar(range(len(sig)), r_vals, color=colors, width=0.7, edgecolor="white")

    ax.axhline(r_min, color="#e74c3c", linestyle="--", linewidth=0.8, label=f"Min detectable r = {r_min:.3f}")
    ax.axhline(0.10, color="#f39c12", linestyle=":", linewidth=0.8, label="Cohen's small (r = 0.10)")
    ax.axhline(0.20, color="#27ae60", linestyle=":", linewidth=0.8, label="r = 0.20 threshold")

    ax.set_xticks(range(len(sig)))
    ax.set_xticklabels(names, rotation=45, ha="right", fontsize=5.5)
    ax.set_ylabel("Point-biserial r")
    ax.set_title("Figure S3: Effect sizes with power thresholds", fontsize=8)
    ax.legend(fontsize=6, loc="upper right")
    fig.tight_layout()
    save(fig, "figure_s3_power_analysis")


# ---------------------------------------------------------------------------
# Figure S4: All 57 substances volcano-style plot (APPENDIX)
# ---------------------------------------------------------------------------
def figure_s4_volcano(data):
    """Scatter plot of r vs -log10(p) for all 57 substances."""
    results = data["results"]

    fig, ax = plt.subplots(figsize=(5, 4))

    for r in results:
        p = r.get("p_value", 1)
        if p <= 0:
            p = 1e-50
        neg_log_p = -np.log10(p)
        sig = r.get("sig_fdr", False)

        color = "#c0392b" if sig else "#bdc3c7"
        size = 20 if sig else 10
        ax.scatter(r["r"], neg_log_p, c=color, s=size, alpha=0.8, edgecolors="none", zorder=2 if sig else 1)

        if sig and r["r"] > 0.15:
            ax.annotate(
                dn(r["substance"]),
                (r["r"], neg_log_p),
                fontsize=5,
                ha="left",
                va="bottom",
                xytext=(3, 3),
                textcoords="offset points",
            )

    # FDR threshold line (approximate)
    fdr_thresh = -np.log10(0.05)
    ax.axhline(fdr_thresh, color="#f39c12", linestyle="--", linewidth=0.5, label="p = 0.05")
    ax.axvline(0, color="gray", linestyle="-", linewidth=0.3)

    ax.set_xlabel("Point-biserial correlation (r)")
    ax.set_ylabel("−log₁₀(p)")  # noqa: RUF001
    ax.set_title("Figure S4: All 57 substances (volcano plot)", fontsize=8)

    from matplotlib.lines import Line2D

    legend_elements = [
        Line2D(
            [0],
            [0],
            marker="o",
            color="#c0392b",
            markersize=5,
            linestyle="None",
            label=f"FDR-significant (n={sum(1 for r in results if r.get('sig_fdr'))})",
        ),
        Line2D(
            [0],
            [0],
            marker="o",
            color="#bdc3c7",
            markersize=4,
            linestyle="None",
            label=f"Non-significant (n={sum(1 for r in results if not r.get('sig_fdr'))})",
        ),
    ]
    ax.legend(handles=legend_elements, fontsize=6)
    fig.tight_layout()
    save(fig, "figure_s4_volcano")


# ---------------------------------------------------------------------------
# Table S1: Full substance table → CSV
# ---------------------------------------------------------------------------
def table_s1_full_substances(data):
    import csv

    results = data["results"]
    results_sorted = sorted(results, key=lambda r: r.get("p_fdr", 1))

    path = FIGURES_DIR / "table_s1_full_substances.csv"
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Rank",
                "Substance",
                "Display_Name",
                "Type",
                "n",
                "n_detected",
                "Detection_Rate_%",
                "r",
                "r_CI_low",
                "r_CI_high",
                "p_raw",
                "q_FDR",
                "FDR_significant",
                "OR",
                "OR_CI_low",
                "OR_CI_high",
                "logit_p",
            ]
        )
        for i, r in enumerate(results_sorted, 1):
            writer.writerow(
                [
                    i,
                    r["substance"],
                    dn(r["substance"]),
                    r.get("type", ""),
                    r.get("n_grukos", ""),
                    r.get("n_detected", ""),
                    f"{r.get('detection_rate', 0):.1f}",
                    f"{r['r']:.4f}",
                    f"{r.get('r_ci_low', ''):.4f}" if r.get("r_ci_low") is not None else "",
                    f"{r.get('r_ci_high', ''):.4f}" if r.get("r_ci_high") is not None else "",
                    f"{r.get('p_value', ''):.6f}" if r.get("p_value") is not None else "",
                    f"{r.get('p_fdr', ''):.6f}" if r.get("p_fdr") is not None else "",
                    "Yes" if r.get("sig_fdr") else "No",
                    f"{r.get('logit_or', ''):.4f}" if r.get("logit_or") is not None else "",
                    f"{r.get('logit_or_ci_low', ''):.4f}" if r.get("logit_or_ci_low") is not None else "",
                    f"{r.get('logit_or_ci_high', ''):.4f}" if r.get("logit_or_ci_high") is not None else "",
                    f"{r.get('logit_p', ''):.6f}" if r.get("logit_p") is not None else "",
                ]
            )
    print(f"  Saved table_s1_full_substances.csv ({len(results_sorted)} rows)")  # noqa: T201


# ---------------------------------------------------------------------------
# Table S5: Dose-response quartiles → CSV
# ---------------------------------------------------------------------------
def table_s5_dose_response(data):
    import csv

    sig = [r for r in data["results"] if r.get("sig_fdr") and r.get("q_rates")]
    sig.sort(key=lambda r: r["r"], reverse=True)

    path = FIGURES_DIR / "table_s5_dose_response.csv"
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Substance",
                "Display_Name",
                "r",
                "Q1_rate_%",
                "Q2_rate_%",
                "Q3_rate_%",
                "Q4_rate_%",
                "Q4_Q1_ratio",
                "Q1_n",
                "Q2_n",
                "Q3_n",
                "Q4_n",
            ]
        )
        for r in sig:
            q = r["q_rates"]
            writer.writerow(
                [
                    r["substance"],
                    dn(r["substance"]),
                    f"{r['r']:.4f}",
                    f"{q.get('q1_rate', 0):.1f}",
                    f"{q.get('q2_rate', 0):.1f}",
                    f"{q.get('q3_rate', 0):.1f}",
                    f"{q.get('q4_rate', 0):.1f}",
                    f"{r.get('q4_q1', 0):.1f}" if r.get("q4_q1") else "",
                    q.get("q1_n", ""),
                    q.get("q2_n", ""),
                    q.get("q3_n", ""),
                    q.get("q4_n", ""),
                ]
            )
    print(f"  Saved table_s5_dose_response.csv ({len(sig)} rows)")  # noqa: T201


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate paper figures from analysis results")
    parser.add_argument("--json-path", type=str, default=str(JSON_PATH), help="Path to groundwater_results.json")
    parser.add_argument(
        "--figures", type=str, default="all", help="Comma-separated figure list (e.g., '2,3,4') or 'all'"
    )
    args = parser.parse_args()

    setup_style()
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Loading results from {args.json_path}...")  # noqa: T201
    data = load_results(args.json_path)
    meta = data.get("metadata", {})
    print(f"  Detection mode: {meta.get('detection_mode')}")  # noqa: T201
    print(f"  Application years: {meta.get('application_years')}")  # noqa: T201
    print(f"  Substances: {len(data['results'])}")  # noqa: T201
    print(f"  FDR-significant: {sum(1 for r in data['results'] if r.get('sig_fdr'))}")  # noqa: T201
    print()  # noqa: T201

    figs = (
        args.figures.split(",") if args.figures != "all" else ["1", "2", "3", "4", "s1", "s2", "s3", "s4", "t1", "t5"]
    )

    if "1" in figs:
        print("Generating Figure 1: Study area map (requires R2)...")  # noqa: T201
        figure1_study_area_map(data)

    if "2" in figs:
        print("Generating Figure 2: Forest plot...")  # noqa: T201
        figure2_forest_plot(data)

    if "3" in figs:
        print("Generating Figure 3: Dose-response curves...")  # noqa: T201
        figure3_dose_response(data)

    if "4" in figs:
        print("Generating Figure 4: OR comparison...")  # noqa: T201
        figure4_or_comparison(data)

    if "s1" in figs:
        print("Generating Figure S1: Negative controls...")  # noqa: T201
        figure_s1_negative_controls(data)

    if "s2" in figs:
        print("Generating Figure S2: Temporal lag...")  # noqa: T201
        figure_s2_temporal_lag(data)

    if "s3" in figs:
        print("Generating Figure S3: Power analysis...")  # noqa: T201
        figure_s3_power_analysis(data)

    if "s4" in figs:
        print("Generating Figure S4: Volcano plot...")  # noqa: T201
        figure_s4_volcano(data)

    if "t1" in figs:
        print("Generating Table S1: Full substances...")  # noqa: T201
        table_s1_full_substances(data)

    if "t5" in figs:
        print("Generating Table S5: Dose-response quartiles...")  # noqa: T201
        table_s5_dose_response(data)

    print(f"\nAll outputs in: {FIGURES_DIR}/")  # noqa: T201


if __name__ == "__main__":
    main()
