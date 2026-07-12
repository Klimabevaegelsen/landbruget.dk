# ruff: noqa
#!/usr/bin/env python
"""Where does a farm's manure go, by herd size? (2023)
Decompose produced manure-N (B_DYRERK C_2016) into destinations:
  own fields (residual) | other farms | biogas
using B_AFTRK deliveries-out (recipient name/CVR + kg N), classified biogas vs other.
Then bucket farms by herd size (produced N) and show destination shares.

Archived as-is for provenance; not lint-checked (ad-hoc research script, not
pipeline code). Needs plant_cvr_resolved.csv, biogas_plants_2023.csv, and
herd_panel_2015_2023.csv as inputs (not archived here for size/PII reasons —
see ../README.md) to actually run.

Run: uv run --no-project --with pandas --with xlrd --with matplotlib python manure_disposition.py
"""

import io
import re
import subprocess

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
from pathlib import Path

import matplotlib.pyplot as plt

OUT = Path(__file__).parent
BR = "r2:landbruget-data/bronze/fertiliser/20260601_055740/In-depth/GR 2023"


def cat(p):
    return subprocess.run(["rclone", "cat", f"{BR}/{p}"], capture_output=True).stdout


def digits(x):
    s = str(x or "").strip()
    if re.fullmatch(r"\d+\.\d+", s):
        s = s.split(".")[0]
    d = re.sub(r"\D", "", s)
    return (d[:8] if len(d) > 8 else d).zfill(8) if d else ""


def norm(s):
    s = re.sub(r"[,.].*$", "", str(s or "").lower())
    s = re.sub(r"\b(a/s|aps|i/s|a m b a|amba|p/s|k/s|holding)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# biogas recipient reference (plant CVRs + names)
res = pd.read_csv(OUT / "plant_cvr_resolved.csv")
plant_cvrs = set(res.loc[res.cvr.notna(), "cvr"].astype(str).map(digits)) - {""}
plants = pd.read_csv(OUT / "biogas_plants_2023.csv")
plant_names = {norm(n) for n in pd.concat([plants.anlaeg, plants.ejer]).dropna()} - {""}
plant_names = {n for n in plant_names if len(n) > 6}
BIO_RE = re.compile(r"biogas|bioenerg|nature energy|biocirc|bigadan|naturgas|greenlab|maabjerg|gfe ", re.IGNORECASE)

# --- afsat (delivered out), per deliverer farm, split biogas vs other ---
a = pd.read_excel(io.BytesIO(cat("V_4061GR_23_ISKV1_B_AFTRK_6B.xls")), dtype=str, keep_default_na=False)
a["deliverer"] = a["CVR"].map(digits)
a["rcpt_cvr"] = a["C_153"].map(digits)
a["kgN"] = pd.to_numeric(a["C_155"], errors="coerce").fillna(0)


def is_biogas(r):
    if r["rcpt_cvr"] and r["rcpt_cvr"] in plant_cvrs:
        return True
    nm = norm(r["C_152"])
    if nm in plant_names or any(pn in nm or nm in pn for pn in plant_names if len(pn) > 8):
        return True
    return bool(BIO_RE.search(str(r["C_152"])))


a["to_biogas"] = a.apply(is_biogas, axis=1)
afs = (
    a.groupby("deliverer")
    .apply(
        lambda g: pd.Series(
            {"afsat_biogas": g.loc[g.to_biogas, "kgN"].sum(), "afsat_other": g.loc[~g.to_biogas, "kgN"].sum()}
        ),
        include_groups=False,
    )
    .reset_index()
)

# --- produced manure N per farm (herd panel 2023) ---
herd = pd.read_csv(OUT / "herd_panel_2015_2023.csv", dtype={"cvr": str})
prod = herd[herd.year == 2023][["cvr", "livestock_n", "cattle_cnt", "pig_cnt"]].rename(
    columns={"livestock_n": "produced"}
)
prod = prod[prod.produced > 0]

df = prod.merge(afs, left_on="cvr", right_on="deliverer", how="left").fillna({"afsat_biogas": 0, "afsat_other": 0})
df["afsat_total"] = df.afsat_biogas + df.afsat_other
# clamp afsat to produced (measurement noise can push afsat slightly over produced)
df["afsat_total_c"] = np.minimum(df.afsat_total, df.produced)
scale = np.where(df.afsat_total > 0, df.afsat_total_c / df.afsat_total, 1)
df["biogas_N"] = df.afsat_biogas * scale
df["other_N"] = df.afsat_other * scale
df["own_N"] = df.produced - df.biogas_N - df.other_N

tot = df.produced.sum()
print(f"farms with produced manure-N (2023): {len(df)}; total produced = {tot / 1e6:.0f} M kg N")
print(
    f"OVERALL share of produced manure-N: own fields {df.own_N.sum() / tot * 100:4.1f}% | "
    f"other farms {df.other_N.sum() / tot * 100:4.1f}% | biogas {df.biogas_N.sum() / tot * 100:4.1f}%"
)

# --- by herd-size quintile (produced N) ---
df["q"] = pd.qcut(df.produced, 5, labels=["Q1 smallest", "Q2", "Q3", "Q4", "Q5 largest"])
# shares per bucket (share of the bucket's total produced N)
g = df.groupby("q", observed=True)
tab = pd.DataFrame(
    {
        "farms": g.cvr.size(),
        "median_produced_tN": (g.produced.median() / 1e3).round(1),
        "share_own_%": (g.own_N.sum() / g.produced.sum() * 100).round(1),
        "share_otherfarm_%": (g.other_N.sum() / g.produced.sum() * 100).round(1),
        "share_biogas_%": (g.biogas_N.sum() / g.produced.sum() * 100).round(1),
    }
)
print("\nDESTINATION OF PRODUCED MANURE-N, BY HERD-SIZE QUINTILE (share of produced N):")
print(tab.to_string())
tab.to_csv(OUT / "manure_disposition_2023.csv")

# stacked bar chart
fig, ax = plt.subplots(figsize=(8.5, 5))
b = ax.bar(tab.index, tab["share_own_%"], label="own fields", color="#3f7d4e")
b2 = ax.bar(tab.index, tab["share_otherfarm_%"], bottom=tab["share_own_%"], label="other farms", color="#c9a24a")
b3 = ax.bar(
    tab.index,
    tab["share_biogas_%"],
    bottom=tab["share_own_%"] + tab["share_otherfarm_%"],
    label="biogas",
    color="#b25b6d",
)
for r, m in zip(tab.index, tab.median_produced_tN):
    ax.text(r, 102, f"~{m:.0f} t N", ha="center", fontsize=8, color="#555")
ax.set_ylabel("share of produced manure-N (%)")
ax.set_ylim(0, 110)
ax.set_title("Where Danish farms' manure goes, by herd size (2023)", weight="bold")
ax.legend(loc="lower center", ncol=3, frameon=False, bbox_to_anchor=(0.5, -0.16))
ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout()
fig.savefig(OUT / "manure_disposition_2023.png", dpi=140)
print("\nwrote manure_disposition_2023.csv + .png")
