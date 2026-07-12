# ruff: noqa
#!/usr/bin/env python
"""Part 3.6 (real-only) — Manure→biogas over time + disposition by herd size.
Only REAL data: afsat (B_AFTRK) for the export trend 2018-2022; produced (C_2016) real for
2022's by-size disposition. 2023 B_AFTRK is partial -> excluded from shares (biogas absolute noted).

Archived as-is for provenance; not lint-checked (ad-hoc research script, not
pipeline code). Needs herd_panel_2015_2023.csv and afsat_panel.csv as inputs
(CVR-level panels, not archived here for size/PII reasons — see ../README.md)
to actually run.

Run: uv run --no-project --with pandas --with numpy --with matplotlib python disposition_over_time.py
"""

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
from pathlib import Path

import matplotlib.pyplot as plt

OUT = Path(__file__).parent
herd = pd.read_csv(OUT / "herd_panel_2015_2023.csv", dtype={"cvr": str})
afs = pd.read_csv(OUT / "afsat_panel.csv", dtype={"cvr": str})

# --- (1) export trend: of manure DELIVERED OUT, share to biogas vs other farms (real, 2018-2022) ---
g = afs[afs.year.between(2018, 2022)].groupby("year").agg(biogas=("afsat_biogas", "sum"), other=("afsat_other", "sum"))
g["exported_Mkg"] = (g.biogas + g.other) / 1e6
g["to_biogas_Mkg"] = g.biogas / 1e6
g["biogas_share_of_exported_%"] = (g.biogas / (g.biogas + g.other) * 100).round(1)
print("MANURE DELIVERED OUT, share to biogas (real; 2023 excluded — partial extract):")
print(g[["exported_Mkg", "to_biogas_Mkg", "biogas_share_of_exported_%"]].round(1).to_string())
b23 = afs[afs.year == 2023].afsat_biogas.sum() / 1e6
print(f"(2023 manure-N to biogas continues to {b23:.1f} M kg, but 2023 other-farm deliveries are incomplete)")

# --- (2) 2022 disposition by herd size (REAL produced C_2016) ---
prod = herd[(herd.year == 2022) & (herd.livestock_n > 0)][["cvr", "livestock_n"]].rename(
    columns={"livestock_n": "produced"}
)
a22 = afs[afs.year == 2022]
df = prod.merge(a22, on="cvr", how="left").fillna({"afsat_biogas": 0, "afsat_other": 0})
df["afsat_total"] = df.afsat_biogas + df.afsat_other
sc = np.where(df.afsat_total > 0, np.minimum(df.afsat_total, df.produced) / df.afsat_total, 1)
df["biogas_N"] = df.afsat_biogas * sc
df["other_N"] = df.afsat_other * sc
df["own_N"] = df.produced - df.biogas_N - df.other_N
df["q"] = pd.qcut(df.produced, 5, labels=["Q1 smallest", "Q2", "Q3", "Q4", "Q5 largest"])
gq = df.groupby("q", observed=True)
tab = pd.DataFrame(
    {
        "farms": gq.cvr.size(),
        "median_tN": (gq.produced.median() / 1e3).round(1),
        "own_%": (gq.own_N.sum() / gq.produced.sum() * 100).round(1),
        "otherfarm_%": (gq.other_N.sum() / gq.produced.sum() * 100).round(1),
        "biogas_%": (gq.biogas_N.sum() / gq.produced.sum() * 100).round(1),
    }
)
tot = df.produced.sum()
print(
    f"\n2022 OVERALL (real): own {df.own_N.sum() / tot * 100:.0f}% | other farms {df.other_N.sum() / tot * 100:.0f}% | biogas {df.biogas_N.sum() / tot * 100:.0f}%"
)
print("2022 DISPOSITION BY HERD SIZE (share of produced manure-N):")
print(tab.to_string())
tab.to_csv(OUT / "manure_disposition_2022.csv")

# --- (3) consolidation (real animal counts) ---
print("\nCONSOLIDATION (real):")
con = (
    herd[herd.year.between(2018, 2022)]
    .groupby("year")
    .apply(
        lambda x: pd.Series(
            {
                "cattle_farms": int((x.cattle_cnt > 0).sum()),
                "cattle_arsdyr_k": x.cattle_cnt.sum() / 1e3,
                "top10pct_cattle_share_%": x[x.cattle_cnt > 0]
                .cattle_cnt.nlargest(max(1, int((x.cattle_cnt > 0).sum() * 0.1)))
                .sum()
                / x.cattle_cnt.sum()
                * 100,
            }
        ),
        include_groups=False,
    )
    .round(1)
)
print(con.to_string())

# --- plots ---
fig, ax = plt.subplots(1, 2, figsize=(13, 5))
ax0b = ax[0].twinx()
ax[0].bar(g.index, g.to_biogas_Mkg, color="#b25b6d", alpha=0.85, label="manure-N to biogas (M kg)")
ax0b.plot(
    g.index, g["biogas_share_of_exported_%"], color="#3f3f3f", marker="o", lw=2, label="% of exported manure → biogas"
)
ax[0].set_ylabel("manure-N to biogas (M kg)")
ax0b.set_ylabel("% of exported manure → biogas")
ax0b.set_ylim(0, 50)
ax[0].set_title("Manure to biogas: rising fast (2018–2022)", weight="bold")
ax[0].legend(loc="upper left", fontsize=8, frameon=False)
ax0b.legend(loc="lower right", fontsize=8, frameon=False)
own, oth, bio = tab["own_%"], tab["otherfarm_%"], tab["biogas_%"]
ax[1].bar(tab.index, own, color="#3f7d4e", label="own fields")
ax[1].bar(tab.index, oth, bottom=own, color="#c9a24a", label="other farms")
ax[1].bar(tab.index, bio, bottom=own + oth, color="#b25b6d", label="biogas")
for r, m in zip(tab.index, tab.median_tN):
    ax[1].text(r, 102, f"~{m:.0f} t N", ha="center", fontsize=8, color="#555")
ax[1].set_ylim(0, 112)
ax[1].set_ylabel("% of produced manure-N")
ax[1].set_title("Where manure goes by herd size (2022)", weight="bold")
ax[1].legend(loc="lower center", ncol=3, frameon=False, bbox_to_anchor=(0.5, -0.18))
for a in [ax[0], ax[1]]:
    a.spines[["top"]].set_visible(False)
plt.setp(ax[1].get_xticklabels(), rotation=20, ha="right")
fig.suptitle("Danish manure → biogas: growing share of exports, concentrated in the largest farms", weight="bold")
fig.tight_layout(rect=[0, 0, 1, 0.95])
fig.savefig(OUT / "disposition_over_time.png", dpi=140)
g.round(1).to_csv(OUT / "disposition_over_time.csv")
print("\nwrote disposition_over_time.{csv,png} + manure_disposition_2022.csv")
