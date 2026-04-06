# ruff: noqa: T201
"""Generate publication-quality figures for the PFAS-Agriculture paper."""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

FIGURES_DIR = Path(__file__).parent / "figures"
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

plt.rcParams.update(
    {
        "font.family": "sans-serif",
        "font.size": 11,
        "axes.linewidth": 0.8,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "figure.facecolor": "white",
    }
)


def fig2_tfa_threshold():
    """TFA Threshold Sensitivity - dual axis bar + line chart."""
    thresholds = ["0.075\n(½ LOQ)", "0.5", "1.0", "2.0", "5.0", "10.0"]
    detection_pct = [100.0, 16.5, 6.8, 2.5, 0.1, 0.0]
    # correlations: None where not computable
    corr_vals = [None, 0.165, 0.178, 0.042, None, None]
    corr_sig = [None, "***", "***", "ns", None, None]

    fig, ax1 = plt.subplots(figsize=(9, 5))
    x = np.arange(len(thresholds))

    # Bars - detection rate
    bars = ax1.bar(
        x,
        detection_pct,
        width=0.55,
        color="#6baed6",
        alpha=0.85,
        edgecolor="#2171b5",
        linewidth=0.6,
        label="Detection rate (%)",
        zorder=2,
    )
    ax1.set_xlabel("TFA Concentration Threshold (µg/L)", fontsize=12)
    ax1.set_ylabel("Detection Rate (%)", fontsize=12, color="#2171b5")
    ax1.set_xticks(x)
    ax1.set_xticklabels(thresholds)
    ax1.tick_params(axis="y", labelcolor="#2171b5")
    ax1.set_ylim(0, 115)

    # Add % labels on bars
    for bar, val in zip(bars, detection_pct, strict=False):
        if val > 2:
            ax1.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 1.5,
                f"{val:.1f}%",
                ha="center",
                va="bottom",
                fontsize=9,
                color="#2171b5",
            )

    # Line - correlation on right axis
    ax2 = ax1.twinx()
    corr_x = [i for i, v in enumerate(corr_vals) if v is not None]
    corr_y = [v for v in corr_vals if v is not None]
    ax2.plot(
        corr_x,
        corr_y,
        "o-",
        color="#cb181d",
        linewidth=2,
        markersize=8,
        markerfacecolor="#cb181d",
        markeredgecolor="white",
        markeredgewidth=1.5,
        label="Point-biserial r",
        zorder=3,
    )
    ax2.axhline(y=0, color="gray", linestyle="--", linewidth=0.7, alpha=0.5)
    ax2.set_ylabel("Point-biserial correlation (r)", fontsize=12, color="#cb181d")
    ax2.tick_params(axis="y", labelcolor="#cb181d")
    ax2.set_ylim(-0.05, 0.25)
    ax2.spines["right"].set_visible(True)

    # Significance annotations
    for i, sig in enumerate(corr_sig):
        if sig and corr_vals[i] is not None:
            offset = 0.015
            ax2.text(
                i, corr_vals[i] + offset, sig, ha="center", va="bottom", fontsize=10, fontweight="bold", color="#cb181d"
            )

    ax1.set_title(
        "TFA Detection Rate and Spatial Correlation\nat Progressive Concentration Thresholds",
        fontsize=13,
        fontweight="bold",
        pad=12,
    )

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right", framealpha=0.9)

    plt.tight_layout()
    fig.savefig(str(FIGURES_DIR / "fig2_tfa_threshold.png"), dpi=300, bbox_inches="tight")
    plt.close()
    print("  -> fig2_tfa_threshold.png")


def fig3_monitoring_stratification():
    """Monitoring Density Stratification - grouped bar chart."""
    substances = [
        "PFOA vs\nfluor.",
        "PFOS vs\nfluor.",
        "Σ-PFOA vs\ntotal",
        "Σ-PFHxS vs\ntotal",
        "SUM PFAS-22\nvs fluor.",
        "SUM PFAS-12\nvs fluor.",
        "SUM PFAS-4\nvs fluor.",
    ]
    low = [-0.024, 0.014, 0.349, 0.000, -0.017, -0.011, 0.000]
    medium = [0.199, -0.043, -0.055, -0.020, 0.016, 0.010, -0.014]
    high = [0.165, 0.121, 0.183, 0.145, 0.132, 0.139, 0.103]

    # Significance markers
    low_sig = ["", "", "**", "", "", "", ""]
    med_sig = ["**", "", "", "", "", "", ""]
    high_sig = ["**", "**", "**", "**", "**", "**", "*"]

    x = np.arange(len(substances))
    width = 0.25

    fig, ax = plt.subplots(figsize=(11, 6))

    bars_low = ax.bar(
        x - width,
        low,
        width,
        label="Low (≤2 wells, n≈1,049)",
        color="#bdbdbd",
        edgecolor="#636363",
        linewidth=0.5,
        zorder=2,
    )
    bars_med = ax.bar(
        x,
        medium,
        width,
        label="Medium (≤4 wells, n≈388)",
        color="#969696",
        edgecolor="#636363",
        linewidth=0.5,
        zorder=2,
    )
    bars_high = ax.bar(
        x + width,
        high,
        width,
        label="High (>4 wells, n≈518)",
        color="#2171b5",
        edgecolor="#08519c",
        linewidth=0.5,
        zorder=2,
    )

    # Significance stars
    def add_stars(bars, sigs, vals):
        for bar, sig, val in zip(bars, sigs, vals, strict=False):
            if sig:
                y = max(val, 0) + 0.012
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    y,
                    sig,
                    ha="center",
                    va="bottom",
                    fontsize=9,
                    fontweight="bold",
                    color="#cb181d",
                )

    add_stars(bars_low, low_sig, low)
    add_stars(bars_med, med_sig, medium)
    add_stars(bars_high, high_sig, high)

    ax.axhline(y=0, color="black", linewidth=0.6)
    ax.set_ylabel("Point-biserial correlation (r)", fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels(substances, fontsize=9)
    ax.set_ylim(-0.15, 0.42)
    ax.legend(loc="upper left", framealpha=0.9, fontsize=10)
    ax.set_title(
        "PFAS-Agricultural Intensity Correlations by Monitoring Density Tertile", fontsize=13, fontweight="bold", pad=12
    )

    # Grid
    ax.yaxis.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    plt.tight_layout()
    fig.savefig(str(FIGURES_DIR / "fig3_monitoring_stratification.png"), dpi=300, bbox_inches="tight")
    plt.close()
    print("  -> fig3_monitoring_stratification.png")


if __name__ == "__main__":
    print("Generating PFAS paper figures...")
    fig2_tfa_threshold()
    fig3_monitoring_stratification()
    print(f"Done. Figures saved to {FIGURES_DIR}/")
