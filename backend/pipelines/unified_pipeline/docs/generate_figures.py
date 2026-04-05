#!/usr/bin/env python3
"""Generate publication-quality figures for the pesticide disaggregation paper."""

from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

FIGURES_DIR = Path(__file__).parent / "figures"
FIGURES_DIR.mkdir(exist_ok=True)

# ── Journal-quality style ────────────────────────────────────────────────

plt.rcParams.update(
    {
        "font.family": "serif",
        "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
        "font.size": 10,
        "axes.labelsize": 11,
        "axes.titlesize": 12,
        "legend.fontsize": 8.5,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "figure.dpi": 300,
        "savefig.dpi": 300,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.linewidth": 0.6,
        "xtick.major.width": 0.6,
        "ytick.major.width": 0.6,
        "lines.linewidth": 1.4,
        "lines.markersize": 5,
        "legend.frameon": True,
        "legend.edgecolor": "#bbbbbb",
        "legend.fancybox": False,
    }
)

# ── Color palette (colorblind-friendly, print-safe) ─────────────────────

COLORS = {
    "blue": "#2166ac",
    "orange": "#d95f02",
    "green": "#1b7837",
    "purple": "#7570b3",
    "red": "#e7298a",
    "grey": "#666666",
    "light_grey": "#cccccc",
    "danger": "#d62728",
    "phase1": "#bdc9e1",
    "phase2": "#74a9cf",
    "phase3": "#2b8cbe",
    "anomaly": "#e34a33",
}

# ── Data from paper tables ──────────────────────────────────────────────

TOLERANCE_LEVELS = [0.0, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0]

COVERAGE_BY_YEAR = {
    2015: [43.7, 72.0, 77.5, 81.8, 83.7, 85.5, 87.4],
    2018: [47.7, 84.6, 88.3, 90.5, 91.5, 92.5, 93.8],
    2020: [45.2, 86.7, 90.8, 92.7, 93.5, 94.4, 95.4],
    2021: [42.0, 85.5, 89.9, 92.1, 92.9, 93.8, 94.8],
    2023: [37.8, 78.2, 85.5, 90.0, 91.8, 93.3, 94.8],
}

AMBIGUITY_2021 = [0.25, 1.8, 3.8, 6.5, 10.2, 15.3, 27.6]

YEARS = list(range(2010, 2024))
COVERAGE_LONGITUDINAL = [
    55.7,
    63.1,
    62.6,
    66.6,  # 2010-2013
    0.0,  # 2014 anomaly
    81.8,
    87.2,
    86.9,
    90.5,
    91.6,  # 2015-2019
    92.7,
    92.1,
    91.2,
    90.0,  # 2020-2023
]


def figure_1_tolerance_sensitivity():
    """Dual-axis chart: coverage vs tolerance + ambiguity rate."""
    fig, ax1 = plt.subplots(figsize=(6.5, 4))

    # Coverage lines — use distinct colors instead of grays
    styles = {
        2015: {"color": COLORS["light_grey"], "marker": "o", "ls": "--", "lw": 1.2, "zorder": 2},
        2018: {"color": COLORS["grey"], "marker": "s", "ls": "--", "lw": 1.2, "zorder": 3},
        2020: {"color": COLORS["blue"], "marker": "D", "ls": "-", "lw": 1.8, "zorder": 5},
        2021: {"color": COLORS["green"], "marker": "^", "ls": "-", "lw": 1.5, "zorder": 4},
        2023: {"color": COLORS["orange"], "marker": "v", "ls": ":", "lw": 1.3, "zorder": 3},
    }

    for year, cov in COVERAGE_BY_YEAR.items():
        s = styles[year]
        ax1.plot(
            TOLERANCE_LEVELS,
            cov,
            marker=s["marker"],
            color=s["color"],
            linestyle=s["ls"],
            linewidth=s["lw"],
            markersize=5,
            label=str(year),
            markeredgecolor="white",
            markeredgewidth=0.5,
            zorder=s["zorder"],
        )

    ax1.set_xlabel("Area tolerance (%)")
    ax1.set_ylabel("Matching coverage (%)")
    ax1.set_ylim(30, 100)
    ax1.set_xlim(-0.3, 10.5)
    ax1.set_xticks(TOLERANCE_LEVELS)
    ax1.set_xticklabels(["0", "0.5", "1", "2", "3", "5", "10"])
    ax1.yaxis.set_major_locator(mticker.MultipleLocator(10))
    ax1.yaxis.set_minor_locator(mticker.MultipleLocator(5))
    ax1.grid(axis="y", alpha=0.2, linewidth=0.5)

    # Ambiguity rate (right axis)
    ax2 = ax1.twinx()
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(True)
    ax2.spines["right"].set_color(COLORS["danger"])
    ax2.spines["right"].set_linewidth(0.6)

    ax2.fill_between(
        TOLERANCE_LEVELS,
        AMBIGUITY_2021,
        alpha=0.08,
        color=COLORS["danger"],
        zorder=1,
    )
    ax2.plot(
        TOLERANCE_LEVELS,
        AMBIGUITY_2021,
        marker="x",
        color=COLORS["danger"],
        linestyle="-.",
        linewidth=1.3,
        markersize=6,
        label="Cross-crop ambiguity",
        zorder=6,
    )
    ax2.set_ylabel("Cross-crop ambiguity (%)", color=COLORS["danger"])
    ax2.tick_params(axis="y", labelcolor=COLORS["danger"], colors=COLORS["danger"])
    ax2.set_ylim(0, 32)

    # Default tolerance annotation
    ax1.axvline(x=2.0, color=COLORS["grey"], linestyle=":", linewidth=0.8, alpha=0.5)
    ax1.annotate(
        "Default (τ = 2%)",
        xy=(2.0, 33),
        fontsize=7.5,
        ha="center",
        color=COLORS["grey"],
        style="italic",
    )

    # Elbow annotation
    ax1.annotate(
        "Rounding\nelbow",
        xy=(0.5, 84),
        xytext=(1.5, 55),
        fontsize=7.5,
        color=COLORS["blue"],
        ha="center",
        arrowprops={"arrowstyle": "->", "color": COLORS["blue"], "lw": 0.8},
    )

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    leg = ax1.legend(
        lines1 + lines2,
        labels1 + labels2,
        loc="lower right",
        framealpha=0.95,
        borderpad=0.6,
        handlelength=2.2,
    )
    leg.get_frame().set_linewidth(0.5)

    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "figure_1_tolerance_sensitivity.png", bbox_inches="tight")
    plt.close(fig)
    print("  Figure 1 saved")


def figure_2_longitudinal_coverage():
    """Bar chart: coverage by year with phase annotations."""
    fig, ax = plt.subplots(figsize=(7, 3.8))

    bar_colors = []
    for year in YEARS:
        if year == 2014:
            bar_colors.append(COLORS["anomaly"])
        elif year <= 2013:
            bar_colors.append(COLORS["phase1"])
        elif year <= 2019:
            bar_colors.append(COLORS["phase2"])
        else:
            bar_colors.append(COLORS["phase3"])

    bars = ax.bar(
        YEARS,
        COVERAGE_LONGITUDINAL,
        color=bar_colors,
        width=0.75,
        edgecolor="white",
        linewidth=0.5,
    )

    # Add value labels on bars
    for _year, cov, bar in zip(YEARS, COVERAGE_LONGITUDINAL, bars, strict=False):
        if cov > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                cov + 1.2,
                f"{cov:.0f}",
                ha="center",
                va="bottom",
                fontsize=6.5,
                color="#444444",
            )

    ax.set_xlabel("Year")
    ax.set_ylabel("Matching coverage (%)")
    ax.set_ylim(0, 105)
    ax.set_xticks(YEARS)
    ax.set_xticklabels([str(y) for y in YEARS], rotation=45, ha="right")
    ax.yaxis.set_major_locator(mticker.MultipleLocator(20))
    ax.grid(axis="y", alpha=0.15, linewidth=0.5)

    # Phase brackets with labels
    bracket_y = 99
    for start, end, label in [
        (2010, 2013, "Phase 1: Sparse FVM\nregistration"),
        (2015, 2019, "Phase 2: Improving\ncoverage"),
        (2020, 2023, "Phase 3: Stable\nhigh coverage"),
    ]:
        mid = (start + end) / 2
        ax.annotate(
            "",
            xy=(start - 0.3, bracket_y),
            xytext=(end + 0.3, bracket_y),
            arrowprops={"arrowstyle": "|-|", "color": "#888888", "lw": 0.6, "mutation_scale": 4},
        )
        ax.text(
            mid,
            bracket_y + 1.5,
            label,
            ha="center",
            va="bottom",
            fontsize=6.5,
            color="#555555",
            linespacing=1.2,
        )

    # 2014 anomaly
    ax.annotate(
        "CVR = NULL\nin FVM 2015",
        xy=(2014, 2),
        xytext=(2014.8, 22),
        fontsize=7,
        color=COLORS["anomaly"],
        arrowprops={"arrowstyle": "->", "color": COLORS["anomaly"], "lw": 0.8},
        ha="left",
    )

    # Legend
    legend_patches = [
        mpatches.Patch(color=COLORS["phase1"], label="Phase 1 (2010-2013)"),
        mpatches.Patch(color=COLORS["phase2"], label="Phase 2 (2015-2019)"),
        mpatches.Patch(color=COLORS["phase3"], label="Phase 3 (2020-2023)"),
        mpatches.Patch(color=COLORS["anomaly"], label="2014 data anomaly"),
    ]
    leg = ax.legend(
        handles=legend_patches,
        loc="upper left",
        fontsize=7.5,
        framealpha=0.95,
        borderpad=0.5,
    )
    leg.get_frame().set_linewidth(0.5)

    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "figure_2_longitudinal_coverage.png", bbox_inches="tight")
    plt.close(fig)
    print("  Figure 2 saved")


def _draw_box(
    ax,
    x,
    y,
    w,
    h,
    text,
    facecolor="#f5f5f5",
    edgecolor="#333333",
    textcolor="#111111",
    fontsize=8,
    fontweight="normal",
    lw=1.0,
    style="round,pad=0.15",
):
    """Draw a rounded box with centered text."""
    box = mpatches.FancyBboxPatch(
        (x - w / 2, y - h / 2),
        w,
        h,
        boxstyle=style,
        facecolor=facecolor,
        edgecolor=edgecolor,
        linewidth=lw,
        zorder=2,
    )
    ax.add_patch(box)
    ax.text(
        x,
        y,
        text,
        ha="center",
        va="center",
        fontsize=fontsize,
        fontweight=fontweight,
        color=textcolor,
        zorder=3,
        linespacing=1.3,
    )
    return box


def _draw_arrow(ax, x1, y1, x2, y2, color="#333333", lw=1.0, style="-|>", linestyle="-"):
    """Draw an arrow between two points."""
    ax.annotate(
        "",
        xy=(x2, y2),
        xytext=(x1, y1),
        arrowprops={"arrowstyle": style, "color": color, "lw": lw, "linestyle": linestyle},
        zorder=1,
    )


def figure_3_pipeline_schematic():
    """Flow diagram: pipeline from data sources to field-level output."""
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    ax.set_xlim(-0.2, 8.2)
    ax.set_ylim(-0.3, 5.8)
    ax.axis("off")

    # ── Row 1: Data sources ──
    src_y = 5.0
    _draw_box(
        ax,
        1.5,
        src_y,
        2.6,
        0.8,
        "SJI Spray Journals\n(Year X)",
        facecolor="#e3f2fd",
        edgecolor=COLORS["blue"],
        lw=1.2,
    )
    _draw_box(
        ax,
        5.5,
        src_y,
        2.6,
        0.8,
        "FVM Field Boundaries\n(Year X+1)",
        facecolor="#e3f2fd",
        edgecolor=COLORS["blue"],
        lw=1.2,
    )

    # Y+1 offset annotation
    ax.annotate(
        "",
        xy=(4.0, 4.45),
        xytext=(3.0, 4.45),
        arrowprops={"arrowstyle": "<->", "color": COLORS["danger"], "lw": 1.2},
    )
    ax.text(
        3.5,
        4.15,
        "Y+1 temporal offset",
        ha="center",
        fontsize=7.5,
        color=COLORS["danger"],
        style="italic",
    )

    # ── Row 2: Matching engine ──
    match_y = 3.2
    _draw_box(
        ax,
        3.5,
        match_y,
        3.2,
        0.9,
        "CVR + Crop Code\nArea Matching (±2%)",
        facecolor="#e8f5e9",
        edgecolor=COLORS["green"],
        fontweight="bold",
        fontsize=9,
        lw=1.3,
    )

    # Arrows: sources → matching
    _draw_arrow(ax, 1.5, src_y - 0.4, 2.6, match_y + 0.45, color=COLORS["blue"])
    _draw_arrow(ax, 5.5, src_y - 0.4, 4.4, match_y + 0.45, color=COLORS["blue"])

    # ── Row 3: Strategies ──
    strat_y = 1.6
    strategies = [
        (1.3, "S1: Full area match\n(~92% of records)", COLORS["phase3"]),
        (4.0, "S2: Non-organic\nfallback (<0.2%)", COLORS["phase2"]),
        (6.7, "S3: Partial field\ncoverage", COLORS["phase1"]),
    ]
    for x, text, color in strategies:
        _draw_box(
            ax,
            x,
            strat_y,
            2.2,
            0.7,
            text,
            facecolor=color + "22",
            edgecolor=color,
            fontsize=7,
            lw=0.8,
        )

    # Arrows: matching → strategies
    for x, _, _ in strategies:
        _draw_arrow(ax, 3.5, match_y - 0.45, x, strat_y + 0.35, color="#888888", lw=0.7)

    # Sequential labels
    for x, label in [(2.1, "1st"), (4.0, "2nd"), (5.6, "3rd")]:
        ax.text(
            x,
            2.25,
            label,
            fontsize=6,
            color="#999999",
            ha="center",
            style="italic",
            fontweight="bold",
        )

    # ── Row 4: Output ──
    out_y = 0.2
    _draw_box(
        ax,
        3.5,
        out_y,
        3.8,
        0.8,
        "Field-level pesticide doses + quality scores",
        facecolor="#333333",
        edgecolor="#111111",
        textcolor="white",
        fontweight="bold",
        fontsize=8.5,
        lw=1.2,
    )

    # Arrows: strategies → output
    for x, _, _ in strategies:
        _draw_arrow(ax, x, strat_y - 0.35, 3.5, out_y + 0.4, color="#888888", lw=0.7)

    # BMD box (small, positioned near output as post-hoc)
    _draw_box(
        ax,
        7.2,
        2.9,
        1.8,
        0.65,
        "BMD Product\nRegistry",
        facecolor="#f5f5f5",
        edgecolor="#aaaaaa",
        lw=0.8,
        textcolor="#888888",
        fontsize=7,
    )
    _draw_arrow(ax, 7.2, 2.55, 5.4, out_y + 0.4, color="#aaaaaa", lw=0.8, linestyle="--")
    ax.text(
        6.7, 1.5, "post-hoc\nvalidation", ha="center", fontsize=6.5, color="#aaaaaa", style="italic"
    )

    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "figure_3_pipeline_schematic.png", bbox_inches="tight")
    plt.close(fig)
    print("  Figure 3 saved")


if __name__ == "__main__":
    print("Generating publication figures...")
    figure_1_tolerance_sensitivity()
    figure_2_longitudinal_coverage()
    figure_3_pipeline_schematic()
    print(f"\nAll figures saved to {FIGURES_DIR}/")
