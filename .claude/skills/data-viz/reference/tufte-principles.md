# Tufte's Principles — Detailed Reference

## The Five Data-Ink Principles

From *The Visual Display of Quantitative Information* (1983):

1. **Above all else show the data**
2. **Maximize the data-ink ratio** — data-ink / total ink ≈ 1.0
3. **Erase non-data-ink** (within reason)
4. **Erase redundant data-ink** (within reason)
5. **Revise and edit**

## Data-Ink Ratio in Practice

### What Counts as Data-Ink
- The bars in a bar chart (their length encodes the value)
- The line in a line chart
- The points in a scatter plot
- Axis tick marks that correspond to data values
- Direct labels on data elements

### What Counts as Non-Data-Ink (candidates for removal)
- Background fills and colors
- Heavy gridlines (lighten to near-invisible or remove)
- Borders around the chart area
- Decorative elements (icons, illustrations, 3D effects)
- Redundant axis labels (when data is directly labeled)
- Legend boxes (replace with direct labeling)

### Removal Strategy

Ask for each element: "If I erase this, does the viewer lose data information?"
- **No** → Remove it
- **Yes, but it's redundant** → Remove the redundant instance
- **Yes, and it's unique** → Keep it

## Chartjunk Categories

Tufte identifies three main offenders:

### 1. Moiré Vibration
Cross-hatching patterns that create optical vibration. In digital contexts: gradients on bars, textured fills, pattern overlays.

**Fix**: Use flat, solid fills. Differentiate with color or position, not texture.

### 2. The Grid
Heavy gridlines that compete with data for visual attention.

**Fix**: Remove gridlines entirely, or reduce to very light (`oklch(95% 0 0)`) horizontal rules. Let the data provide its own structure.

### 3. The Duck
Decorative graphics that have been made to look like data (or data contorted into decorative shapes). Named after a building shaped like a duck.

**Fix**: Let the data shape itself. A bar is a bar. A line is a line. Don't turn bars into buildings, pictograms, or illustrations.

## Small Multiples

### When to Use
- Comparing the same measure across many categories (>4)
- Showing change over time for multiple entities
- Exploring distributions across subgroups
- Any situation where overlaying would create spaghetti

### Implementation Rules
- **Same scale** across all panels (critical for honest comparison)
- **Same axis range** — even if some panels have less data
- **Minimal per-panel decoration** — shared axes, shared title
- **Grid layout** — 2–4 columns, as many rows as needed
- **Panel labels** — short, prominent, top-left of each panel
- **Shared legend** — one for the entire set, or better: direct labeling

### Example Structure
```
[Title: Crop yield by region, 2015–2025]
[Subtitle: Tonnes per hectare, major grain crops]

┌─────────┐ ┌─────────┐ ┌─────────┐
│ Sjælland │ │  Fyn    │ │ Jylland │
│ ╱‾‾╲    │ │  ╱‾╲   │ │ ╱‾‾‾╲  │
│╱    ╲   │ │ ╱   ╲  │ │╱     ╲ │
└─────────┘ └─────────┘ └─────────┘
  2015  2025   2015  2025   2015  2025

[Source: Danmarks Statistik, 2025]
```

## Sparklines

Tufte coined the term: "data-intense, design-simple, word-sized graphics."

### Use Cases
- Inline with table rows (show trend alongside numbers)
- Dashboard KPI cards (show trajectory, not just current value)
- Text paragraphs (embed tiny charts in prose)

### Rules
- No axes, no labels, no gridlines — context comes from surrounding text
- Same height as text line height
- Optional: mark first, last, min, max with dots
- Color: single muted tone, or match text color

## Lie Factor

```
Lie Factor = (size of effect shown in graphic) / (size of effect in data)
```

- **Lie Factor = 1.0** → Honest representation
- **Lie Factor > 1.05** → Exaggerating the effect
- **Lie Factor < 0.95** → Understating the effect

### Common Violations
- **Area encoding**: Doubling a circle's radius quadruples its area → 4× lie factor
- **Truncated axes**: Starting a bar at 50 instead of 0 → effect appears much larger
- **Aspect ratio manipulation**: Stretching/compressing time axis changes perceived slope
- **3D perspective**: Rear bars appear smaller due to perspective

### Prevention
- Bar charts: always start at 0
- Bubble charts: scale by area, not radius (`r = sqrt(value)`)
- Line charts: maintain reasonable aspect ratio (banking to 45°)
- Never use 3D
