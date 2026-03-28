---
name: data-viz
description: |
  Create truthful, accessible, publication-quality data visualizations inspired by Edward Tufte and data journalism.
  Use this skill when building charts, graphs, dashboards, or any visual representation of data.
  Produces clean, annotated, responsive visualizations that respect the data and the reader.
  Keywords: chart, graph, visualization, dashboard, bar chart, line chart, scatter, map, data, metrics, sparkline
---

# Data Visualization Skill

Create truthful, accessible, publication-quality data visualizations. Every pixel must earn its place. Every chart must respect both the data and the reader.

## Philosophy

> "Above all else show the data." — Edward Tufte

This skill draws from three traditions:

1. **Tufte's statistical graphics** — maximize data-ink, eliminate chartjunk, use small multiples
2. **Data journalism** (NYT Graphics, FT, Reuters, The Pudding) — annotate generously, tell stories, guide the reader
3. **Accessibility-first design** — WCAG compliance, colorblind-safe, screen-reader friendly

The synthesis: charts should be **dense with information, generous with explanation, and accessible to everyone**.

---

## Decision Framework

### 1. What Are You Showing?

| Analytic Goal | First Choice | Second Choice | Avoid |
|---|---|---|---|
| **Comparison** (A vs B) | Bar chart (horizontal if >5 items) | Dot plot, lollipop | Pie chart |
| **Trend over time** | Line chart | Area chart (if showing volume) | Bar chart for >12 points |
| **Distribution** | Histogram, box plot | Violin plot, beeswarm | Pie chart |
| **Relationship** (X↔Y) | Scatter plot | Connected scatter | Dual-axis line |
| **Composition** (parts of whole) | Stacked bar (if few categories) | Treemap, waffle | Pie chart, 3D anything |
| **Geospatial** | Choropleth, dot map | Hex bin, cartogram | 3D globe for flat data |
| **Ranking** | Horizontal bar (sorted) | Slope chart, bump chart | Vertical bar unsorted |
| **Change / Flow** | Slope chart, Sankey | Waterfall | Stacked area (>4 series) |
| **Small-n detail** | Table, inline sparklines | Lollipop chart | Any chart with <5 points |

### 2. How Much Data?

- **<5 values**: Use a table or single number with context. Charts need density.
- **5–30 values**: Standard charts. Label directly, no legend needed.
- **30–1000 values**: Aggregation or small multiples. Consider interaction.
- **1000+ values**: Hex bins, density plots, aggregated views. Never plot raw.

### 3. Chart Selection Checklist

Before coding, answer:
- [ ] Can I explain why this chart type (not another) best serves the data?
- [ ] Would a table be clearer? (For <5 data points, often yes)
- [ ] Am I showing one clear thing, not three muddled things?
- [ ] Does the visual encoding (position, length, color) match the data type?

---

## Core Principles

### Tufte's Laws
→ *Consult [tufte-principles reference](reference/tufte-principles.md) for detailed rules and examples.*

1. **Maximize data-ink ratio**: Every drop of ink should present new information. Remove gridlines, borders, backgrounds, legends (label directly instead), and decorative elements that don't encode data.
2. **Eliminate chartjunk**: No 3D effects, no gradients on bars, no decorative icons, no moiré patterns. If it doesn't represent data, delete it.
3. **Use small multiples**: Instead of one cluttered chart, show many small aligned charts sharing the same scale. Enables comparison without cognitive overload.
4. **Lie factor ≈ 1**: The visual representation of a number should be proportional to the number itself. Never truncate bar charts. Be cautious with area encodings.
5. **Data density**: Strive for charts that reward sustained attention. A chart should contain more information than the paragraph it replaces.

### Data Journalism Practices
→ *Consult [annotation-strategy reference](reference/annotation-strategy.md) for labeling, callouts, and narrative techniques.*

1. **Annotate generously**: Add context directly on the chart — key events, inflection points, policy changes. The FT and NYT annotate *within* the chart, not in a separate paragraph.
2. **Title = insight, not description**: "Farm pesticide use dropped 30% after 2020 regulation" beats "Pesticide Use 2015–2025". The title should state the takeaway.
3. **Subtitle = method + scope**: "Tonnes of active ingredient per hectare, Danish farms with >50ha, annual" — tells the reader exactly what they're seeing.
4. **Source line always**: Every chart needs a source attribution. `Source: Landbrugsstyrelsen, 2025`
5. **Direct labeling over legends**: Label the data series on or near the line/bar. Legends force the reader's eye to ping-pong.

### Accessibility Requirements
→ *Consult [accessibility reference](reference/accessibility.md) for WCAG compliance, ARIA, and testing.*

1. **Colorblind-safe palettes**: Use blue-orange as default diverging. Never rely on red-green distinction alone.
2. **Contrast ratios**: 3:1 minimum for graphical elements (WCAG 2.1 AA), 4.5:1 for text.
3. **Not color alone**: Use pattern, shape, or direct labels alongside color. A chart printed in grayscale should still be readable.
4. **Alt text**: Every chart gets a meaningful `aria-label` describing the trend/insight, not "bar chart of data".
5. **Keyboard navigation**: Interactive charts must be navigable via keyboard.
6. **Reduced motion**: Respect `prefers-reduced-motion`. Animations should enhance, not gatekeep.

---

## Anti-Patterns (Never Do These)

| Anti-Pattern | Why It's Wrong | Do This Instead |
|---|---|---|
| **Truncated bar chart** (y-axis not at 0) | Exaggerates differences by 2–10×. 83% of viewers are misled. | Start bars at 0. Always. |
| **Dual y-axes** | Implies correlation where none exists. Scale is arbitrary. | Two separate charts, aligned horizontally. |
| **Pie chart** | Humans can't compare angles. >3 slices = unreadable. | Horizontal bar chart, sorted. |
| **3D effects** | Distorts perspective, obscures values. Pure chartjunk. | Flat 2D. Always. |
| **Rainbow color scale** | Perceptually non-uniform, colorblind-hostile, hides patterns. | Sequential single-hue or diverging blue-orange. |
| **Spaghetti chart** (>4 overlapping lines) | Unreadable tangle. No insight possible. | Small multiples or highlight one series, gray the rest. |
| **Gratuitous animation** | Delays comprehension, frustrates repeat viewers. | Static with optional progressive reveal. |
| **Decorative icons/illustrations** | Distort area encoding, add noise. Tufte's "ducks." | Let the data shape itself. |
| **Legend instead of direct labels** | Forces eye to shuttle between chart and legend. | Label on the data series directly. |
| **Unsorted bars** | Alphabetical order hides the story. | Sort by value (descending usually). |

---

## Implementation

### Tech Stack

**Primary**: Recharts (React-native, declarative, SVG-based)
**Complex/Custom**: Observable Plot or D3.js (via useRef pattern)
**Maps**: MapLibre GL (already in the project stack)
**Tables with sparklines**: Custom SVG or Recharts `<Sparkline>`

### Component Architecture

```
components/viz/
  Chart.tsx            — Wrapper: responsive container, theme, a11y
  BarChart.tsx         — Horizontal/vertical bar with direct labels
  LineChart.tsx        — Time series with annotations
  ScatterPlot.tsx      — Relationship chart
  SmallMultiples.tsx   — Grid of aligned mini-charts
  Sparkline.tsx        — Inline trend indicator
  ChartAnnotation.tsx  — Callout, reference line, event marker
  ChartSource.tsx      — Source attribution footer
```

### Responsive Strategy
→ *Consult [responsive-charts reference](reference/responsive-charts.md) for breakpoints and mobile adaptation.*

- Use container queries, not viewport queries
- Measure container width, redraw chart to fit
- Mobile: reduce tick count, enlarge touch targets (44×44px min), stack legends vertically
- Never just shrink — adapt (e.g., horizontal bars become vertical on narrow screens)

### Color System
→ *Consult [color-for-data reference](reference/color-for-data.md) for palettes, scales, and OKLCH usage.*

**Categorical** (max 5–7 distinct categories):
```
Blue:    oklch(55% 0.15 250)
Orange:  oklch(65% 0.15 55)
Teal:    oklch(60% 0.12 195)
Rose:    oklch(55% 0.15 15)
Purple:  oklch(50% 0.15 300)
```

**Sequential** (low→high): Single hue, vary lightness (95%→30%)
**Diverging** (negative↔positive): Blue ↔ neutral ↔ Orange

---

## Code Patterns

### Chart Wrapper (Responsive + Accessible)

```tsx
function ChartContainer({
  title,
  subtitle,
  source,
  ariaLabel,
  children,
}: {
  title: string;
  subtitle?: string;
  source: string;
  ariaLabel: string;
  children: React.ReactNode;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);

  useEffect(() => {
    if (!ref.current) return;
    const observer = new ResizeObserver(([entry]) => {
      setWidth(entry.contentRect.width);
    });
    observer.observe(ref.current);
    return () => observer.disconnect();
  }, []);

  return (
    <figure
      ref={ref}
      role="figure"
      aria-label={ariaLabel}
      className="w-full"
    >
      <figcaption>
        <p className="text-lg font-semibold leading-tight">{title}</p>
        {subtitle && (
          <p className="text-sm text-neutral-500 mt-0.5">{subtitle}</p>
        )}
      </figcaption>
      <div className="mt-3">{children}</div>
      <p className="text-xs text-neutral-400 mt-2">Source: {source}</p>
    </figure>
  );
}
```

### Direct Labeling Pattern

```tsx
// Instead of <Legend />, label on the line/bar directly
<text
  x={lastPoint.x + 6}
  y={lastPoint.y}
  className="text-xs fill-current"
  dominantBaseline="middle"
>
  {seriesName}
</text>
```

### Annotation Pattern

```tsx
<ReferenceLine
  x="2020-06"
  stroke="var(--color-neutral-300)"
  strokeDasharray="4 4"
  label={{
    value: "Regulation enacted",
    position: "top",
    className: "text-xs fill-neutral-500",
  }}
/>
```

---

## Quality Checklist

Before shipping any visualization:

- [ ] **Title states the insight**, not just the topic
- [ ] **Subtitle defines scope** (units, time range, population)
- [ ] **Source attributed** at bottom
- [ ] **Chart type justified** — is this the best encoding for this data?
- [ ] **Sorted meaningfully** (by value, not alphabet)
- [ ] **Directly labeled** — no legend ping-pong
- [ ] **Y-axis starts at 0** for bar charts
- [ ] **Colorblind-safe** — tested with simulator
- [ ] **Contrast ≥ 3:1** for all graphical elements
- [ ] **Alt text** describes the insight, not the chart type
- [ ] **Responsive** — tested at 320px, 768px, 1280px
- [ ] **No chartjunk** — every visual element encodes data or aids comprehension
- [ ] **Annotations** added for key events/outliers
- [ ] **Data-ink ratio** — can anything be removed without losing information?
