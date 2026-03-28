# Annotation Strategy — Detailed Reference

Annotations are the bridge between data and understanding. Research shows that well-annotated charts significantly outperform minimal charts in reader comprehension and engagement.

## The Annotation Hierarchy

### Level 1: Structural (Always Present)
- **Insight title**: States the finding, not the topic
- **Scope subtitle**: Units, time range, population, methodology
- **Axis labels**: Only when not self-evident from title/subtitle
- **Source line**: Dataset name, year, organization

### Level 2: Data Labels (Prefer Over Legend)
- **Direct labels**: On or adjacent to data elements
- **End-of-line labels**: Series name at the end of each line
- **Value labels**: On bars/points when precision matters
- **Min/Max markers**: Highlight extremes when they're the story

### Level 3: Contextual (Add When They Illuminate)
- **Reference lines**: Industry average, target, regulatory threshold
- **Event markers**: Policy changes, crises, seasons that explain shifts
- **Callout annotations**: Short text + leader line pointing to key data
- **Trend annotations**: "↑ 30% since 2020" directly on the chart

### Level 4: Narrative (For Storytelling Pieces)
- **Scrollytelling integration**: Progressive reveal as user scrolls
- **Step-by-step annotations**: Guided tour through the data
- **Comparison annotations**: "Same period last year: X"

## Title Writing

### Pattern: Insight + Direction + Magnitude

```
BAD:  "Pesticide Use in Denmark"
      (Description — tells me the topic, not the finding)

GOOD: "Danish pesticide use fell 30% after 2020 regulation"
      (Insight — tells me what happened and why it matters)

GOOD: "Sjælland farms use 3× more pesticide per hectare than Jylland"
      (Comparison — states the surprising finding)

GOOD: "Pig transport distances doubled since 2018"
      (Trend — clear direction and timeframe)
```

### Subtitle Pattern

```
"[Unit of measurement], [population/scope], [time range]"

Examples:
"Kg active ingredient per hectare, farms >50ha, 2015–2025"
"Monthly average temperature (°C), all weather stations, 2020–2025"
"Number of pig transports per week, national total"
```

## Direct Labeling Techniques

### Line Charts
Place series label at the rightmost point, slightly offset:
```
                          ╱ Sjælland
    ╱‾‾‾╲    ╱‾‾‾‾‾‾‾‾‾╱
   ╱      ╲╱            Fyn
  ╱
```
- If lines cross: label the last visible segment
- If lines are close: stagger label y-positions
- Color-match the label to the line

### Bar Charts
Place value at the end of the bar:
```
Sjælland  ████████████ 1,245
Fyn       █████████ 932
Jylland   ███████ 715
```
- Inside the bar if bar is wide enough and contrast is sufficient
- Outside (right of) the bar otherwise
- Always right-aligned for horizontal bars

### Scatter Plots
Label notable outliers only. Don't label every point:
```
    ·  · ·
  ·   ·    · ← [Farm X: unusually high]
·   · · ·
  ·  ·   ·
```

## Reference Lines and Bands

### When to Add
- **Regulatory thresholds**: "EU limit: 50 mg/L"
- **Averages**: "National mean" (but be careful — means can mislead)
- **Targets**: "2030 goal"
- **Historical baselines**: "Pre-regulation level"

### Styling
- Dashed line, low contrast (`oklch(80% 0 0)`)
- Label at the end of the line, not in a tooltip
- Never bolder than the data lines

```tsx
<ReferenceLine
  y={50}
  stroke="oklch(75% 0 0)"
  strokeDasharray="6 4"
  label={{
    value: "EU limit (50 mg/L)",
    position: "right",
    className: "text-[11px] fill-neutral-400"
  }}
/>
```

## Event Markers

For time-series charts, mark events that explain data shifts:

```tsx
<ReferenceLine
  x="2020-06-15"
  stroke="oklch(70% 0 0)"
  strokeDasharray="3 3"
>
  <Label
    value="New pesticide regulation"
    position="top"
    className="text-[11px] fill-neutral-500"
    angle={0}
  />
</ReferenceLine>
```

### Positioning Rules
- Above the chart area for events
- To the right of the data for values
- Never overlapping data points
- Use leader lines (thin, light) to connect label to reference point

## Callout Annotations

For highlighting specific data points with explanation:

```
    ╱‾‾╲
   ╱    ╲         ┌──────────────────────┐
  ╱      ·←───────│ 2022 peak: drought   │
 ╱        ╲       │ reduced crop yield    │
╱          ╲      └──────────────────────┘
```

### Rules
- Short text (1–2 lines max)
- Leader line: thin, light gray, optional arrowhead
- Position to avoid occluding other data
- Use sparingly — 1–3 per chart maximum

## The FT/NYT Annotation Style

Financial Times and New York Times graphics share a recognizable annotation style:

1. **Serif font for annotations** (contrast with sans-serif data labels)
2. **Muted gray for structural elements** (axes, gridlines, reference)
3. **Bold color only for data** (the lines, bars, points)
4. **Generous whitespace** around annotations
5. **Conversational tone** in callouts ("This spike coincides with...")
6. **Always cite methodology** in a footnote

## Annotation Don'ts

- Don't annotate everything — highlight what's surprising or important
- Don't use tooltips as a substitute for visible annotations (tooltips are hidden by default)
- Don't put the insight in a paragraph below the chart — put it ON the chart
- Don't use annotation arrows that cross data lines
- Don't annotate obvious points — if the bar is clearly the tallest, you don't need to say "highest"
