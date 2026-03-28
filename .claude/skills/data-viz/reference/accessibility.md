# Accessibility — Detailed Reference

8% of men and 0.5% of women have color vision deficiency. Accessibility isn't optional — it's a requirement for truthful data communication.

## WCAG 2.1 AA Requirements for Charts

### Contrast Ratios
| Element | Minimum Ratio | Notes |
|---|---|---|
| Text labels | 4.5:1 | Against background |
| Large text (≥18px / 14px bold) | 3:1 | Against background |
| Graphical elements (bars, lines, points) | 3:1 | Against background |
| Adjacent data series | 3:1 | Between each other |

### Non-Text Content (1.1.1)
Every chart must have a text alternative that describes the **insight**, not the chart type.

```tsx
// BAD
aria-label="Bar chart"
aria-label="Chart showing data"

// GOOD
aria-label="Danish pesticide use declined 30% between 2020 and 2025, with the sharpest drop in Sjælland"
aria-label="Pig transport distances are longest in Jylland, averaging 145km compared to 62km in Sjælland"
```

### Use of Color (1.4.1)
Color must not be the sole means of conveying information.

**Always pair color with at least one of:**
- Shape (circles vs squares vs triangles for scatter plots)
- Pattern (solid vs dashed vs dotted for lines)
- Direct text labels
- Position (small multiples instead of overlaid colored series)

## Colorblind-Safe Palettes

### Default Categorical Palette (5 colors)

These colors are distinguishable across all common color vision deficiencies:

```css
--viz-blue:    oklch(55% 0.15 250);   /* Safe for all types */
--viz-orange:  oklch(65% 0.15 55);    /* Distinct from blue in all types */
--viz-teal:    oklch(60% 0.12 195);   /* Separates from blue by lightness */
--viz-rose:    oklch(55% 0.15 15);    /* Warm, distinct in deuteranopia */
--viz-purple:  oklch(50% 0.15 300);   /* Dark, distinct by lightness */
```

### Sequential Palette (single hue, varying lightness)
For ordered data (low → high):
```css
--seq-1: oklch(95% 0.03 250);  /* Lightest */
--seq-2: oklch(80% 0.08 250);
--seq-3: oklch(65% 0.12 250);
--seq-4: oklch(50% 0.15 250);
--seq-5: oklch(35% 0.15 250);  /* Darkest */
```

### Diverging Palette (negative ↔ positive)
Blue ↔ neutral ↔ Orange:
```css
--div-neg-2: oklch(45% 0.15 250);  /* Strong blue */
--div-neg-1: oklch(70% 0.10 250);  /* Light blue */
--div-zero:  oklch(92% 0.01 90);   /* Near-white warm */
--div-pos-1: oklch(70% 0.10 55);   /* Light orange */
--div-pos-2: oklch(50% 0.15 55);   /* Strong orange */
```

### Colors to AVOID in Combination
| Combination | Problem | Who It Affects |
|---|---|---|
| Red + Green | Indistinguishable | Deuteranopia, protanopia (8% of men) |
| Green + Brown | Merge together | Deuteranopia |
| Blue + Purple | Very similar | Tritanopia |
| Red + Orange | Blur together | Protanopia |
| Light green + Yellow | Merge | Deuteranopia |

## Screen Reader Support

### Chart Structure
```tsx
<figure role="figure" aria-label="[Insight description]">
  <figcaption>
    <h3>[Insight title]</h3>
    <p>[Scope subtitle]</p>
  </figcaption>

  {/* Visible chart */}
  <svg role="img" aria-hidden="true">
    {/* Chart content — hidden from screen readers */}
  </svg>

  {/* Screen reader data table (visually hidden) */}
  <table className="sr-only">
    <caption>[Same as aria-label]</caption>
    <thead>
      <tr><th>Category</th><th>Value</th></tr>
    </thead>
    <tbody>
      {data.map(d => (
        <tr key={d.label}>
          <td>{d.label}</td>
          <td>{d.value}</td>
        </tr>
      ))}
    </tbody>
  </table>

  <p className="text-xs text-neutral-400">Source: {source}</p>
</figure>
```

### Why a Hidden Table?
SVG charts are opaque to screen readers. A visually-hidden `<table>` provides the same data in an accessible format. This is the approach used by NYT, FT, and the BBC.

### Visually Hidden Class
```css
.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border-width: 0;
}
```

## Keyboard Navigation

For interactive charts (tooltips, filters, selections):

```tsx
// Make data points focusable
<circle
  tabIndex={0}
  role="img"
  aria-label={`${label}: ${value}`}
  onKeyDown={(e) => {
    if (e.key === 'Enter' || e.key === ' ') {
      showTooltip(dataPoint);
    }
  }}
/>
```

### Focus Indicators
- Visible focus ring on all interactive elements
- `outline: 2px solid oklch(55% 0.15 250)` (matches brand blue)
- `outline-offset: 2px`
- Never `outline: none` without a visible alternative

## Reduced Motion

```tsx
const prefersReducedMotion =
  typeof window !== 'undefined' &&
  window.matchMedia('(prefers-reduced-motion: reduce)').matches;

// In chart config:
const animationDuration = prefersReducedMotion ? 0 : 600;
```

Or in CSS:
```css
@media (prefers-reduced-motion: reduce) {
  svg * {
    animation-duration: 0s !important;
    transition-duration: 0s !important;
  }
}
```

## Testing Checklist

- [ ] Run Chrome DevTools color blindness simulator (Rendering → Emulate vision deficiencies)
- [ ] Print chart in grayscale — still readable?
- [ ] Navigate chart with keyboard only — all data accessible?
- [ ] Screen reader announces chart insight and data values?
- [ ] Text meets 4.5:1 contrast ratio?
- [ ] Graphical elements meet 3:1 contrast ratio?
- [ ] Color is never the sole differentiator?
- [ ] Reduced motion preference respected?
