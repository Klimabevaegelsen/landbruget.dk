# Responsive Charts — Detailed Reference

Charts must work from 320px mobile to 2560px ultrawide. Don't just shrink — adapt.

## Strategy: Container-Based Resize

ViewBox scaling distorts text and labels. Instead, measure the container and redraw.

### ResizeObserver Pattern

```tsx
import { useRef, useState, useEffect } from 'react';

function useContainerSize() {
  const ref = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ width: 0, height: 0 });

  useEffect(() => {
    if (!ref.current) return;
    const observer = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect;
      setSize({ width, height });
    });
    observer.observe(ref.current);
    return () => observer.disconnect();
  }, []);

  return { ref, ...size };
}

// Usage
function MyChart({ data }: { data: DataPoint[] }) {
  const { ref, width } = useContainerSize();

  return (
    <div ref={ref} className="w-full">
      {width > 0 && (
        <ResponsiveContainer width={width} height={width * 0.5}>
          <LineChart data={data}>
            {/* Chart content adapts to width */}
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
```

## Breakpoint Adaptations

### Narrow (<480px — Mobile)

**Layout changes:**
- Horizontal bars → stay horizontal (don't flip to vertical — worse on mobile)
- Reduce margins: `{ top: 10, right: 10, bottom: 30, left: 40 }`
- Stack title + subtitle vertically
- Move legend below chart (if legend exists at all)

**Data density:**
- Reduce tick count: `tickCount={3}` for axes
- Abbreviate labels: "Jan" not "January", "1K" not "1,000"
- Hide minor gridlines entirely
- Consider showing fewer data points (every other month, etc.)

**Touch targets:**
- Minimum 44×44px for interactive elements (WCAG 2.5.5)
- Increase hit area around data points: `r={8}` minimum for scatter dots
- Use `touchAction: 'pan-y'` to prevent chart from stealing scroll

**Typography:**
- Axis labels: 11px minimum
- Annotations: 12px minimum
- Title: 16px minimum

### Medium (480–768px — Tablet)

- Standard chart proportions
- Full tick labels (months, not abbreviated)
- Direct labeling preferred over legend
- Aspect ratio: 16:9 or 3:2

### Wide (768px+ — Desktop)

- Full annotations with leader lines
- Small multiples in 2–4 column grid
- Sparklines at full detail
- Generous margins for breathing room

## Aspect Ratio

### Default Ratios by Chart Type

| Chart Type | Recommended Ratio | Why |
|---|---|---|
| Line chart (time series) | 3:1 to 4:1 | Wide to show temporal progression |
| Bar chart (horizontal) | Height = bars × 28px | Scale with data count |
| Bar chart (vertical) | 2:1 | Standard comparison |
| Scatter plot | 1:1 | Preserve relationship proportions |
| Small multiples | 1:1 per panel | Consistent grid |
| Sparkline | 4:1, inline | Word-sized |

### Banking to 45°

Tufte and Cleveland showed that line chart slopes are best perceived at ~45°. If your time series has strong trends, adjust aspect ratio so the average slope banks to 45°.

```tsx
// Rough calculation
const yRange = Math.max(...data.map(d => d.value)) - Math.min(...data.map(d => d.value));
const xRange = data.length;
const aspectRatio = xRange / yRange; // Use as width:height
```

## Container Queries (Modern CSS)

For component-level responsiveness:

```css
.chart-container {
  container-type: inline-size;
  container-name: chart;
}

/* Compact layout for small containers */
@container chart (max-width: 400px) {
  .chart-title { font-size: 14px; }
  .chart-subtitle { display: none; }
  .chart-axis-label { font-size: 10px; }
  .chart-annotation { display: none; } /* Show on wider only */
}

/* Full layout for larger containers */
@container chart (min-width: 600px) {
  .chart-annotation { display: block; }
  .chart-legend { position: absolute; top: 0; right: 0; }
}
```

## Mobile-Specific Patterns

### Swipeable Small Multiples
On mobile, show one panel at a time with swipe navigation:

```tsx
function MobileSmallMultiples({ panels }: { panels: Panel[] }) {
  const [activeIndex, setActiveIndex] = useState(0);

  return (
    <div className="overflow-x-auto snap-x snap-mandatory flex">
      {panels.map((panel, i) => (
        <div
          key={panel.name}
          className="snap-center shrink-0 w-full"
        >
          <Chart data={panel.data} title={panel.name} />
        </div>
      ))}
    </div>
  );
}
```

### Progressive Disclosure
Show summary on mobile, detail on tap:

1. **Default**: Show the headline number or trend
2. **On tap/click**: Expand to full chart
3. **On second tap**: Show data table

### Scroll-Linked Charts
For narrative data pieces on mobile, pin the chart and scroll text over it:

```css
.chart-sticky {
  position: sticky;
  top: 0;
  height: 60vh;
  z-index: 0;
}

.narrative-text {
  position: relative;
  z-index: 1;
  pointer-events: none;
}

.narrative-text p {
  pointer-events: auto;
  background: oklch(100% 0 0 / 0.9);
  padding: 1rem;
  margin: 60vh 0;
}
```

## Performance on Mobile

- Use `will-change: transform` on animated chart elements
- Debounce resize handlers: `requestAnimationFrame` or 100ms throttle
- For large datasets on mobile, pre-aggregate server-side
- Lazy load charts below the fold with `IntersectionObserver`
- Prefer SVG for <500 elements, Canvas for >500
