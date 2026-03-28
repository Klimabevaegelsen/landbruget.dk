# Color for Data — Detailed Reference

Color in data visualization serves a specific purpose: encoding information. Every color choice must be justifiable.

## Three Types of Color Scales

### 1. Categorical (Qualitative)
**Purpose**: Distinguish unrelated categories (no inherent order)
**Rule**: Maximum perceptual distance between colors, equal visual weight

```css
/* 5-color categorical (colorblind-safe) */
--cat-1: oklch(55% 0.15 250);  /* Blue */
--cat-2: oklch(65% 0.15 55);   /* Orange */
--cat-3: oklch(60% 0.12 195);  /* Teal */
--cat-4: oklch(55% 0.15 15);   /* Rose */
--cat-5: oklch(50% 0.15 300);  /* Purple */

/* 3-color categorical (when fewer categories) */
--cat-1: oklch(55% 0.15 250);  /* Blue */
--cat-2: oklch(65% 0.15 55);   /* Orange */
--cat-3: oklch(55% 0.12 160);  /* Teal-green */
```

**Limits**: Use maximum 5–7 categorical colors. Beyond that, the eye cannot reliably distinguish. If you have more categories, use small multiples or group minor categories into "Other."

### 2. Sequential (Quantitative, Ordered)
**Purpose**: Encode magnitude (low → high)
**Rule**: Single hue, vary lightness monotonically

```css
/* Blue sequential (5 steps) */
--seq-1: oklch(95% 0.02 250);  /* Near white — low values */
--seq-2: oklch(82% 0.06 250);
--seq-3: oklch(68% 0.10 250);
--seq-4: oklch(52% 0.14 250);
--seq-5: oklch(38% 0.15 250);  /* Dark blue — high values */

/* Warm sequential (for different context) */
--seq-w1: oklch(95% 0.02 55);
--seq-w2: oklch(80% 0.08 55);
--seq-w3: oklch(65% 0.12 55);
--seq-w4: oklch(50% 0.15 50);
--seq-w5: oklch(38% 0.14 45);
```

**Why single hue?** Multi-hue sequential scales (e.g., yellow → green → blue) create false perceptual boundaries. The viewer sees "categories" where there's a continuum.

### 3. Diverging (Two-Directional)
**Purpose**: Show deviation from a midpoint (negative ↔ zero ↔ positive)
**Rule**: Two hues with a neutral midpoint, symmetric lightness

```css
/* Blue ↔ Orange diverging */
--div-n3: oklch(40% 0.15 250);  /* Strong negative */
--div-n2: oklch(55% 0.12 250);
--div-n1: oklch(75% 0.06 250);  /* Mild negative */
--div-0:  oklch(93% 0.01 90);   /* Neutral midpoint */
--div-p1: oklch(75% 0.06 55);   /* Mild positive */
--div-p2: oklch(55% 0.12 55);
--div-p3: oklch(45% 0.15 55);   /* Strong positive */
```

**Critical**: The midpoint must be perceptually neutral (near-white or light gray). If the midpoint has visual weight, it distorts the viewer's sense of center.

## OKLCH Color Space

### Why OKLCH Over HSL/Hex?
- **Perceptually uniform**: Equal steps in lightness look equal to the human eye
- **Predictable chroma**: Saturation doesn't shift with hue changes
- **Better interpolation**: Gradients between OKLCH colors don't pass through muddy midpoints

### OKLCH Anatomy
```
oklch(L% C H)
       │  │ │
       │  │ └── Hue: 0–360 (color wheel position)
       │  └──── Chroma: 0–0.4 (saturation intensity)
       └─────── Lightness: 0%–100% (dark to light)
```

### Key Hue Angles
| Hue | Angle | Data Viz Use |
|---|---|---|
| Red | ~25 | Danger, negative, decrease |
| Orange | ~55 | Warm accent, secondary |
| Yellow | ~90 | Caution (use sparingly — low contrast on white) |
| Green | ~145 | Positive, increase, nature |
| Teal | ~195 | Neutral alternative to blue |
| Blue | ~250 | Primary, trust, default |
| Purple | ~300 | Tertiary, often gendered — use carefully |

## Color and Meaning

### Semantic Associations (Danish/European Context)
- **Red**: Negative, decrease, danger, loss
- **Green**: Positive, increase, growth, compliance
- **Blue**: Neutral, water, institutional
- **Orange/Amber**: Warning, attention, moderate concern
- **Gray**: Inactive, baseline, historical comparison

### When to Break Convention
If your data has a natural association (e.g., political parties, crop types), use those colors. Don't force a "clean palette" that makes the reader work harder to map meaning.

## Highlighting Strategy

### The Gray + Accent Pattern
The most effective technique from data journalism:

1. Show all data in **light gray** (`oklch(85% 0 0)`)
2. Highlight the series of interest in **bold color**
3. Label only the highlighted series

```tsx
{allSeries.map(series => (
  <Line
    key={series.name}
    data={series.data}
    stroke={
      series.name === highlightedSeries
        ? 'oklch(55% 0.15 250)'  // Bold blue
        : 'oklch(85% 0 0)'       // Muted gray
    }
    strokeWidth={series.name === highlightedSeries ? 2.5 : 1}
  />
))}
```

This pattern is used extensively by the FT, NYT, and The Economist. It allows showing context (all data) while focusing attention (one series).

## Common Mistakes

### Rainbow Scales
Never use rainbow (ROYGBIV) for sequential data. It:
- Creates false boundaries (yellow "band" looks different from green "band")
- Is invisible to colorblind viewers (red-green collapse)
- Has non-uniform perceived lightness (yellow appears brighter than blue at same chroma)

### Too Many Colors
If you need 8+ distinct colors, your chart design is wrong. Solutions:
- Group minor categories into "Other"
- Use small multiples (one color per panel)
- Use the gray + accent pattern (highlight one at a time)

### Color as Decoration
Never add color "for visual interest." Every color should encode data or aid comprehension. A single-color bar chart (all bars the same blue) is perfectly fine — don't make each bar a different color unless color encodes a dimension.

## Dark Mode Considerations

When supporting dark mode:
- Increase lightness of all chart colors by 10–15%
- Use a very dark gray background (`oklch(15% 0 0)`), not pure black
- Ensure grid lines are subtle (`oklch(25% 0 0)`)
- Test contrast ratios in both modes
- Use `light-dark()` CSS function for automatic switching:

```css
--viz-blue: light-dark(oklch(55% 0.15 250), oklch(70% 0.15 250));
--viz-grid: light-dark(oklch(92% 0 0), oklch(25% 0 0));
--viz-text: light-dark(oklch(30% 0 0), oklch(85% 0 0));
```
