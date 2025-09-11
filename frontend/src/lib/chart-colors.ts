// Tailwind v4 Compatible Chart Colors
// Uses OKLCH color space with proper dark mode support

export const chartColors = {
  // Professional palette using CSS custom properties (for non-Recharts components)
  data: [
    'oklch(var(--color-chart-1))', // Primary - adapts to theme
    'oklch(var(--color-chart-2))', // Secondary
    'oklch(var(--color-chart-3))', // Tertiary
    'oklch(var(--color-chart-4))', // Quaternary
    'oklch(var(--color-chart-5))', // Quinary
    'oklch(var(--color-muted))', // System muted
    'oklch(var(--color-border))', // System border
    'oklch(var(--color-accent))', // System accent
  ],

  // Recharts-compatible colors using HSL custom properties (Tailwind v4 compatible)
  recharts: [
    'hsl(var(--chart-color-1-hsl))', // Theme-aware via CSS custom properties
    'hsl(var(--chart-color-2-hsl))',
    'hsl(var(--chart-color-3-hsl))',
    'hsl(var(--chart-color-4-hsl))',
    'hsl(var(--chart-color-5-hsl))',
    'hsl(var(--chart-color-1-hsl))', // Repeat for more series
    'hsl(var(--chart-color-2-hsl))',
    'hsl(var(--chart-color-3-hsl))',
  ],

  // Theme-aware semantic colors
  primary: 'oklch(var(--color-chart-1))',
  secondary: 'oklch(var(--color-chart-2))',
  muted: 'oklch(var(--color-muted-foreground))',
  accent: 'oklch(var(--color-chart-3))',

  // Agricultural domain colors
  organic: 'oklch(var(--color-organic))',
  conventional: 'oklch(var(--color-conventional))',
  highRisk: 'oklch(var(--color-high-risk))',
  lowRisk: 'oklch(var(--color-low-risk))',
};

// Generate chart configuration for multiple data series
export function generateChartConfig(seriesNames: string[]) {
  const config: Record<string, { label: string; color: string }> = {};

  seriesNames.forEach((name, index) => {
    config[name] = {
      label: name,
      color: chartColors.data[index % chartColors.data.length],
    };
  });

  return config;
}

// Midday-inspired axis styling with theme support
export const chartAxisStyles = {
  x: {
    tickLine: false,
    axisLine: false,
    tickMargin: 8,
    tick: {
      fill: 'oklch(var(--color-muted-foreground))',
      fontSize: 12,
      fontFamily: 'var(--font-plus-jakarta-sans)',
    },
  },
  y: {
    tickLine: false,
    axisLine: false,
    tickMargin: 8,
    tick: {
      fill: 'oklch(var(--color-muted-foreground))',
      fontSize: 12,
      fontFamily: 'var(--font-plus-jakarta-sans)',
    },
  },
};

// Professional grid styling with theme support
export const chartGridStyles = {
  strokeDasharray: '3 3',
  vertical: false,
  stroke: 'oklch(var(--color-border))',
  strokeOpacity: 0.5,
};
