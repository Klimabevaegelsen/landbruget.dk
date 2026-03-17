# Components — Landbruget.dk

## Data Source Transparency (REQUIRED)

Components displaying agricultural data MUST accept a `source` prop:

```typescript
interface DataComponentProps {
  data: FieldData;
  source: 'api' | 'owner' | 'mixed'; // REQUIRED
}
```

Applies to:

- Any component with "Data", "Metric", or "Stat" in the name
- Components in `fields/`, `companies/`, `farms/`
- Any component displaying agricultural data

Always render a `<SourceBadge source={source} />` alongside the data.

## No Business Logic

Components are presentational only:

- No direct Supabase imports (enforced by oxlint)
- No API calls or data fetching
- No complex calculations or data transformations
- Accept data via props, emit events via callbacks
- Business logic belongs in: Server Components (`app/`), hooks (`hooks/`), or services (`services/`)

## Directory Layout

```
components/
├── ui/        # 48 Radix-based primitives (all 'use client')
├── chart/     # Chart components
├── climate/   # Climate/emissions displays
├── company/   # Company detail views
├── field-analysis/  # Field analysis tools
├── homepage/  # Landing page sections
├── kommuner/  # Municipality views
├── layout/    # Layout wrappers + templates
├── pesticide-analysis/  # Pesticide data displays
└── common/    # Cross-domain shared components
```

## Map Components

- Must be `'use client'` components
- Lazy load with `next/dynamic` and show loading skeleton
- Use PMTiles for large datasets (not GeoJSON)
- Clean up map instance on unmount
- Import: `import Map from 'react-map-gl/maplibre'`

## Gotchas

- Always use `cn()` from `lib/utils.ts` for conditional class merging
- Use `data-testid` on all interactive and key display elements
- Use oxlint (`npm run lint`), NOT eslint
- Use oxfmt (`npm run format`), NOT prettier
- Check existing components for patterns before creating new ones
