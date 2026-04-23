# Pesticide Visualization — Landbruget.dk

Standalone Next.js 16 app: H3 hexagonal heatmap visualization of PFAS exposure
and pesticide load data across Denmark. Requires Node >= 22.12.0.

## Commands

```bash
npm run dev           # Dev server (Turbopack)
npm run build         # Production build
npm run lint          # oxlint (primary linter)
npm run lint:eslint   # next lint (secondary, for Next.js-specific rules)
npm run type-check    # tsc --noEmit
npm run format        # oxfmt
npm run format:check  # oxfmt --check
```

## Architecture

- **MapLibre GL** (`^5.6.1`) + **PMTiles** (`^3.2.1`) for base map tiles
- **Zustand v5** for state management (6 stores — much heavier than main frontend)
- **Framer Motion** (`^11.11.0`) for animations
- **Chroma.js** (`^3.1.0`) for color scales
- **Radix UI** for accessible primitives (popover, slider, switch, tooltip)
- **TypeScript**: `strict: false` in tsconfig (relaxed mode)

## Zustand Stores (`src/stores/`)

- `map-store.ts` — Viewport, data mode, layer visibility, tooltip/selection state
- `pmtiles-store.ts` — PMTiles tile source management (24-hour cache)
- `resolution-store.ts` — H3 resolution: `'kommune' | 8 | 10` (zoom-dependent)
- `temporal-store.ts` — Year selection (default 2023), animation playback, cumulative mode
- `ui-store.ts` — Sidebar, theme, mobile detection, performance mode

## Key Types

- `DataMode`: `'pesticide_total' | 'pfas' | 'diquat' | 'glyphosate'`
- `YearSelection`: `number | 'total'`
- Resolution zoom thresholds: `>=12` → res 10, `>=9` → res 8, else → kommune

## Key Services

- `src/services/pmtiles-discovery.ts` — `PMTilesDiscoveryService` discovers tile URLs from manifest at `data.pesticidkortet.dk`
- `src/lib/basemaps.ts` — Denmark PMTiles URL: `pmtiles://https://data.pesticidkortet.dk/pmtiles/protomaps_denmark.pmtiles`
- `src/lib/shared-constants.ts` — Years (2020-2025), GCS paths, cache TTLs, visualization limits

## Key Differences from Main Frontend

- Uses **oxlint + oxfmt** (same as main frontend)
- Uses **PMTiles manifest discovery directly** instead of the main frontend's API wrapper pattern
- Heavy Zustand usage (6 stores vs 1 in main frontend)
- TypeScript `strict: false` (vs strict in main frontend)
- No Playwright tests configured
- Performance-critical: WebGL rendering of 10k+ hexagons

## Environment Variables

```bash
No required runtime env vars for data fetching.
```

Optional: `NEXT_PUBLIC_DEFAULT_MAP_CENTER_LAT` (55.7), `NEXT_PUBLIC_DEFAULT_MAP_CENTER_LON` (12.0), `NEXT_PUBLIC_DEFAULT_MAP_ZOOM` (7), `NEXT_PUBLIC_MAX_H3_HEXAGONS` (10000), `NEXT_PUBLIC_MAX_BNBO_POLYGONS` (1000), `NEXT_PUBLIC_MAX_BBR_BUILDINGS` (5000)

## Gotchas

- H3 resolution affects performance dramatically — use `NEXT_PUBLIC_MAX_H3_HEXAGONS` to limit
- All map components require `'use client'`
- PMTiles cached with 24-hour TTL in pmtiles-store
- Temporal playback animation can cause memory leaks — check cleanup in temporal-store
- Webpack fallbacks disable `fs`, `net`, `tls`, `crypto`, `stream`, `url`, `zlib`, `http`, `https`, `assert`, `os`, `path`, `child_process` on client
- H3 and BNBO are PMTiles-owned in the live runtime; BBR-specific frontend parity is still unresolved and should not be reintroduced via browser Supabase reads
