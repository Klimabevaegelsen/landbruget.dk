# Agricultural Data Frontend

Interactive map visualization of Danish agricultural and environmental data.

## Features

- Interactive map with MapLibre GL JS and PMTiles
- Agricultural fields, wetland areas, environmental projects
- Company pages with climate, pesticide, and compliance data
- Municipality-level rankings and statistics
- Homepage with national statistics and rankings
- Global search across farms, companies, and locations
- Layer controls and data filtering
- Field analysis tools (markanalyse)
- Mobile-optimized responsive design with WCAG 2.1 AA compliance
- Dark mode support

## Tech Stack

| Technology     | Purpose                                  |
| -------------- | ---------------------------------------- |
| Next.js 16     | App Router, Server Components, Turbopack |
| React 19       | UI framework                             |
| TypeScript     | Type safety (strict mode)                |
| Tailwind CSS 4 | Styling (mobile-first, dark mode)        |
| MapLibre GL JS | Map rendering                            |
| PMTiles        | Vector tile serving                      |
| Zustand        | Minimal global state (URL hash only)     |
| Radix UI       | Accessible component primitives          |
| CVA            | Variant-based component styling          |

## Getting Started

```bash
cp .env.example .env.local    # Configure Supabase credentials
npm install
npm run dev                   # http://localhost:3000 (Turbopack)
```

## Project Structure

```
src/
├── app/                    # App Router pages
│   ├── (main)/             # Main layout group
│   ├── admin/              # Admin pages
│   ├── markanalyse/        # Field analysis
│   └── api/                # API routes
├── components/             # UI components
│   ├── ui/                 # Radix-based primitives
│   ├── homepage/           # Homepage sections
│   ├── company/            # Company detail pages
│   ├── climate/            # Carbon emissions display
│   ├── environmental/      # Environmental data
│   ├── kommuner/           # Municipality views
│   ├── field-analysis/     # Field analysis tools
│   ├── pesticide-analysis/ # Pesticide data
│   ├── chart/              # Chart components
│   ├── layout/             # Layout components
│   └── common/             # Shared components
├── hooks/                  # Custom hooks (caching, navigation, gestures)
├── lib/                    # Utilities (cn, env, cache-utils)
├── services/               # Data fetching layer
│   └── supabase/           # apiFetch() wrappers per domain
├── stores/                 # Zustand (hashStore only)
├── types/                  # Third-party type definitions
├── content/                # Static content
└── utils/                  # Utility functions
```

## Architecture

- **Data fetching**: All HTTP goes through `apiFetch()` in `services/supabase/config.ts`. Components never import Supabase directly — they use service modules or hooks.
- **Environment vars**: Accessed via `lib/env.ts`, not `process.env` directly.
- **Caching**: Tuesday-based localStorage expiration via `lib/cache-utils.ts` (data updates weekly on Tuesdays). Custom hooks (`useCompanyCache`, `useRankingsCache`, `useHomepageStatsCache`) manage cache per domain.
- **State**: Zustand is minimal — only `stores/hashStore.ts` for URL hash state. React hooks for everything else.
- **Components**: Functional only, explicit `*Props` interfaces, `forwardRef` for HTML wrappers, CVA for variants, `cn()` for conditional classes.

## Commands

| Command                | Description                             |
| ---------------------- | --------------------------------------- |
| `npm run dev`          | Dev server (Turbopack)                  |
| `npm test`             | Playwright E2E (all browsers)           |
| `npm run test:smoke`   | Quick smoke test                        |
| `npm run test:ui`      | Interactive Playwright UI mode          |
| `npm run lint`         | oxlint (not ESLint)                     |
| `npm run format`       | oxfmt (includes Tailwind class sorting) |
| `npm run format:check` | Check formatting without modifying      |
| `npm run build`        | Production build                        |

## Testing

Tests use Playwright for E2E testing across Chromium, Firefox, WebKit, and mobile.

- Tests live in `tests/` directory
- Use `data-testid` attributes exclusively for selectors
- See [`tests/README.md`](tests/README.md) for conventions and examples

## Linting

This project uses **oxlint** (not ESLint) and **oxfmt** (not Prettier). A custom oxlint plugin at [`oxlint-plugin-landbruget/`](oxlint-plugin-landbruget/) enforces architectural rules like preventing direct Supabase imports in UI components.
