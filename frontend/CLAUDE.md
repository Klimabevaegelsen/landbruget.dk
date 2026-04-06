# Frontend — Landbruget.dk

Next.js 16 (App Router, Turbopack), React 19, TypeScript strict, Tailwind CSS 4, oxlint + oxfmt.

## Commands

```bash
npm run dev           # Dev server (Turbopack)
npm test              # Playwright E2E (all browsers)
npm run test:smoke    # Quick smoke test (~30s)
npm run test:ui       # Interactive Playwright debugger
npm run lint          # oxlint (NOT eslint)
npm run format        # oxfmt (includes Tailwind class sorting)
npm run format:check  # Check formatting without modifying
```

## Data Fetching

All data is served from pre-computed JSON on R2 CDN. Fetching goes through `dataFetch()` in `services/data/config.ts`:

```typescript
import { dataFetch } from '@/services/data/config';
const data = await dataFetch<CompanyData>('/companies/12345678.json');
```

- Service modules in `services/data/` wrap `dataFetch()` per domain (climate, company, etc.)
- Server-side caching in `lib/server-cache.ts` uses `unstable_cache` with weekly revalidation
- API routes in `app/api/data/` proxy cached R2 data to the client
- Components never fetch directly — use hooks or services
- Environment vars accessed via `lib/env.ts`, not `process.env` directly

## Component Patterns

- **Functional only** — no class components
- **Explicit props interfaces** — named `*Props`, no `any`
- **`forwardRef`** for HTML element wrappers (Input, Dialog) — always set `displayName`
- **CVA** (class-variance-authority) for variant-based components (Button, Badge)
- **`cn()`** helper from `lib/utils.ts` for all conditional class merging

## State Management

- **Zustand** is minimal — only `stores/hashStore.ts` for URL hash state
- **React hooks** for everything else: `useState`, `useCallback`, `useMemo`
- **Custom hooks** in `hooks/` for caching: `useCompanyCache`, `useRankingsCache`, `useHomepageStatsCache`

## Caching Strategy

Data updates weekly (Tuesdays). Caching reflects this:

- **Client**: Tuesday-based localStorage expiration via `lib/cache-utils.ts` (500-entry max)
- **HTTP**: `stale-while-revalidate` headers configured in `next.config.ts`
- **PMTiles**: 1-year immutable cache headers

## Testing (Playwright)

- Use `data-testid` attributes exclusively for selectors
- Tests live in `tests/` directory
- Runs against Chromium, Firefox, WebKit + mobile variants
- No unit tests — Playwright E2E only

## Directory Structure

```
src/
├── app/           # App Router (route groups, layouts, pages)
├── components/    # UI components
│   └── ui/        # 48 Radix-based primitives (all 'use client')
├── hooks/         # Custom hooks (caching, navigation, toasts)
├── lib/           # Utilities (cn, env, cache-utils, csv-download)
├── services/      # Data layer (data/config.ts = dataFetch from R2 CDN)
├── stores/        # Zustand (minimal — hashStore only)
├── types/         # .d.ts for third-party type defs only
└── content/       # Static content
```

## Lint-Enforced Rules (oxlint custom plugin)

These rules are enforced by `npm run lint` via the custom plugin in `oxlint-plugin-landbruget/plugin.js`:

- **Max 150 lines per file** — split large components into focused modules
- **No service imports in components** — `src/components/` can't import from `@/services/` (type imports OK). Data flows through props or hooks.
- **No raw `process.env`** — use `@/lib/env.ts` instead (except in `lib/env.ts` and `app/api/`)
- **`data-testid` on interactives** — every `<button>`, `<input>`, `<select>`, `<textarea>` needs `data-testid`
- **No `any` types** — use proper TypeScript types
- **No default exports** — use named exports (`export function X`) except in Next.js page/layout/error files. Named exports are grep-able.
- **Absolute imports only** — use `@/components/X` not `../../components/X`. Agents navigate absolute paths better.
- **No inline styles** — use Tailwind classes instead of `style={{...}}`. `style={variable}` is allowed for computed values.
- **No hardcoded API URLs** — no `supabase.co` or `localhost` strings. Use `@/lib/env.ts`.
- **No hardcoded gray-scale colors** — no `zinc-*`, `slate-*`, `gray-*`, `neutral-*`, `stone-*` in className. Use semantic tokens (`text-foreground`, `text-muted-foreground`, `bg-card`, `bg-muted`, `border-border`, etc.) so dark/light mode works.

Config: `frontend/.oxlintrc.json` | Plugin: `frontend/oxlint-plugin-landbruget/plugin.js`

## Common Mistakes to Avoid

- Using Supabase JS client — use `dataFetch()` from `services/data/config.ts` instead
- Using ESLint — use `oxlint` (`npm run lint`)
- Using Prettier — use `oxfmt` (`npm run format`)
- Putting business logic in components — use hooks or services
- Using `any` types — strict mode is enforced
- Creating new Zustand stores without need — prefer React hooks for local state
