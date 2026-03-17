# Data Explorer — Landbruget.dk

Standalone Next.js 16 app: natural-language queries over agricultural datasets
using DuckDB-WASM (browser-side SQL) + Google Gemini (NL-to-SQL). Zero backend — all SQL runs in the browser.

## Commands

```bash
npm run dev     # Dev server (Turbopack enabled in next.config.ts)
npm run build   # Production build (uses --webpack internally for DuckDB-WASM compat)
npm run lint    # Oxlint
npm run format  # Oxfmt (auto-format)
npm run format:check  # Oxfmt (check only)
npm start       # Start production server
```

## Architecture

- **DuckDB-WASM** (`@duckdb/duckdb-wasm@^1.29.0`) runs entirely in-browser — no backend SQL
- **Data**: Parquet files on Cloudflare R2, registered as DuckDB views via `registerParquetTable()`
- **AI**: Google Gemini (`@google/generative-ai@^0.24.1`) — 2-stage pipeline: table selection → SQL generation
- **Visualization**: `@json-render/react` + `@json-render/shadcn` for AI-generated UI specs
- **Auth**: Cookie-based (`data_auth` cookie, 7-day expiry, FNV-like hash of password)
- **Charts**: Recharts (`^3.8.0`) for bar/line/pie, MapLibre GL for maps, TanStack Table for grids
- **SQL Editor**: CodeMirror 6 with `@codemirror/lang-sql`

## Key Modules

- `src/lib/duckdb.ts` — DuckDB-WASM singleton: `initDuckDB()`, `executeQuery()`, `registerParquetTable()`, `exportToCSV()`
- `src/lib/r2.ts` — `getR2BaseUrl()`: checks `NEXT_PUBLIC_R2_URL` → `NEXT_PUBLIC_R2_BASE_URL` → hardcoded default
- `src/lib/auth.ts` — `getAuthConfig()`: password hash, cookie config
- `src/lib/render/catalog.ts` — Component catalog + `VISUALIZATION_RULES` for AI chart selection
- `src/lib/render/spec-utils.ts` — `injectData()`, `summarizeResults()`, `inferType()`

## API Routes

- `POST /api/ask` — Gemini NL-to-SQL (input: `{question}`, output: `{sql, explanation, tables, tableUrls}`)
- `POST /api/visualize` — Gemini result-to-spec (input: `{question, sql, results}`, output: `{spec}`)
- `POST /api/auth` — Password auth (sets `data_auth` HttpOnly cookie)
- `GET /api/health` — Health check (returns 503 if R2/Gemini not configured)

## Environment Variables

```bash
GOOGLE_API_KEY                 # Required: Gemini API key (server-side only)
SITE_PASSWORD                  # Required: auth password (or DATA_EXPLORER_PASSWORD)
NEXT_PUBLIC_R2_URL             # R2 public bucket URL for Parquet files
```

## Key Differences from Main Frontend

- Uses **oxlint/oxfmt** (same as other frontends), ESLint available via `npm run lint:eslint`
- Uses **Next.js 16** with Turbopack dev + webpack build
- No Playwright tests configured
- No Supabase — all data via DuckDB-WASM + R2 Parquet
- Uses `@json-render` for dynamic UI rendering

## Gotchas

- Build uses webpack (not Turbopack) — required for DuckDB-WASM `asyncWebAssembly` support
- DuckDB-WASM is a singleton — never create multiple connections
- Gemini model: always use `gemini-flash-latest` or `gemini-pro-latest`, never pinned versions
- Webpack config sets `fs`, `net`, `tls` to `false` for client bundles
- Deployed to Vercel region `arn1` (Stockholm) with security headers
