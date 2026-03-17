# Landbruget.dk

**Making Danish agricultural data transparent and universally accessible.**

Landbruget.dk organizes data from 18+ Danish government sources into a single, queryable platform. We collect, clean, and publish agricultural, environmental, and regulatory data so that journalists, researchers, and citizens can hold the industry accountable.

## Project Structure

```
landbruget.dk/
├── frontend/               # Next.js 16 — interactive map + data visualization
├── frontend-pesticide/     # Next.js 16 — PFAS/pesticide exposure maps
├── data-explorer/          # Next.js 16 — browser-based SQL queries on Parquet files
├── backend/                # Python data pipelines (medallion architecture)
│   ├── pipelines/          # 12 data pipelines (CHR, unified, climate, etc.)
│   └── common/             # Shared utilities (DuckDB, R2, CRS)
├── supabase/               # PostgreSQL migrations + Edge Functions
├── schema/                 # Data catalog (183 datasets) + relationship docs
├── docs/                   # Pipeline index, data lineage, troubleshooting
├── scripts/                # Utility scripts (worktree setup)
└── .github/                # CI/CD workflows (30+ GitHub Actions)
```

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Frontend** | Next.js 16, React 19, TypeScript, Tailwind CSS 4 | Web applications |
| **Maps** | MapLibre GL JS, PMTiles | Geospatial visualization |
| **Backend** | Python 3.11+, DuckDB, ibis-framework | Data pipelines |
| **Database** | Supabase (PostgreSQL 15 + PostGIS) | Storage + API |
| **Data Storage** | Cloudflare R2 | Raw + processed data |
| **CI/CD** | GitHub Actions | Pipeline orchestration |
| **Deployment** | Vercel | Frontend hosting |
| **Linting** | oxlint (frontend), ruff (backend) | Code quality |
| **Testing** | Playwright (frontend), pytest (backend) | Quality assurance |

## Quick Start

### Prerequisites

- Node.js 18+
- Python 3.11+
- Supabase CLI

### Frontend

```bash
cd frontend
cp .env.example .env.local    # Configure Supabase credentials
npm install
npm run dev                   # http://localhost:3000
```

### Data Explorer

```bash
cd data-explorer
cp .env.local.example .env.local  # Add Google API key for Gemini
npm install
npm run dev                       # http://localhost:3000
```

### Backend Pipelines

```bash
cd backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Run a specific pipeline
cd pipelines/unified_pipeline
python -m unified_pipeline bronze --source cadastral
```

## Data Architecture

### Medallion Architecture

All data flows through three layers:

- **Bronze** — Raw data preserved exactly as received from sources. No transformations.
- **Silver** — Cleaned, validated, and standardized. Type coercion, deduplication, format normalization.
- **Gold** — Analysis-ready. Joins across datasets on CVR/CHR/BFE identifiers, derived metrics.

### Coordinate Reference System

All geospatial processing uses **EPSG:25832** (UTM 32N, meters). Data is transformed to **EPSG:4326** (WGS84) only at the final Supabase upload step.

### Data Identifiers

All datasets join on one or more of:

| Identifier | Name | Format | Purpose |
|------------|------|--------|---------|
| **CVR** | Company Registration | 8 digits | Links to companies |
| **CHR** | Central Herd Register | 6 digits | Links to livestock herds |
| **BFE** | Cadastral ID | Variable | Links to land parcels |

## Data Sources

We collect from 18+ official Danish government sources including:

- **Landbrugsstyrelsen** — Field boundaries, crop data, agricultural subsidies
- **Fødevarestyrelsen (FVM)** — Livestock registry (CHR), veterinary data, pig movements
- **Miljøstyrelsen** — Pesticide database (BMD), environmental company registry (DMA)
- **Geodatastyrelsen** — Cadastral data, administrative boundaries
- **Danmarks Statistik** — Agricultural statistics
- **DMI** — Weather and climate data
- **Arbejdstilsynet** — Workplace safety inspections
- **Datafordeleren** — Property ownership data
- **GEUS** — Borehole pesticide data (Dataverse)

See [`docs/PIPELINE_INDEX.md`](docs/PIPELINE_INDEX.md) for the full pipeline documentation.

## Pipelines

| Pipeline | Purpose | Schedule |
|----------|---------|----------|
| [`unified_pipeline`](backend/pipelines/unified_pipeline/) | 18+ government data sources | Weekly (Mon 2 AM UTC) |
| [`chr_pipeline`](backend/pipelines/chr_pipeline/) | Livestock registry + veterinary data | Weekly |
| [`svineflytning_pipeline`](backend/pipelines/svineflytning_pipeline/) | Pig movement tracking | Weekly (Wed 2 AM UTC) |
| [`climate`](backend/pipelines/climate/) | Farm-level CO2e emissions | On demand |
| [`bmd_scraper`](backend/pipelines/bmd_scraper/) | Pesticide database | Monthly |
| [`dma_scraper`](backend/pipelines/dma_scraper/) | Environmental company registry | Monthly |
| [`drive_data_pipeline`](backend/pipelines/drive_data_pipeline/) | Google Drive regulatory docs | On demand |
| [`bbr_buildings`](backend/pipelines/bbr_buildings/) | Building registry | Monthly |
| [`arbejdstilsynet_inspections`](backend/pipelines/arbejdstilsynet_inspections/) | Workplace safety inspections | On demand |
| [`h3_pfas_exposure_pipeline`](backend/pipelines/h3_pfas_exposure_pipeline/) | PFAS exposure mapping | Weekly |
| [`property_owners_sftp`](backend/pipelines/property_owners_sftp/) | Property ownership data | Manual |

## Development

### Running Tests

```bash
# Frontend
cd frontend && npm test         # Playwright E2E
cd frontend && npm run lint     # oxlint

# Backend
cd backend && source venv/bin/activate
python -m pytest                # pytest
ruff check . && ruff format .   # Lint + format
```

### Branch Naming

Format: `<type>/<short-description>` — e.g. `feat/map-view`, `fix/chr-data-load`

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`, `perf`, `build`

### Commit Messages

```
<type>(<scope>): <subject>
```

Examples: `feat(frontend): add interactive map view`, `fix(pipeline): correct CHR transformation`

## Contributing

1. Create a branch from `main` following the naming convention above
2. Make your changes
3. Run all tests (`npm test` + `pytest`)
4. Run linters (`npm run lint` + `ruff check .`)
5. Open a pull request — all PRs require review before merge

## License

See repository license file.
