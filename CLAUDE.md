# Landbruget.dk

Public transparency project: organize Danish agricultural data and make it universally accessible.

## Tech Stack

- **Frontend**: Next.js 15, React 19, TypeScript, Tailwind CSS 4, MapLibre GL
- **Backend**: Python 3.11+, DuckDB, Pandas, GeoPandas
- **Database**: Supabase (PostgreSQL 15 + PostGIS)
- **Linting**: oxlint (frontend), ruff (backend)
- **Testing**: Playwright E2E (frontend), Pytest (backend)
- **Infra**: GCS (data), Vercel (deploy), GitHub Actions (CI/CD)

## Quick Start

```bash
bd ready --json                      # Check available tasks
cd frontend && npm run test:smoke    # Validate setup
```

## Key Commands

```bash
# Frontend
cd frontend && npm run dev           # Dev server (Turbopack)
cd frontend && npm test              # Playwright E2E
cd frontend && npm run lint          # oxlint

# Backend
cd backend && source venv/bin/activate
python -m pytest                     # Run tests
cd pipelines/<name> && python main.py  # Run pipeline

# Database
supabase migration new <name>        # New migration
supabase db push                     # Push migrations
```

## Before ANY Commit

```bash
cd frontend && npm test && npm run lint
cd backend && python -m pytest
```

## Critical Rules

1. **Never commit `.env` files** — use `.env.example` only
2. **Never modify database directly** — always use migrations
3. **Always run tests** before marking work complete
4. **Always validate data transformations twice** — public transparency project
5. **Use absolute paths** in Conductor workspaces
6. **Data joinability**: all data must join on CVR, CHR, BFE, or geospatial coordinates
7. **Medallion architecture**: Bronze (raw) → Silver (cleaned) → Gold (analysis-ready)
8. **CRS**: Process in EPSG:25832, transform to EPSG:4326 only at Supabase upload

## Task Management

- **Beads** (`bd`): Cross-session persistent tasks
- **TodoWrite**: Within-session step tracking
- Never use markdown TODO lists

## SDD Workflow

`/specify` → `/plan` → `/tasks` → `/implement` — see `.claude/specs/README.md`

## Reference

- **Rules**: `.claude/rules/` (testing, security, data-quality, architecture, pipelines, environment, git-workflow)
- **Skills**: `.claude/skills/` (data-pipeline, playwright-testing, supabase-migration, code-review, gcs-data-catalog)
- **Commands**: `.claude/commands/` (run-tests, run-pipeline, db-migrate, fix-lint, validate-data, create-pr, new-component)
- **Pipeline docs**: `docs/PIPELINE_INDEX.md`
- **Troubleshooting**: `docs/troubleshooting/`
