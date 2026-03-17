# Landbruget.dk

Public transparency project: organize Danish agricultural data and make it universally accessible.

## Tech Stack

- **Frontend**: Next.js 16, React 19, TypeScript, Tailwind CSS 4, MapLibre GL
- **Backend**: Python 3.11+, DuckDB, Pandas, GeoPandas
- **Database**: Supabase (PostgreSQL 15 + PostGIS)
- **Linting**: oxlint (frontend), ruff (backend)
- **Testing**: Playwright E2E (frontend), Pytest (backend)
- **Infra**: GCS/R2 (data), Vercel (deploy), GitHub Actions (CI/CD)

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

## Reference

- **Rules**: `.claude/rules/` — architecture, data-quality, environment, git-workflow, pipelines, security, testing
- **Skills**: `.claude/skills/` — data-pipeline, playwright-testing, supabase-migration, code-review, gcs-data-catalog
- **Commands**: `.claude/commands/` — run-tests, run-pipeline, db-migrate, fix-lint, validate-data, create-pr, new-component
- **Pipeline docs**: `docs/PIPELINE_INDEX.md`
- **Troubleshooting**: `docs/troubleshooting/`
