# Landbruget.dk - Agent Development Guide

## Project Overview

**Mission**: Organize information about the Danish agricultural sector and make it universally accessible and useful.

**Architecture**: Monorepo with Python backend (data pipelines) and Next.js frontend (React 19, TypeScript)

**Database**: Supabase (PostgreSQL with PostGIS)

**Infrastructure**:
- Data Storage: Google Cloud Storage (GCS)
- Deployment: Vercel (frontend)
- CI/CD: GitHub Actions

## Core Architecture Principles

### 1. Data-Centric Design
- All data must be joinable on: CVR (company ID), CHR (herd ID), BFE (cadastral ID), or geospatial coordinates
- Follow medallion architecture: Bronze (raw) → Silver (cleaned) → Gold (analysis-ready)
- Data sources update weekly (Mondays 2 AM UTC) or via manual PRs

### 2. Separation of Concerns
- **Backend**: Data ingestion, transformation, API endpoints
- **Frontend**: Data visualization, user interaction
- **Supabase**: Database, RLS policies, materialized views

### 3. Testing Philosophy
- **Frontend**: Playwright E2E tests for critical user flows
- **Backend**: Pytest for pipeline logic and data quality
- TDD workflow strongly preferred for new features
- Tests must pass before any PR merge

## Key Commands

### Frontend Development
```bash
cd frontend
npm run dev              # Start Next.js dev server (with Turbopack)
npm test                 # Run Playwright E2E tests
npm run test:ui          # Playwright UI mode
npm run lint             # Run oxlint (50-100x faster than ESLint)
npm run format           # Format with Prettier
npm run test:smoke       # Quick smoke tests
```

### Backend Development
```bash
cd backend
source venv/bin/activate  # Activate Python virtual environment
python -m pytest          # Run all tests
cd pipelines/<pipeline_name> && python main.py  # Run specific pipeline
```

### Database Management
```bash
supabase link --project-ref <ref>  # Link to remote Supabase project
supabase db pull                    # Pull latest schema
supabase migration new <name>       # Create new migration
supabase db push                    # Push migrations to remote
```

## Project Structure

```
.
├── frontend/           # Next.js 15 app with TypeScript
│   ├── src/
│   │   ├── app/       # Next.js app router pages
│   │   ├── components/ # Reusable React components
│   │   ├── hooks/     # Custom React hooks
│   │   ├── lib/       # Utilities and helpers
│   │   ├── services/  # API client services
│   │   ├── stores/    # Zustand state management
│   │   └── types/     # TypeScript type definitions
│   └── public/        # Static assets
│
├── backend/
│   ├── api/           # FastAPI endpoints
│   ├── common/        # Shared utilities
│   ├── migrations/    # Database migrations (legacy)
│   └── pipelines/     # Data ingestion pipelines
│       ├── unified_pipeline/    # 18+ Danish govt data sources
│       ├── chr_pipeline/        # Livestock tracking (CHR)
│       ├── drive_data_pipeline/ # Regulatory compliance data
│       ├── svineflytning_pipeline/ # Pig movement tracking
│       └── [others]/            # See docs/PIPELINE_INDEX.md
│
├── supabase/
│   ├── migrations/    # Supabase SQL migrations
│   └── functions/     # Edge functions
│
├── docs/              # Comprehensive documentation
│   ├── analysis/      # Research findings
│   ├── troubleshooting/ # Problem resolution guides
│   └── PIPELINE_INDEX.md # Complete pipeline documentation
│
└── scripts/           # Utility scripts
    ├── setup-worktree.sh  # Conductor workspace setup
    └── [others]/
```

## Critical Files & Locations

### Frontend Key Files
- `frontend/src/app/page.tsx` - Homepage/landing page
- `frontend/src/components/` - All reusable UI components
- `frontend/src/services/supabase.ts` - Supabase client setup
- `frontend/package.json` - Dependencies and scripts
- `frontend/.env.example` - Environment variable template

### Backend Key Files
- `backend/pipelines/unified_pipeline/` - Primary data collection system
- `backend/common/` - Shared utilities across pipelines
- `backend/pyproject.toml` - Python dependencies
- `backend/.env.example` - Environment variable template

### Database
- `supabase/migrations/` - All database schema changes
- `supabase/migrations/20250830083344_remote_schema.sql` - Current full schema

## Environment Variables

### Frontend (`.env`)
```bash
NEXT_PUBLIC_API_URL=<supabase_project_url>
NEXT_PUBLIC_API_KEY=<supabase_anon_key>
```

### Backend (`.env`)
```bash
SUPABASE_URL=<supabase_project_url>
SUPABASE_KEY=<supabase_service_role_key>
GCS_BUCKET=<google_cloud_storage_bucket>
GCS_CREDENTIALS=<path_to_service_account_json>
```

## Coding Standards

### TypeScript/React
- Use functional components with hooks
- Prefer composition over inheritance
- Use TypeScript strict mode
- Component file structure: `ComponentName.tsx`
- Test file structure: `ComponentName.test.tsx`
- Keep components small and focused

### Python
- Follow PEP 8 style guide
- Use type hints (Python 3.11+)
- Docstrings for all public functions
- Use `ruff` for linting/formatting
- Test file structure: `test_*.py` or `*_test.py`

### Git Workflow
- Branch naming: `<type>/<short-description>` (e.g., `feat/add-map-view`, `fix/data-loading`)
- Commit messages: Conventional Commits format
  - `feat:` new features
  - `fix:` bug fixes
  - `docs:` documentation changes
  - `refactor:` code refactoring
  - `test:` adding/updating tests
  - `chore:` maintenance tasks

### Code Review
- All code must be reviewed before merge
- Tests must pass (frontend + backend)
- No secrets in code (use .env files)
- oxlint must pass (frontend)
- Keep PRs focused and atomic

## Data Sources & Dependencies

### Primary Data Sources
1. **Danish Agricultural Agency** (Landbrugsstyrelsen) - Field boundaries, crop data
2. **CHR Registry** - Livestock tracking and health
3. **Cadastre** (Geodatastyrelsen) - Land ownership, parcels
4. **Environmental Agency** (Miljøstyrelsen) - Pesticides, compliance
5. **Statistics Denmark** (Danmarks Statistik) - Agricultural statistics
6. **Weather Data** (DMI) - Climate and meteorological data

See `docs/PIPELINE_INDEX.md` for complete list (18+ sources).

### Key Data Identifiers
- **CVR**: Danish company registration number (8 digits)
- **CHR**: Central Husbandry Register number (farm/herd ID)
- **BFE**: Cadastral parcel number (field ID)
- **Enhedsnummer**: CVR unit identifier (individual farm/location)

## MCP Server Configuration

This project is configured to work with the following MCP servers:

### Supabase MCP Server
```bash
npx -y @supabase/mcp-server-supabase@latest --features=database,docs
```

Capabilities:
- Query database schema and data
- Create/modify tables
- Run SQL queries
- Manage migrations
- Access documentation

### Recommended Additional MCP Servers
- **GitHub MCP** - For PR management and issue tracking
- **Web Search MCP** - For researching Danish agricultural regulations
- **File System MCP** - For local file operations

## Agent-Specific Guidelines

### When Starting a New Task
1. **Read context**: Check relevant CLAUDE.md files and documentation
2. **Understand scope**: Read the issue/task description thoroughly
3. **Check dependencies**: Ensure all required files and data are available
4. **Plan approach**: Break down complex tasks into smaller steps
5. **Use `/clear`**: Clear context between unrelated tasks

### Testing Requirements
- **Always run tests** before marking tasks complete
- **Frontend**: `cd frontend && npm test`
- **Backend**: `cd backend && python -m pytest`
- **Write new tests** for new features (TDD preferred)

### Common Pitfalls to Avoid
1. **Don't modify** `.env` files - only `.env.example` templates
2. **Don't commit** secrets or credentials
3. **Don't skip** oxlint checks in frontend
4. **Don't break** existing tests without fixing them
5. **Don't modify** database directly - always use migrations
6. **Don't forget** to update documentation when changing architecture

### When Stuck or Confused
1. Check `docs/troubleshooting/` for common issues
2. Review `docs/PIPELINE_INDEX.md` for data pipeline details
3. Look at similar existing code for patterns
4. Ask for clarification if requirements are ambiguous

## Important Notes

### Data Quality & Transparency
- This is a **public transparency project** for Danish agriculture
- Data accuracy is paramount - double-check transformations
- Document all data sources and transformations
- Be honest about data limitations

### Performance Considerations
- Large datasets: Use streaming/pagination
- GCS files: Cache locally when possible
- Database queries: Use materialized views for complex aggregations
- Frontend: Lazy load components and data

### Security
- Never commit `.env` files
- Use environment variables for all secrets
- Implement proper RLS (Row Level Security) in Supabase
- Validate all user inputs

## Quick Reference Links

- **Pipeline Documentation**: `docs/PIPELINE_INDEX.md`
- **Data Lineage**: `docs/DATA_LINEAGE_COMPREHENSIVE.md`
- **Troubleshooting**: `docs/troubleshooting/`
- **Supabase Dashboard**: https://supabase.com/dashboard
- **Project README**: `README.md`

## Commit Message Format

Follow Conventional Commits:

```
<type>(<scope>): <subject>

<body>

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
```

Examples:
- `feat(frontend): add interactive map view for field data`
- `fix(pipeline): correct CHR data transformation logic`
- `docs(readme): update setup instructions for Conductor`
- `refactor(backend): simplify GCS upload utilities`

---

*This guide follows the principles outlined in the "Agent-Native Development Playbook" for systematic, high-quality AI-assisted development.*
