# Landbruget.dk - Agent Development Guide

## Conductor Workspace Setup

**Workspace Path**: `/Users/martincollignon/conductor/landbruget.dk/.conductor/la-paz-v5`

**CRITICAL**: All file operations MUST use absolute paths starting with this workspace directory. Never read/write to the parent repository at `/Users/martincollignon/conductor/landbruget.dk`.

---

## Spec-Driven Development (SDD)

For new features, use the four-phase SDD workflow:

1. `/specify <feature>` - Create structured specification
2. `/plan` - Design technical architecture
3. `/tasks` - Break down into atomic tasks
4. `/implement [n]` - Execute with TDD validation

See `.claude/specs/README.md` for full workflow documentation.

---

## Available Commands

| Command | Purpose |
|---------|---------|
| `/specify` | Start SDD: Create feature spec |
| `/plan` | SDD Phase 2: Technical design |
| `/tasks` | SDD Phase 3: Task breakdown |
| `/implement` | SDD Phase 4: TDD execution |
| `/run-tests` | Execute test suites |
| `/run-pipeline` | Run data pipeline |
| `/db-migrate` | Create Supabase migration |
| `/validate-data` | Data quality checks |
| `/create-pr` | Create pull request |
| `/fix-lint` | Fix linting errors |
| `/new-component` | Scaffold React component |

---

## Quick Start (First 60 Seconds)

1. **Check Beads**: `bd ready --json` - See what tasks are ready
2. **If assigned task exists**: `bd show <id>` - Get details
3. **Verify setup**: `cd frontend && npm run test:smoke` - Quick validation
4. **Branch naming**: Use `<type>/<concrete-description>` (max 30 chars)

---

## Task Management Hierarchy

### 1. Beads (Primary) - Persistent Cross-Session Tasks
```bash
bd ready --json    # Show unblocked tasks
bd new <title>     # Create new issue
bd show <id>       # Show issue details
bd done <id>       # Mark task complete
bd blocks <id> <blocker-id>  # Set dependencies
bd ls              # List all issues
```

**Use Beads for**:
- Issues, bugs, features that persist across sessions
- Work that needs tracking across multiple agents
- Tasks with dependencies on other work

### 2. TodoWrite Tool (Secondary) - Within-Session Tracking
**Use TodoWrite for**:
- Breaking down current work into steps
- Tracking progress during active session
- Planning complex tasks before execution

### 3. Never Use Markdown TODO Lists
Both Beads and TodoWrite replace markdown checklists.

**When to Create Issues**:
- **Discovered work**: Found bugs or necessary refactors
- **Complex features**: Need to break down into smaller tasks
- **Dependencies**: Work that blocks or is blocked by other tasks

---

## Project Overview

**Mission**: Organize information about the Danish agricultural sector and make it universally accessible and useful.

**Architecture**: Monorepo with Python backend (data pipelines) and Next.js frontend (React 19, TypeScript)

**Database**: Supabase (PostgreSQL with PostGIS)

**Infrastructure**:
- Data Storage: Google Cloud Storage (GCS)
- Deployment: Vercel (frontend)
- CI/CD: GitHub Actions

**Tech Stack**:
- Frontend: Next.js 15, React 19, TypeScript, Tailwind CSS, MapLibre GL JS
- Backend: Python 3.11+, FastAPI, Pandas, GeoPandas
- Database: Supabase (PostgreSQL 15 + PostGIS)
- Testing: Playwright (E2E), Pytest
- Linting: oxlint (frontend), ruff (backend)

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
npm run test:ui          # Playwright UI mode (interactive debugging)
npm run test:smoke       # Quick smoke tests (~30 seconds)
npm run lint             # Run oxlint (50-100x faster than ESLint)
npm run format           # Format with Prettier
```

### Backend Development
```bash
cd backend
source venv/bin/activate  # Activate Python virtual environment
python -m pytest          # Run all tests
python -m pytest -v       # Verbose output
python -m pytest -k test_name  # Run specific test
```

### Running Data Pipelines
**Activate environment first**:
```bash
cd backend && source venv/bin/activate
```

**Run specific pipeline**:
```bash
cd pipelines/<pipeline_name> && python main.py
```

**Common pipelines**:
- `unified_pipeline` - 18+ Danish government data sources
- `chr_pipeline` - Livestock tracking (CHR registry)
- `svineflytning_pipeline` - Pig movement tracking
- `drive_data_pipeline` - Regulatory compliance data

### Database Management
```bash
supabase status                     # Check connection status
supabase link --project-ref <ref>   # Link to remote Supabase project
supabase db pull                    # Pull latest schema
supabase migration new <name>       # Create new migration
supabase db push                    # Push migrations to remote
```

### Testing Checklist
**Before ANY commit or marking task complete**:
```bash
# Frontend tests (required)
cd frontend && npm test

# Backend tests (required)
cd backend && python -m pytest

# Linting (required for frontend)
cd frontend && npm run lint
```

**Fast feedback during development**:
```bash
npm run test:smoke    # Quick validation (~30 seconds)
npm run test:ui       # Interactive Playwright debugging
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

### Frontend (`.env.local`)
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

### Environment Setup Validation
**Check your setup is working**:
```bash
# Frontend - Should start dev server on http://localhost:3000
cd frontend && npm run dev

# Backend - Should import without errors
cd backend && source venv/bin/activate && python -c "import supabase; print('✓ Backend env OK')"

# Supabase - Should show linked project
supabase status
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
- **Branch naming**: `<type>/<short-description>` (max 30 chars, concrete language)
  - Good: `feat/map-view`, `fix/chr-data-load`, `refactor/gcs-upload`
  - Bad: `feat/improvements`, `fix/bug`, `update-stuff`
- **Commit messages**: Conventional Commits format
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
1. **Check Beads**: Run `bd ready --json` and `bd show <id>` for assigned work
2. **Read context**: Check relevant documentation in `docs/`
3. **Understand scope**: Read the issue/task description thoroughly
4. **Plan approach**: Use TodoWrite to break down complex tasks
5. **Verify setup**: Run quick smoke tests before starting

### Testing Requirements
- **Always run tests** before marking tasks complete or pushing commits
- **Frontend**: `cd frontend && npm test` (Playwright E2E)
- **Backend**: `cd backend && python -m pytest`
- **Linting**: `cd frontend && npm run lint` (oxlint must pass)
- **Write new tests** for new features (TDD workflow preferred)

### Code Reference Format
**Always** use `file_path:line_number` format when referencing code:
- ✅ `frontend/src/app/page.tsx:42`
- ✅ `backend/pipelines/chr_pipeline/main.py:156`
- ❌ "in the homepage component"
- ❌ "in the CHR pipeline"

### Critical Agent Rules (MUST FOLLOW)

**Absolute Prohibitions**:
1. ❌ **Never commit `.env` files** (only modify `.env.example`)
2. ❌ **Never commit secrets or credentials**
3. ❌ **Never modify database directly** (always use migrations)
4. ❌ **Never create files without user request**
5. ❌ **Never create documentation proactively** (only when explicitly asked)
6. ❌ **Never skip tests** before marking work complete
7. ❌ **Never break existing tests** without fixing them

**Required Actions**:
1. ✅ **Always run tests** before marking Bead task done
2. ✅ **Always use migrations** for database schema changes
3. ✅ **Always validate data transformations twice** (public transparency project)
4. ✅ **Always use absolute paths** in this Conductor workspace
5. ✅ **Always check oxlint** passes for frontend changes

### Common Pitfalls to Avoid
1. **File paths**: Use workspace absolute paths, not parent repo paths
2. **React version**: This project uses React 19 (not 18)
3. **Data accuracy**: Double-check transformations - this is public data
4. **Test failures**: Don't mark complete until all tests pass
5. **Pipeline environments**: Always activate venv before running pipelines
6. **GCS costs**: Cache files locally when testing pipelines repeatedly

### Decision Guide

**"Should I create a new file?"**
- ✅ Only if absolutely required for the feature
- ❌ Prefer editing existing files
- ❌ Never create docs/README proactively

**"Should I use Beads or TodoWrite?"**
- **Beads**: Task will take multiple sessions or needs cross-agent tracking
- **TodoWrite**: Breaking down work within current session

**"Should I run tests?"**
- ✅ Always before marking Bead task done
- ✅ After any logic changes
- ✅ When user says "done", "finish", "complete"

**"Should I create a migration?"**
- ✅ Any database schema change (tables, columns, indexes)
- ✅ Adding/modifying RLS policies
- ✅ Creating/updating materialized views
- ❌ Just inserting data (use pipeline)

### When Stuck or Confused
1. Check `docs/troubleshooting/` for known issues
2. Review `docs/PIPELINE_INDEX.md` for data pipeline details
3. Look at similar existing code for patterns
4. Run `supabase status` to verify database connection
5. Check `.env` variables are set correctly
6. Ask for clarification if requirements are ambiguous

## Important Notes

### Data Quality & Transparency
- This is a **public transparency project** for Danish agriculture
- Data accuracy is paramount - **double-check all transformations**
- Document all data sources and transformations
- Be honest about data limitations
- All data must be joinable on: CVR, CHR, BFE, or geospatial coordinates

### Performance & Cost Considerations

**GCS Operations**:
- Cache files locally when running pipelines repeatedly
- Use `gsutil -m` for parallel transfers (faster)
- Avoid re-uploading unchanged files
- Be mindful of egress costs when downloading large datasets

**Supabase/Database**:
- Use materialized views for complex aggregations
- Add indexes for frequently joined columns (CVR, CHR, BFE)
- Use pagination for queries returning >1000 rows
- Prefer RPC functions for complex operations

**Frontend**:
- Lazy load components and data
- Use Next.js Image component for optimization
- Implement virtualization for long lists
- Cache map tiles with PMTiles

### Security
- Never commit `.env` files
- Use environment variables for all secrets
- Implement proper RLS (Row Level Security) in Supabase
- Validate all user inputs
- Review RLS policies before exposing new tables

## Common Failures & Recovery

### Tests Failing
1. Check recent commits: `git log -3 --oneline`
2. Review test output for specific failure location
3. Run with verbose output: `npm test -- --reporter=line` or `pytest -v`
4. Don't mark task complete until all tests pass
5. Check if tests need updating for intentional behavior changes

### Pipeline Errors
1. Verify `.env` variables are set: `cat backend/.env`
2. Check GCS credentials path exists
3. Activate venv: `cd backend && source venv/bin/activate`
4. Look in `docs/troubleshooting/` for known issues
5. Check pipeline-specific README in `backend/pipelines/<name>/`

### Supabase Connection Issues
1. Check link status: `supabase status`
2. Verify environment variables:
   - `SUPABASE_URL` should end with `.supabase.co`
   - `SUPABASE_KEY` should be long string (not empty)
3. Test connection: `supabase db pull` (should not error)
4. Check network connectivity and firewall settings

### Frontend Not Starting
1. Check Node version: `node --version` (should be 18+)
2. Clear cache: `rm -rf .next node_modules && npm install`
3. Verify `.env.local` exists with Supabase credentials
4. Check port 3000 is available: `lsof -i :3000`

### Migration Failures
1. Check migration syntax: SQL errors are common
2. Verify migrations are in order: `ls supabase/migrations/`
3. Try resetting local: `supabase db reset` (caution: loses local data)
4. Check remote state: Log into Supabase Dashboard

## Quick Reference Links

### Agent Configuration
- **SDD Workflow**: `.claude/specs/README.md`
- **Testing Rules**: `.claude/rules/testing.md`
- **Data Quality Rules**: `.claude/rules/data-quality.md`
- **Security Rules**: `.claude/rules/security.md`
- **Skills**: `.claude/skills/` (data-pipeline, playwright-testing, supabase-migration, code-review)
- **Commands**: `.claude/commands/`

### Documentation
- **Pipeline Documentation**: `docs/PIPELINE_INDEX.md`
- **Data Lineage**: `docs/DATA_LINEAGE_COMPREHENSIVE.md`
- **Troubleshooting**: `docs/troubleshooting/`
- **Supabase Dashboard**: https://supabase.com/dashboard
- **Project README**: `README.md`
- **Pipeline Template**: `docs/templates/PIPELINE_README_TEMPLATE.md`

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
