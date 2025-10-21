# Agent-Native Development Setup - Landbruget.dk

## Overview

This document summarizes the complete agent-native development setup for the landbruget.dk project, implemented following the principles from the "Agent-Native Development Playbook (October 2025)".

## Implementation Date

October 21, 2025

## What Was Implemented

### 1. Conductor Configuration (`conductor.json`)

Created a Conductor configuration file that enables automated workspace provisioning with the following scripts:

- **setup**: `./scripts/setup-worktree.sh` - Automated environment setup for new worktrees
- **test**: Run frontend E2E tests
- **test:backend**: Run backend Python tests
- **dev**: Start frontend development server
- **lint**: Run oxlint (50-100x faster than ESLint)
- **format**: Format code with Prettier

**Location**: `/conductor.json`

### 2. Automated Worktree Setup Script

Created a comprehensive setup script (`scripts/setup-worktree.sh`) that automates:

#### Frontend Setup
- ✅ Install Node.js dependencies (npm ci/install)
- ✅ Create/symlink `.env` file from main repository
- ✅ Verify Supabase environment variables
- ✅ Check oxlint installation

#### Backend Setup
- ✅ Create Python virtual environment (venv)
- ✅ Install pip-tools
- ✅ Install Python dependencies (if requirements.txt or pyproject.toml exists)
- ✅ Create/symlink backend `.env` file
- ✅ Verify environment configuration

#### Security & Validation
- ✅ Verify `.env` files are gitignored
- ✅ Provide clear instructions for missing credentials
- ✅ Symlink to main repo .env (prevents duplicate credential management)
- ✅ Display helpful next steps for developers

**Location**: `/scripts/setup-worktree.sh`

**Tested**: ✅ Script successfully provisions a complete development environment

### 3. Comprehensive CLAUDE.md Documentation

Created a hierarchical documentation system for AI agents:

#### Root Level: `/CLAUDE.md`
Provides project-wide context including:
- Project mission and architecture overview
- Core architectural principles (data-centric design, separation of concerns, testing philosophy)
- Key commands for frontend, backend, and database
- Complete project structure explanation
- Data sources and dependencies (18+ Danish government sources)
- Environment variable configuration
- Coding standards (TypeScript, Python, Git workflow)
- MCP server configuration guidance
- Agent-specific guidelines and common pitfalls
- Commit message format (Conventional Commits)

**Key Sections**:
- Technology stack overview
- Critical file locations
- Data identifiers (CVR, CHR, BFE, Enhedsnummer)
- MCP server setup (Supabase, GitHub, Web Search)
- Agent workflow guidelines
- Common issues and solutions

#### Frontend: `/frontend/CLAUDE.md`
Specialized documentation for frontend development:
- Complete technology stack (Next.js 15, React 19, TypeScript, Tailwind CSS)
- Directory structure and component organization
- Component development guidelines (patterns, rules, styling)
- State management with Zustand
- Data fetching patterns (Server vs Client Components)
- MapLibre and PMTiles integration
- Forms and validation (React Hook Form + Zod)
- Playwright E2E testing
- Performance optimization techniques
- Common patterns (loading states, error boundaries)
- Linting and formatting with oxlint
- Troubleshooting guide

**Key Features**:
- Explicit TypeScript patterns
- Map rendering best practices
- Testing guidelines
- Performance optimization strategies

#### Backend: `/backend/CLAUDE.md`
Specialized documentation for backend development:
- Technology stack (Python 3.11+, DuckDB, Pandas, GeoPandas)
- Pipeline architecture (medallion: Bronze → Silver → Gold)
- Standard pipeline structure and templates
- Data quality standards for each layer
- Common utilities (GCS, Supabase)
- Testing guidelines with pytest
- Key data identifiers and validation
- Geospatial data handling (EPSG:25832 → EPSG:4326)
- Performance optimization (DuckDB, batching, lazy loading)
- Documentation requirements

**Key Features**:
- Complete pipeline development guide
- Data quality best practices
- Reusable utility patterns
- Comprehensive testing examples

## Architecture Decisions

### 1. Medallion Architecture for Data Pipelines

Following industry best practices, all data pipelines implement:
- **Bronze Layer**: Raw, immutable data from sources
- **Silver Layer**: Cleaned, validated, standardized data
- **Gold Layer**: Analysis-ready, enriched datasets

This ensures:
- Complete audit trail
- Reproducible transformations
- Clear data lineage
- High data quality

### 2. Monorepo Structure

Keeping frontend and backend in a single repository enables:
- Shared documentation
- Coordinated releases
- Easier cross-stack development
- Single source of truth

### 3. Technology Choices Aligned with Agent Development

- **oxlint**: 50-100x faster than ESLint for rapid feedback loops
- **Playwright**: Reliable E2E testing with agent-friendly APIs
- **Supabase**: SQL-based, agent can query and modify schema
- **TypeScript strict mode**: Catch errors at compile time
- **Conventional Commits**: Structured, parseable commit messages

## MCP Server Recommendations

Based on the project's technology stack and the Agent-Native Development Playbook, the following MCP servers are recommended:

### Currently Documented

1. **Supabase MCP Server** ✅
   ```bash
   npx -y @supabase/mcp-server-supabase@latest --features=database,docs
   ```
   - Query and modify database schema
   - Run SQL queries
   - Manage migrations
   - Access Supabase documentation

### Recommended for Future Implementation

2. **Vercel MCP Server** (for deployment management)
   - Manage deployments
   - Analyze logs
   - Configure project settings

3. **GitHub MCP Server** (for PR and issue management)
   - Create and manage pull requests
   - Track issues and beads
   - Review code changes

4. **Custom Project MCP Server** (future consideration)
   - Secure gateway to Danish government APIs (Cadastre, BDNB, Géorisques)
   - Authentication via OAuth
   - Expose internal business logic

## Agent Workflow Integration

### Recommended Workflows

#### 1. Feature Development (Explore, Plan, Code, Commit)
1. **Explore**: Use Claude to brainstorm and design
2. **Plan**: Create detailed implementation plan (can use Beads in future)
3. **Code**: Execute plan in isolated worktree
4. **Commit**: Atomic commits with tests passing

#### 2. Test-Driven Development (TDD)
1. **Write Tests**: Define expected behavior
2. **Confirm Failure**: Ensure tests fail correctly
3. **Implement**: Write minimal code to pass tests
4. **Iterate**: Refactor until all tests pass

#### 3. Dual-Agent Pattern (Future Enhancement)
- **Architect Agent**: High-level planning and code review
- **Implementer Agent**: Tactical execution of tasks
- **Human**: Orchestration and approval

### Session Management

The documentation emphasizes:
- Use `/clear` between unrelated tasks
- Keep context focused and relevant
- Read CLAUDE.md files at appropriate levels
- Check documentation before implementing

## Testing Setup

### Frontend Testing
- **Framework**: Playwright
- **Type**: End-to-end (E2E) tests
- **Command**: `cd frontend && npm test`
- **Coverage**: Critical user flows, accessibility

### Backend Testing
- **Framework**: Pytest
- **Type**: Unit and integration tests
- **Command**: `cd backend && python -m pytest`
- **Coverage**: Pipeline logic, data transformations

## Data Quality Framework

### Key Data Identifiers
- **CVR**: 8-digit company registration number
- **CHR**: 6-digit herd registration number
- **BFE**: Cadastral parcel number (format: kommune-ejerlav-matr)
- **Geospatial**: EPSG:4326 (WGS84) for storage, EPSG:25832 for Danish sources

### Quality Standards
- Type validation at every layer
- Deduplication based on business keys
- Completeness and consistency tracking
- Transparent documentation of limitations

## Security Considerations

### Environment Variables
- ✅ All `.env` files are gitignored
- ✅ Only `.env.example` templates are committed
- ✅ Worktree script symlinks to main repo (single source of credentials)
- ✅ Clear warnings about not committing secrets

### Database Access
- Frontend uses Supabase anon key (NEXT_PUBLIC_API_KEY)
- Backend uses service role key (SUPABASE_SERVICE_ROLE_KEY)
- Row Level Security (RLS) policies enforce access control

## Integration with Existing Project

### Preserved Existing Structure
- ✅ No modifications to existing code
- ✅ Documentation augments existing README
- ✅ Setup script works with existing package.json and dependencies
- ✅ Compatible with current Git workflow

### Additions Only
- `conductor.json` - New file for Conductor integration
- `scripts/setup-worktree.sh` - New automated setup script
- `CLAUDE.md` - Project root agent documentation
- `frontend/CLAUDE.md` - Frontend-specific documentation
- `backend/CLAUDE.md` - Backend-specific documentation

## Next Steps for Full Agent-Native Adoption

### Phase 1: Foundation (✅ Complete)
- ✅ Conductor configuration
- ✅ Automated worktree setup
- ✅ Comprehensive CLAUDE.md documentation
- ✅ MCP server recommendations

### Phase 2: Enhanced Testing (Recommended)
- [ ] Expand Playwright test coverage
- [ ] Add visual regression testing
- [ ] Implement automated test generation
- [ ] Create test data fixtures

### Phase 3: Linting Rules (Future)
- [ ] Create custom oxlint plugin (`oxlint-plugin-landbruget.dk`)
- [ ] Implement architectural rules (e.g., no direct DB imports in UI)
- [ ] Enforce data source tagging
- [ ] Security guardrails (no eval, proper sanitization)

### Phase 4: Task Management (Future)
- [ ] Implement Beads for agent memory
- [ ] Create task dependency graphs
- [ ] Track discovered work during implementation
- [ ] Build audit trail of development decisions

### Phase 5: Custom MCP Server (Future)
- [ ] Build project-specific MCP server
- [ ] Integrate Danish government APIs
- [ ] Implement OAuth authentication with Clerk
- [ ] Expose internal business logic safely

## Success Metrics

The setup is successful if agents can:
1. ✅ Provision a complete development environment in < 2 minutes
2. ✅ Understand project architecture from CLAUDE.md files
3. ✅ Run tests and verify changes
4. ✅ Follow coding standards automatically
5. ✅ Navigate the codebase efficiently
6. ✅ Access database via Supabase MCP server

## Resources & References

### Documentation Created
- `/conductor.json` - Conductor configuration
- `/scripts/setup-worktree.sh` - Automated setup script
- `/CLAUDE.md` - Root project documentation
- `/frontend/CLAUDE.md` - Frontend guide
- `/backend/CLAUDE.md` - Backend guide
- `/AGENTIC_SETUP_SUMMARY.md` - This document

### External Resources
- [Conductor Documentation](https://conductor.build/)
- [Claude Code Best Practices](https://www.anthropic.com/engineering/claude-code-best-practices)
- [Supabase MCP Server](https://supabase.com/docs/guides/getting-started/mcp)
- [Agent-Native Development Playbook](uploaded reference document)

## Maintenance

### Updating Documentation
- Review CLAUDE.md files quarterly
- Update when major architectural changes occur
- Keep technology stack versions current
- Add new patterns as they emerge

### Setup Script Maintenance
- Test after major dependency updates
- Update environment variable checks as needed
- Add new setup steps for new services
- Keep error messages helpful and current

## Conclusion

This setup transforms landbruget.dk into an agent-native development environment where AI agents are first-class citizens. The combination of:

1. **Automated provisioning** (Conductor + setup script)
2. **Comprehensive documentation** (hierarchical CLAUDE.md files)
3. **Modern tooling** (oxlint, Playwright, Supabase)
4. **Clear standards** (medallion architecture, Conventional Commits)

...creates a foundation for high-velocity, high-quality AI-assisted development while maintaining human oversight and engineering excellence.

The key insight: **AI agents amplify elite engineering practices; they don't replace them.** This setup ensures that every agent interaction builds on a solid foundation of tests, documentation, and architectural clarity.

---

*Setup implemented on October 21, 2025 by Claude Code following the Agent-Native Development Playbook principles.*
