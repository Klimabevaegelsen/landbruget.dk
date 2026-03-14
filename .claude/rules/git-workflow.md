# Git Workflow

## Branch Naming

Format: `<type>/<short-description>` (max 30 chars, concrete language)

- Good: `feat/map-view`, `fix/chr-data-load`, `refactor/gcs-upload`
- Bad: `feat/improvements`, `fix/bug`, `update-stuff`

## Commit Messages

Conventional Commits format:

```
<type>(<scope>): <subject>

<body>

Co-Authored-By: Claude <noreply@anthropic.com>
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`, `perf`, `build`

Examples:
- `feat(frontend): add interactive map view for field data`
- `fix(pipeline): correct CHR data transformation logic`
- `refactor(backend): simplify GCS upload utilities`

## Code Review

- All code must be reviewed before merge
- Tests must pass (frontend + backend)
- No secrets in code (use .env files)
- oxlint must pass (frontend)
- Keep PRs focused and atomic

## Code Reference Format

Always use `file_path:line_number` format:
- `frontend/src/app/page.tsx:42`
- `backend/pipelines/chr_pipeline/main.py:156`
