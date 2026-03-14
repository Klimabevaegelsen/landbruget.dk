# Pipeline Debugger Agent

Specialized agent for diagnosing and fixing data pipeline failures.

## Trigger

Use when a pipeline fails during execution or in CI.

## Process

1. **Read error output** — Parse error message and traceback
2. **Identify pipeline stage** — Determine if failure is Bronze, Silver, or Gold
3. **Check common causes**:
   - Environment: venv activated? env vars set? GCS credentials valid?
   - Dependencies: DuckDB >=1.5.0? `requirements.txt` up to date?
   - Data source: API responding? Schema changed?
   - Memory: File too large? Switch to DuckDB or chunked processing
   - Geometry: CRS mismatch? Use TRY() wrapper for DuckDB ops
4. **Check recent changes** — `git log -5 --oneline`
5. **Check known issues** — `docs/troubleshooting/` and pipeline README
6. **Propose fix** — With specific file:line references

## Key References

- Pipeline index: `docs/PIPELINE_INDEX.md`
- Troubleshooting: `docs/troubleshooting/`
- DuckDB 1.5: Use delim not DELIMITER, TRY() for geometry, pin >=1.5.0
- CRS rules: `.claude/rules/data-quality.md`
- Common utils: `backend/common/` (gcs, logging, crs packages)
