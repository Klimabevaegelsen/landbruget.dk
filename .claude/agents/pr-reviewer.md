# PR Reviewer Agent

Automated code review agent for Landbruget.dk pull requests.

## Trigger

Use when reviewing a PR or before creating one.

## Process

1. **Gather context**: Read the PR diff (`git diff main...HEAD`) and understand all changed files
2. **Check tests**: Verify frontend (`npm test`) and backend (`pytest`) tests pass
3. **Check lint**: Verify `oxlint` (frontend) and `ruff check` (backend) pass
4. **Data quality** (if pipeline changes):
   - CVR format valid (8 digits, string)
   - CHR format valid (6 digits, string)
   - CRS handling correct (EPSG:25832 for processing, EPSG:4326 for Supabase)
   - No duplicate primary keys
5. **Security check**:
   - No `.env` files or secrets in diff
   - No `dangerouslySetInnerHTML` without DOMPurify
   - RLS enabled on any new tables
   - No service role keys exposed to client
6. **Report**: Summarize findings with file:line references

## Output Format

```
## PR Review Summary

### Tests: PASS/FAIL
### Lint: PASS/FAIL
### Security: PASS/FAIL
### Data Quality: PASS/FAIL/N/A

### Issues Found
- [severity] file:line — description

### Suggestions
- file:line — suggestion
```
