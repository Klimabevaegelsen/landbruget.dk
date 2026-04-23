# /run-tests - Execute Test Suites

Run frontend and/or backend tests with proper reporting.

## Usage

```
/run-tests              # Run all tests (frontend + backend)
/run-tests frontend     # Run frontend E2E tests only
/run-tests backend      # Run backend pytest only
/run-tests smoke        # Run quick smoke tests only
```

## Process

### 1. Determine Scope

Based on argument:
- `frontend` → Playwright E2E tests
- `backend` → Pytest
- `smoke` → Frontend smoke tests only
- (no arg) → Run all tests

### 2. Run Tests

**Frontend Tests:**
```bash
cd $CLAUDE_PROJECT_DIR/frontend
npm test
```

**Backend Tests:**
```bash
cd $CLAUDE_PROJECT_DIR
uv run --all-packages --group dev pytest -v
```

**Smoke Tests (fast):**
```bash
cd $CLAUDE_PROJECT_DIR/frontend
npm run test:smoke
```

### 3. Capture Results

Parse test output for:
- Total tests run
- Tests passed
- Tests failed
- Test duration

### 4. Report Failures

For each failure, report:
- Test name
- File path with line number
- Error message
- Expected vs Actual (if assertion)

## Output Format

```
## Test Results Summary

### Frontend (Playwright E2E)
- **Total**: 45 tests
- **Passed**: 43 ✅
- **Failed**: 2 ❌
- **Duration**: 1m 23s

### Failures

1. **`e2e/search.spec.ts:42`** - should display search results
   - Error: Expected element to be visible
   - Locator: `[data-testid="results"]`

2. **`e2e/map.spec.ts:78`** - should zoom on scroll
   - Error: Timeout waiting for element
   - Locator: `[data-testid="zoom-indicator"]`

### Backend (Pytest)
- **Total**: 28 tests
- **Passed**: 28 ✅
- **Failed**: 0
- **Duration**: 4.2s

---
**Overall**: 71/73 tests passed (97.3%)
```

## Quick Commands

```bash
# Frontend only
cd frontend && npm test

# Backend only
uv run --all-packages --group dev pytest -v

# Smoke tests (< 30 seconds)
cd frontend && npm run test:smoke

# Specific test file
cd frontend && npm test -- e2e/search.spec.ts

# Tests matching pattern
cd frontend && npm test -- --grep "search"

# UI mode (interactive debugging)
cd frontend && npm run test:ui

# With browser visible
cd frontend && npm run test:headed
```

## After Running

If tests fail:
1. Identify failing test file and line
2. Read the test to understand expected behavior
3. Check if it's a test issue or code issue
4. Fix and re-run tests

If all tests pass:
1. Proceed with next task
2. Mark related Bead as done (if applicable)
