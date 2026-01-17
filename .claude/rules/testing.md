# Testing Rules

## Test-Driven Development (TDD)

When implementing features, follow TDD:

1. **Write test FIRST** - Before any implementation code
2. **Run test** - Confirm it fails (red)
3. **Implement minimum** - Just enough to pass
4. **Run test** - Confirm it passes (green)
5. **Refactor** - Clean up while keeping tests green

## Test Requirements

### Before Any Commit
```bash
cd frontend && npm test    # Playwright E2E
cd backend && pytest       # Python tests
cd frontend && npm run lint # Linting
```

### Before Marking Task Complete
- All existing tests must pass
- New tests written for new functionality
- No skipped tests without documented reason

## Frontend Testing (Playwright)

### Selectors
- ALWAYS use `data-testid` attributes
- Name format: `kebab-case` (e.g., `search-input`, `farm-details-close`)
- Be specific and contextual

### Test Structure
```typescript
test.describe('Feature', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should [expected behavior]', async ({ page }) => {
    // Arrange → Act → Assert
  });
});
```

### What to Test
- Critical user flows
- Form submissions
- Error states
- Loading states
- Responsive behavior

### What NOT to Test
- Implementation details
- Third-party libraries
- Styling (unless critical)

## Backend Testing (Pytest)

### Test Data Quality
- CVR format validation
- CHR format validation
- Geospatial CRS (EPSG:4326)
- No duplicates

### Test Transformations
- Bronze → Silver accuracy
- Silver → Gold aggregations
- Edge cases (null values, invalid input)

## Quick Reference

```bash
# Fast feedback
npm run test:smoke

# Debug mode
npm run test:ui

# Specific test
npm test -- --grep "search"

# Backend verbose
pytest -v
```
