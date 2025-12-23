---
name: playwright-testing
description: |
  Activates when writing E2E tests, debugging test failures, or working with Playwright.
  Use this skill for: writing test cases, using data-testid selectors, handling async operations,
  mocking API responses, running tests in different modes, and debugging flaky tests.
  Keywords: test, playwright, e2e, spec, expect, locator, data-testid, assertion
---

# Playwright E2E Testing Skill

This skill provides guidance for writing and debugging Playwright E2E tests for Landbruget.dk.

## Activation Context

This skill activates when:
- Writing new E2E tests
- Debugging test failures
- Working with Playwright configuration
- Handling async test operations
- Mocking API responses

## Quick Reference

### Running Tests

```bash
cd frontend

# Run all tests
npm test

# Run specific test file
npm test -- e2e/feature.spec.ts

# Run tests matching pattern
npm test -- --grep "search"

# Run in UI mode (interactive)
npm run test:ui

# Run smoke tests only (fast feedback)
npm run test:smoke

# Run with browser visible
npm run test:headed

# Debug mode
npm test -- --debug
```

## Test Structure

### Standard Test File

```typescript
// e2e/feature-name.spec.ts
import { test, expect } from '@playwright/test';

test.describe('Feature Name', () => {
  test.beforeEach(async ({ page }) => {
    // Setup before each test
    await page.goto('/');
  });

  test('should [describe expected behavior]', async ({ page }) => {
    // Arrange - Setup test state
    const input = page.locator('[data-testid="search-input"]');

    // Act - Perform actions
    await input.fill('test query');
    await page.click('[data-testid="search-button"]');

    // Assert - Verify results
    await expect(page.locator('[data-testid="results"]')).toBeVisible();
    await expect(page.locator('.result-item')).toHaveCount(5);
  });
});
```

### Data-testid Convention

Always use `data-testid` attributes for reliable test selectors:

```typescript
// In component
<button data-testid="submit-button">Submit</button>

// In test
await page.click('[data-testid="submit-button"]');
```

**Naming Convention:**
- Use kebab-case: `data-testid="field-search-input"`
- Be specific: `data-testid="map-zoom-in"` not `data-testid="button"`
- Include context: `data-testid="farm-details-close"` not `data-testid="close"`

## Common Patterns

### Waiting for Elements

```typescript
// Wait for element to be visible
await expect(page.locator('[data-testid="loading"]')).toBeHidden();
await expect(page.locator('[data-testid="content"]')).toBeVisible();

// Wait for network request
await page.waitForResponse('**/api/fields');

// Wait for navigation
await Promise.all([
  page.waitForNavigation(),
  page.click('[data-testid="nav-link"]'),
]);
```

### Form Testing

```typescript
test('should submit form successfully', async ({ page }) => {
  // Fill form
  await page.fill('[data-testid="name-input"]', 'Test Farm');
  await page.selectOption('[data-testid="type-select"]', 'agriculture');
  await page.check('[data-testid="terms-checkbox"]');

  // Submit
  await page.click('[data-testid="submit-button"]');

  // Verify success
  await expect(page.locator('[data-testid="success-message"]')).toBeVisible();
});
```

### Map Testing

```typescript
test('should zoom map on scroll', async ({ page }) => {
  const map = page.locator('[data-testid="map-container"]');

  // Get initial zoom level
  const initialZoom = await map.getAttribute('data-zoom');

  // Perform zoom action
  await map.hover();
  await page.mouse.wheel(0, -100);

  // Wait for animation
  await page.waitForTimeout(500);

  // Verify zoom changed
  const newZoom = await map.getAttribute('data-zoom');
  expect(Number(newZoom)).toBeGreaterThan(Number(initialZoom));
});
```

### Mocking API Responses

```typescript
test('should display data from API', async ({ page }) => {
  // Mock the API response
  await page.route('**/api/fields', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify([
        { id: 1, name: 'Test Field', area: 100 },
      ]),
    });
  });

  await page.goto('/fields');

  // Verify mocked data is displayed
  await expect(page.locator('[data-testid="field-name"]')).toHaveText('Test Field');
});
```

### Error State Testing

```typescript
test('should show error message on API failure', async ({ page }) => {
  // Mock API error
  await page.route('**/api/fields', async (route) => {
    await route.fulfill({
      status: 500,
      body: JSON.stringify({ error: 'Server error' }),
    });
  });

  await page.goto('/fields');

  await expect(page.locator('[data-testid="error-message"]')).toBeVisible();
  await expect(page.locator('[data-testid="retry-button"]')).toBeVisible();
});
```

## Accessibility Testing

```typescript
// Use semantic locators when possible
await page.getByRole('button', { name: 'Submit' }).click();
await page.getByLabel('Email').fill('test@example.com');
await page.getByText('Welcome').isVisible();
```

## Debugging Techniques

### Visual Debugging

```typescript
// Take screenshot
await page.screenshot({ path: 'debug.png', fullPage: true });

// Slow down execution
test.use({ actionTimeout: 5000 });

// Pause execution (in debug mode)
await page.pause();
```

### Console Logging

```typescript
// Capture console messages
page.on('console', (msg) => console.log('Browser:', msg.text()));

// Capture network errors
page.on('requestfailed', (request) =>
  console.log('Failed:', request.url(), request.failure()?.errorText)
);
```

## Smoke Tests

Create fast smoke tests for critical paths:

```typescript
// e2e/smoke/critical-paths.spec.ts
import { test, expect } from '@playwright/test';

test.describe('Smoke Tests', () => {
  test('homepage loads', async ({ page }) => {
    await page.goto('/');
    await expect(page).toHaveTitle(/Landbruget/);
  });

  test('map displays', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('[data-testid="map-container"]')).toBeVisible();
  });

  test('search works', async ({ page }) => {
    await page.goto('/');
    await page.fill('[data-testid="search-input"]', '12345678');
    await expect(page.locator('[data-testid="search-results"]')).toBeVisible();
  });
});
```

## Test Quality Checklist

Before marking test work complete:
- [ ] Tests use data-testid for selectors
- [ ] Tests cover all acceptance criteria
- [ ] Tests handle loading states
- [ ] Tests handle error states
- [ ] Tests are independent (no shared state)
- [ ] No hardcoded timeouts (use proper waits)
- [ ] Tests pass consistently (not flaky)
