# Frontend Testing Guide

Complete guide to testing in the Landbruget.dk frontend application using Playwright and Vitest.

## Table of Contents

- [Overview](#overview)
- [Test Types](#test-types)
- [Running Tests](#running-tests)
- [Writing Tests](#writing-tests)
- [Data-TestID Convention](#data-testid-convention)
- [Test Utilities](#test-utilities)
- [Component Testing](#component-testing)
- [E2E Testing](#e2e-testing)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

---

## Overview

This project uses:

- Playwright for E2E browser coverage
- Vitest for server-side route unit tests

Playwright provides:

- Fast, reliable test execution
- Multiple browser support (Chromium, Firefox, WebKit)
- Mobile device emulation
- Network request mocking
- Visual regression testing capabilities

### Project Setup

- **E2E Tests**: Located in `frontend/tests/` with config `playwright.config.ts`
- **Route Unit Tests**: Located in `frontend/tests/routes/` with config `vitest.routes.config.ts`
- **Component Tests**: Located in `frontend/tests/components/` with config `playwright-ct.config.ts`
- **Test Utilities**: Located in `frontend/tests/utils/test-helpers.ts`

---

## Test Types

### 1. E2E (End-to-End) Tests

Tests complete user workflows in a real browser environment.

**When to use:**

- Testing critical user journeys (search, navigation, data display)
- Testing integration between multiple components
- Testing page-level functionality

**Example:**

```typescript
// tests/homepage.spec.ts
import { test, expect } from '@playwright/test';

test('should allow user to search for company', async ({ page }) => {
  await page.goto('/');

  // Type in search input
  await page.fill('[data-testid="global-search-input"]', '12345678');

  // Wait for results
  await page.waitForSelector('[data-testid="search-results-container"]');

  // Verify results appeared
  const results = page.locator('[data-testid="search-result-card"]');
  await expect(results).toHaveCount(1);
});
```

### 2. Component Tests (Experimental)

Tests individual React components in isolation.

**When to use:**

- Testing complex component logic
- Testing component variants and states
- Faster feedback than E2E tests

**Example:**

```typescript
// tests/components/Button.spec.tsx
import { test, expect } from '@playwright/experimental-ct-react';
import { Button } from '@/components/ui/button';

test('should render button with correct variant', async ({ mount }) => {
  const component = await mount(
    <Button variant="destructive">Delete</Button>
  );

  await expect(component).toHaveText('Delete');
  await expect(component).toHaveClass(/destructive/);
});
```

### 3. Route Unit Tests

Tests Next.js route handlers directly in a Node runtime without starting the dev server.

**When to use:**

- Testing `src/app/api/**/route.ts` behavior
- Verifying cache headers, status codes, and JSON payloads
- Mocking server dependencies such as `next/cache`, `fetch`, or data loaders

**Example:**

```typescript
// tests/routes/homepage-statistics.route.test.ts
import { describe, expect, it, vi } from 'vitest';
import { GET } from '@/app/api/homepage-statistics/route';

vi.mock('@/lib/server-cache', () => ({
  getCachedHomepageStatistics: vi
    .fn()
    .mockResolvedValue({ totals: { companies: 123 } }),
}));

describe('GET /api/homepage-statistics', () => {
  it('returns cached data', async () => {
    const response = await GET();

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({
      totals: { companies: 123 },
    });
  });
});
```

---

## Running Tests

### Quick Commands

```bash
# Run all E2E tests
npm test

# Run route unit tests
npm run test:routes

# Run route unit tests in watch mode
npm run test:routes:watch

# Run tests in UI mode (interactive)
npm run test:ui

# Run tests with browser visible
npm run test:headed

# Run specific test file
npx playwright test tests/homepage.spec.ts

# Run tests matching pattern
npx playwright test --grep "search"

# Run smoke tests (fast subset)
npm run test:smoke

# Debug mode (step through tests)
npm run test:debug
```

### CI/CD Integration

Tests run automatically in GitHub Actions on:

- Every pull request
- Every push to main branch
- Manual workflow dispatch

**CI Configuration:** `.github/workflows/playwright.yml`

---

## Writing Tests

### Basic Test Structure

```typescript
import { test, expect } from '@playwright/test';

test.describe('Feature Name', () => {
  // Run before each test in this describe block
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should do something', async ({ page }) => {
    // Arrange: Set up test conditions
    await page.fill('[data-testid="input"]', 'test value');

    // Act: Perform action
    await page.click('[data-testid="submit-button"]');

    // Assert: Verify outcome
    await expect(page.locator('[data-testid="result"]')).toBeVisible();
  });

  test('should handle error state', async ({ page }) => {
    // Test error scenarios
  });
});
```

### Using Test Utilities

Import utilities from `tests/utils/test-helpers.ts`:

```typescript
import { test } from '@playwright/test';
import {
  waitForTestId,
  clickTestId,
  fillTestId,
  expectTestIdVisible,
  generateMockCompany,
} from './utils/test-helpers';

test('should search using utilities', async ({ page }) => {
  await page.goto('/');

  // Use utility functions
  await fillTestId(page, 'global-search-input', '12345678');
  await waitForTestId(page, 'search-results-container');
  await expectTestIdVisible(page, 'search-result-card');
});
```

---

## Data-TestID Convention

### Naming Rules

**Format:** `kebab-case` (lowercase with hyphens)

**Pattern:** `{component}-{element}-{descriptor}`

**Examples:**

- `global-search-input` - Main search input field
- `search-result-card` - Individual search result
- `layer-switch-fields` - Layer toggle switch for fields
- `download-csv-button` - CSV download button
- `table-row-0` - First table row (indexed)

### Adding data-testid Attributes

#### In JSX Components

```tsx
// Simple element
<input
  type="text"
  data-testid="company-name-input"
  {...props}
/>

// Button
<Button
  onClick={handleSubmit}
  data-testid="submit-form-button"
>
  Submit
</Button>

// Dynamic testid (list items)
{items.map((item, index) => (
  <div
    key={item.id}
    data-testid={`list-item-${index}`}
  >
    {item.name}
  </div>
))}

// Conditional testid
<div
  data-testid={isActive ? 'active-indicator' : 'inactive-indicator'}
>
  {status}
</div>
```

#### Component-Level testid

```tsx
interface ComponentProps {
  className?: string;
  'data-testid'?: string;
}

export function Component({
  className,
  'data-testid': testId,
}: ComponentProps) {
  return (
    <div className={className} data-testid={testId}>
      {/* content */}
    </div>
  );
}
```

### Finding Elements with data-testid

```typescript
// In Playwright tests
await page.locator('[data-testid="element-name"]').click();
await page.fill('[data-testid="input-name"]', 'value');
await expect(page.locator('[data-testid="result"]')).toBeVisible();

// Using utility functions
await clickTestId(page, 'element-name');
await fillTestId(page, 'input-name', 'value');
await expectTestIdVisible(page, 'result');
```

---

## Test Utilities

### Wait Utilities

```typescript
import {
  waitForTestId,
  waitForApiResponse,
  waitForNetworkIdle,
} from './utils/test-helpers';

// Wait for element by testid
await waitForTestId(page, 'search-results', { timeout: 5000 });

// Wait for API response
await waitForApiResponse(page, '/api/search');

// Wait for network to settle
await waitForNetworkIdle(page);
```

### Interaction Utilities

```typescript
import { clickTestId, fillTestId, typeTestId } from './utils/test-helpers';

// Click element
await clickTestId(page, 'submit-button');

// Fill input (instant)
await fillTestId(page, 'search-input', 'test query');

// Type with realistic delay
await typeTestId(page, 'search-input', 'test query', { delay: 50 });
```

### Assertion Utilities

```typescript
import {
  expectTestIdVisible,
  expectTestIdToContainText,
  expectTestIdCount,
} from './utils/test-helpers';

// Assert visible
await expectTestIdVisible(page, 'search-results');

// Assert contains text
await expectTestIdToContainText(page, 'result-title', 'Expected Title');

// Assert count
await expectTestIdCount(page, 'search-result-card', 5);
```

### Mock Data Generators

```typescript
import {
  generateMockCVR,
  generateMockCompany,
  generateMockSearchResults,
} from './utils/test-helpers';

// Generate CVR number
const cvr = generateMockCVR(); // "12345678"

// Generate company data
const company = generateMockCompany({
  name: 'Custom Name',
  cvr: '12345678',
});

// Generate multiple results
const results = generateMockSearchResults(10);
```

### Network Mocking

```typescript
import { mockApiResponse, mockApiError } from './utils/test-helpers';

// Mock successful response
await mockApiResponse(page, '**/api/search*', {
  results: [{ id: '1', name: 'Test Company' }],
  total: 1,
});

// Mock error response
await mockApiError(page, '**/api/search*', 500, 'Server Error');
```

---

## Component Testing

### Setting Up Component Tests

Component tests use Playwright's experimental CT (Component Testing) feature.

**Configuration:** `playwright-ct.config.ts`

### Writing Component Tests

```typescript
// tests/components/Button.spec.tsx
import { test, expect } from '@playwright/experimental-ct-react';
import { Button } from '@/components/ui/button';

test.describe('Button Component', () => {
  test('should render with default variant', async ({ mount }) => {
    const component = await mount(<Button>Click Me</Button>);

    await expect(component).toBeVisible();
    await expect(component).toHaveText('Click Me');
  });

  test('should render destructive variant', async ({ mount }) => {
    const component = await mount(
      <Button variant="destructive">Delete</Button>
    );

    await expect(component).toHaveClass(/destructive/);
  });

  test('should handle click events', async ({ mount }) => {
    let clicked = false;
    const component = await mount(
      <Button onClick={() => { clicked = true; }}>
        Click Me
      </Button>
    );

    await component.click();
    expect(clicked).toBe(true);
  });
});
```

### Testing Component States

```typescript
test('should show loading state', async ({ mount }) => {
  const component = await mount(
    <SearchComponent isLoading={true} />
  );

  await expect(component.locator('[data-testid="search-loading"]')).toBeVisible();
});

test('should show error state', async ({ mount }) => {
  const component = await mount(
    <SearchComponent error="Search failed" />
  );

  await expect(component.locator('[data-testid="search-error"]')).toBeVisible();
  await expect(component).toContainText('Search failed');
});
```

---

## E2E Testing

### Page Object Pattern

Organize complex page interactions:

```typescript
// tests/pages/SearchPage.ts
import { Page } from '@playwright/test';
import {
  fillTestId,
  clickTestId,
  expectTestIdVisible,
} from '../utils/test-helpers';

export class SearchPage {
  constructor(private page: Page) {}

  async goto() {
    await this.page.goto('/');
  }

  async search(query: string) {
    await fillTestId(this.page, 'global-search-input', query);
    await expectTestIdVisible(this.page, 'search-results-container');
  }

  async getResultCount() {
    const results = this.page.locator('[data-testid="search-result-card"]');
    return await results.count();
  }

  async clickFirstResult() {
    await clickTestId(this.page, 'search-result-card');
  }
}

// Using in tests
import { SearchPage } from './pages/SearchPage';

test('should navigate from search', async ({ page }) => {
  const searchPage = new SearchPage(page);

  await searchPage.goto();
  await searchPage.search('12345678');
  await searchPage.clickFirstResult();

  await expect(page).toHaveURL(/\/virksomhed\//);
});
```

### Testing User Flows

```typescript
test.describe('Complete User Journey', () => {
  test('should search, view company, and navigate back', async ({ page }) => {
    // 1. Start at homepage
    await page.goto('/');
    await expectTestIdVisible(page, 'global-search-input');

    // 2. Search for company
    await fillTestId(page, 'global-search-input', '12345678');
    await waitForTestId(page, 'search-results-container');

    // 3. Click first result
    await clickTestId(page, 'search-result-card');
    await expect(page).toHaveURL(/\/virksomhed\//);

    // 4. Verify company page loaded
    await expectTestIdVisible(page, 'company-details');

    // 5. Navigate back
    await page.goBack();
    await expect(page).toHaveURL('/');
  });
});
```

### Testing Mobile Responsiveness

```typescript
import { setMobileViewport } from './utils/test-helpers';

test('should work on mobile', async ({ page }) => {
  await setMobileViewport(page);
  await page.goto('/');

  // Test mobile-specific UI
  await expectTestIdVisible(page, 'mobile-menu-button');
});

test.describe('Mobile Navigation', () => {
  test.use({ viewport: { width: 375, height: 667 } });

  test('should open mobile menu', async ({ page }) => {
    await page.goto('/');
    await clickTestId(page, 'mobile-menu-button');
    await expectTestIdVisible(page, 'mobile-menu');
  });
});
```

---

## Best Practices

### 1. Use data-testid for Selectors

**Good:**

```typescript
await page.click('[data-testid="submit-button"]');
```

**Bad:**

```typescript
await page.click('.btn.btn-primary'); // Breaks if CSS changes
await page.click('button:nth-child(3)'); // Fragile position-based selector
```

### 2. Write Descriptive Test Names

**Good:**

```typescript
test('should display error message when search returns no results', async ({
  page,
}) => {
  // test implementation
});
```

**Bad:**

```typescript
test('test search', async ({ page }) => {
  // test implementation
});
```

### 3. Keep Tests Independent

Each test should be able to run alone without depending on other tests.

**Good:**

```typescript
test.beforeEach(async ({ page }) => {
  await page.goto('/');
  // Set up fresh state for each test
});

test('test A', async ({ page }) => {
  // Independent test
});

test('test B', async ({ page }) => {
  // Independent test
});
```

**Bad:**

```typescript
test('create user', async ({ page }) => {
  // Creates user...
});

test('login as user', async ({ page }) => {
  // Depends on previous test
});
```

### 4. Use Appropriate Waits

**Good:**

```typescript
await page.waitForSelector('[data-testid="results"]');
await expect(page.locator('[data-testid="results"]')).toBeVisible();
```

**Bad:**

```typescript
await page.waitForTimeout(5000); // Arbitrary wait
```

### 5. Test User Behavior, Not Implementation

**Good:**

```typescript
test('should show filtered results when user applies filter', async ({
  page,
}) => {
  await clickTestId(page, 'organic-filter');
  await expectTestIdVisible(page, 'filtered-results');
});
```

**Bad:**

```typescript
test('should call setFilter function when checkbox clicked', async ({
  page,
}) => {
  // Testing implementation details
});
```

### 6. Handle Loading and Error States

```typescript
test('should show loading state during search', async ({ page }) => {
  await page.goto('/');

  // Start search
  await fillTestId(page, 'search-input', 'test');

  // Verify loading state appears
  await expectTestIdVisible(page, 'search-loading');

  // Wait for results
  await waitForTestId(page, 'search-results');

  // Verify loading state disappears
  await expectTestIdNotVisible(page, 'search-loading');
});
```

### 7. Use Test Fixtures for Common Setup

```typescript
// playwright.config.ts
export default defineConfig({
  use: {
    baseURL: 'http://localhost:3000',
    trace: 'on-first-retry',
  },
});

// In tests
test('should work', async ({ page }) => {
  await page.goto('/'); // Uses baseURL
});
```

### 8. Group Related Tests

```typescript
test.describe('Search Functionality', () => {
  test.describe('Valid Searches', () => {
    test('should search by CVR', async ({ page }) => {});
    test('should search by company name', async ({ page }) => {});
  });

  test.describe('Error Handling', () => {
    test('should show error for invalid CVR', async ({ page }) => {});
    test('should show error for no results', async ({ page }) => {});
  });
});
```

---

## Troubleshooting

### Common Issues

#### 1. Element Not Found

**Error:**

```
Timeout 30000ms exceeded waiting for selector "[data-testid='element']"
```

**Solutions:**

- Verify the data-testid exists in the component
- Check if element is conditionally rendered
- Increase timeout if needed: `{ timeout: 60000 }`
- Use `page.locator().isVisible()` to debug

#### 2. Flaky Tests

**Symptoms:** Tests pass sometimes, fail other times

**Solutions:**

- Add explicit waits: `waitForSelector`, `waitForLoadState`
- Avoid `waitForTimeout` - use event-based waits instead
- Use `test.beforeEach` to ensure clean state
- Check for race conditions in component logic

#### 3. Slow Tests

**Solutions:**

- Use `test:smoke` for quick feedback
- Mock API responses instead of real network calls
- Parallelize tests: `fullyParallel: true` in config
- Use `page.goto('/', { waitUntil: 'domcontentloaded' })` for faster loads

#### 4. Tests Work Locally but Fail in CI

**Solutions:**

- Check viewport size (CI may use different size)
- Increase timeouts in CI environment
- Use `webServer` in playwright.config.ts to ensure dev server is running
- Check for timing issues (CI may be slower)

### Debugging Tips

#### Visual Debugging

```bash
# Run with browser visible
npm run test:headed

# Run in debug mode (step through)
npm run test:debug

# Run in UI mode (interactive)
npm run test:ui
```

#### Console Logging

```typescript
import { setupConsoleLogging } from './utils/test-helpers';

test('debug test', async ({ page }) => {
  setupConsoleLogging(page); // Log browser console to terminal

  // Add debug points
  console.log('Current URL:', page.url());

  // Pause execution
  await page.pause(); // Opens Playwright Inspector
});
```

#### Screenshots on Failure

```typescript
test('screenshot on fail', async ({ page }, testInfo) => {
  try {
    await page.goto('/');
    // ... test steps
  } catch (error) {
    await page.screenshot({ path: `test-failure-${testInfo.title}.png` });
    throw error;
  }
});
```

---

## Additional Resources

### Playwright Documentation

- [Playwright API](https://playwright.dev/docs/api/class-playwright)
- [Best Practices](https://playwright.dev/docs/best-practices)
- [Selectors](https://playwright.dev/docs/selectors)
- [Component Testing](https://playwright.dev/docs/test-components)

### Project-Specific

- Main README: `../README.md`
- Frontend Guide: `../CLAUDE.md`
- Component Guidelines: `../src/components/CLAUDE.md`
- Existing Tests: `./` (this directory)

---

## Contributing

When adding new features:

1. **Add data-testid attributes** to interactive elements
2. **Write tests** for critical user paths
3. **Update this README** if introducing new patterns
4. **Run tests** before committing: `npm test`

**Testing Checklist:**

- [ ] Added data-testid to new components
- [ ] Wrote E2E tests for user flows
- [ ] Tests pass locally: `npm test`
- [ ] Tests pass in headed mode: `npm run test:headed`
- [ ] No new console errors or warnings
