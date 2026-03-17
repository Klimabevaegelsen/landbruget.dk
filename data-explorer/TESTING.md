# Testing Guide - Data Explorer

## Testing Strategy

The Data Explorer components should be tested at three levels:

1. **Unit Tests**: Individual utility functions
2. **Component Tests**: React component behavior
3. **E2E Tests**: Full user workflows with Playwright

## Unit Tests (DuckDB Utilities)

### Test File: `src/lib/duckdb.test.ts`

```typescript
import { describe, test, expect, beforeAll, afterAll } from "@jest/globals";
import {
  initDuckDB,
  executeQuery,
  registerParquetTable,
  getTableSchema,
  closeDuckDB,
} from "./duckdb";

describe("DuckDB Utilities", () => {
  beforeAll(async () => {
    await initDuckDB();
  });

  afterAll(async () => {
    await closeDuckDB();
  });

  test("should initialize DuckDB", async () => {
    // Already initialized in beforeAll
    expect(true).toBe(true);
  });

  test("should execute simple query", async () => {
    const result = await executeQuery("SELECT 1 as num, 'test' as str");
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({ num: 1, str: "test" });
  });

  test("should register and query Parquet table", async () => {
    // Using a public test Parquet file
    const testUrl = "https://example.com/test.parquet";
    await registerParquetTable("test_table", testUrl);

    const schema = await getTableSchema("test_table");
    expect(schema).toBeTruthy();
    expect(Array.isArray(schema)).toBe(true);
  });

  test("should handle query errors gracefully", async () => {
    await expect(executeQuery("INVALID SQL")).rejects.toThrow();
  });
});
```

## Component Tests (React Testing Library)

### Test File: `src/components/SQLEditor.test.tsx`

```typescript
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { SQLEditor } from './SQLEditor';

describe('SQLEditor', () => {
  test('renders with initial query', () => {
    render(
      <SQLEditor
        initialQuery="SELECT * FROM test"
        onExecute={() => {}}
      />
    );

    expect(screen.getByText(/SQL Query/i)).toBeInTheDocument();
  });

  test('calls onExecute when Run Query button is clicked', async () => {
    const mockExecute = jest.fn();

    render(
      <SQLEditor
        initialQuery="SELECT 1"
        onExecute={mockExecute}
      />
    );

    const runButton = screen.getByText(/Run Query/i);
    fireEvent.click(runButton);

    await waitFor(() => {
      expect(mockExecute).toHaveBeenCalledWith('SELECT 1');
    });
  });

  test('disables execution when disabled prop is true', () => {
    render(
      <SQLEditor
        initialQuery="SELECT 1"
        onExecute={() => {}}
        disabled={true}
      />
    );

    const runButton = screen.getByText(/Run Query/i);
    expect(runButton).toBeDisabled();
  });

  test('shows loading state when isExecuting is true', () => {
    render(
      <SQLEditor
        initialQuery="SELECT 1"
        onExecute={() => {}}
        isExecuting={true}
      />
    );

    expect(screen.getByText(/Executing/i)).toBeInTheDocument();
  });
});
```

### Test File: `src/components/ResultsTable.test.tsx`

```typescript
import { render, screen } from '@testing-library/react';
import { ResultsTable } from './ResultsTable';

describe('ResultsTable', () => {
  const mockData = [
    { id: 1, name: 'Test', value: 100 },
    { id: 2, name: 'Sample', value: 200 },
  ];

  test('renders data in table format', () => {
    render(<ResultsTable data={mockData} />);

    expect(screen.getByText('Test')).toBeInTheDocument();
    expect(screen.getByText('Sample')).toBeInTheDocument();
  });

  test('shows loading state', () => {
    render(<ResultsTable data={[]} loading={true} />);

    expect(screen.getByText(/Executing query/i)).toBeInTheDocument();
  });

  test('shows error state', () => {
    render(<ResultsTable data={[]} error="Query failed" />);

    expect(screen.getByText(/Query Error/i)).toBeInTheDocument();
    expect(screen.getByText(/Query failed/i)).toBeInTheDocument();
  });

  test('shows empty state when no data', () => {
    render(<ResultsTable data={[]} />);

    expect(screen.getByText(/No Results/i)).toBeInTheDocument();
  });

  test('displays export button when data exists', () => {
    render(<ResultsTable data={mockData} />);

    expect(screen.getByText(/Export CSV/i)).toBeInTheDocument();
  });
});
```

### Test File: `src/components/DatasetBrowser.test.tsx`

```typescript
import { render, screen, waitFor } from '@testing-library/react';
import { DatasetBrowser } from './DatasetBrowser';

// Mock fetch
global.fetch = jest.fn();

describe('DatasetBrowser', () => {
  const mockManifest = {
    version: '1.0',
    generatedAt: '2024-01-10T19:00:00Z',
    datasets: [
      {
        name: 'test_dataset',
        displayName: 'Test Dataset',
        description: 'A test dataset',
        url: 'https://example.com/test.parquet',
        rowCount: 1000,
        sizeBytes: 5242880,
        lastUpdated: '2024-01-10',
        columns: 5,
      },
    ],
  };

  beforeEach(() => {
    (global.fetch as jest.Mock).mockResolvedValue({
      ok: true,
      json: async () => mockManifest,
    });
  });

  test('loads and displays datasets from manifest', async () => {
    render(
      <DatasetBrowser
        r2BaseUrl="https://example.com"
        onTableSelect={() => {}}
      />
    );

    await waitFor(() => {
      expect(screen.getByText('Test Dataset')).toBeInTheDocument();
    });
  });

  test('shows loading state while fetching', () => {
    render(
      <DatasetBrowser
        r2BaseUrl="https://example.com"
        onTableSelect={() => {}}
      />
    );

    expect(screen.getByText(/Loading datasets/i)).toBeInTheDocument();
  });

  test('handles fetch errors', async () => {
    (global.fetch as jest.Mock).mockRejectedValue(new Error('Network error'));

    render(
      <DatasetBrowser
        r2BaseUrl="https://example.com"
        onTableSelect={() => {}}
      />
    );

    await waitFor(() => {
      expect(screen.getByText(/Error Loading Datasets/i)).toBeInTheDocument();
    });
  });
});
```

## E2E Tests (Playwright)

### Test File: `e2e/data-explorer.spec.ts`

```typescript
import { test, expect } from "@playwright/test";

test.describe("Data Explorer", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/example");
  });

  test("should load and display datasets", async ({ page }) => {
    // Wait for datasets to load
    await expect(page.getByText("Available Datasets")).toBeVisible();

    // Should show at least one dataset
    await expect(page.locator('[data-testid="dataset-item"]').first()).toBeVisible();
  });

  test("should expand dataset to show schema", async ({ page }) => {
    // Click to expand first dataset
    await page.locator('[data-testid="dataset-item"]').first().click();

    // Should show schema
    await expect(page.getByText("Schema")).toBeVisible();
  });

  test("should select dataset and populate query", async ({ page }) => {
    // Expand and select first dataset
    await page.locator('[data-testid="dataset-item"]').first().click();
    await page.getByRole("button", { name: /Query This Dataset/i }).click();

    // Should populate SQL editor
    const editor = page.locator(".cm-content");
    await expect(editor).toContainText("SELECT");
  });

  test("should execute query and display results", async ({ page }) => {
    // Select dataset
    await page.locator('[data-testid="dataset-item"]').first().click();
    await page.getByRole("button", { name: /Query This Dataset/i }).click();

    // Execute query
    await page.getByRole("button", { name: /Run Query/i }).click();

    // Wait for results
    await expect(page.getByText("Query Results")).toBeVisible();
    await expect(page.locator("table")).toBeVisible();
  });

  test("should export results to CSV", async ({ page }) => {
    // Execute a query first
    await page.locator('[data-testid="dataset-item"]').first().click();
    await page.getByRole("button", { name: /Query This Dataset/i }).click();
    await page.getByRole("button", { name: /Run Query/i }).click();

    // Wait for results
    await expect(page.getByText("Query Results")).toBeVisible();

    // Set up download listener
    const downloadPromise = page.waitForEvent("download");

    // Click export
    await page.getByRole("button", { name: /Export CSV/i }).click();

    // Verify download
    const download = await downloadPromise;
    expect(download.suggestedFilename()).toMatch(/\.csv$/);
  });

  test("should handle query errors", async ({ page }) => {
    // Select dataset
    await page.locator('[data-testid="dataset-item"]').first().click();

    // Enter invalid SQL
    const editor = page.locator(".cm-content");
    await editor.click();
    await page.keyboard.type("INVALID SQL QUERY");

    // Execute
    await page.getByRole("button", { name: /Run Query/i }).click();

    // Should show error
    await expect(page.getByText(/Query Error/i)).toBeVisible();
  });

  test("should paginate large result sets", async ({ page }) => {
    // Execute query with many results
    await page.locator('[data-testid="dataset-item"]').first().click();
    const editor = page.locator(".cm-content");
    await editor.click();
    await page.keyboard.press("Meta+A"); // Select all
    await page.keyboard.type("SELECT * FROM dataset LIMIT 200");
    await page.getByRole("button", { name: /Run Query/i }).click();

    // Wait for results
    await expect(page.getByText("Query Results")).toBeVisible();

    // Should show pagination controls
    await expect(page.getByLabel("Next page")).toBeVisible();

    // Click next page
    await page.getByLabel("Next page").click();

    // Should show page 2
    await expect(page.getByText(/Page 2/i)).toBeVisible();
  });

  test("should sort columns", async ({ page }) => {
    // Execute query
    await page.locator('[data-testid="dataset-item"]').first().click();
    await page.getByRole("button", { name: /Query This Dataset/i }).click();
    await page.getByRole("button", { name: /Run Query/i }).click();

    // Wait for results
    await expect(page.getByText("Query Results")).toBeVisible();

    // Click column header to sort
    const firstColumnHeader = page.locator("th").first();
    await firstColumnHeader.click();

    // Should show sort indicator
    await expect(page.locator('[data-icon="chevron-up"]')).toBeVisible();
  });
});
```

## Manual Testing Checklist

### Basic Functionality

- [ ] Dataset browser loads and displays datasets from manifest
- [ ] Clicking a dataset expands to show schema
- [ ] Selecting a dataset populates SQL editor with default query
- [ ] Run Query button executes query and displays results
- [ ] Results table shows data with correct formatting
- [ ] Export CSV downloads file with correct data

### Error Handling

- [ ] Invalid manifest URL shows error message
- [ ] Malformed manifest JSON shows error message
- [ ] Invalid SQL query shows error message
- [ ] Network failures are handled gracefully
- [ ] Large result sets don't crash browser

### Performance

- [ ] DuckDB initializes in <3 seconds
- [ ] Simple queries execute in <1 second
- [ ] Complex queries with aggregation execute in <5 seconds
- [ ] Large result sets (10k+ rows) render without lag
- [ ] Pagination works smoothly

### UI/UX

- [ ] Loading states are clear and responsive
- [ ] Error messages are helpful and actionable
- [ ] Keyboard shortcuts work (Cmd+Enter to execute)
- [ ] Table columns are sortable
- [ ] Pagination controls are intuitive
- [ ] Dark theme is applied correctly (CodeMirror)

### Browser Compatibility

- [ ] Works in Chrome (latest)
- [ ] Works in Firefox (latest)
- [ ] Works in Safari (latest)
- [ ] Works in Edge (latest)
- [ ] Mobile responsive (basic functionality)

## Running Tests

```bash
# Unit and component tests (once Jest is set up)
npm run test

# E2E tests with Playwright
npm run test:e2e

# E2E tests in UI mode (interactive)
npm run test:e2e:ui

# E2E tests in headed mode (see browser)
npm run test:e2e:headed
```

## Test Data Setup

For E2E testing, you'll need:

1. **Test Parquet files**: Small sample datasets on R2
2. **Test manifest**: A manifest.json pointing to test files
3. **Test environment variables**: R2 URLs in `.env.test`

Example test manifest:

```json
{
  "version": "1.0",
  "generatedAt": "2024-01-10T19:00:00Z",
  "datasets": [
    {
      "name": "test_small",
      "displayName": "Small Test Dataset",
      "description": "10 rows for quick testing",
      "url": "https://test-bucket.r2.dev/test_small.parquet",
      "rowCount": 10,
      "sizeBytes": 1024,
      "lastUpdated": "2024-01-10",
      "columns": 5
    },
    {
      "name": "test_large",
      "displayName": "Large Test Dataset",
      "description": "10,000 rows for pagination testing",
      "url": "https://test-bucket.r2.dev/test_large.parquet",
      "rowCount": 10000,
      "sizeBytes": 524288,
      "lastUpdated": "2024-01-10",
      "columns": 10
    }
  ]
}
```

## CI/CD Integration

Add to `.github/workflows/test.yml`:

```yaml
- name: Install dependencies
  run: npm ci

- name: Run unit tests
  run: npm test

- name: Run E2E tests
  run: npm run test:e2e
  env:
    R2_BASE_URL: ${{ secrets.TEST_R2_URL }}
```
