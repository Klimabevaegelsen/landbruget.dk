import { Page, expect } from '@playwright/test';

/**
 * Test Utilities and Helpers
 *
 * Common utilities for Playwright E2E and component testing
 */

// ============================================================================
// WAIT UTILITIES
// ============================================================================

/**
 * Wait for element to be visible with custom timeout
 */
export async function waitForElement(
  page: Page,
  selector: string,
  options?: { timeout?: number }
) {
  const timeout = options?.timeout ?? 10000;
  await page.waitForSelector(selector, { state: 'visible', timeout });
}

/**
 * Wait for network to be idle (no requests for specified duration)
 */
export async function waitForNetworkIdle(
  page: Page,
  options?: { timeout?: number; idleTime?: number }
) {
  const timeout = options?.timeout ?? 10000;
  const idleTime = options?.idleTime ?? 500;

  await page.waitForLoadState('networkidle', { timeout });
  await page.waitForTimeout(idleTime);
}

/**
 * Wait for specific data-testid element
 */
export async function waitForTestId(
  page: Page,
  testId: string,
  options?: { timeout?: number }
) {
  await waitForElement(page, `[data-testid="${testId}"]`, options);
}

/**
 * Wait for API response matching URL pattern
 */
export async function waitForApiResponse(
  page: Page,
  urlPattern: string | RegExp,
  options?: { timeout?: number }
) {
  const timeout = options?.timeout ?? 10000;
  return await page.waitForResponse(
    (response) => {
      const url = response.url();
      const matches =
        typeof urlPattern === 'string'
          ? url.includes(urlPattern)
          : urlPattern.test(url);
      return matches && response.status() === 200;
    },
    { timeout }
  );
}

// ============================================================================
// ELEMENT INTERACTION UTILITIES
// ============================================================================

/**
 * Click element by data-testid
 */
export async function clickTestId(
  page: Page,
  testId: string,
  options?: { force?: boolean; timeout?: number }
) {
  await page.click(`[data-testid="${testId}"]`, options);
}

/**
 * Fill input by data-testid
 */
export async function fillTestId(
  page: Page,
  testId: string,
  value: string,
  options?: { delay?: number }
) {
  await page.fill(`[data-testid="${testId}"]`, value, options);
}

/**
 * Type into input by data-testid with realistic delay
 */
export async function typeTestId(
  page: Page,
  testId: string,
  value: string,
  options?: { delay?: number }
) {
  const delay = options?.delay ?? 50; // Default 50ms between keystrokes
  await page.type(`[data-testid="${testId}"]`, value, { delay });
}

/**
 * Select option by data-testid
 */
export async function selectTestId(page: Page, testId: string, value: string) {
  await page.selectOption(`[data-testid="${testId}"]`, value);
}

/**
 * Check if element with data-testid is visible
 */
export async function isTestIdVisible(
  page: Page,
  testId: string
): Promise<boolean> {
  try {
    const element = page.locator(`[data-testid="${testId}"]`);
    return await element.isVisible();
  } catch {
    return false;
  }
}

// ============================================================================
// ASSERTION UTILITIES
// ============================================================================

/**
 * Assert element with data-testid is visible
 */
export async function expectTestIdVisible(page: Page, testId: string) {
  await expect(page.locator(`[data-testid="${testId}"]`)).toBeVisible();
}

/**
 * Assert element with data-testid contains text
 */
export async function expectTestIdToContainText(
  page: Page,
  testId: string,
  text: string
) {
  await expect(page.locator(`[data-testid="${testId}"]`)).toContainText(text);
}

/**
 * Assert element with data-testid has exact text
 */
export async function expectTestIdToHaveText(
  page: Page,
  testId: string,
  text: string
) {
  await expect(page.locator(`[data-testid="${testId}"]`)).toHaveText(text);
}

/**
 * Assert element with data-testid has count
 */
export async function expectTestIdCount(
  page: Page,
  testId: string,
  count: number
) {
  await expect(page.locator(`[data-testid="${testId}"]`)).toHaveCount(count);
}

/**
 * Assert element with data-testid is not visible
 */
export async function expectTestIdNotVisible(page: Page, testId: string) {
  await expect(page.locator(`[data-testid="${testId}"]`)).not.toBeVisible();
}

// ============================================================================
// MOCK DATA GENERATORS
// ============================================================================

/**
 * Generate mock CVR number (8 digits)
 */
export function generateMockCVR(): string {
  const cvr = Math.floor(10000000 + Math.random() * 90000000);
  return cvr.toString();
}

/**
 * Generate mock CHR number (6 digits)
 */
export function generateMockCHR(): string {
  const chr = Math.floor(100000 + Math.random() * 900000);
  return chr.toString();
}

/**
 * Generate mock company data
 */
export interface MockCompany {
  id: string;
  cvr: string;
  name: string;
  address: string;
  type: string;
}

export function generateMockCompany(
  overrides?: Partial<MockCompany>
): MockCompany {
  const cvr = generateMockCVR();
  return {
    id: `company-${cvr}`,
    cvr,
    name: `Test Virksomhed ${cvr.slice(0, 4)}`,
    address: 'Testvej 123, 1234 Testby',
    type: 'Landbrug',
    ...overrides,
  };
}

/**
 * Generate mock search results
 */
export function generateMockSearchResults(count: number): MockCompany[] {
  return Array.from({ length: count }, (_, i) =>
    generateMockCompany({
      id: `company-${i}`,
      name: `Test Virksomhed ${i + 1}`,
    })
  );
}

/**
 * Generate mock field data
 */
export interface MockField {
  id: string;
  bfe: string;
  area: number;
  cropType: string;
  organic: boolean;
}

export function generateMockField(overrides?: Partial<MockField>): MockField {
  const bfe = `0101-${Math.floor(100000 + Math.random() * 900000)}-1a`;
  return {
    id: `field-${bfe}`,
    bfe,
    area: Math.floor(10 + Math.random() * 990), // 10-1000 hectares
    cropType: 'Hvede',
    organic: Math.random() > 0.8, // 20% organic
    ...overrides,
  };
}

/**
 * Generate mock table data
 */
export function generateMockTableData<T extends Record<string, any>>(
  count: number,
  generator: (index: number) => T
): T[] {
  return Array.from({ length: count }, (_, i) => generator(i));
}

// ============================================================================
// NAVIGATION UTILITIES
// ============================================================================

/**
 * Navigate to page and wait for network idle
 */
export async function navigateAndWait(
  page: Page,
  url: string,
  options?: { timeout?: number }
) {
  await page.goto(url, { waitUntil: 'networkidle', timeout: options?.timeout });
}

/**
 * Reload page and wait for network idle
 */
export async function reloadAndWait(
  page: Page,
  options?: { timeout?: number }
) {
  await page.reload({ waitUntil: 'networkidle', timeout: options?.timeout });
}

// ============================================================================
// SCREENSHOT UTILITIES
// ============================================================================

/**
 * Take screenshot of element by data-testid
 */
export async function screenshotTestId(
  page: Page,
  testId: string,
  path: string
) {
  const element = page.locator(`[data-testid="${testId}"]`);
  await element.screenshot({ path });
}

/**
 * Take full page screenshot
 */
export async function screenshotFullPage(page: Page, path: string) {
  await page.screenshot({ path, fullPage: true });
}

// ============================================================================
// ACCESSIBILITY UTILITIES
// ============================================================================

/**
 * Check for basic accessibility issues using axe-core
 * Requires @axe-core/playwright to be installed
 */
export async function checkAccessibility(page: Page) {
  // This is a placeholder - implement with @axe-core/playwright if needed
  // import { injectAxe, checkA11y } from '@axe-core/playwright';
  // await injectAxe(page);
  // await checkA11y(page);

  // For now, just check for basic issues
  const images = await page.locator('img').all();
  for (const img of images) {
    const alt = await img.getAttribute('alt');
    if (alt === null) {
      console.warn(
        'Image missing alt attribute:',
        await img.getAttribute('src')
      );
    }
  }
}

// ============================================================================
// FORM UTILITIES
// ============================================================================

/**
 * Fill form fields by data-testid mapping
 */
export async function fillForm(page: Page, fields: Record<string, string>) {
  for (const [testId, value] of Object.entries(fields)) {
    await fillTestId(page, testId, value);
  }
}

/**
 * Submit form by data-testid
 */
export async function submitForm(page: Page, formTestId: string) {
  await page.locator(`[data-testid="${formTestId}"]`).press('Enter');
}

// ============================================================================
// DEBUGGING UTILITIES
// ============================================================================

/**
 * Log page console messages (useful for debugging)
 */
export function setupConsoleLogging(page: Page) {
  page.on('console', (msg) => {
    const type = msg.type();
    if (type === 'error' || type === 'warning') {
      console.log(`[Browser ${type}]:`, msg.text());
    }
  });
}

/**
 * Wait for selector with detailed error message
 */
export async function waitForSelectorWithError(
  page: Page,
  selector: string,
  errorMessage?: string
) {
  try {
    await page.waitForSelector(selector, { timeout: 10000 });
  } catch {
    const customError = errorMessage || `Failed to find element: ${selector}`;
    throw new Error(`${customError}\nPage URL: ${page.url()}`);
  }
}

// ============================================================================
// MOBILE UTILITIES
// ============================================================================

/**
 * Set viewport to mobile size
 */
export async function setMobileViewport(page: Page) {
  await page.setViewportSize({ width: 375, height: 667 }); // iPhone SE
}

/**
 * Set viewport to tablet size
 */
export async function setTabletViewport(page: Page) {
  await page.setViewportSize({ width: 768, height: 1024 }); // iPad
}

/**
 * Set viewport to desktop size
 */
export async function setDesktopViewport(page: Page) {
  await page.setViewportSize({ width: 1920, height: 1080 }); // Full HD
}

// ============================================================================
// NETWORK MOCKING UTILITIES
// ============================================================================

/**
 * Mock API response with custom data
 */
export async function mockApiResponse(
  page: Page,
  urlPattern: string | RegExp,
  responseData: any,
  statusCode: number = 200
) {
  await page.route(urlPattern, (route) => {
    route.fulfill({
      status: statusCode,
      contentType: 'application/json',
      body: JSON.stringify(responseData),
    });
  });
}

/**
 * Mock API error
 */
export async function mockApiError(
  page: Page,
  urlPattern: string | RegExp,
  statusCode: number = 500,
  errorMessage: string = 'Internal Server Error'
) {
  await page.route(urlPattern, (route) => {
    route.fulfill({
      status: statusCode,
      contentType: 'application/json',
      body: JSON.stringify({ error: errorMessage }),
    });
  });
}

/**
 * Clear all route mocks
 */
export async function clearAllMocks(page: Page) {
  await page.unrouteAll();
}
