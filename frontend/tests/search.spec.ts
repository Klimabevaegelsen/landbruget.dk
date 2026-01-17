/**
 * E2E Tests for Global Search Functionality
 *
 * Tests search input, results, tab switching, and filtering
 */

import { test, expect } from '@playwright/test';

test.describe('Search Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display search input on homepage', async ({ page }) => {
    // Look for search input
    const searchInput = page.locator(
      '[data-testid="search-input"], input[type="search"], input[placeholder*="Søg"]'
    );

    await expect(searchInput.first()).toBeVisible();
  });

  test('should accept text input in search field', async ({ page }) => {
    const searchInput = page.locator(
      '[data-testid="search-input"], input[type="search"], input[placeholder*="Søg"]'
    ).first();

    await searchInput.fill('Arla');

    const value = await searchInput.inputValue();
    expect(value).toBe('Arla');
  });

  test('should show search results when typing', async ({ page }) => {
    const searchInput = page.locator(
      '[data-testid="search-input"], input[type="search"], input[placeholder*="Søg"]'
    ).first();

    // Type search query
    await searchInput.fill('test');

    // Wait for results to appear (with debounce delay)
    await page.waitForTimeout(500);

    // Check for results container
    const resultsContainer = page.locator(
      '[data-testid="search-results"], [role="listbox"], .search-results'
    );

    // Results should appear or no results message should show
    const hasResults = await resultsContainer.count() > 0;
    const hasNoResults = await page.locator('text=/ingen resultater|no results/i').count() > 0;

    expect(hasResults || hasNoResults).toBe(true);
  });

  test('should navigate to company when clicking search result', async ({ page }) => {
    const searchInput = page.locator(
      '[data-testid="search-input"], input[type="search"], input[placeholder*="Søg"]'
    ).first();

    await searchInput.fill('Arla');
    await page.waitForTimeout(500);

    // Look for search result items
    const resultItems = page.locator(
      '[data-testid="search-result"], [role="option"]'
    );

    if (await resultItems.count() > 0) {
      const firstResult = resultItems.first();
      await firstResult.click();

      // Should navigate to a detail page
      await page.waitForLoadState('networkidle');

      // URL should have changed
      expect(page.url()).not.toBe('http://localhost:3000/');
    }
  });

  test('should clear search input with clear button', async ({ page }) => {
    const searchInput = page.locator(
      '[data-testid="search-input"], input[type="search"], input[placeholder*="Søg"]'
    ).first();

    await searchInput.fill('test query');

    // Look for clear button
    const clearButton = page.locator(
      '[data-testid="search-clear"], button[aria-label*="clear"], button[aria-label*="ryd"]'
    );

    if (await clearButton.count() > 0) {
      await clearButton.first().click();

      const value = await searchInput.inputValue();
      expect(value).toBe('');
    }
  });

  test('should handle empty search gracefully', async ({ page }) => {
    const searchInput = page.locator(
      '[data-testid="search-input"], input[type="search"], input[placeholder*="Søg"]'
    ).first();

    await searchInput.fill('');
    await searchInput.press('Enter');

    // Should not crash or show error
    await page.waitForTimeout(500);

    // Page should still be functional
    await expect(searchInput).toBeVisible();
  });

  test('should show loading state while searching', async ({ page }) => {
    // Slow down network to see loading state
    await page.route('**/api/**', async route => {
      await new Promise(resolve => setTimeout(resolve, 1000));
      await route.continue();
    });

    const searchInput = page.locator(
      '[data-testid="search-input"], input[type="search"], input[placeholder*="Søg"]'
    ).first();

    await searchInput.fill('test');

    // Look for loading indicator
    const loadingIndicator = page.locator(
      '[data-testid="search-loading"], .animate-spin, [aria-busy="true"]'
    );

    // Should show loading state briefly
    const hasLoading = await loadingIndicator.count() > 0;
    expect(hasLoading).toBe(true);
  });

  test('should filter results by category tabs', async ({ page }) => {
    const searchInput = page.locator(
      '[data-testid="search-input"], input[type="search"], input[placeholder*="Søg"]'
    ).first();

    await searchInput.fill('test');
    await page.waitForTimeout(500);

    // Look for category tabs
    const categoryTabs = page.locator(
      '[data-testid*="category-tab"], [role="tab"]'
    );

    if (await categoryTabs.count() > 1) {
      // Click second tab
      await categoryTabs.nth(1).click();

      // Results should update
      await page.waitForTimeout(300);

      // Tab should be active
      const secondTab = categoryTabs.nth(1);
      const isActive = await secondTab.getAttribute('aria-selected') === 'true' ||
                      await secondTab.getAttribute('data-state') === 'active';

      expect(isActive).toBe(true);
    }
  });

  test('should highlight search terms in results', async ({ page }) => {
    const searchInput = page.locator(
      '[data-testid="search-input"], input[type="search"], input[placeholder*="Søg"]'
    ).first();

    const searchTerm = 'test';
    await searchInput.fill(searchTerm);
    await page.waitForTimeout(500);

    // Look for highlighted text
    const highlightedText = page.locator(
      'mark, .highlight, [data-testid="search-highlight"]'
    );

    // If results have highlighting, check it
    if (await highlightedText.count() > 0) {
      await expect(highlightedText.first()).toBeVisible();
    }
  });

  test('should show recent searches if available', async ({ page }) => {
    const searchInput = page.locator(
      '[data-testid="search-input"], input[type="search"], input[placeholder*="Søg"]'
    ).first();

    // Perform a search
    await searchInput.fill('test search');
    await page.waitForTimeout(500);

    // Click a result if available
    const resultItems = page.locator('[data-testid="search-result"]');
    if (await resultItems.count() > 0) {
      await resultItems.first().click();
      await page.waitForLoadState('networkidle');

      // Go back and open search again
      await page.goBack();
      await searchInput.click();

      // Look for recent searches
      const recentSearches = page.locator(
        '[data-testid="recent-searches"], text=/seneste søgninger|recent searches/i'
      );

      // If recent searches feature exists, it should be visible
      if (await recentSearches.count() > 0) {
        await expect(recentSearches.first()).toBeVisible();
      }
    }
  });
});
