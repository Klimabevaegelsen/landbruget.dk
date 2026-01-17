/**
 * E2E Tests for Company Detail Pages
 *
 * Tests company data loading, navigation, and interactions
 */

import { test, expect } from '@playwright/test';
import { waitForTestId, expectTestIdVisible } from './utils/test-helpers';

test.describe('Company Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should navigate to company page from ranking table', async ({
    page,
  }) => {
    // Wait for ranking tables to load
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    // Click on the first company link
    const companyLinks = page.locator('[data-testid="company-link"]');
    if ((await companyLinks.count()) > 0) {
      const href = await companyLinks.first().getAttribute('href');
      expect(href).toContain('/company/');

      await companyLinks.first().click();

      // Wait for company page to load
      await page.waitForLoadState('networkidle');

      // Verify we're on a company page
      expect(page.url()).toContain('/company/');
    }
  });

  test('should display company header information', async ({ page }) => {
    // Navigate to homepage and find a company
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const companyLinks = page.locator('[data-testid="company-link"]');
    if ((await companyLinks.count()) > 0) {
      await companyLinks.first().click();
      await page.waitForLoadState('networkidle');

      // Check for company name
      await expect(page.locator('h1, h2').first()).toBeVisible();

      // Check for CVR number if displayed
      const cvrText = page.locator('text=/CVR|\\d{8}/i');
      if ((await cvrText.count()) > 0) {
        await expect(cvrText.first()).toBeVisible();
      }
    }
  });

  test('should display company details sections', async ({ page }) => {
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const companyLinks = page.locator('[data-testid="company-link"]');
    if ((await companyLinks.count()) > 0) {
      await companyLinks.first().click();
      await page.waitForLoadState('networkidle');

      // Wait for content to load
      await page.waitForTimeout(2000);

      // Check for main content area
      const mainContent = page.locator('main, [role="main"]');
      await expect(mainContent).toBeVisible();
    }
  });

  test('should show loading state while fetching data', async ({ page }) => {
    // Intercept API calls to simulate slow loading
    await page.route('**/api/**', async (route) => {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      await route.continue();
    });

    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const companyLinks = page.locator('[data-testid="company-link"]');
    if ((await companyLinks.count()) > 0) {
      await companyLinks.first().click();

      // Check for loading indicator (could be skeleton, spinner, etc.)
      const loadingIndicators = page.locator(
        '[data-testid*="loading"], [data-testid*="skeleton"], .animate-pulse'
      );

      // At least one loading indicator should be visible initially
      const hasLoading = (await loadingIndicators.count()) > 0;
      expect(hasLoading).toBe(true);
    }
  });

  test('should handle back navigation correctly', async ({ page }) => {
    const homepageUrl = page.url();

    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const companyLinks = page.locator('[data-testid="company-link"]');
    if ((await companyLinks.count()) > 0) {
      await companyLinks.first().click();
      await page.waitForLoadState('networkidle');

      // Go back
      await page.goBack();
      await page.waitForLoadState('networkidle');

      // Should be back on homepage
      expect(page.url()).toBe(homepageUrl);

      // Ranking tables should still be visible
      await expectTestIdVisible(page, 'ranking-table');
    }
  });

  test('should display company address if available', async ({ page }) => {
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const companyLinks = page.locator('[data-testid="company-link"]');
    if ((await companyLinks.count()) > 0) {
      await companyLinks.first().click();
      await page.waitForLoadState('networkidle');

      // Look for address indicators (postal code, city, street)
      const addressElements = page.locator(
        'text=/\\d{4}|[A-Z][a-zæøå]+vej|[A-Z][a-zæøå]+gade|[A-Z][a-zæøå]+sted/i'
      );

      // If address exists, check it's visible
      if ((await addressElements.count()) > 0) {
        await expect(addressElements.first()).toBeVisible();
      }
    }
  });

  test('should handle company not found gracefully', async ({ page }) => {
    // Try to navigate to a non-existent company
    await page.goto('/company/nonexistent-company-id-12345');
    await page.waitForLoadState('networkidle');

    // Should show some kind of error or not found message
    const errorIndicators = page.locator(
      'text=/ikke fundet|not found|fejl|error/i'
    );

    // Either shows error or redirects to homepage
    const hasError = (await errorIndicators.count()) > 0;
    const isRedirected = page.url() === 'http://localhost:3000/';

    expect(hasError || isRedirected).toBe(true);
  });

  test('should display company data tabs if available', async ({ page }) => {
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const companyLinks = page.locator('[data-testid="company-link"]');
    if ((await companyLinks.count()) > 0) {
      await companyLinks.first().click();
      await page.waitForLoadState('networkidle');

      // Look for tab navigation
      const tabs = page.locator('[role="tab"], [data-testid*="tab"]');
      if ((await tabs.count()) > 0) {
        await expect(tabs.first()).toBeVisible();
      }
    }
  });
});
