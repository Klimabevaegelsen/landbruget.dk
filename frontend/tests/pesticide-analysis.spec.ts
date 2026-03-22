/**
 * E2E Tests for Pesticide Analysis Page (/pesticidanalyse)
 *
 * Tests page load, heading, and the PesticideAnalysisVisualization component.
 */

import { test, expect } from '@playwright/test';

test.describe('Pesticide Analysis Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/pesticidanalyse');
  });

  test('should load and display heading', async ({ page }) => {
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible({ timeout: 10000 });
    await expect(h1).toContainText('Pesticidanalyse');
  });

  test('should display description text', async ({ page }) => {
    await expect(
      page.locator('text=Analyser pesticidanvendelse')
    ).toBeVisible();
  });

  test('should render visualization container', async ({ page }) => {
    // The PesticideAnalysisVisualization component should render
    // Wait for the main content area to load
    await page.waitForLoadState('networkidle', { timeout: 15000 });

    // Page should have content below the heading
    const mainContent = page.locator('.container');
    await expect(mainContent.first()).toBeVisible();
  });

  test('should have navigation visible', async ({ page }) => {
    await expect(page.locator('nav')).toBeVisible();
  });

  test('should have no critical console errors', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    await page.goto('/pesticidanalyse');
    await page.waitForLoadState('networkidle', { timeout: 15000 });

    const criticalErrors = consoleErrors.filter(
      (err) =>
        !err.includes('favicon') &&
        !err.includes('third-party cookie') &&
        !err.includes('DevTools') &&
        !err.includes('Failed to fetch') // API may not be available in test
    );

    expect(criticalErrors).toEqual([]);
  });
});
