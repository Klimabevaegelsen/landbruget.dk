/**
 * E2E Tests for Kommune Rankings Page (/kommuner)
 *
 * Tests page load, heading, loading state, and ranking tables with mocked API.
 */

import { test, expect } from '@playwright/test';
import { mockApiResponse } from './utils/test-helpers';

const MOCK_RANKINGS_RESPONSE = {
  rankings: {
    land_use: [
      {
        municipality: 'Ringkøbing-Skjern',
        rank: 1,
        value: 125000,
        metric: 'total_agricultural_area_ha',
        additional_data: { unique_companies: 450, total_fields: 8500 },
      },
      {
        municipality: 'Varde',
        rank: 2,
        value: 98000,
        metric: 'total_agricultural_area_ha',
        additional_data: { unique_companies: 380, total_fields: 6200 },
      },
    ],
    pesticide_burden: [
      {
        municipality: 'Tønder',
        rank: 1,
        value: 42.5,
        metric: 'total_pesticide_burden',
        additional_data: { total_applications: 12000 },
      },
    ],
  },
  metadata: {
    year: 2024,
    total_municipalities: 98,
    generated_at: '2026-03-20T00:00:00Z',
    categories_included: ['land_use', 'pesticide_burden'],
  },
};

test.describe('Kommune Rankings Page', () => {
  test('should load and display heading', async ({ page }) => {
    // Mock the API to avoid real network calls
    await mockApiResponse(
      page,
      /\/api\/supabase\/functions\/kommuner/,
      MOCK_RANKINGS_RESPONSE
    );

    await page.goto('/kommuner');

    const h1 = page.locator('h1');
    await expect(h1).toBeVisible({ timeout: 10000 });
    await expect(h1).toContainText('Kommune Ranglister');
  });

  test('should display ranking tables with mocked data', async ({ page }) => {
    await mockApiResponse(
      page,
      /\/api\/supabase\/functions\/kommuner/,
      MOCK_RANKINGS_RESPONSE
    );

    await page.goto('/kommuner');

    // Wait for loading to finish — look for table content
    await expect(page.locator('text=Ringkøbing-Skjern')).toBeVisible({
      timeout: 10000,
    });

    // Verify ranking table structure
    await expect(page.locator('text=Størst landbrugsareal')).toBeVisible();
    await expect(page.locator('text=Varde')).toBeVisible();
  });

  test('should show loading state initially', async ({ page }) => {
    // Delay the API response to observe loading state
    await page.route(/\/api\/supabase\/functions\/kommuner/, async (route) => {
      await new Promise((resolve) => setTimeout(resolve, 2000));
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(MOCK_RANKINGS_RESPONSE),
      });
    });

    await page.goto('/kommuner');

    // Loading indicator should be visible
    await expect(page.locator('text=Indlæser kommuneranglister')).toBeVisible();
  });

  test('should show error state on API failure', async ({ page }) => {
    await page.route(/\/api\/supabase\/functions\/kommuner/, (route) => {
      route.fulfill({
        status: 500,
        contentType: 'application/json',
        body: JSON.stringify({ error: 'Internal Server Error' }),
      });
    });

    await page.goto('/kommuner');

    // Error message should appear
    await expect(page.locator('text=Fejl ved indlæsning')).toBeVisible({
      timeout: 10000,
    });

    // Retry button should be visible
    await expect(page.locator('text=Prøv igen')).toBeVisible();
  });

  test('should have navigation visible', async ({ page }) => {
    await mockApiResponse(
      page,
      /\/api\/supabase\/functions\/kommuner/,
      MOCK_RANKINGS_RESPONSE
    );

    await page.goto('/kommuner');
    await expect(page.locator('nav')).toBeVisible();
  });
});
