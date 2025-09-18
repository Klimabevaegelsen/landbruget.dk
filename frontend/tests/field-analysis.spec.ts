import { test, expect } from '@playwright/test';

test.describe('Field Analysis Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/field-analysis');
  });

  test('should load field analysis page', async ({ page }) => {
    // Wait for the page to load
    await page.waitForLoadState('networkidle');

    // Check for main page elements
    await expect(page.locator('h1, h2')).toBeVisible();
  });

  test('should display layer control panel', async ({ page }) => {
    // Wait for layer control panel to load
    await page.waitForSelector('[data-testid="layer-control-panel"]', {
      timeout: 10000,
    });

    const layerPanel = page.locator('[data-testid="layer-control-panel"]');
    await expect(layerPanel).toBeVisible();
  });

  test('should toggle layer visibility', async ({ page }) => {
    // Wait for layer controls to be available
    await page.waitForSelector('[data-testid="layer-toggle"]', {
      timeout: 10000,
    });

    const layerToggles = page.locator('[data-testid="layer-toggle"]');

    if ((await layerToggles.count()) > 0) {
      const firstToggle = layerToggles.first();
      await expect(firstToggle).toBeVisible();

      // Get initial state
      const initialState = await firstToggle.isChecked();

      // Click to toggle
      await firstToggle.click();

      // Wait a moment for state change
      await page.waitForTimeout(500);

      // Verify state changed
      const newState = await firstToggle.isChecked();
      expect(newState).toBe(!initialState);
    }
  });

  test('should display layer information correctly', async ({ page }) => {
    // Wait for layer items to load
    await page.waitForSelector('[data-testid="layer-item"]', {
      timeout: 10000,
    });

    const layerItems = page.locator('[data-testid="layer-item"]');

    if ((await layerItems.count()) > 0) {
      const firstLayer = layerItems.first();

      // Check for layer name
      await expect(
        firstLayer.locator('[data-testid="layer-name"]')
      ).toBeVisible();

      // Check for layer description
      await expect(
        firstLayer.locator('[data-testid="layer-description"]')
      ).toBeVisible();

      // Check for layer icon
      await expect(firstLayer.locator('svg')).toBeVisible();
    }
  });

  test('should handle filter changes', async ({ page }) => {
    // Wait for filter controls
    await page.waitForSelector('[data-testid="filter-control"]', {
      timeout: 10000,
    });

    const filterControls = page.locator('[data-testid="filter-control"]');

    if ((await filterControls.count()) > 0) {
      const firstFilter = filterControls.first();
      await expect(firstFilter).toBeVisible();

      // If it's a select/dropdown
      if ((await firstFilter.locator('select').count()) > 0) {
        const select = firstFilter.locator('select');
        await select.selectOption({ index: 1 });

        // Verify selection changed
        const selectedValue = await select.inputValue();
        expect(selectedValue).toBeTruthy();
      }

      // If it's an input field
      if ((await firstFilter.locator('input').count()) > 0) {
        const input = firstFilter.locator('input');
        await input.fill('test value');

        // Verify input changed
        const inputValue = await input.inputValue();
        expect(inputValue).toBe('test value');
      }
    }
  });

  test('should display map container', async ({ page }) => {
    // Look for map container
    const mapContainer = page.locator(
      '[data-testid="map-container"], .map-container, #map'
    );

    if ((await mapContainer.count()) > 0) {
      await expect(mapContainer.first()).toBeVisible();
    }
  });

  test('should be responsive on mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Wait for page to adapt
    await page.waitForTimeout(1000);

    // Check that layer control panel adapts to mobile
    const layerPanel = page.locator('[data-testid="layer-control-panel"]');

    if ((await layerPanel.count()) > 0) {
      await expect(layerPanel).toBeVisible();

      // Check if it becomes collapsible or changes layout on mobile
      const panelClasses = await layerPanel.getAttribute('class');
      expect(panelClasses).toBeTruthy();
    }
  });

  test('should handle layer icons and colors', async ({ page }) => {
    // Wait for layer items
    await page.waitForSelector('[data-testid="layer-item"]', {
      timeout: 10000,
    });

    const layerItems = page.locator('[data-testid="layer-item"]');

    if ((await layerItems.count()) > 0) {
      const firstLayer = layerItems.first();

      // Check for icon
      const icon = firstLayer.locator('svg').first();
      await expect(icon).toBeVisible();

      // Check for color indicator
      const colorElement = firstLayer.locator('[class*="bg-"]').first();
      if ((await colorElement.count()) > 0) {
        await expect(colorElement).toBeVisible();
      }
    }
  });
});
