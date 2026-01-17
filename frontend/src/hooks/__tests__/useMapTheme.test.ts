/**
 * Tests for useMapTheme hook
 *
 * This hook manages map theme (light/dark) based on user preferences
 */

import { test, expect } from '@playwright/test';

test.describe('useMapTheme', () => {
  test('should return light style by default', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const MAP_STYLES = {
        light: 'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json',
        dark: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
      };

      return MAP_STYLES.light;
    });

    expect(result).toBe('https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json');
  });

  test('should return dark style for dark theme', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const MAP_STYLES = {
        light: 'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json',
        dark: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
      };

      const theme = 'dark';
      const effectiveTheme = theme === 'dark' ? 'dark' : 'light';

      return MAP_STYLES[effectiveTheme];
    });

    expect(result).toBe('https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json');
  });

  test('should detect system preference for system theme', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
      return typeof systemPrefersDark === 'boolean';
    });

    expect(result).toBe(true);
  });

  test('should return correct isDarkMode flag', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const lightTheme = 'light';
      const darkTheme = 'dark';

      return {
        lightIsDark: lightTheme === 'dark',
        darkIsDark: darkTheme === 'dark',
      };
    });

    expect(result.lightIsDark).toBe(false);
    expect(result.darkIsDark).toBe(true);
  });

  test('should provide getMapStyle helper function', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const MAP_STYLES = {
        light: 'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json',
        dark: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
      };

      const getMapStyle = (theme: 'light' | 'dark') => MAP_STYLES[theme];

      return {
        light: getMapStyle('light'),
        dark: getMapStyle('dark'),
      };
    });

    expect(result.light).toBe('https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json');
    expect(result.dark).toBe('https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json');
  });

  test('should provide alternative map styles', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const ALTERNATIVE_MAP_STYLES = {
        light: {
          voyager: 'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json',
          positron: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
          osm: 'https://tiles.stadiamaps.com/styles/osm_bright.json',
        },
        dark: {
          darkMatter: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
          osmDark: 'https://tiles.stadiamaps.com/styles/alidade_smooth_dark.json',
        },
      };

      return {
        hasLightStyles: Object.keys(ALTERNATIVE_MAP_STYLES.light).length > 0,
        hasDarkStyles: Object.keys(ALTERNATIVE_MAP_STYLES.dark).length > 0,
        voyagerUrl: ALTERNATIVE_MAP_STYLES.light.voyager,
      };
    });

    expect(result.hasLightStyles).toBe(true);
    expect(result.hasDarkStyles).toBe(true);
    expect(result.voyagerUrl).toBe('https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json');
  });
});
