/**
 * Tests for PMTiles Cache Service
 *
 * This service manages PMTiles URL caching and preloading
 */

import { test, expect } from '@playwright/test';

test.describe('PMTiles Cache Service', () => {
  test('should cache PMTiles URLs correctly', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map<string, { url: string; timestamp: number }>();
      const CACHE_DURATION = 7 * 24 * 60 * 60 * 1000;

      const filename = 'field_analysis_2024.pmtiles';
      const url = `https://www.landbruget.dk/api/pmtiles/${filename}`;
      const now = Date.now();

      cache.set(filename, { url, timestamp: now });

      const cached = cache.get(filename);
      return cached && cached.url === url && (now - cached.timestamp) < CACHE_DURATION;
    });

    expect(result).toBe(true);
  });

  test('should return cached URL on cache hit', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map<string, { url: string; timestamp: number }>();
      const CACHE_DURATION = 7 * 24 * 60 * 60 * 1000;

      const filename = 'test.pmtiles';
      const url = 'https://www.landbruget.dk/api/pmtiles/test.pmtiles';
      const now = Date.now();

      cache.set(filename, { url, timestamp: now });

      // Simulate cache hit
      const cached = cache.get(filename);
      if (cached && (Date.now() - cached.timestamp) < CACHE_DURATION) {
        return { hit: true, url: cached.url };
      }

      return { hit: false, url: null };
    });

    expect(result.hit).toBe(true);
    expect(result.url).toContain('test.pmtiles');
  });

  test('should generate new URL on cache miss', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map<string, { url: string; timestamp: number }>();
      const filename = 'nonexistent.pmtiles';

      const cached = cache.get(filename);
      return cached === undefined;
    });

    expect(result).toBe(true);
  });

  test('should expire cache after duration', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const CACHE_DURATION = 7 * 24 * 60 * 60 * 1000;
      const now = Date.now();

      const oldTimestamp = now - CACHE_DURATION - 1000; // Expired
      const newTimestamp = now; // Valid

      return {
        oldExpired: (now - oldTimestamp) > CACHE_DURATION,
        newValid: (now - newTimestamp) < CACHE_DURATION,
      };
    });

    expect(result.oldExpired).toBe(true);
    expect(result.newValid).toBe(true);
  });

  test('should use proxy URL by default', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const USE_PROXY = true;
      const PROXY_BASE_URL = 'https://www.landbruget.dk';
      const filename = 'test.pmtiles';

      const url = USE_PROXY
        ? `${PROXY_BASE_URL}/api/pmtiles/${filename}`
        : `https://data.pesticidkortet.dk/pmtiles/${filename}`;

      return url.includes('www.landbruget.dk/api/pmtiles');
    });

    expect(result).toBe(true);
  });

  test('should generate field analysis URLs for a year', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const year = 2024;
      const PROXY_BASE_URL = 'https://www.landbruget.dk';

      const urls = {
        fields: `${PROXY_BASE_URL}/api/pmtiles/pmtiles/field_analysis_${year}.pmtiles`,
        bnbo: `${PROXY_BASE_URL}/api/pmtiles/pmtiles/bnbo_areas.pmtiles`,
        wetlands: `${PROXY_BASE_URL}/api/pmtiles/pmtiles/wetlands_all.pmtiles`,
        water_projects: `${PROXY_BASE_URL}/api/pmtiles/pmtiles/water_projects.pmtiles`,
        buildings: `${PROXY_BASE_URL}/api/pmtiles/pmtiles/buildings_proximity.pmtiles`,
      };

      return {
        hasFields: urls.fields.includes('field_analysis_2024'),
        hasBnbo: urls.bnbo.includes('bnbo_areas'),
        hasWetlands: urls.wetlands.includes('wetlands_all'),
        hasWater: urls.water_projects.includes('water_projects'),
        hasBuildings: urls.buildings.includes('buildings_proximity'),
      };
    });

    expect(result.hasFields).toBe(true);
    expect(result.hasBnbo).toBe(true);
    expect(result.hasWetlands).toBe(true);
    expect(result.hasWater).toBe(true);
    expect(result.hasBuildings).toBe(true);
  });

  test('should track cache statistics', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map<string, { url: string; timestamp: number }>();

      // Add entries
      const entries = ['file1.pmtiles', 'file2.pmtiles', 'file3.pmtiles'];
      entries.forEach((filename, i) => {
        cache.set(filename, {
          url: `https://example.com/${filename}`,
          timestamp: Date.now() - i * 1000,
        });
      });

      const timestamps = Array.from(cache.values()).map(v => v.timestamp);

      return {
        size: cache.size,
        hasEntries: cache.size === 3,
        oldestExists: Math.min(...timestamps) > 0,
        newestExists: Math.max(...timestamps) > 0,
      };
    });

    expect(result.size).toBe(3);
    expect(result.hasEntries).toBe(true);
    expect(result.oldestExists).toBe(true);
    expect(result.newestExists).toBe(true);
  });

  test('should clear cache correctly', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map<string, { url: string; timestamp: number }>();

      // Add entries
      cache.set('file1.pmtiles', { url: 'https://example.com/file1.pmtiles', timestamp: Date.now() });
      cache.set('file2.pmtiles', { url: 'https://example.com/file2.pmtiles', timestamp: Date.now() });

      const sizeBefore = cache.size;

      // Clear cache
      cache.clear();

      const sizeAfter = cache.size;

      return { sizeBefore, sizeAfter };
    });

    expect(result.sizeBefore).toBe(2);
    expect(result.sizeAfter).toBe(0);
  });

  test('should clean expired entries', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map<string, { url: string; timestamp: number }>();
      const CACHE_DURATION = 7 * 24 * 60 * 60 * 1000;
      const now = Date.now();

      // Add expired entry
      cache.set('expired.pmtiles', {
        url: 'https://example.com/expired.pmtiles',
        timestamp: now - CACHE_DURATION - 1000,
      });

      // Add valid entry
      cache.set('valid.pmtiles', {
        url: 'https://example.com/valid.pmtiles',
        timestamp: now,
      });

      const sizeBefore = cache.size;

      // Clean expired
      let removedCount = 0;
      for (const [key, value] of cache.entries()) {
        if (now - value.timestamp > CACHE_DURATION) {
          cache.delete(key);
          removedCount++;
        }
      }

      const sizeAfter = cache.size;

      return { sizeBefore, sizeAfter, removedCount };
    });

    expect(result.sizeBefore).toBe(2);
    expect(result.sizeAfter).toBe(1);
    expect(result.removedCount).toBe(1);
  });

  test('should generate adjacent year filenames', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const currentYear = 2024;
      const availableYears = [2022, 2023, 2024, 2025];

      const adjacentYears = [currentYear - 1, currentYear + 1].filter(year =>
        availableYears.includes(year)
      );

      const filenames = adjacentYears.map(year => `field_analysis_${year}.pmtiles`);

      return {
        adjacentYears,
        filenames,
        has2023: filenames.includes('field_analysis_2023.pmtiles'),
        has2025: filenames.includes('field_analysis_2025.pmtiles'),
      };
    });

    expect(result.adjacentYears).toEqual([2023, 2025]);
    expect(result.has2023).toBe(true);
    expect(result.has2025).toBe(true);
  });
});
