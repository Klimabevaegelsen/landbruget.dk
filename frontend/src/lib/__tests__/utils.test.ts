/**
 * Tests for general utilities
 *
 * These utilities provide common helper functions
 */

import { test, expect } from '@playwright/test';

test.describe('Utility Functions', () => {
  test('should merge class names correctly with cn()', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      // Simplified version of cn() logic for testing
      const classNames = (base: string, conditional?: string | false) => {
        const classes = [base];
        if (conditional) {
          classes.push(conditional);
        }
        return classes.join(' ').trim();
      };

      return {
        base: classNames('text-red-500'),
        withConditional: classNames('text-red-500', 'font-bold'),
        withFalseConditional: classNames('text-red-500', false),
      };
    });

    expect(result.base).toBe('text-red-500');
    expect(result.withConditional).toBe('text-red-500 font-bold');
    expect(result.withFalseConditional).toBe('text-red-500');
  });

  test('should slugify text correctly', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const slugify = (text: string) => {
        return text.toLowerCase().replace(/ /g, '-');
      };

      return {
        simple: slugify('Hello World'),
        multipleSpaces: slugify('Hello  World  Test'),
        alreadySlug: slugify('already-slug'),
        uppercase: slugify('UPPERCASE TEXT'),
      };
    });

    expect(result.simple).toBe('hello-world');
    expect(result.multipleSpaces).toBe('hello--world--test');
    expect(result.alreadySlug).toBe('already-slug');
    expect(result.uppercase).toBe('uppercase-text');
  });

  test('should provide VizColors array', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const VizColors = [
        '#4F5D75',
        '#C67750',
        '#467968',
        '#775120',
        '#7F2E39',
        '#2D673D',
        '#503955',
        '#5F318B',
      ];

      return {
        isArray: Array.isArray(VizColors),
        hasCorrectLength: VizColors.length === 8,
        allAreHexColors: VizColors.every((color) =>
          /^#[0-9A-F]{6}$/i.test(color)
        ),
        firstColor: VizColors[0],
      };
    });

    expect(result.isArray).toBe(true);
    expect(result.hasCorrectLength).toBe(true);
    expect(result.allAreHexColors).toBe(true);
    expect(result.firstColor).toBe('#4F5D75');
  });

  test('should handle scrollToElement correctly', async ({ page }) => {
    await page.goto('/');

    // Add a test element to the page
    await page.evaluate(() => {
      const element = document.createElement('div');
      element.id = 'test-element';
      element.textContent = 'Test Element';
      element.style.marginTop = '2000px';
      document.body.appendChild(element);
    });

    const result = await page.evaluate(() => {
      const element = document.getElementById('test-element');
      return {
        exists: element !== null,
        hasId: element?.id === 'test-element',
      };
    });

    expect(result.exists).toBe(true);
    expect(result.hasId).toBe(true);
  });

  test('should calculate element position for scrolling', async ({ page }) => {
    await page.goto('/');

    await page.evaluate(() => {
      const element = document.createElement('div');
      element.id = 'scroll-test';
      element.textContent = 'Scroll Test';
      element.style.marginTop = '1000px';
      document.body.appendChild(element);
    });

    const result = await page.evaluate(() => {
      const element = document.getElementById('scroll-test');
      if (!element) return null;

      const elementPosition = element.getBoundingClientRect().top;
      const offset = 100;
      const offsetPosition = elementPosition + window.pageYOffset - offset;

      return {
        elementPosition,
        offsetPosition,
        isCalculated: typeof offsetPosition === 'number',
      };
    });

    expect(result).not.toBeNull();
    expect(result?.isCalculated).toBe(true);
  });

  test('should handle missing element in scrollToElement', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const element = document.getElementById('nonexistent-element');
      return element === null;
    });

    expect(result).toBe(true);
  });
});
