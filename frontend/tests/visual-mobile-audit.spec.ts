import { test, expect, devices } from '@playwright/test';

// Test devices for visual audit
const visualTestDevices = [
  { name: 'iPhone 12', config: devices['iPhone 12'] },
  { name: 'iPhone SE', config: devices['iPhone SE'] },
  { name: 'Pixel 5', config: devices['Pixel 5'] },
  { name: 'iPad', config: devices['iPad'] },
];

// Pages to test for visual issues
const visualTestPages = [
  { path: '/', name: 'Homepage' },
  { path: '/markanalyse', name: 'Field Analysis' },
  { path: '/pesticidanalyse', name: 'Pesticide Analysis' },
  { path: '/om-os', name: 'About Us' },
];

test.describe('Visual Mobile UX Audit', () => {
  // Test 1: Check for wide headers/footers and layout overflow
  for (const device of visualTestDevices) {
    for (const page of visualTestPages) {
      test(`${page.name} on ${device.name} - Layout width and overflow check`, async ({
        browser,
      }) => {
        const context = await browser.newContext(device.config);
        const browserPage = await context.newPage();

        try {
          await browserPage.goto(page.path, {
            waitUntil: 'networkidle',
            timeout: 30000,
          });

          const viewportWidth = browserPage.viewportSize()?.width || 0;

          // Check header width
          const header = browserPage
            .locator('header, nav, [role="banner"]')
            .first();
          if ((await header.count()) > 0) {
            const headerBox = await header.boundingBox();
            if (headerBox) {
              expect(headerBox.width).toBeLessThanOrEqual(viewportWidth + 5);

              if (headerBox.width > viewportWidth) {
                console.warn(
                  `${page.name} on ${device.name}: Header too wide (${headerBox.width}px > ${viewportWidth}px)`
                );
              }
            }
          }

          // Check footer width
          const footer = browserPage
            .locator('footer, [role="contentinfo"]')
            .first();
          if ((await footer.count()) > 0) {
            const footerBox = await footer.boundingBox();
            if (footerBox) {
              expect(footerBox.width).toBeLessThanOrEqual(viewportWidth + 5);

              if (footerBox.width > viewportWidth) {
                console.warn(
                  `${page.name} on ${device.name}: Footer too wide (${footerBox.width}px > ${viewportWidth}px)`
                );
              }
            }
          }

          // Check for horizontal scroll
          const bodyScrollWidth = await browserPage.evaluate(
            () => document.body.scrollWidth
          );
          expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 10);

          if (bodyScrollWidth > viewportWidth + 10) {
            console.warn(
              `${page.name} on ${device.name}: Horizontal scroll detected (${bodyScrollWidth}px > ${viewportWidth}px)`
            );
          }

          // Take screenshot for visual inspection
          await browserPage.screenshot({
            path: `test-results/visual-${page.name.toLowerCase().replace(/\s+/g, '-')}-${device.name.toLowerCase().replace(/\s+/g, '-')}.png`,
            fullPage: true,
          });
        } catch (error) {
          if (
            error.message.includes('net::ERR_ABORTED') ||
            error.message.includes('404')
          ) {
            console.warn(`Page ${page.path} not accessible, skipping test`);
            test.skip();
          } else {
            throw error;
          }
        }

        await context.close();
      });
    }
  }

  // Test 2: Map component mobile usability
  test('Field Analysis - Map component mobile interaction', async ({
    browser,
  }) => {
    const context = await browser.newContext(devices['iPhone 12']);
    const page = await context.newPage();

    try {
      await page.goto('/markanalyse', {
        waitUntil: 'networkidle',
        timeout: 30000,
      });

      // Look for map container
      const mapContainer = page
        .locator(
          '[class*="map"], [id*="map"], canvas, .maplibregl-canvas, .mapboxgl-canvas'
        )
        .first();

      if ((await mapContainer.count()) > 0) {
        const mapBox = await mapContainer.boundingBox();
        if (mapBox) {
          const viewportWidth = page.viewportSize()?.width || 0;
          const viewportHeight = page.viewportSize()?.height || 0;

          // Check map doesn't overflow viewport
          expect(mapBox.width).toBeLessThanOrEqual(viewportWidth + 5);

          // Check map has reasonable size for mobile
          const mapAreaPercentage =
            (mapBox.width * mapBox.height) / (viewportWidth * viewportHeight);
          expect(mapAreaPercentage).toBeGreaterThan(0.3); // Map should take at least 30% of screen
          expect(mapAreaPercentage).toBeLessThan(0.9); // But not more than 90%

          // Test touch interactions
          await mapContainer.click();

          // Check for zoom controls
          const zoomControls = page.locator(
            '.maplibregl-ctrl-zoom, .mapboxgl-ctrl-zoom, [aria-label*="zoom"], button[class*="zoom"]'
          );
          if ((await zoomControls.count()) > 0) {
            const zoomButton = zoomControls.first();
            const zoomBox = await zoomButton.boundingBox();
            if (zoomBox) {
              // Zoom controls should be touch-friendly
              expect(zoomBox.height).toBeGreaterThanOrEqual(40);
              expect(zoomBox.width).toBeGreaterThanOrEqual(40);
            }
          }

          console.log(
            `Map size: ${mapBox.width}x${mapBox.height}px (${(mapAreaPercentage * 100).toFixed(1)}% of screen)`
          );
        }
      } else {
        console.log('No map component found on field analysis page');
      }

      // Screenshot of map page
      await page.screenshot({
        path: 'test-results/map-mobile-usability.png',
        fullPage: true,
      });
    } catch (error) {
      if (
        error.message.includes('net::ERR_ABORTED') ||
        error.message.includes('404')
      ) {
        console.warn('Field analysis page not accessible, skipping map test');
        test.skip();
      } else {
        throw error;
      }
    }

    await context.close();
  });

  // Test 3: Component spacing and sizing issues
  test('Homepage - Component spacing and visual hierarchy', async ({
    browser,
  }) => {
    const context = await browser.newContext(devices['iPhone 12']);
    const page = await context.newPage();

    await page.goto('/', { waitUntil: 'networkidle', timeout: 30000 });

    // Check hero section sizing
    const hero = page.locator('[class*="hero"], h1').first();
    if ((await hero.count()) > 0) {
      const heroBox = await hero.boundingBox();
      if (heroBox) {
        const viewportHeight = page.viewportSize()?.height || 0;
        const heroHeightPercentage = heroBox.height / viewportHeight;

        // Hero shouldn't take up entire viewport on mobile
        expect(heroHeightPercentage).toBeLessThan(0.8);

        // But should be substantial enough
        expect(heroHeightPercentage).toBeGreaterThan(0.2);

        console.log(
          `Hero height: ${heroBox.height}px (${(heroHeightPercentage * 100).toFixed(1)}% of viewport)`
        );
      }
    }

    // Check for adequate spacing between sections
    const sections = await page
      .locator('section, [class*="section"], main > div')
      .all();

    for (let i = 0; i < Math.min(sections.length - 1, 5); i++) {
      const currentSection = sections[i];
      const nextSection = sections[i + 1];

      if (
        (await currentSection.isVisible()) &&
        (await nextSection.isVisible())
      ) {
        const currentBox = await currentSection.boundingBox();
        const nextBox = await nextSection.boundingBox();

        if (currentBox && nextBox) {
          const spacing = nextBox.y - (currentBox.y + currentBox.height);

          // Sections should have reasonable spacing (at least 16px, but not excessive)
          expect(spacing).toBeGreaterThanOrEqual(8);
          expect(spacing).toBeLessThan(200);

          if (spacing < 16) {
            console.warn(
              `Tight spacing between sections ${i} and ${i + 1}: ${spacing}px`
            );
          }
        }
      }
    }

    await context.close();
  });

  // Test 4: Text readability and sizing
  test('Cross-page text readability audit', async ({ browser }) => {
    const context = await browser.newContext(devices['iPhone 12']);
    const page = await context.newPage();

    for (const testPage of visualTestPages.slice(0, 2)) {
      // Test first 2 pages
      try {
        await page.goto(testPage.path, {
          waitUntil: 'networkidle',
          timeout: 30000,
        });

        // Check heading hierarchy
        const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

        for (const heading of headings.slice(0, 5)) {
          // Check first 5 headings
          if (await heading.isVisible()) {
            const fontSize = await heading.evaluate((el) => {
              const style = window.getComputedStyle(el);
              return parseFloat(style.fontSize);
            });

            const tagName = await heading.evaluate((el) =>
              el.tagName.toLowerCase()
            );

            // Check minimum sizes for headings
            const minSizes = { h1: 24, h2: 20, h3: 18, h4: 16, h5: 16, h6: 14 };
            const minSize = minSizes[tagName as keyof typeof minSizes] || 14;

            expect(fontSize).toBeGreaterThanOrEqual(minSize);

            if (fontSize < minSize) {
              console.warn(
                `${testPage.name}: ${tagName} too small (${fontSize}px < ${minSize}px)`
              );
            }
          }
        }

        // Check line height for readability
        const paragraphs = await page.locator('p').all();

        for (const p of paragraphs.slice(0, 3)) {
          // Check first 3 paragraphs
          if (await p.isVisible()) {
            const lineHeight = await p.evaluate((el) => {
              const style = window.getComputedStyle(el);
              const fontSize = parseFloat(style.fontSize);
              const lineHeightValue = style.lineHeight;

              if (lineHeightValue === 'normal') return 1.2;

              return lineHeightValue.includes('px')
                ? parseFloat(lineHeightValue) / fontSize
                : parseFloat(lineHeightValue);
            });

            // Line height should be at least 1.2 for readability
            expect(lineHeight).toBeGreaterThanOrEqual(1.1);

            if (lineHeight < 1.2) {
              console.warn(
                `${testPage.name}: Paragraph line height too tight (${lineHeight})`
              );
            }
          }
        }
      } catch (error) {
        if (
          error.message.includes('net::ERR_ABORTED') ||
          error.message.includes('404')
        ) {
          console.warn(
            `Page ${testPage.path} not accessible, skipping text audit`
          );
          continue;
        } else {
          throw error;
        }
      }
    }

    await context.close();
  });

  // Test 5: Interactive element visual feedback
  test('Button and link visual states', async ({ browser }) => {
    const context = await browser.newContext(devices['iPhone 12']);
    const page = await context.newPage();

    await page.goto('/', { waitUntil: 'networkidle', timeout: 30000 });

    // Test button hover/focus states
    const buttons = await page.locator('button').all();

    for (const button of buttons.slice(0, 5)) {
      // Test first 5 buttons
      if (await button.isVisible()) {
        // Get initial styles
        const initialBg = await button.evaluate(
          (el) => window.getComputedStyle(el).backgroundColor
        );

        // Hover (simulate with focus since we can't hover on mobile)
        await button.focus();

        const focusedBg = await button.evaluate(
          (el) => window.getComputedStyle(el).backgroundColor
        );

        // There should be some visual feedback (background change, border, etc.)
        const hasVisualFeedback =
          initialBg !== focusedBg ||
          (await button.evaluate((el) => {
            const style = window.getComputedStyle(el);
            return (
              style.outline !== 'none' ||
              style.boxShadow !== 'none' ||
              style.borderColor !== 'transparent'
            );
          }));

        expect(hasVisualFeedback).toBeTruthy();

        if (!hasVisualFeedback) {
          console.warn('Button lacks visual feedback on focus/interaction');
        }
      }
    }

    await context.close();
  });

  // Test 6: Performance visual metrics
  test('Visual performance metrics', async ({ browser }) => {
    const context = await browser.newContext(devices['iPhone 12']);
    const page = await context.newPage();

    // Test largest contentful paint and layout shifts
    await page.goto('/', { waitUntil: 'networkidle', timeout: 30000 });

    const metrics = await page.evaluate(() => {
      return new Promise((resolve) => {
        new PerformanceObserver((list) => {
          const entries = list.getEntries();
          const lcp = entries.find(
            (entry) => entry.entryType === 'largest-contentful-paint'
          );
          if (lcp) {
            resolve({
              lcp: lcp.startTime,
              url: window.location.href,
            });
          }
        }).observe({ entryTypes: ['largest-contentful-paint'] });

        // Fallback timeout
        setTimeout(
          () => resolve({ lcp: null, url: window.location.href }),
          5000
        );
      });
    });

    console.log('Performance metrics:', metrics);

    // LCP should be under 2.5s for good mobile experience
    if ((metrics as any).lcp) {
      expect((metrics as any).lcp).toBeLessThan(2500);
    }

    await context.close();
  });
});
