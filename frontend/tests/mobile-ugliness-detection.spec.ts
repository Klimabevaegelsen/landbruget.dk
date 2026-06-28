import { test, expect, devices } from '@playwright/test';

// Test devices focusing on problematic screen sizes
const uglinessTestDevices = [
  { name: 'iPhone SE', config: devices['iPhone SE'] }, // 320px - smallest
  { name: 'iPhone 12', config: devices['iPhone 12'] }, // 390px - common
  { name: 'Pixel 5', config: devices['Pixel 5'] }, // 393px - common Android
  { name: 'Galaxy S8', config: devices['Galaxy S8'] }, // 360px - older Android
];

const testPages = [
  { path: '/', name: 'Homepage' },
  { path: '/markanalyse', name: 'Field Analysis' },
  { path: '/pesticidanalyse', name: 'Pesticide Analysis' },
  { path: '/om-os', name: 'About Us' },
];

test.describe('Mobile Ugliness Detection Tests', () => {
  const skipUnsupportedMobileContext = (browserName: string) => {
    test.skip(
      browserName === 'firefox',
      'Firefox does not support Playwright mobile emulation contexts'
    );
  };

  // Test 1: Text wrapping and overflow detection
  for (const device of uglinessTestDevices) {
    for (const page of testPages) {
      test(`${page.name} on ${device.name} - Text wrapping and overflow`, async ({
        browser,
        browserName,
      }) => {
        skipUnsupportedMobileContext(browserName);
        const context = await browser.newContext(device.config);
        const browserPage = await context.newPage();

        try {
          await browserPage.goto(page.path, {
            waitUntil: 'networkidle',
            timeout: 30000,
          });

          const viewportWidth = browserPage.viewportSize()?.width || 0;

          // Check for text elements that might be too wide
          const textElements = await browserPage
            .locator('h1, h2, h3, p, span, div')
            .all();

          let textOverflowCount = 0;

          for (const element of textElements.slice(0, 20)) {
            // Check first 20 text elements
            if (await element.isVisible()) {
              const box = await element.boundingBox();
              const textContent = await element.textContent();

              if (box && textContent && textContent.trim().length > 0) {
                // Check if text element is wider than viewport
                if (box.width > viewportWidth) {
                  textOverflowCount++;
                  console.warn(
                    `${page.name} on ${device.name}: Text overflow - "${textContent.slice(0, 50)}..." (${box.width}px > ${viewportWidth}px)`
                  );
                }

                // Check for very long unbroken text that might cause issues
                const words = textContent.split(/\s+/);
                const longestWord = words.reduce(
                  (longest, word) =>
                    word.length > longest.length ? word : longest,
                  ''
                );

                if (longestWord.length > 25) {
                  // Very long words can cause overflow
                  console.warn(
                    `${page.name} on ${device.name}: Very long word detected: "${longestWord}"`
                  );
                }
              }
            }
          }

          // Allow some flexibility but flag excessive overflow
          expect(textOverflowCount).toBeLessThan(3);
        } catch (error) {
          if (
            error.message.includes('net::ERR_ABORTED') ||
            error.message.includes('404')
          ) {
            test.skip();
          } else {
            throw error;
          }
        }

        await context.close();
      });
    }
  }

  // Test 2: Element overlap detection
  test('Homepage - Element overlap and collision detection', async ({
    browser,
    browserName,
  }) => {
    skipUnsupportedMobileContext(browserName);
    const context = await browser.newContext(devices['iPhone SE']); // Test on smallest screen
    const page = await context.newPage();

    await page.goto('/', { waitUntil: 'networkidle', timeout: 30000 });

    // Get all visible elements with significant size
    const elements = await page
      .locator(
        'button, a, input, h1, h2, h3, img, div[class*="card"], div[class*="section"]'
      )
      .all();

    const visibleElements = [];

    for (const element of elements.slice(0, 30)) {
      // Check first 30 elements
      if (await element.isVisible()) {
        const box = await element.boundingBox();
        if (box && box.width > 10 && box.height > 10) {
          // Only consider elements with meaningful size
          visibleElements.push({ element, box });
        }
      }
    }

    let overlapCount = 0;

    // Check for overlaps
    for (let i = 0; i < visibleElements.length; i++) {
      for (let j = i + 1; j < visibleElements.length; j++) {
        const box1 = visibleElements[i].box;
        const box2 = visibleElements[j].box;

        // Check if boxes overlap
        const overlap = !(
          box1.x + box1.width < box2.x ||
          box2.x + box2.width < box1.x ||
          box1.y + box1.height < box2.y ||
          box2.y + box2.height < box1.y
        );

        if (overlap) {
          // Check if it's a significant overlap (not just touching edges)
          const overlapArea =
            Math.max(
              0,
              Math.min(box1.x + box1.width, box2.x + box2.width) -
                Math.max(box1.x, box2.x)
            ) *
            Math.max(
              0,
              Math.min(box1.y + box1.height, box2.y + box2.height) -
                Math.max(box1.y, box2.y)
            );

          const minArea = Math.min(
            box1.width * box1.height,
            box2.width * box2.height
          );

          if (overlapArea > minArea * 0.1) {
            // More than 10% overlap is concerning
            overlapCount++;
            console.warn(
              `Element overlap detected: ${overlapArea.toFixed(0)}px² overlap`
            );
          }
        }
      }
    }

    // Some overlap is normal (nested elements), but excessive overlap indicates layout issues
    expect(overlapCount).toBeLessThan(5);

    await context.close();
  });

  // Test 3: Button and form element cramping detection
  for (const device of uglinessTestDevices.slice(0, 2)) {
    // Test on 2 devices
    test(`Button cramming detection on ${device.name}`, async ({
      browser,
      browserName,
    }) => {
      skipUnsupportedMobileContext(browserName);
      const context = await browser.newContext(device.config);
      const page = await context.newPage();

      await page.goto('/', { waitUntil: 'networkidle', timeout: 30000 });

      // Look for button groups that might be crammed together
      const buttons = await page
        .locator(
          'button, a[role="button"], input[type="button"], input[type="submit"]'
        )
        .all();

      let crammedButtonCount = 0;

      for (let i = 0; i < buttons.length - 1; i++) {
        const button1 = buttons[i];
        const button2 = buttons[i + 1];

        if ((await button1.isVisible()) && (await button2.isVisible())) {
          const box1 = await button1.boundingBox();
          const box2 = await button2.boundingBox();

          if (box1 && box2) {
            // Check if buttons are on the same row (similar Y position)
            const sameRow = Math.abs(box1.y - box2.y) < 20;

            if (sameRow) {
              // Calculate spacing between buttons
              const spacing = Math.abs(box1.x + box1.width - box2.x);

              if (spacing < 8) {
                // Less than 8px spacing is too cramped
                crammedButtonCount++;
                console.warn(
                  `Buttons too close together: ${spacing.toFixed(0)}px spacing`
                );
              }
            }
          }
        }
      }

      expect(crammedButtonCount).toBeLessThan(3);

      await context.close();
    });
  }

  // Test 4: Image scaling and aspect ratio issues
  test('Image scaling and proportion issues', async ({
    browser,
    browserName,
  }) => {
    skipUnsupportedMobileContext(browserName);
    const context = await browser.newContext(devices['iPhone SE']);
    const page = await context.newPage();

    await page.goto('/', { waitUntil: 'networkidle', timeout: 30000 });

    const images = await page.locator('img').all();

    let imageIssueCount = 0;

    for (const img of images) {
      if (await img.isVisible()) {
        const box = await img.boundingBox();
        const naturalDimensions = await img.evaluate((el) => ({
          naturalWidth: (el as HTMLImageElement).naturalWidth,
          naturalHeight: (el as HTMLImageElement).naturalHeight,
        }));

        if (box && naturalDimensions.naturalWidth > 0) {
          const viewportWidth = page.viewportSize()?.width || 0;

          // Check if image is too wide for viewport
          if (box.width > viewportWidth + 5) {
            imageIssueCount++;
            console.warn(
              `Image too wide: ${box.width}px on ${viewportWidth}px viewport`
            );
          }

          // Check for severely distorted aspect ratios
          const naturalRatio =
            naturalDimensions.naturalWidth / naturalDimensions.naturalHeight;
          const displayRatio = box.width / box.height;

          const distortion =
            Math.abs(naturalRatio - displayRatio) / naturalRatio;

          if (distortion > 0.2) {
            // More than 20% distortion
            imageIssueCount++;
            console.warn(
              `Image aspect ratio distorted: ${distortion.toFixed(2)} (${displayRatio.toFixed(2)} vs ${naturalRatio.toFixed(2)})`
            );
          }
        }
      }
    }

    expect(imageIssueCount).toBeLessThan(2);

    await context.close();
  });

  // Test 5: Inconsistent spacing detection
  test('Inconsistent spacing and alignment detection', async ({
    browser,
    browserName,
  }) => {
    skipUnsupportedMobileContext(browserName);
    const context = await browser.newContext(devices['iPhone 12']);
    const page = await context.newPage();

    await page.goto('/', { waitUntil: 'networkidle', timeout: 30000 });

    // Look for sections/containers that should have consistent spacing
    const sections = await page
      .locator('section, [class*="section"], main > div, [class*="container"]')
      .all();

    const spacings = [];

    for (let i = 0; i < Math.min(sections.length - 1, 10); i++) {
      const section1 = sections[i];
      const section2 = sections[i + 1];

      if ((await section1.isVisible()) && (await section2.isVisible())) {
        const box1 = await section1.boundingBox();
        const box2 = await section2.boundingBox();

        if (box1 && box2) {
          const spacing = box2.y - (box1.y + box1.height);
          if (spacing >= 0) {
            // Only consider positive spacing
            spacings.push(spacing);
          }
        }
      }
    }

    if (spacings.length > 2) {
      // Check for consistency in spacing
      const avgSpacing = spacings.reduce((a, b) => a + b, 0) / spacings.length;
      const inconsistentSpacings = spacings.filter(
        (spacing) => Math.abs(spacing - avgSpacing) > avgSpacing * 0.5 // More than 50% deviation
      );

      if (inconsistentSpacings.length > 0) {
        console.warn(
          `Inconsistent spacing detected. Average: ${avgSpacing.toFixed(0)}px, Inconsistent: ${inconsistentSpacings.map((s) => s.toFixed(0)).join(', ')}px`
        );
      }

      // Allow some inconsistency but flag excessive variation
      expect(inconsistentSpacings.length).toBeLessThan(spacings.length * 0.4);
    }

    await context.close();
  });

  // Test 6: Typography hierarchy and readability issues
  test('Typography hierarchy and readability issues', async ({
    browser,
    browserName,
  }) => {
    skipUnsupportedMobileContext(browserName);
    const context = await browser.newContext(devices['iPhone SE']);
    const page = await context.newPage();

    await page.goto('/', { waitUntil: 'networkidle', timeout: 30000 });

    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

    const headingSizes = [];

    for (const heading of headings.slice(0, 10)) {
      if (await heading.isVisible()) {
        const fontSize = await heading.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return parseFloat(style.fontSize);
        });

        const tagName = await heading.evaluate((el) =>
          el.tagName.toLowerCase()
        );

        headingSizes.push({ tagName, fontSize });
      }
    }

    // Check for proper hierarchy (h1 should be largest, h2 smaller, etc.)
    const h1Sizes = headingSizes
      .filter((h) => h.tagName === 'h1')
      .map((h) => h.fontSize);
    const h2Sizes = headingSizes
      .filter((h) => h.tagName === 'h2')
      .map((h) => h.fontSize);
    const h3Sizes = headingSizes
      .filter((h) => h.tagName === 'h3')
      .map((h) => h.fontSize);

    let hierarchyIssues = 0;

    if (h1Sizes.length > 0 && h2Sizes.length > 0) {
      const maxH1 = Math.max(...h1Sizes);
      const maxH2 = Math.max(...h2Sizes);

      if (maxH2 >= maxH1) {
        hierarchyIssues++;
        console.warn(
          `Typography hierarchy issue: H2 (${maxH2}px) >= H1 (${maxH1}px)`
        );
      }
    }

    if (h2Sizes.length > 0 && h3Sizes.length > 0) {
      const maxH2 = Math.max(...h2Sizes);
      const maxH3 = Math.max(...h3Sizes);

      if (maxH3 >= maxH2) {
        hierarchyIssues++;
        console.warn(
          `Typography hierarchy issue: H3 (${maxH3}px) >= H2 (${maxH2}px)`
        );
      }
    }

    expect(hierarchyIssues).toBeLessThan(2);

    await context.close();
  });

  // Test 7: Form field alignment and spacing issues
  test('Form field ugliness detection', async ({ browser, browserName }) => {
    skipUnsupportedMobileContext(browserName);
    const context = await browser.newContext(devices['iPhone 12']);
    const page = await context.newPage();

    // Test pages that likely have forms
    const formPages = ['/', '/markanalyse', '/pesticidanalyse'];

    for (const formPage of formPages.slice(0, 2)) {
      try {
        await page.goto(formPage, {
          waitUntil: 'domcontentloaded',
          timeout: 15000,
        });
        await expect(page.locator('body')).toBeVisible();

        const formElements = await page
          .locator('input, textarea, select, button[type="submit"]')
          .all();

        if (formElements.length > 1) {
          let formIssues = 0;

          // Check for misaligned form elements
          const formBoxes = [];

          for (const element of formElements.slice(0, 10)) {
            if (await element.isVisible()) {
              const box = await element.boundingBox();
              if (box) {
                formBoxes.push(box);
              }
            }
          }

          // Check for consistent left alignment
          if (formBoxes.length > 1) {
            const leftPositions = formBoxes.map((box) => box.x);
            const avgLeft =
              leftPositions.reduce((a, b) => a + b, 0) / leftPositions.length;

            const misalignedElements = leftPositions.filter(
              (left) => Math.abs(left - avgLeft) > 20
            );

            if (misalignedElements.length > 0) {
              formIssues++;
              console.warn(
                `${formPage}: Form elements misaligned - ${misalignedElements.length} elements off by >20px`
              );
            }
          }

          // Check for cramped form spacing
          for (let i = 0; i < formBoxes.length - 1; i++) {
            const box1 = formBoxes[i];
            const box2 = formBoxes[i + 1];

            const verticalSpacing = Math.abs(box2.y - (box1.y + box1.height));

            if (verticalSpacing < 8 && verticalSpacing > 0) {
              // Elements too close vertically
              formIssues++;
              console.warn(
                `${formPage}: Form elements too close together: ${verticalSpacing.toFixed(0)}px spacing`
              );
            }
          }

          expect(formIssues).toBeLessThan(3);
        }
      } catch (error) {
        if (
          error.message.includes('net::ERR_ABORTED') ||
          error.message.includes('404')
        ) {
          console.warn(`Page ${formPage} not accessible, skipping form test`);
          continue;
        } else {
          throw error;
        }
      }
    }

    await context.close();
  });

  // Test 8: Color contrast and visual accessibility ugliness
  test('Visual accessibility and contrast issues', async ({
    browser,
    browserName,
  }) => {
    skipUnsupportedMobileContext(browserName);
    const context = await browser.newContext(devices['iPhone 12']);
    const page = await context.newPage();

    await page.goto('/', { waitUntil: 'networkidle', timeout: 30000 });

    // Check for potential contrast issues by examining text elements
    const textElements = await page
      .locator('p, span, a, button, h1, h2, h3')
      .all();

    let contrastIssues = 0;

    for (const element of textElements.slice(0, 15)) {
      if (await element.isVisible()) {
        const styles = await element.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            backgroundColor: style.backgroundColor,
            fontSize: parseFloat(style.fontSize),
          };
        });

        // Check for very light text (might be hard to read)
        if (styles.color.includes('rgba(') && styles.color.includes(', 0.')) {
          const opacity = parseFloat(
            styles.color.match(/, (0\.\d+)\)/)?.[1] || '1'
          );
          if (opacity < 0.6) {
            contrastIssues++;
            console.warn(`Low opacity text detected: ${styles.color}`);
          }
        }

        // Check for very small light text (double problem)
        if (
          styles.fontSize < 14 &&
          (styles.color.includes('gray') || styles.color.includes('rgba'))
        ) {
          contrastIssues++;
          console.warn(
            `Small light text detected: ${styles.fontSize}px with color ${styles.color}`
          );
        }
      }
    }

    // Allow some light text (for secondary content) but flag excessive use
    expect(contrastIssues).toBeLessThan(5);

    await context.close();
  });

  // Test 9: Navigation and menu ugliness
  test('Navigation menu layout and spacing issues', async ({
    browser,
    browserName,
  }) => {
    skipUnsupportedMobileContext(browserName);
    const context = await browser.newContext(devices['iPhone SE']);
    const page = await context.newPage();

    await page.goto('/', { waitUntil: 'networkidle', timeout: 30000 });

    // Check mobile menu if it exists
    const mobileMenuButton = page
      .locator(
        '[aria-label*="menu"], [aria-label*="Menu"], button[class*="menu"]'
      )
      .first();

    if ((await mobileMenuButton.count()) > 0) {
      await mobileMenuButton.click();

      // Wait for menu to open
      await page.waitForTimeout(500);

      const menuItems = await page
        .locator('[role="menu"] a, [role="navigation"] a, nav a')
        .all();

      let menuIssues = 0;

      for (let i = 0; i < Math.min(menuItems.length - 1, 5); i++) {
        const item1 = menuItems[i];
        const item2 = menuItems[i + 1];

        if ((await item1.isVisible()) && (await item2.isVisible())) {
          const box1 = await item1.boundingBox();
          const box2 = await item2.boundingBox();

          if (box1 && box2) {
            const spacing = box2.y - (box1.y + box1.height);

            if (spacing < 4) {
              // Menu items too close
              menuIssues++;
              console.warn(
                `Menu items too close: ${spacing.toFixed(0)}px spacing`
              );
            }

            if (spacing > 50) {
              // Menu items too far apart
              menuIssues++;
              console.warn(
                `Menu items too far apart: ${spacing.toFixed(0)}px spacing`
              );
            }
          }
        }
      }

      expect(menuIssues).toBeLessThan(3);
    }

    await context.close();
  });

  // Test 10: Performance-related visual ugliness (layout shifts, slow loading)
  test('Performance-related visual issues', async ({
    browser,
    browserName,
  }) => {
    skipUnsupportedMobileContext(browserName);
    const context = await browser.newContext(devices['iPhone 12']);
    const page = await context.newPage();

    // Track layout shifts
    await page.addInitScript(() => {
      window.layoutShifts = [];
      new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          if (entry.entryType === 'layout-shift') {
            window.layoutShifts.push(entry.value);
          }
        }
      }).observe({ entryTypes: ['layout-shift'] });
    });

    await page.goto('/', { waitUntil: 'networkidle', timeout: 30000 });

    // Wait a bit for any additional layout shifts
    await page.waitForTimeout(2000);

    const layoutShifts = await page.evaluate(() => window.layoutShifts || []);

    if (layoutShifts.length > 0) {
      const totalCLS = layoutShifts.reduce((sum, shift) => sum + shift, 0);
      console.log(`Cumulative Layout Shift: ${totalCLS.toFixed(3)}`);

      // CLS should be less than 0.1 for good user experience
      expect(totalCLS).toBeLessThan(0.1);
    }

    await context.close();
  });
});
