# Playwright Real-Time Testing Workflow

## Overview
This document outlines the real-time testing workflow using Playwright MCP integration with Cursor for the landbruget.dk frontend.

## Setup Complete
- ✅ Playwright MCP installed globally
- ✅ Cursor MCP configuration in `.cursor/mcp.json`
- ✅ Playwright config in `frontend/playwright.config.ts`
- ✅ Test scripts added to `frontend/package.json`
- ✅ Example tests created in `frontend/tests/`

## Real-Time Testing Rules

### When to Test
**ALWAYS** run Playwright MCP tests after:
1. **Component Changes** - Any modifications to React components
2. **UI Updates** - Changes to styling, layout, or visual elements
3. **Navigation Changes** - Updates to routing, links, or menu items
4. **Interactive Features** - New buttons, forms, toggles, or user interactions
5. **Responsive Changes** - Mobile/desktop layout modifications
6. **Before Commits** - Validate all changes work before committing

### Testing Workflow

#### 1. Start Development Server
```bash
cd frontend
npm run dev
```

#### 2. Make Your Changes
Edit components, styles, or functionality as needed.

#### 3. Real-Time Testing
Use Playwright MCP integration to:

**Basic Validation:**
```typescript
// Navigate to your page
await page.goto('http://localhost:3000');

// Test component visibility
const component = page.locator('[data-testid="your-component"]');
await expect(component).toBeVisible();
```

**Interactive Testing:**
```typescript
// Test user interactions
await page.click('[data-testid="button"]');
await page.fill('[data-testid="input"]', 'test value');
await page.selectOption('[data-testid="select"]', 'option-value');
```

**Responsive Testing:**
```typescript
// Test mobile view
await page.setViewportSize({ width: 375, height: 667 });
await expect(page.locator('[data-testid="mobile-menu"]')).toBeVisible();

// Test desktop view
await page.setViewportSize({ width: 1920, height: 1080 });
```

#### 4. Available Test Commands
- `npm test` - Run all tests headless
- `npm run test:ui` - Run with Playwright UI (recommended for development)
- `npm run test:headed` - Run tests in browser (visual feedback)
- `npm run test:debug` - Debug mode with breakpoints

## Component-Specific Testing

### RankingTable Component
```typescript
// Test ranking table display
const rankingTable = page.locator('[data-testid="ranking-table"]');
await expect(rankingTable).toBeVisible();
await expect(rankingTable.locator('.card-header')).toBeVisible();

// Test company navigation
const companyLink = page.locator('[data-testid="company-link"]').first();
await expect(companyLink).toHaveAttribute('href');
```

### LayerControlPanel Component
```typescript
// Test layer toggles
const layerToggle = page.locator('[data-testid="layer-toggle"]').first();
await layerToggle.click();

// Test filter changes
const filterControl = page.locator('[data-testid="filter-control"]').first();
await filterControl.selectOption('new-value');
```

### Navigation Components
```typescript
// Test sidenav
const sidenav = page.locator('[data-testid="sidenav"]');
await expect(sidenav).toBeVisible();

// Test mobile menu
await page.setViewportSize({ width: 375, height: 667 });
const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
await mobileMenuButton.click();
```

## MCP Integration Commands

### Browser Initialization
```typescript
// Initialize browser with your app
await page.goto('http://localhost:3000');
```

### Get Page Context
```typescript
// Get current page context for analysis
const context = await page.getContext();
```

### Execute Custom Code
```typescript
// Run custom Playwright code
const result = await page.evaluate(() => {
  // Custom JavaScript to run in browser
  return document.title;
});
```

### Take Screenshots
```typescript
// Capture current state
await page.screenshot({ path: 'current-state.png' });
```

## Best Practices

### 1. Test-Driven Development
- Write tests for new features before implementing
- Use MCP to validate implementations match expectations
- Test edge cases and error states

### 2. Component Testing Strategy
- Test component rendering
- Test user interactions
- Test responsive behavior
- Test accessibility features

### 3. Continuous Validation
- Test after every significant change
- Use visual regression testing for UI changes
- Validate cross-browser compatibility

### 4. Error Handling
- Test error states and loading states
- Validate form validation works
- Test network failure scenarios

## Integration with Development Workflow

### Pre-Commit Checklist
1. ✅ Component renders correctly
2. ✅ User interactions work
3. ✅ Responsive design functions
4. ✅ No console errors
5. ✅ Accessibility features work
6. ✅ Performance is acceptable

### Debugging Failed Tests
1. Use `npm run test:debug` for step-by-step debugging
2. Take screenshots at failure points
3. Check browser console for errors
4. Validate network requests complete successfully

## Example Real-Time Testing Session

```typescript
// 1. Start testing session
await page.goto('http://localhost:3000');

// 2. Test homepage loads
await expect(page.locator('h1')).toBeVisible();

// 3. Test navigation
await page.click('[data-testid="field-analysis-link"]');
await expect(page).toHaveURL(/field-analysis/);

// 4. Test component functionality
const layerPanel = page.locator('[data-testid="layer-control-panel"]');
await expect(layerPanel).toBeVisible();

// 5. Test responsive behavior
await page.setViewportSize({ width: 375, height: 667 });
await expect(layerPanel).toBeVisible(); // Should adapt to mobile

// 6. Validate and continue development
```

This workflow ensures every frontend change is immediately validated and working correctly before moving to the next task.
