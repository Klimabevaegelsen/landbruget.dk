# Frontend Testing Integration with Pre-Commit Hooks

## Overview
Playwright testing is now integrated into the development workflow through pre-commit hooks and linting processes. This ensures that frontend changes are automatically validated before commits.

## Automatic Testing Triggers

### Pre-Commit Hook Integration
- **Trigger**: Any changes to files in `frontend/src/` with extensions `.ts`, `.tsx`, `.js`, `.jsx`
- **Action**: Runs `npm run test:smoke` automatically
- **Behavior**: 
  - ✅ **Success**: Commit proceeds normally
  - ❌ **Failure**: Shows warning but doesn't block commit (allows for emergency fixes)
  - 🔧 **Debug**: Run `npm run test:debug` to investigate issues

### Available Test Commands

#### Development Testing
```bash
# Full test suite with UI
npm run test:ui

# Run tests in headed browser (visual)
npm run test:headed

# Debug mode with breakpoints
npm run test:debug
```

#### Pre-Commit Testing
```bash
# Fast smoke tests (used by pre-commit hook)
npm run test:smoke

# Full test suite (headless)
npm test
```

## How It Works

### 1. File Change Detection
The pre-commit hook monitors changes to:
- `frontend/src/**/*.{ts,tsx,js,jsx}`
- Component files, pages, hooks, utilities

### 2. Automatic Test Execution
When frontend files change:
1. Pre-commit hook triggers
2. Starts Next.js dev server (if not running)
3. Runs smoke tests against `http://localhost:3000`
4. Reports results

### 3. Test Configuration
- **Smoke Tests**: Use `playwright.smoke.config.ts` for speed
  - Single browser (Chromium)
  - 1 minute server timeout
  - No retries (fail fast)
  - Line reporter for clean output

- **Full Tests**: Use `playwright.config.ts` for comprehensive testing
  - Multiple browsers
  - 2 minute server timeout
  - Retries on CI
  - HTML reporter

## Smoke Test Coverage

The smoke tests (`tests/example.spec.ts`) verify:

### Basic Functionality
- ✅ Homepage loads without 404/500 errors
- ✅ No critical JavaScript errors in console
- ✅ Basic HTML structure exists
- ✅ Navigation elements are present and functional

### Error Detection
- Console errors (filtered for warnings)
- Page load failures
- Navigation issues
- Component rendering problems

## Real-Time Testing Workflow

### During Development
1. **Make changes** to React components
2. **Pre-commit hook automatically**:
   - Starts dev server
   - Runs smoke tests
   - Reports any issues
3. **If tests pass**: Commit proceeds
4. **If tests fail**: Warning shown, investigate with `npm run test:debug`

### Manual Testing
```bash
# Test specific components
npm run test:headed tests/homepage.spec.ts

# Test with MCP integration (real-time)
# Use Cursor's Playwright MCP to interact with live application
```

## MCP Integration Benefits

With Playwright MCP configured in Cursor:
- **Real-time DOM access** during development
- **Interactive testing** as you build
- **Immediate feedback** on component changes
- **Visual debugging** of test failures

## Troubleshooting

### Common Issues

#### Server Not Starting
```bash
# Check if port 3000 is in use
lsof -i :3000

# Kill existing processes
pkill -f "next dev"
```

#### Tests Timing Out
- Increase timeout in `playwright.smoke.config.ts`
- Check for slow API calls or large bundles
- Verify dev server starts correctly

#### Pre-commit Hook Not Running
```bash
# Reinstall pre-commit hooks
pre-commit install

# Test manually
pre-commit run playwright-smoke-test --all-files
```

## Best Practices

### 1. Component Testing
- Add `data-testid` attributes to key components
- Test user interactions, not implementation details
- Focus on critical user flows

### 2. Performance
- Keep smoke tests fast (< 30 seconds)
- Use full test suite for comprehensive testing
- Leverage MCP for interactive development testing

### 3. Maintenance
- Update tests when component APIs change
- Add new smoke tests for critical features
- Review test failures before forcing commits

## Integration with Existing Workflow

This testing integration works alongside:
- ✅ **ESLint**: Code quality checks
- ✅ **Prettier**: Code formatting
- ✅ **Ruff**: Python linting
- ✅ **Secret detection**: Security scanning
- ✅ **Debug code detection**: Development hygiene

The testing layer adds functional validation to ensure your frontend actually works as expected, not just that it compiles and looks good.

## Future Enhancements

Potential additions:
- Visual regression testing
- Performance budgets
- Accessibility testing
- Cross-browser CI testing
- Component-specific test generation

This setup provides a solid foundation for reliable frontend development with automatic quality assurance.
